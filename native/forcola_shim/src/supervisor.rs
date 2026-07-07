//! The supervising event loop: spawns the child, forwards its output,
//! and applies the kill sequence on timeout, KILL frame, or stdin EOF.

use crate::cgroup::{self, Cgroup};
use crate::credit::{Credit, Permit};
use crate::frame::{
    self, Frame, TAG_CREDIT, TAG_EOF, TAG_ERROR, TAG_EXIT, TAG_KILL, TAG_SPAWN, TAG_STDERR,
    TAG_STDIN, TAG_STDOUT,
};
use crate::privdrop::{self, DropPlan};
use crate::protocol::{ErrorReport, ExitReport, SpawnRequest};

use rustix::process::{self, Pid, Signal};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Events fed into the supervising loop from stdin, the child waiter, and
/// the timeout timer.
enum Event {
    Stdin(Frame),
    /// Clean EOF on the shim's own stdin: the BEAM is gone.
    StdinClosed,
    StdinError(io::Error),
    ChildExited(ChildOutcome),
    TimedOut,
}

/// How the child ended, in a form ready to become an EXIT frame.
struct ChildOutcome {
    status: Option<i32>,
    signal: Option<i32>,
}

/// How long to wait, after the child has been reaped, for the output pump
/// threads to hit EOF on the child's pipes before writing the EXIT frame.
/// In the normal case the writers are already dead and EOF is immediate;
/// the bound exists so a surviving process that inherited the pipe (e.g.
/// a backgrounded grandchild the kill path never ran against) cannot
/// delay the EXIT report forever.
const OUTPUT_DRAIN_GRACE: Duration = Duration::from_secs(1);

/// Runs the shim: read one SPAWN frame, supervise the child, and keep
/// handling protocol frames until the child has exited.
pub fn run<R: Read + Send + 'static, W: Write + Send + 'static>(
    mut stdin: R,
    stdout: W,
) -> io::Result<()> {
    let out = Arc::new(Mutex::new(stdout));

    // The first frame must be SPAWN; anything else is a protocol error we
    // report and exit on, since there is nothing else useful to do without
    // a child to supervise.
    let spawn_frame = match frame::read_frame(&mut stdin)? {
        Some(f) if f.tag == TAG_SPAWN => f,
        Some(_) => {
            write_error(&out, "expected SPAWN as the first frame");
            return Ok(());
        }
        None => return Ok(()), // BEAM closed stdin before spawning anything.
    };

    let request: SpawnRequest = match serde_json::from_slice(&spawn_frame.payload) {
        Ok(r) => r,
        Err(e) => {
            write_error(&out, &format!("invalid SPAWN payload: {e}"));
            return Ok(());
        }
    };

    let (mut child, pty_master, cgroup) = match spawn_child(&request) {
        Ok(c) => c,
        Err(e) => {
            write_error(&out, &format!("spawn failed: {e}"));
            return Ok(());
        }
    };

    let pgid = Pid::from_child(&child);
    let kill_grace = Duration::from_millis(request.kill_grace_ms);

    // Backpressure is opt-in: a `window_bytes` in the SPAWN payload gates the
    // stdout pump on a shared read budget. Absent, the pump reads eagerly and
    // the default path is byte-for-byte unchanged. The budget starts at zero,
    // so the pump parks until the BEAM's first CREDIT frame.
    let credit = request.window_bytes.map(|_| Credit::new());

    let (tx, rx): (Sender<Event>, Receiver<Event>) = mpsc::channel();

    // Pump completion channel: each pump sends one () when its source hits
    // EOF, so the supervising loop can drain output before reporting EXIT.
    let (pump_done_tx, pump_done_rx) = mpsc::channel();

    // Child stdin: for the pipe path, take ownership of the write pipe so we
    // can forward STDIN frames and close it on EOF frames. For the pty path,
    // stdin and stdout share the master fd; we write STDIN frames to the
    // master and read child output from it.
    let mut child_stdin: Option<Box<dyn Write + Send>>;
    let mut pumps = 0;

    match pty_master {
        Some(master) => {
            // A pty merges stdout and stderr onto one terminal, so there is
            // a single output stream. Reading and writing the same master fd
            // from two threads is safe: read and write on a pty master are
            // independent. Duplicate it so the pump thread and the stdin
            // writer each own a handle.
            let read_half = master;
            let write_half = read_half.try_clone()?;
            pumps += spawn_output_pump(
                Some(File::from(read_half)),
                TAG_STDOUT,
                Arc::clone(&out),
                pump_done_tx.clone(),
                credit.clone(),
            );
            // Dropping the last pump_done_tx sender is what lets drain_pumps
            // terminate; keep the count honest by dropping the spare here.
            drop(pump_done_tx);
            child_stdin = Some(Box::new(File::from(write_half)));
        }
        None => {
            child_stdin = child
                .stdin
                .take()
                .map(|s| Box::new(s) as Box<dyn Write + Send>);
            pumps += spawn_output_pump(
                child.stdout.take(),
                TAG_STDOUT,
                Arc::clone(&out),
                pump_done_tx.clone(),
                credit.clone(),
            );
            let stderr_tag = if request.merge_stderr {
                TAG_STDOUT
            } else {
                TAG_STDERR
            };
            // STDERR is never gated: backpressure bounds the child's stdout
            // stream only. Under merge_stderr the stderr bytes ride the STDOUT
            // tag but stay eager.
            pumps += spawn_output_pump(
                child.stderr.take(),
                stderr_tag,
                Arc::clone(&out),
                pump_done_tx,
                None,
            );
        }
    }

    spawn_stdin_reader(stdin, tx.clone());
    // The waiter thread takes ownership of `child` entirely: it is the
    // single, sole caller of `Child::wait`, so there is never a double
    // reap race. The supervising loop only ever learns the child's exit
    // through `Event::ChildExited`.
    spawn_waiter(child, tx.clone());

    if let Some(timeout_ms) = request.timeout_ms {
        spawn_timeout_timer(timeout_ms, tx.clone());
    }

    let beam_gone = supervise(
        &rx,
        pgid,
        kill_grace,
        &mut child_stdin,
        &out,
        pumps,
        &pump_done_rx,
        cgroup.as_ref(),
        credit.as_ref(),
    );

    // Backpressure linger: the child is reaped and the EXIT frame is written,
    // but the BEAM may still grant credit (Port.command) while it drains the
    // frames it has already buffered. If the shim exited now, its stdin read
    // end would close and that racing write would send the BEAM an epipe exit
    // signal, killing the stream. Keep draining inbound frames (which keeps the
    // stdin reader running and the pipe open) until the BEAM closes the port
    // after it has consumed EXIT. Skipped when the BEAM is already gone, and
    // never entered on the eager path, so the default behavior is unchanged.
    if credit.is_some() && !beam_gone {
        drain_inbound_until_closed(&rx);
    }

    Ok(())
}

/// Drains inbound events after EXIT so late CREDIT writes from the BEAM are
/// absorbed and the shim's stdin stays open until the BEAM closes the port
/// (reported as `StdinClosed`) or the reader thread ends. Blocks on `recv`, so
/// it does not busy-spin.
fn drain_inbound_until_closed(rx: &Receiver<Event>) {
    loop {
        match rx.recv() {
            Ok(Event::StdinClosed) | Ok(Event::StdinError(_)) => return,
            Ok(_) => continue,
            Err(_) => return,
        }
    }
}

/// Builds and spawns the child process, calling `setsid()` in the child
/// before exec so it leads its own process group.
///
/// Returns the child, the master side of the pty pair (pty mode only), and the
/// prepared cgroup (when `cgroup: true` was requested and a delegated cgroup v2
/// subtree was available). The caller reads child output from the master and
/// writes STDIN frames to it. In pipe mode the second element is `None` and the
/// child's stdio is the usual set of pipes on the `Child`. The third element is
/// `None` on the default path, on fallback, and on non-Linux.
fn spawn_child(request: &SpawnRequest) -> io::Result<(Child, Option<OwnedFd>, Option<Cgroup>)> {
    let (program, args) = request
        .argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "argv must not be empty"))?;

    let mut cmd = Command::new(program);
    cmd.args(args);

    if let Some(cd) = &request.cd {
        cmd.current_dir(cd);
    }
    if !request.env.is_empty() {
        cmd.envs(&request.env);
    }

    // Resolve user/group names to numeric ids in the PARENT, before fork, and
    // diff them against the shim's current identity. getpwnam/getgrnam/
    // getgrouplist/getgroups are not async-signal-safe and must never run
    // inside pre_exec. A resolution failure fails closed: we never spawn, so
    // the child cannot run as the wrong (current) user.
    let plan = privdrop::resolve(request.user.as_ref(), request.group.as_ref())?;

    // Prepare the cgroup in the PARENT, before fork: detect cgroup v2, create a
    // delegated child cgroup, and open its cgroup.procs fd. Never fails: a
    // missing/undelegated cgroup returns None and the child runs with
    // process-group kill only. The child moves ITSELF into the cgroup in
    // pre_exec (see below), so a fast double-fork cannot escape a
    // move-after-spawn window.
    let cgroup = if request.cgroup {
        cgroup::prepare()
    } else {
        None
    };
    let placement = cgroup.as_ref().map(|cg| cg.placement());

    if request.pty {
        spawn_child_pty(cmd, request, plan, placement).map(|(c, m)| (c, m, cgroup))
    } else {
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Safety: the closure calls setsid() (async-signal-safe), then joins the
        // cgroup by writing its own pid to the parent-opened fd (a single
        // allocation-free write, see Placement::join_in_child), then, when a
        // drop was requested, the numeric setgroups/setgid/setuid syscalls via
        // DropPlan::apply. All names/fds were resolved/opened in the parent; the
        // closure allocates nothing beyond reading the moved `plan`/`placement`.
        // It runs in the forked child between fork and exec, per `pre_exec`'s
        // contract. Joining the cgroup right after setsid, before any user code
        // runs, means every descendant the child forks inherits the cgroup.
        unsafe {
            cmd.pre_exec(move || {
                process::setsid()
                    .map(|_| ())
                    .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))?;
                if let Some(placement) = &placement {
                    placement.join_in_child()?;
                }
                if let Some(plan) = &plan {
                    plan.apply()?;
                }
                Ok(())
            });
        }

        cmd.spawn().map(|child| (child, None, cgroup))
    }
}

/// Spawns the child attached to a freshly allocated pty. The master fd stays
/// with the shim; the child's stdin, stdout, and stderr all point at the
/// slave, and after `setsid` the child claims the slave as its controlling
/// terminal (`TIOCSCTTY`). Because a pty carries one bidirectional stream,
/// the child's stderr is merged into the same terminal as its stdout.
fn spawn_child_pty(
    mut cmd: Command,
    request: &SpawnRequest,
    plan: Option<DropPlan>,
    placement: Option<cgroup::Placement>,
) -> io::Result<(Child, Option<OwnedFd>)> {
    use rustix::pty::{grantpt, openpt, ptsname, unlockpt, OpenptFlags};

    let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY).map_err(errno)?;
    grantpt(&master).map_err(errno)?;
    unlockpt(&master).map_err(errno)?;

    let slave_name = ptsname(&master, Vec::new()).map_err(errno)?;
    let slave = rustix::fs::open(
        slave_name.as_c_str(),
        rustix::fs::OFlags::RDWR | rustix::fs::OFlags::NOCTTY,
        rustix::fs::Mode::empty(),
    )
    .map_err(errno)?;

    // The child inherits three dups of the slave as fds 0/1/2. Give each of
    // stdin/stdout/stderr its own OwnedFd so std does not close a shared fd
    // more than once.
    let slave_in = slave.try_clone()?;
    let slave_out = slave.try_clone()?;
    let slave_err = slave;
    cmd.stdin(Stdio::from(slave_in))
        .stdout(Stdio::from(slave_out))
        .stderr(Stdio::from(slave_err));

    let winsize = window_size(request);

    // Safety: the closure runs between fork and exec. It calls only
    // async-signal-safe operations (setsid, ioctl, the single allocation-free
    // cgroup.procs write in Placement::join_in_child, and the numeric
    // setgroups/setgid/setuid in DropPlan::apply) and touches no parent memory
    // beyond the copied `winsize` and moved `plan`/`placement` values. fd 0 is
    // the child's slave side after std has wired up stdio, so TIOCSCTTY on it
    // makes the pty the controlling terminal. The cgroup join runs before the
    // credential drop, since after setuid the child may lose write access to
    // cgroup.procs (delegation is tied to the shim's uid). The credential drop
    // runs last, after the terminal is claimed, so setting the controlling
    // terminal still has the privilege it needs; after setuid that privilege
    // may be gone.
    unsafe {
        cmd.pre_exec(move || {
            process::setsid().map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))?;

            let stdin = std::os::fd::BorrowedFd::borrow_raw(0);
            process::ioctl_tiocsctty(stdin)
                .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))?;

            if let Some(ws) = winsize {
                rustix::termios::tcsetwinsize(stdin, ws)
                    .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))?;
            }

            if let Some(placement) = &placement {
                placement.join_in_child()?;
            }

            if let Some(plan) = &plan {
                plan.apply()?;
            }
            Ok(())
        });
    }

    let child = cmd.spawn()?;
    Ok((child, Some(master)))
}

/// Builds the initial pty window size from the request, if either dimension
/// was provided. A dimension left unset defaults to 0, which the terminal
/// treats as "unspecified".
fn window_size(request: &SpawnRequest) -> Option<rustix::termios::Winsize> {
    match (request.pty_rows, request.pty_cols) {
        (None, None) => None,
        (rows, cols) => Some(rustix::termios::Winsize {
            ws_row: rows.unwrap_or(0),
            ws_col: cols.unwrap_or(0),
            ws_xpixel: 0,
            ws_ypixel: 0,
        }),
    }
}

/// Maps a rustix errno into a std io error.
fn errno(e: rustix::io::Errno) -> io::Error {
    io::Error::from_raw_os_error(e.raw_os_error())
}

/// Pumps bytes from a child pipe into framed messages on `out`, sending
/// one `()` on `done` when the pipe is exhausted. Returns the number of
/// pumps spawned (0 or 1); 0 only if the pipe wasn't captured, which
/// shouldn't happen given `Stdio::piped()`.
///
/// With `credit` present the pump is gated: it reads the child only while it
/// holds read budget, which is how backpressure reaches the producer. With
/// `credit` `None` the pump reads eagerly (the default path).
fn spawn_output_pump<P: Read + Send + 'static, W: Write + Send + 'static>(
    pipe: Option<P>,
    tag: u8,
    out: Arc<Mutex<W>>,
    done: Sender<()>,
    credit: Option<Credit>,
) -> usize {
    let Some(mut pipe) = pipe else { return 0 };
    thread::spawn(move || {
        let mut buf = [0u8; 8192];
        match credit {
            None => pump_eager(&mut pipe, tag, &out, &mut buf),
            Some(credit) => pump_gated(&mut pipe, tag, &out, &mut buf, &credit),
        }
        let _ = done.send(());
    });
    1
}

/// The default, ungated pump: read as fast as the pipe allows, forward every
/// chunk. Unchanged from the pre-backpressure behavior.
fn pump_eager<P: Read, W: Write>(pipe: &mut P, tag: u8, out: &Arc<Mutex<W>>, buf: &mut [u8]) {
    loop {
        match pipe.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                let mut w = out.lock().unwrap();
                if frame::write_frame(&mut *w, tag, &buf[..n]).is_err() {
                    break;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
}

/// The backpressure pump: read the child only while the budget allows. At
/// zero credit it parks on the condvar (no busy-spin); the OS pipe fills and
/// the child's next write blocks. A CREDIT grant wakes it; an uncork (child
/// exited) lets it drain the pipe to EOF so buffered output is not lost.
fn pump_gated<P: Read, W: Write>(
    pipe: &mut P,
    tag: u8,
    out: &Arc<Mutex<W>>,
    buf: &mut [u8],
    credit: &Credit,
) {
    loop {
        let n = match credit.await_permit(buf.len()) {
            Permit::Uncork => match pipe.read(buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            },
            Permit::Read(cap) => match pipe.read(&mut buf[..cap]) {
                Ok(0) => break,
                Ok(n) => {
                    credit.consume(n);
                    n
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            },
        };
        let mut w = out.lock().unwrap();
        if frame::write_frame(&mut *w, tag, &buf[..n]).is_err() {
            break;
        }
    }
}

/// Reads protocol frames off the shim's stdin and forwards them as
/// `Event`s. Clean EOF is reported as `Event::StdinClosed`.
fn spawn_stdin_reader<R: Read + Send + 'static>(mut stdin: R, tx: Sender<Event>) {
    thread::spawn(move || loop {
        match frame::read_frame(&mut stdin) {
            Ok(Some(f)) => {
                if tx.send(Event::Stdin(f)).is_err() {
                    return;
                }
            }
            Ok(None) => {
                let _ = tx.send(Event::StdinClosed);
                return;
            }
            Err(e) => {
                let _ = tx.send(Event::StdinError(e));
                return;
            }
        }
    });
}

/// Owns `child` for the rest of its life: blocks on its exit and reports
/// the outcome as an `Event`. This is the sole caller of `Child::wait`
/// for this child.
fn spawn_waiter(mut child: Child, tx: Sender<Event>) {
    thread::spawn(move || {
        let outcome = match child.wait() {
            Ok(status) => ChildOutcome {
                status: status.code(),
                signal: status.signal(),
            },
            Err(_) => ChildOutcome {
                status: None,
                signal: None,
            },
        };
        let _ = tx.send(Event::ChildExited(outcome));
    });
}

/// Sends `Event::TimedOut` after `timeout_ms` elapses.
fn spawn_timeout_timer(timeout_ms: u64, tx: Sender<Event>) {
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(timeout_ms));
        let _ = tx.send(Event::TimedOut);
    });
}

/// The main supervising loop: consumes events until the child has exited,
/// drains the output pumps, then writes the EXIT frame.
/// Runs the event loop until the child exits, drains output, and writes the
/// EXIT frame. Returns whether the BEAM had already vanished (stdin EOF or
/// error), which the caller uses to decide whether to linger for late credit.
#[allow(clippy::too_many_arguments)]
fn supervise<W: Write>(
    rx: &Receiver<Event>,
    pgid: Pid,
    kill_grace: Duration,
    child_stdin: &mut Option<Box<dyn Write + Send>>,
    out: &Arc<Mutex<W>>,
    pumps: usize,
    pump_done: &Receiver<()>,
    cgroup: Option<&Cgroup>,
    credit: Option<&Credit>,
) -> bool {
    let mut timed_out = false;
    let mut beam_gone = false;

    let outcome = loop {
        match rx.recv() {
            Ok(Event::Stdin(f)) if f.tag == TAG_STDIN => {
                if let Some(w) = child_stdin.as_mut() {
                    let _ = w.write_all(&f.payload);
                }
            }
            Ok(Event::Stdin(f)) if f.tag == TAG_EOF => {
                *child_stdin = None; // drop closes the pipe
            }
            Ok(Event::Stdin(f)) if f.tag == TAG_KILL => {
                kill_group(pgid, kill_grace, cgroup);
            }
            Ok(Event::Stdin(f)) if f.tag == TAG_CREDIT => {
                // Grant the stdout pump more read budget. Ignored when
                // backpressure was not requested (no shared budget exists).
                if let (Some(c), Some(n)) = (credit, crate::credit::parse_grant(&f.payload)) {
                    c.grant(n);
                }
            }
            Ok(Event::Stdin(_)) => {
                // Unknown/unexpected tag mid-session; ignore rather than
                // tearing down a running child over a protocol wrinkle.
            }
            Ok(Event::StdinClosed) => {
                // The BEAM is gone. Kill the group; keep waiting for the
                // real exit event from the waiter thread so we reap the
                // child properly, but there's no one left to report to.
                beam_gone = true;
                kill_group(pgid, kill_grace, cgroup);
            }
            Ok(Event::StdinError(e)) => {
                eprintln!("forcola_shim: stdin read error, treating as BEAM death: {e}");
                beam_gone = true;
                kill_group(pgid, kill_grace, cgroup);
            }
            Ok(Event::TimedOut) => {
                timed_out = true;
                kill_group(pgid, kill_grace, cgroup);
            }
            Ok(Event::ChildExited(outcome)) => break Some(outcome),
            Err(_) => break None,
        }
    };

    let outcome = outcome.unwrap_or(ChildOutcome {
        status: None,
        signal: None,
    });

    // The child is reaped. Uncork the stdout pump so a credit-starved pump
    // wakes and drains whatever the child left in the pipe to EOF, instead of
    // parking until the drain grace elapses and losing that trailing output.
    if let Some(c) = credit {
        c.uncork();
    }

    // Tear the contained subtree down after the child is reaped, whether it
    // was killed or exited on its own. `finish_cgroup` writes cgroup.kill (in
    // case a daemonizer escaped the process group and is still alive),
    // confirms the cgroup drained, and rmdirs it. Runs even on beam_gone so a
    // vanished BEAM does not leak the cgroup directory.
    if let Some(cg) = cgroup {
        finish_cgroup(cg, kill_grace);
    }

    if beam_gone {
        // Nothing to report to: the reader/writer on the other end of
        // stdout is the same dead BEAM that closed stdin. Attempting to
        // write would either block on a full pipe with no reader or
        // error out; either way there is no value in trying.
        return true;
    }

    // The child has been reaped, but the output pumps may not have hit
    // EOF on its pipes yet; without this the EXIT frame can overtake
    // output the child wrote just before dying, and the BEAM (which
    // treats EXIT as the terminator) would drop it.
    drain_pumps(pumps, pump_done);

    let report = ExitReport {
        status: outcome.status,
        signal: outcome.signal,
        timed_out,
        contained: cgroup.is_some(),
    };
    if let Ok(payload) = serde_json::to_vec(&report) {
        let mut w = out.lock().unwrap();
        let _ = frame::write_frame(&mut *w, TAG_EXIT, &payload);
    }

    false
}

/// Final cgroup teardown after the child is reaped: SIGKILL any escapee still
/// in the subtree via `cgroup.kill`, confirm the cgroup drained (bounded by the
/// kill grace), then rmdir it. Layered on top of the process-group kill, which
/// has already run; this only matters for a descendant that left the process
/// group by daemonizing.
fn finish_cgroup(cg: &Cgroup, grace: Duration) {
    // SIGKILL the whole subtree. This is the backstop that reaches a
    // deliberate daemonizer the process-group kill could not.
    cg.kill();

    // Wait for the cgroup to drain, in addition to the process-group death
    // probe the kill path already did. Bound the wait so a wedged cgroup
    // cannot delay the EXIT report forever.
    let deadline = Instant::now() + grace + Duration::from_millis(500);
    while Instant::now() < deadline {
        if cg.drained() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    // rmdir the now-empty cgroup. Best-effort: if it is somehow non-empty the
    // delegation owner reaps it later.
    cg.remove();
}

/// Waits (bounded by `OUTPUT_DRAIN_GRACE`) for `pumps` output pump
/// threads to report EOF on the child's pipes.
fn drain_pumps(pumps: usize, pump_done: &Receiver<()>) {
    let deadline = Instant::now() + OUTPUT_DRAIN_GRACE;
    for _ in 0..pumps {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        if pump_done.recv_timeout(deadline - now).is_err() {
            return;
        }
    }
}

/// SIGTERM the whole process group, then SIGKILL after `grace` if it
/// hasn't died. Confirms death via a signal-0 liveness probe before
/// returning.
///
/// When a cgroup is present, `cgroup.kill` is written after the group kill so a
/// descendant that escaped the process group by daemonizing is SIGKILLed too.
/// This is layered on top of the process-group kill, never in place of it: the
/// SIGTERM/SIGKILL group sequence is unchanged; the cgroup write is added at
/// the end. The final drain-and-rmdir happens once, after the child is reaped,
/// in `finish_cgroup`.
fn kill_group(pgid: Pid, grace: Duration, cgroup: Option<&Cgroup>) {
    let _ = process::kill_process_group(pgid, Signal::TERM);

    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if !group_alive(pgid) {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    if group_alive(pgid) {
        let _ = process::kill_process_group(pgid, Signal::KILL);
        // Give the kernel a moment to finish tearing the group down.
        let hard_deadline = Instant::now() + Duration::from_millis(500);
        while group_alive(pgid) && Instant::now() < hard_deadline {
            thread::sleep(Duration::from_millis(10));
        }
    }

    // Backstop for a deliberate daemonizer that left the process group: the
    // group kill above never reached it, but cgroup.kill SIGKILLs everything
    // still in the subtree at once.
    if let Some(cg) = cgroup {
        cg.kill();
    }
}

/// Signal-0 liveness probe: does at least one process in the group still
/// exist?
///
/// Limitation: once the group is dead, its pgid can be recycled by the
/// kernel, in which case the probe would match an unrelated new group and
/// report it alive. The window between group death and the probe is
/// narrow, and the failure mode is a spurious extra SIGKILL to a group we
/// no longer own (which EPERM would usually block anyway), so we accept
/// it rather than track individual members.
fn group_alive(pgid: Pid) -> bool {
    probe_says_alive(process::test_kill_process_group(pgid))
}

/// Maps the result of the `kill(-pgid, 0)` probe to group liveness.
///
/// `kill(2)` on a process group returns EPERM if any member could not be
/// signaled, even when the signal was (or would have been) delivered to
/// the other members; macOS exhibits this readily. So EPERM means the
/// group still exists and must be treated as alive, or the caller would
/// skip SIGKILL escalation while members survive. Only ESRCH proves the
/// group is gone. Any other error is treated as alive so escalation
/// proceeds; the safe failure mode is an unnecessary SIGKILL, not a
/// leaked process group.
fn probe_says_alive(probe: rustix::io::Result<()>) -> bool {
    match probe {
        Ok(()) => true,
        Err(rustix::io::Errno::SRCH) => false,
        Err(_) => true,
    }
}

fn write_error<W: Write>(out: &Arc<Mutex<W>>, reason: &str) {
    let report = ErrorReport { reason };
    if let Ok(payload) = serde_json::to_vec(&report) {
        let mut w = out.lock().unwrap();
        let _ = frame::write_frame(&mut *w, TAG_ERROR, &payload);
    }
}

#[cfg(test)]
mod tests {
    use super::{probe_says_alive, pump_eager, pump_gated};
    use crate::credit::Credit;
    use crate::frame::{read_frame, TAG_STDOUT};
    use rustix::io::Errno;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    // Decodes concatenated frames back into their joined payload bytes.
    fn joined_payloads(bytes: &[u8]) -> Vec<u8> {
        let mut cur = Cursor::new(bytes.to_vec());
        let mut out = Vec::new();
        while let Some(f) = read_frame(&mut cur).unwrap() {
            assert_eq!(f.tag, TAG_STDOUT);
            out.extend_from_slice(&f.payload);
        }
        out
    }

    fn wait_for_len(out: &Arc<Mutex<Vec<u8>>>, target: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while out.lock().unwrap().len() < target {
            assert!(
                Instant::now() < deadline,
                "output never reached {target} bytes"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn gated_pump_pauses_at_zero_credit_and_resumes_on_grant() {
        let data: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
        let out = Arc::new(Mutex::new(Vec::<u8>::new()));
        let credit = Credit::new();

        let out2 = Arc::clone(&out);
        let credit2 = credit.clone();
        let data2 = data.clone();
        let handle = thread::spawn(move || {
            let mut pipe = Cursor::new(data2);
            let mut buf = [0u8; 8192];
            pump_gated(&mut pipe, TAG_STDOUT, &out2, &mut buf, &credit2);
        });

        // Grant 40 bytes: the pump forwards exactly one 40-byte frame (5 bytes
        // of framing) and then parks, unable to read further without credit.
        credit.grant(40);
        wait_for_len(&out, 45);
        thread::sleep(Duration::from_millis(60));
        assert_eq!(
            out.lock().unwrap().len(),
            45,
            "pump read past its credit while parked"
        );

        // Grant the remaining 60: forwarded, then parks again (no credit, and
        // not yet uncorked, even though the pipe is at EOF).
        credit.grant(60);
        wait_for_len(&out, 110);
        thread::sleep(Duration::from_millis(60));
        assert_eq!(out.lock().unwrap().len(), 110);

        // Uncork lets the pump observe EOF and exit.
        credit.uncork();
        handle.join().unwrap();

        assert_eq!(joined_payloads(&out.lock().unwrap()), data);
    }

    #[test]
    fn eager_pump_forwards_everything_without_credit() {
        let data: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
        let out = Arc::new(Mutex::new(Vec::<u8>::new()));

        let mut pipe = Cursor::new(data.clone());
        let mut buf = [0u8; 8192];
        pump_eager(&mut pipe, TAG_STDOUT, &out, &mut buf);

        assert_eq!(joined_payloads(&out.lock().unwrap()), data);
    }

    #[test]
    fn probe_ok_means_alive() {
        assert!(probe_says_alive(Ok(())));
    }

    #[test]
    fn probe_esrch_means_dead() {
        assert!(!probe_says_alive(Err(Errno::SRCH)));
    }

    #[test]
    fn probe_eperm_means_alive() {
        // macOS returns EPERM from kill(-pgid, ...) when any single group
        // member is unsignalable, even though the group still exists.
        assert!(probe_says_alive(Err(Errno::PERM)));
    }

    #[test]
    fn probe_other_errors_mean_alive() {
        assert!(probe_says_alive(Err(Errno::INVAL)));
    }
}
