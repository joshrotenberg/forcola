//! The supervising event loop: spawns the child, forwards its output,
//! and applies the kill sequence on timeout, KILL frame, or stdin EOF.

use crate::frame::{
    self, Frame, TAG_EOF, TAG_ERROR, TAG_EXIT, TAG_KILL, TAG_SPAWN, TAG_STDERR, TAG_STDIN,
    TAG_STDOUT,
};
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

    let (mut child, pty_master) = match spawn_child(&request) {
        Ok(c) => c,
        Err(e) => {
            write_error(&out, &format!("spawn failed: {e}"));
            return Ok(());
        }
    };

    let pgid = Pid::from_child(&child);
    let kill_grace = Duration::from_millis(request.kill_grace_ms);

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
            );
            let stderr_tag = if request.merge_stderr {
                TAG_STDOUT
            } else {
                TAG_STDERR
            };
            pumps += spawn_output_pump(
                child.stderr.take(),
                stderr_tag,
                Arc::clone(&out),
                pump_done_tx,
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

    supervise(
        rx,
        pgid,
        kill_grace,
        &mut child_stdin,
        &out,
        pumps,
        &pump_done_rx,
    );

    Ok(())
}

/// Builds and spawns the child process, calling `setsid()` in the child
/// before exec so it leads its own process group.
///
/// Returns the child and, in pty mode, the master side of the pty pair; the
/// caller reads child output from the master and writes STDIN frames to it.
/// In pipe mode the second element is `None` and the child's stdio is the
/// usual set of pipes on the `Child`.
fn spawn_child(request: &SpawnRequest) -> io::Result<(Child, Option<OwnedFd>)> {
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

    if request.pty {
        spawn_child_pty(cmd, request)
    } else {
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Safety: the closure only calls setsid(), which is async-signal-safe
        // and does not allocate or touch the parent's memory. It runs in the
        // forked child between fork and exec, per `pre_exec`'s contract.
        unsafe {
            cmd.pre_exec(|| {
                process::setsid()
                    .map(|_| ())
                    .map_err(|e| io::Error::from_raw_os_error(e.raw_os_error()))
            });
        }

        cmd.spawn().map(|child| (child, None))
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
    // async-signal-safe operations (setsid, ioctl) and touches no parent
    // memory beyond the copied `winsize` value. fd 0 is the child's slave
    // side after std has wired up stdio, so TIOCSCTTY on it makes the pty
    // the controlling terminal.
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
fn spawn_output_pump<P: Read + Send + 'static, W: Write + Send + 'static>(
    pipe: Option<P>,
    tag: u8,
    out: Arc<Mutex<W>>,
    done: Sender<()>,
) -> usize {
    let Some(mut pipe) = pipe else { return 0 };
    thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match pipe.read(&mut buf) {
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
        let _ = done.send(());
    });
    1
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
fn supervise<W: Write>(
    rx: Receiver<Event>,
    pgid: Pid,
    kill_grace: Duration,
    child_stdin: &mut Option<Box<dyn Write + Send>>,
    out: &Arc<Mutex<W>>,
    pumps: usize,
    pump_done: &Receiver<()>,
) {
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
                kill_group(pgid, kill_grace);
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
                kill_group(pgid, kill_grace);
            }
            Ok(Event::StdinError(e)) => {
                eprintln!("forcola_shim: stdin read error, treating as BEAM death: {e}");
                beam_gone = true;
                kill_group(pgid, kill_grace);
            }
            Ok(Event::TimedOut) => {
                timed_out = true;
                kill_group(pgid, kill_grace);
            }
            Ok(Event::ChildExited(outcome)) => break Some(outcome),
            Err(_) => break None,
        }
    };

    let outcome = outcome.unwrap_or(ChildOutcome {
        status: None,
        signal: None,
    });

    if beam_gone {
        // Nothing to report to: the reader/writer on the other end of
        // stdout is the same dead BEAM that closed stdin. Attempting to
        // write would either block on a full pipe with no reader or
        // error out; either way there is no value in trying.
        return;
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
    };
    if let Ok(payload) = serde_json::to_vec(&report) {
        let mut w = out.lock().unwrap();
        let _ = frame::write_frame(&mut *w, TAG_EXIT, &payload);
    }
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
fn kill_group(pgid: Pid, grace: Duration) {
    let _ = process::kill_process_group(pgid, Signal::TERM);

    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if !group_alive(pgid) {
            return;
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
    use super::probe_says_alive;
    use rustix::io::Errno;

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
