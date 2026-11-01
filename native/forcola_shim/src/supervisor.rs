//! The supervising event loop: spawns the child, forwards its output,
//! and applies the kill sequence on timeout, KILL frame, or stdin EOF.

use crate::cgroup::{self, Cgroup};
use crate::credit::{Credit, Permit};
use crate::frame::{
    self, Frame, TAG_CAPABILITIES, TAG_CREDIT, TAG_EOF, TAG_ERROR, TAG_EXIT, TAG_HELLO, TAG_KILL,
    TAG_SPAWN, TAG_SPAWNED, TAG_STDERR, TAG_STDERR_CREDIT, TAG_STDIN, TAG_STDOUT,
};
use crate::identity;
use crate::privdrop::{self, DropPlan};
use crate::protocol::{
    Capabilities, CgroupMode, EnvMode, ErrorReport, ExitReport, HelloRequest, SpawnRequest,
};
use crate::spawn_error::{self, ChildReporter, ReportReader, SpawnFailure, Stage};

use rustix::process::{self, Pid, Signal};
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, OnceLock};
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

/// How long to wait in eager mode (or after a strict drain was cancelled),
/// after the child has been reaped, for output pumps to hit EOF before EXIT.
/// In the normal case the writers are already dead and EOF is immediate;
/// the bound exists so a surviving process that inherited the pipe (e.g.
/// a backgrounded grandchild the kill path never ran against) cannot
/// delay the EXIT report forever.
const OUTPUT_DRAIN_GRACE: Duration = Duration::from_secs(1);

/// Runs the shim: read one SPAWN frame, supervise the child, and keep
/// handling protocol frames until the child has exited.
pub fn run<R: Read + Send + 'static, W: Write + Send + 'static>(
    stdin: R,
    stdout: W,
) -> io::Result<()> {
    run_with_capabilities(stdin, stdout, supported_features)
}

fn run_with_capabilities<R, W, F>(mut stdin: R, stdout: W, capabilities: F) -> io::Result<()>
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
    F: FnOnce() -> Vec<&'static str>,
{
    let out = Arc::new(Mutex::new(stdout));

    // SPAWN is mandatory, with an optional capability request before it.
    // Optional capability exchange MUST finish before any child is spawned.
    // Old shims reject this first tag instead of ignoring new SPAWN keys.
    let mut selected = SelectedFeatures::default();
    let first = frame::read_frame(&mut stdin)?;
    let first = match first {
        Some(f) if f.tag == TAG_HELLO => {
            let features = capabilities();
            selected = match select_features(&f.payload, &features) {
                Ok(selected) => selected,
                Err(reason) => {
                    write_error(&out, &reason);
                    return Ok(());
                }
            };
            let capabilities = Capabilities {
                protocol: 1,
                features,
            };
            let payload = serde_json::to_vec(&capabilities)?;
            let mut w = out.lock().unwrap();
            frame::write_frame(&mut *w, TAG_CAPABILITIES, &payload)?;
            drop(w);
            frame::read_frame(&mut stdin)?
        }
        other => other,
    };
    let spawn_frame = match first {
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

    if !matches!(
        (request.interrupt_signal, request.interrupt_grace_ms),
        (None, None) | (Some(_), Some(1..=4_294_967_295))
    ) {
        write_error(
            &out,
            "interrupt_signal requires a positive interrupt_grace_ms (u32)",
        );
        return Ok(());
    }

    let (mut child, pty_master, cgroup) =
        match spawn_child(&request, selected.structured_spawn_errors) {
            Ok(c) => c,
            Err(e) => {
                write_spawn_error(&out, &e, selected.structured_spawn_errors);
                return Ok(());
            }
        };

    let pgid = Pid::from_child(&child);
    let kill_grace = Duration::from_millis(request.kill_grace_ms);

    if selected.spawn_identity {
        // No output pumps or waiter exist yet. Fast exits remain unreaped,
        // retaining their native start time and preventing pid reuse until the
        // identity has been captured. The shared writer preserves this frame
        // ahead of every stdout/stderr frame, including for pty children.
        let identity = match identity::read(child.id()) {
            Ok(identity) => identity,
            Err(error) => {
                abort_spawn(child, cgroup.as_ref(), kill_grace);
                write_error(&out, &format!("spawn identity failed: {error}"));
                return Ok(());
            }
        };
        let written = serde_json::to_vec(&identity)
            .map_err(io::Error::other)
            .and_then(|payload| {
                frame::write_frame(&mut *out.lock().unwrap(), TAG_SPAWNED, &payload)
            });
        if let Err(error) = written {
            abort_spawn(child, cgroup.as_ref(), kill_grace);
            return Err(error);
        }
    }

    // Backpressure is opt-in: a `window_bytes` in the SPAWN payload gates the
    // stdout pump on a shared read budget. Absent, the pump reads eagerly and
    // the default path is byte-for-byte unchanged. The budget starts at zero,
    // so the pump parks until the BEAM's first CREDIT frame.
    let credit = request.window_bytes.map(|_| Credit::new());
    let stderr_credit = request.stderr_window_bytes.map(|_| Credit::new());

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
            // Stderr is eager by default. Duplex pull mode supplies a
            // separate read budget so one idle pipe cannot block the other.
            // Under merge_stderr the stderr bytes ride the STDOUT tag;
            // pull mode rejects that combination for pipe-backed children.
            pumps += spawn_output_pump(
                child.stderr.take(),
                stderr_tag,
                Arc::clone(&out),
                pump_done_tx,
                stderr_credit.clone(),
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
        stderr_credit.as_ref(),
        request.strict_output,
        request.interrupt_grace_ms.map(Duration::from_millis),
    );

    // Backpressure linger: the child is reaped and the EXIT frame is written,
    // but the BEAM may still grant credit (Port.command) while it drains the
    // frames it has already buffered. If the shim exited now, its stdin read
    // end would close and that racing write would send the BEAM an epipe exit
    // signal, killing the stream. Keep draining inbound frames (which keeps the
    // stdin reader running and the pipe open) until the BEAM closes the port
    // after it has consumed EXIT. Skipped when the BEAM is already gone, and
    // never entered on the eager path, so the default behavior is unchanged.
    if (credit.is_some() || stderr_credit.is_some()) && !beam_gone {
        drain_inbound_until_closed(&rx);
    }

    Ok(())
}

fn supported_features() -> Vec<&'static str> {
    static IDENTITY_AVAILABLE: OnceLock<bool> = OnceLock::new();
    supported_features_with_probe(&IDENTITY_AVAILABLE, || {
        identity::read(std::process::id()).map(|_| ())
    })
}

fn supported_features_with_probe<F>(
    identity_available: &OnceLock<bool>,
    probe: F,
) -> Vec<&'static str>
where
    F: FnOnce() -> io::Result<()>,
{
    let mut features = vec!["bounded_run_v1", "structured_spawn_errors_v1"];
    // Probe the running shim when HELLO is handled, including an empty HELLO,
    // rather than assuming that a supported OS makes its process metadata
    // readable. This catches unavailable procfs or denied libproc access
    // before SPAWN, but does not guarantee access to a child after a privilege
    // change. The post-spawn read still fails closed and cleans up that child.
    if *identity_available.get_or_init(|| probe().is_ok()) {
        features.push("spawn_identity_v1");
    }
    features
}

/// Selected outbound extensions are independent of advertised capabilities.
/// Future TRACKED selection can add its own field without changing SPAWNED.
#[derive(Debug, Default, PartialEq, Eq)]
struct SelectedFeatures {
    spawn_identity: bool,
    structured_spawn_errors: bool,
}

fn select_features(payload: &[u8], supported: &[&str]) -> Result<SelectedFeatures, String> {
    let request = if payload.is_empty() {
        HelloRequest::default()
    } else {
        serde_json::from_slice::<HelloRequest>(payload)
            .map_err(|error| format!("invalid HELLO payload: {error}"))?
    };
    for feature in &request.features {
        if !supported.contains(&feature.as_str()) {
            return Err(format!("unsupported feature: {feature}"));
        }
    }
    Ok(SelectedFeatures {
        spawn_identity: request
            .features
            .iter()
            .any(|feature| feature == "spawn_identity_v1"),
        structured_spawn_errors: request
            .features
            .iter()
            .any(|feature| feature == "structured_spawn_errors_v1"),
    })
}

/// Startup reporting failed before the waiter took ownership. Stop the owned
/// group immediately, including a cgroup backstop when present, then reap the
/// child and remove its cgroup. No early error may leave a live child behind.
fn abort_spawn(mut child: Child, cgroup: Option<&Cgroup>, grace: Duration) {
    let _ = process::kill_process_group(Pid::from_child(&child), Signal::KILL);
    if let Some(cgroup) = cgroup {
        cgroup.kill();
    }
    let _ = child.wait();
    if let Some(cgroup) = cgroup {
        finish_cgroup(cgroup, grace);
    }
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
/// prepared cgroup (when requested and a delegated cgroup v2 subtree was
/// available). The caller reads child output from the master and
/// writes STDIN frames to it. In pipe mode the second element is `None` and the
/// child's stdio is the usual set of pipes on the `Child`. The third element is
/// `None` on the default path, on fallback, and on non-Linux.
fn spawn_child(
    request: &SpawnRequest,
    structured: bool,
) -> Result<(Child, Option<OwnedFd>, Option<Cgroup>), SpawnFailure> {
    let (program, args) = request.argv.split_first().ok_or_else(|| {
        SpawnFailure::new(
            Stage::Spawn,
            io::Error::new(io::ErrorKind::InvalidInput, "argv must not be empty"),
            None,
        )
    })?;

    let mut cmd = Command::new(program);
    cmd.args(args);

    // std performs current_dir before pre_exec, hiding which syscall failed.
    // With structured reporting, perform the same chdir in our child hook and
    // report its actual errno. Legacy spawning retains std's original path.
    if !structured {
        if let Some(cd) = &request.cd {
            cmd.current_dir(cd);
        }
    }
    if request.env_mode == EnvMode::Replace {
        cmd.env_clear();
    }
    if !request.env.is_empty() {
        cmd.envs(&request.env);
    }

    // Resolve user/group names to numeric ids in the PARENT, before fork, and
    // diff them against the shim's current identity. getpwnam/getgrnam/
    // getgrouplist/getgroups are not async-signal-safe and must never run
    // inside pre_exec. A resolution failure fails closed: we never spawn, so
    // the child cannot run as the wrong (current) user.
    let plan = privdrop::resolve(request.user.as_ref(), request.group.as_ref())
        .map_err(|error| SpawnFailure::new(Stage::Privdrop, error, None))?;

    // Prepare the cgroup in the PARENT, before fork: detect cgroup v2, create a
    // delegated child cgroup, and open its cgroup.procs fd. Best-effort mode
    // falls back to process-group kill when setup is unavailable; required
    // mode fails before fork. The child moves ITSELF into the cgroup in
    // pre_exec (see below), so a fast double-fork cannot escape a
    // move-after-spawn window.
    let cgroup = match request.cgroup {
        CgroupMode::BestEffort(false) => None,
        CgroupMode::BestEffort(true) => cgroup::prepare(),
        CgroupMode::Required(_) => Some(cgroup::prepare_required()?),
    };
    let placement = cgroup.as_ref().map(|cg| cg.placement());
    let cgroup_path = cgroup.as_ref().map(|cg| cg.placement_path());
    let master = if request.pty {
        Some(
            prepare_child_pty(&mut cmd)
                .map_err(|error| SpawnFailure::new(Stage::Pty, error, None))?,
        )
    } else {
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        None
    };
    let (reader, reporter) = if structured {
        let (reader, reporter) =
            spawn_error::channel().map_err(|error| SpawnFailure::new(Stage::Spawn, error, None))?;
        (Some(reader), Some(reporter))
    } else {
        (None, None)
    };
    // std defers reporting invalid current_dir bytes until spawn, after our
    // parent-side setup. Convert here to preserve that failure precedence
    // when account resolution, required cgroup preparation or pty setup fails.
    let cwd = if structured {
        request
            .cd
            .as_ref()
            .map(|cd| {
                CString::new(cd.as_bytes()).map_err(|_| {
                    SpawnFailure::new(
                        Stage::Cwd,
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "nul byte found in provided data",
                        ),
                        Some(cd.clone()),
                    )
                })
            })
            .transpose()?
    } else {
        None
    };
    let setup = ChildSetup {
        cwd,
        reporter,
        plan,
        placement,
        pty: request.pty,
        winsize: window_size(request),
    };
    install_child_setup(&mut cmd, setup);
    let child = finish_spawn(
        cmd,
        reader,
        program,
        request.cd.as_deref(),
        cgroup_path.as_deref(),
    )?;
    Ok((child, master, cgroup))
}

/// Configures a freshly allocated pty before spawning. The master fd stays
/// with the shim; the child's stdin, stdout, and stderr all point at the
/// slave, and after `setsid` the child claims the slave as its controlling
/// terminal (`TIOCSCTTY`). Because a pty carries one bidirectional stream,
/// the child's stderr is merged into the same terminal as its stdout.
fn prepare_child_pty(cmd: &mut Command) -> io::Result<OwnedFd> {
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

    Ok(master)
}

struct ChildSetup {
    cwd: Option<CString>,
    reporter: Option<ChildReporter>,
    plan: Option<DropPlan>,
    placement: Option<cgroup::Placement>,
    pty: bool,
    winsize: Option<rustix::termios::Winsize>,
}

impl ChildSetup {
    fn check(&self, stage: Stage, result: io::Result<()>) -> io::Result<()> {
        match &self.reporter {
            Some(reporter) => reporter.check(stage, result),
            None => result,
        }
    }

    fn apply(&self) -> io::Result<()> {
        if let Some(cwd) = &self.cwd {
            // SAFETY: cwd was converted to a NUL-terminated CString in the
            // parent. chdir is async-signal-safe and this closure runs before exec.
            let result = if unsafe { libc::chdir(cwd.as_ptr()) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            };
            self.check(Stage::Cwd, result)?;
        }
        self.check(Stage::Spawn, process::setsid().map(|_| ()).map_err(errno))?;
        if self.pty {
            // SAFETY: std has already connected fd 0 to our parent-opened slave.
            let stdin = unsafe { std::os::fd::BorrowedFd::borrow_raw(0) };
            self.check(Stage::Pty, process::ioctl_tiocsctty(stdin).map_err(errno))?;
            if let Some(winsize) = self.winsize {
                self.check(
                    Stage::Pty,
                    rustix::termios::tcsetwinsize(stdin, winsize).map_err(errno),
                )?;
            }
        }
        if let Some(placement) = &self.placement {
            self.check(Stage::Cgroup, placement.join_in_child())?;
        }
        if let Some(plan) = &self.plan {
            self.check(Stage::Privdrop, plan.apply())?;
        }
        if let Some(reporter) = &self.reporter {
            reporter.ready_for_exec()?;
        }
        Ok(())
    }
}

fn install_child_setup(cmd: &mut Command, setup: ChildSetup) {
    // SAFETY: everything that allocates or resolves names was prepared in the
    // parent. apply uses only chdir, setsid, terminal ioctls, numeric credential
    // syscalls and fixed stack-buffer writes. It does not format, allocate or
    // lock. Cgroup placement precedes privilege drop; chdir precedes both.
    unsafe {
        cmd.pre_exec(move || setup.apply());
    }
}

fn finish_spawn(
    mut cmd: Command,
    reader: Option<ReportReader>,
    argv0: &str,
    cwd: Option<&str>,
    cgroup_path: Option<&str>,
) -> Result<Child, SpawnFailure> {
    let result = cmd.spawn();
    // Command owns the closure and its report writer in the parent. Drop it
    // before reading, or a failure before pre_exec would wait for EOF forever.
    drop(cmd);
    result.map_err(|error| match reader {
        Some(reader) => reader.failure(error, argv0, cwd, cgroup_path),
        None => SpawnFailure::new(Stage::Spawn, error, None),
    })
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
            Permit::Cancel => break,
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
    stderr_credit: Option<&Credit>,
    strict_output: bool,
    interrupt_grace: Option<Duration>,
) -> bool {
    let mut timed_out = false;
    let mut beam_gone = false;
    let mut group_confirmed = true;
    // Once any terminal path has run the group kill, do not run it again when
    // the waiter reports the leader's exit. Besides being redundant, a second
    // kill after the group has disappeared widens the already-small pgid reuse
    // window. A normal ChildExited event with no prior kill still runs the
    // sequence so background descendants cannot outlive their leader.
    let mut group_cleanup_started = false;
    let mut discard_output = false;

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
                discard_output = true;
                if !group_cleanup_started {
                    group_cleanup_started = true;
                    group_confirmed = kill_group(pgid, kill_grace, cgroup, interrupt_grace);
                }
            }
            Ok(Event::Stdin(f)) if f.tag == TAG_CREDIT => {
                // Grant the stdout pump more read budget. Ignored when
                // backpressure was not requested (no shared budget exists).
                if let (Some(c), Some(n)) = (credit, crate::credit::parse_grant(&f.payload)) {
                    c.grant(n);
                }
            }
            Ok(Event::Stdin(f)) if f.tag == TAG_STDERR_CREDIT => {
                if let (Some(c), Some(n)) = (stderr_credit, crate::credit::parse_grant(&f.payload))
                {
                    c.grant(n);
                }
            }
            Ok(Event::Stdin(_)) => {
                // Unknown/unexpected tag mid-session; ignore rather than
                // tearing down a running child over a protocol wrinkle.
            }
            Ok(Event::StdinClosed) => {
                discard_output = true;
                // The BEAM is gone. Kill the group; keep waiting for the
                // real exit event from the waiter thread so we reap the
                // child properly, but there's no one left to report to.
                beam_gone = true;
                if !group_cleanup_started {
                    group_cleanup_started = true;
                    group_confirmed = kill_group(pgid, kill_grace, cgroup, interrupt_grace);
                }
            }
            Ok(Event::StdinError(e)) => {
                discard_output = true;
                eprintln!("forcola_shim: stdin read error, treating as BEAM death: {e}");
                beam_gone = true;
                if !group_cleanup_started {
                    group_cleanup_started = true;
                    group_confirmed = kill_group(pgid, kill_grace, cgroup, interrupt_grace);
                }
            }
            Ok(Event::TimedOut) => {
                discard_output = true;
                timed_out = true;
                if !group_cleanup_started {
                    group_cleanup_started = true;
                    group_confirmed = kill_group(pgid, kill_grace, cgroup, interrupt_grace);
                }
            }
            Ok(Event::ChildExited(outcome)) => {
                // The direct child can exit after starting background work that
                // remains in its process group. Reaping the leader alone does
                // not terminate those descendants: they are reparented to pid
                // 1 and would survive the shim. Run the normal TERM -> KILL
                // sequence before reporting EXIT so every ordinary descendant
                // is gone when the BEAM sees the leader's status.
                if !group_cleanup_started {
                    group_confirmed = kill_group(pgid, kill_grace, cgroup, interrupt_grace);
                }
                break Some(outcome);
            }
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
    if !strict_output {
        if let Some(c) = credit {
            c.uncork();
        }
        if let Some(c) = stderr_credit {
            c.uncork();
        }
    }

    // Tear the contained subtree down after the child is reaped, whether it
    // was killed or exited on its own. `finish_cgroup` writes cgroup.kill (in
    // case a daemonizer escaped the process group and is still alive),
    // confirms the cgroup drained, and rmdirs it only after confirmation. Runs
    // even on beam_gone so a vanished BEAM does not leak the cgroup directory.
    let cgroup_confirmed = cgroup.is_none_or(|cg| finish_cgroup(cg, kill_grace));

    if beam_gone {
        if let Some(c) = credit {
            c.cancel();
        }
        if let Some(c) = stderr_credit {
            c.cancel();
        }
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
    let (output_truncated, disappeared_during_drain) = if strict_output {
        if discard_output {
            if let Some(c) = credit {
                c.cancel();
            }
            if let Some(c) = stderr_credit {
                c.cancel();
            }
            let _ = drain_pumps(pumps, pump_done);
            (true, false)
        } else {
            let (drained, cancelled, disappeared) =
                drain_pumps_strict(pumps, pump_done, rx, credit, stderr_credit);
            (!drained || cancelled, disappeared)
        }
    } else {
        (!drain_pumps(pumps, pump_done), false)
    };

    if disappeared_during_drain {
        return true;
    }

    let report = ExitReport {
        status: outcome.status,
        signal: outcome.signal,
        timed_out,
        confirmed: group_confirmed && cgroup_confirmed,
        contained: cgroup.is_some(),
        output_truncated,
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
/// group by daemonizing. Returns true only when the cgroup was observed empty;
/// an expired drain deadline leaves the directory in place and is reported as
/// an unconfirmed EXIT.
fn finish_cgroup(cg: &Cgroup, grace: Duration) -> bool {
    // SIGKILL the whole subtree. This is the backstop that reaches a
    // deliberate daemonizer the process-group kill could not.
    cg.kill();

    // Wait for the cgroup to drain, in addition to the process-group death
    // probe the kill path already did. Bound the wait so a wedged cgroup
    // cannot delay the EXIT report forever.
    let drained = wait_until(
        grace + Duration::from_millis(500),
        Duration::from_millis(20),
        || cg.drained(),
    );

    // Only remove a cgroup we observed empty. A live subtree is left for the
    // delegation owner and surfaced to the BEAM as unconfirmed.
    if drained {
        cg.remove();
    }

    drained
}

/// Waits (bounded by `OUTPUT_DRAIN_GRACE`) for `pumps` output pump
/// threads to report EOF on the child's pipes.
fn drain_pumps(pumps: usize, pump_done: &Receiver<()>) -> bool {
    let deadline = Instant::now() + OUTPUT_DRAIN_GRACE;
    for _ in 0..pumps {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        if pump_done.recv_timeout(deadline - now).is_err() {
            return false;
        }
    }
    true
}

/// In duplex pull mode, wait for explicit read credit even after child exit.
/// A slow consumer therefore leaves the pumps parked rather than uncorking
/// them into an unbounded BEAM mailbox. KILL or BEAM loss cancels the drain;
/// the EXIT report then marks the output as truncated.
fn drain_pumps_strict(
    pumps: usize,
    pump_done: &Receiver<()>,
    rx: &Receiver<Event>,
    stdout_credit: Option<&Credit>,
    stderr_credit: Option<&Credit>,
) -> (bool, bool, bool) {
    let mut completed = 0;

    while completed < pumps {
        while let Ok(event) = rx.try_recv() {
            match event {
                Event::Stdin(f) if f.tag == TAG_CREDIT => {
                    if let (Some(c), Some(n)) =
                        (stdout_credit, crate::credit::parse_grant(&f.payload))
                    {
                        c.grant(n);
                    }
                }
                Event::Stdin(f) if f.tag == TAG_STDERR_CREDIT => {
                    if let (Some(c), Some(n)) =
                        (stderr_credit, crate::credit::parse_grant(&f.payload))
                    {
                        c.grant(n);
                    }
                }
                Event::Stdin(f) if f.tag == TAG_KILL => {
                    if let Some(c) = stdout_credit {
                        c.cancel();
                    }
                    if let Some(c) = stderr_credit {
                        c.cancel();
                    }
                    return (drain_pumps(pumps - completed, pump_done), true, false);
                }
                Event::StdinClosed | Event::StdinError(_) => {
                    if let Some(c) = stdout_credit {
                        c.cancel();
                    }
                    if let Some(c) = stderr_credit {
                        c.cancel();
                    }
                    return (false, true, true);
                }
                _ => {}
            }
        }

        match pump_done.recv_timeout(Duration::from_millis(10)) {
            Ok(()) => completed += 1,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return (false, true, false),
        }
    }

    (true, false, false)
}

/// Optionally SIGINT the group with its own grace, then SIGTERM and SIGKILL
/// after `grace` if it
/// hasn't died. Confirms death via a signal-0 liveness probe before
/// returning. Returns false when the hard post-SIGKILL deadline expires while
/// the group still appears alive.
///
/// When a cgroup is present, `cgroup.kill` is written after the group kill so a
/// descendant that escaped the process group by daemonizing is SIGKILLed too.
/// This is layered on top of the process-group kill, never in place of it: the
/// SIGTERM/SIGKILL group sequence is unchanged; the cgroup write is added at
/// the end. The final drain-and-rmdir happens once, after the child is reaped,
/// in `finish_cgroup`.
fn kill_group(
    pgid: Pid,
    grace: Duration,
    cgroup: Option<&Cgroup>,
    interrupt_grace: Option<Duration>,
) -> bool {
    if let Some(grace) = interrupt_grace {
        let _ = process::kill_process_group(pgid, Signal::INT);
        if wait_until(grace, Duration::from_millis(10), || !group_alive(pgid)) {
            if let Some(cg) = cgroup {
                cg.kill();
            }
            return true;
        }
    }
    let _ = process::kill_process_group(pgid, Signal::TERM);

    let mut confirmed = wait_until(grace, Duration::from_millis(20), || !group_alive(pgid));

    if !confirmed {
        let _ = process::kill_process_group(pgid, Signal::KILL);
        confirmed = wait_until(
            Duration::from_millis(500),
            Duration::from_millis(10),
            || !group_alive(pgid),
        );
    }

    // Backstop for a deliberate daemonizer that left the process group: the
    // group kill above never reached it, but cgroup.kill SIGKILLs everything
    // still in the subtree at once.
    if let Some(cg) = cgroup {
        cg.kill();
    }

    confirmed
}

/// Polls `done` until it succeeds or a monotonic deadline expires. The
/// predicate is checked once even for a zero timeout, so an already-complete
/// teardown can be recognized without sleeping.
fn wait_until<F>(timeout: Duration, poll_interval: Duration, mut done: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if done() {
            return true;
        }

        let now = Instant::now();
        if now >= deadline {
            return false;
        }

        thread::sleep(poll_interval.min(deadline - now));
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
    let report = ErrorReport {
        reason,
        spawn_error: None,
    };
    if let Ok(payload) = serde_json::to_vec(&report) {
        let mut w = out.lock().unwrap();
        let _ = frame::write_frame(&mut *w, TAG_ERROR, &payload);
    }
}

fn write_spawn_error<W: Write>(out: &Arc<Mutex<W>>, failure: &SpawnFailure, structured: bool) {
    let reason = format!("spawn failed: {failure}");
    let report = ErrorReport {
        reason: &reason,
        spawn_error: structured.then(|| failure.report()),
    };
    if let Ok(payload) = serde_json::to_vec(&report) {
        let mut writer = out.lock().unwrap();
        let _ = frame::write_frame(&mut *writer, TAG_ERROR, &payload);
    }
}

#[cfg(test)]
mod tests {
    use super::{probe_says_alive, pump_eager, pump_gated, wait_until};
    use crate::credit::Credit;
    use crate::frame::{read_frame, TAG_STDOUT};
    use rustix::io::Errno;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    #[cfg(target_os = "linux")]
    #[test]
    fn actual_cgroup_placement_write_failure_is_reported_before_exec() {
        use super::{
            finish_spawn, install_child_setup, prepare_child_pty, write_spawn_error, ChildSetup,
        };
        use crate::cgroup::Placement;
        use crate::frame::TAG_ERROR;
        use crate::spawn_error::{channel, Stage};
        use std::fs::File;
        use std::process::{Command, Stdio};

        for pty in [false, true] {
            // A read-only fd makes the actual Placement::join_in_child write
            // fail with EBADF regardless of delegated cgroup availability.
            // No production configuration or environment fault switch exists.
            let procs = File::open("/dev/null").unwrap();
            let placement = Placement::from_test_fd(&procs);
            let (reader, reporter) = channel().unwrap();
            let mut cmd = Command::new("/bin/echo");
            cmd.arg("must-not-execute");
            let _master = if pty {
                Some(prepare_child_pty(&mut cmd).unwrap())
            } else {
                cmd.stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());
                None
            };
            install_child_setup(
                &mut cmd,
                ChildSetup {
                    cwd: None,
                    reporter: Some(reporter),
                    plan: None,
                    placement: Some(placement),
                    pty,
                    winsize: None,
                },
            );
            // Command returns Err only after its failed child has been reaped.
            let failure = finish_spawn(
                cmd,
                Some(reader),
                "/bin/echo",
                None,
                Some("/test/cgroup.procs"),
            )
            .unwrap_err();
            assert_eq!(failure.stage, Stage::Cgroup);
            assert_eq!(failure.error.raw_os_error(), Some(libc::EBADF));
            assert_eq!(failure.path.as_deref(), Some("/test/cgroup.procs"));
            let out = Arc::new(Mutex::new(Vec::new()));
            write_spawn_error(&out, &failure, true);
            let mut frames = Cursor::new(out.lock().unwrap().clone());
            let frame = read_frame(&mut frames).unwrap().unwrap();
            assert_eq!(frame.tag, TAG_ERROR);
            let report: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
            assert_eq!(report["spawn_error"]["stage"], "cgroup");
            assert_eq!(report["spawn_error"]["reason"], "ebadf");
            assert_eq!(report["spawn_error"]["errno"], libc::EBADF);
            assert_eq!(report["spawn_error"]["path"], "/test/cgroup.procs");
            assert!(read_frame(&mut frames).unwrap().is_none());
        }
    }

    #[test]
    fn identity_capability_probe_caches_success_and_failure() {
        use std::cell::Cell;
        use std::io;
        use std::sync::OnceLock;

        for available in [false, true] {
            let cache = OnceLock::new();
            let calls = Cell::new(0);
            let first = super::supported_features_with_probe(&cache, || {
                calls.set(calls.get() + 1);
                if available {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "metadata denied",
                    ))
                }
            });
            let again = super::supported_features_with_probe(&cache, || {
                calls.set(calls.get() + 1);
                // A different later result must not change this shim's offer.
                if available {
                    Err(io::Error::other("metadata became unavailable"))
                } else {
                    Ok(())
                }
            });
            assert_eq!(calls.get(), 1);
            assert_eq!(first, again);
            assert!(first.contains(&"bounded_run_v1"));
            assert_eq!(first.contains(&"spawn_identity_v1"), available);
        }
    }

    #[test]
    fn unavailable_identity_rejects_selection_without_reading_spawn() {
        use crate::frame::{write_frame, TAG_ERROR, TAG_HELLO};
        use std::io::{self, Read, Write};
        use std::sync::OnceLock;

        struct HelloOnly(Cursor<Vec<u8>>);
        impl Read for HelloOnly {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                assert!(
                    self.0.position() < self.0.get_ref().len() as u64,
                    "an unavailable requested capability must fail before reading SPAWN"
                );
                self.0.read(bytes)
            }
        }

        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut input = Vec::new();
        write_frame(
            &mut input,
            TAG_HELLO,
            br#"{"features":["spawn_identity_v1"]}"#,
        )
        .unwrap();
        let output = Arc::new(Mutex::new(Vec::new()));
        let cache = OnceLock::new();
        super::run_with_capabilities(
            HelloOnly(Cursor::new(input)),
            Capture(Arc::clone(&output)),
            || {
                super::supported_features_with_probe(&cache, || {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "procfs unavailable",
                    ))
                })
            },
        )
        .unwrap();

        let mut frames = Cursor::new(output.lock().unwrap().clone());
        let error = read_frame(&mut frames).unwrap().unwrap();
        assert_eq!(error.tag, TAG_ERROR);
        let error: serde_json::Value = serde_json::from_slice(&error.payload).unwrap();
        assert_eq!(error["reason"], "unsupported feature: spawn_identity_v1");
        assert!(read_frame(&mut frames).unwrap().is_none());
    }

    #[test]
    fn selected_features_require_explicit_supported_selection() {
        let supported = ["bounded_run_v1", "spawn_identity_v1"];
        for payload in [
            b"".as_slice(),
            br#"{"features":[]}"#,
            br#"{"features":["bounded_run_v1"]}"#,
        ] {
            assert_eq!(
                super::select_features(payload, &supported).unwrap(),
                super::SelectedFeatures::default()
            );
        }
        for payload in [
            br#"{"features":["spawn_identity_v1"]}"#.as_slice(),
            br#"{"features":["bounded_run_v1","spawn_identity_v1"]}"#,
        ] {
            assert_eq!(
                super::select_features(payload, &supported).unwrap(),
                super::SelectedFeatures {
                    spawn_identity: true,
                    structured_spawn_errors: false
                }
            );
        }
        assert!(super::select_features(br#"{"features":["tracked_v1"]}"#, &supported).is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn failed_identity_delivery_kills_and_reaps_the_child() {
        use crate::frame::{write_frame, TAG_CAPABILITIES, TAG_HELLO, TAG_SPAWN, TAG_SPAWNED};
        use std::io::{self, Write};

        struct FailIdentity {
            bytes: Arc<Mutex<Vec<u8>>>,
            flushes: usize,
        }

        impl Write for FailIdentity {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.bytes.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                self.flushes += 1;
                if self.flushes == 2 {
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "identity delivery failed",
                    ))
                } else {
                    Ok(())
                }
            }
        }

        let mut input = Vec::new();
        write_frame(
            &mut input,
            TAG_HELLO,
            br#"{"features":["spawn_identity_v1"]}"#,
        )
        .unwrap();
        write_frame(
            &mut input,
            TAG_SPAWN,
            br#"{"argv":["sleep","30"],"kill_grace_ms":0}"#,
        )
        .unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = FailIdentity {
            bytes: Arc::clone(&bytes),
            flushes: 0,
        };
        assert!(super::run(Cursor::new(input), writer).is_err());
        let mut frames = Cursor::new(bytes.lock().unwrap().clone());
        assert_eq!(
            read_frame(&mut frames).unwrap().unwrap().tag,
            TAG_CAPABILITIES
        );
        let identity = read_frame(&mut frames).unwrap().unwrap();
        assert_eq!(identity.tag, TAG_SPAWNED);
        let identity: serde_json::Value = serde_json::from_slice(&identity.payload).unwrap();
        let pid = rustix::process::Pid::from_raw(identity["pid"].as_i64().unwrap() as i32).unwrap();
        assert_eq!(rustix::process::test_kill_process(pid), Err(Errno::SRCH));
        assert_eq!(
            rustix::process::test_kill_process_group(pid),
            Err(Errno::SRCH)
        );
    }

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

    #[test]
    fn wait_until_reports_success() {
        let mut attempts = 0;
        let completed = wait_until(Duration::from_millis(100), Duration::ZERO, || {
            attempts += 1;
            attempts == 3
        });
        assert!(completed);
        assert_eq!(attempts, 3);
    }

    #[test]
    fn wait_until_reports_deadline_expiry() {
        assert!(!wait_until(Duration::ZERO, Duration::ZERO, || false));
    }
}
