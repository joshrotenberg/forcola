//! End-to-end tests: spawn the real `forcola_shim` binary and drive it
//! over its stdin/stdout pipes exactly as the BEAM would.

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const TAG_SPAWN: u8 = 0x01;
const TAG_KILL: u8 = 0x04;

const TAG_STDOUT: u8 = 0x11;
const TAG_STDERR: u8 = 0x12;
const TAG_EXIT: u8 = 0x13;
const TAG_ERROR: u8 = 0x14;

struct Frame {
    tag: u8,
    payload: Vec<u8>,
}

fn write_frame<W: Write>(w: &mut W, tag: u8, payload: &[u8]) {
    let len = (payload.len() + 1) as u32;
    w.write_all(&len.to_be_bytes()).unwrap();
    w.write_all(&[tag]).unwrap();
    w.write_all(payload).unwrap();
    w.flush().unwrap();
}

fn read_frame<R: Read>(r: &mut R) -> Option<Frame> {
    let mut len_buf = [0u8; 4];
    if r.read_exact(&mut len_buf).is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).ok()?;
    Some(Frame {
        tag: body[0],
        payload: body[1..].to_vec(),
    })
}

fn shim_binary() -> &'static str {
    env!("CARGO_BIN_EXE_forcola_shim")
}

fn start_shim() -> Child {
    Command::new(shim_binary())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start forcola_shim")
}

fn spawn_payload(
    argv: &[&str],
    timeout_ms: Option<u64>,
    kill_grace_ms: Option<u64>,
    merge_stderr: bool,
) -> Vec<u8> {
    let mut obj = serde_json::json!({
        "argv": argv,
        "merge_stderr": merge_stderr,
    });
    if let Some(t) = timeout_ms {
        obj["timeout_ms"] = serde_json::json!(t);
    }
    if let Some(g) = kill_grace_ms {
        obj["kill_grace_ms"] = serde_json::json!(g);
    }
    serde_json::to_vec(&obj).unwrap()
}

/// Reads frames until an EXIT or ERROR frame is seen, collecting stdout
/// bytes along the way. Returns (stdout_bytes, exit_frame).
fn drain_until_exit<R: Read>(r: &mut R) -> (Vec<u8>, Option<Frame>) {
    let mut stdout_bytes = Vec::new();
    loop {
        match read_frame(r) {
            Some(f) if f.tag == TAG_STDOUT => stdout_bytes.extend_from_slice(&f.payload),
            Some(f) if f.tag == TAG_STDERR => {}
            Some(f) if f.tag == TAG_EXIT || f.tag == TAG_ERROR => return (stdout_bytes, Some(f)),
            Some(_) => {}
            None => return (stdout_bytes, None),
        }
    }
}

#[test]
fn normal_exit_reports_status() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["sh", "-c", "exit 7"], None, None, false),
    );

    let (_out, exit_frame) = drain_until_exit(&mut stdout);
    let exit_frame = exit_frame.expect("expected an EXIT frame");
    assert_eq!(exit_frame.tag, TAG_EXIT);

    let report: serde_json::Value = serde_json::from_slice(&exit_frame.payload).unwrap();
    assert_eq!(report["status"], 7);
    assert_eq!(report["timed_out"], false);

    let _ = child.wait();
}

#[test]
fn signal_exit_reports_signal() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    // `kill -TERM $$` sends SIGTERM to the shell itself.
    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["sh", "-c", "kill -TERM $$"], None, None, false),
    );

    let (_out, exit_frame) = drain_until_exit(&mut stdout);
    let exit_frame = exit_frame.expect("expected an EXIT frame");
    let report: serde_json::Value = serde_json::from_slice(&exit_frame.payload).unwrap();

    assert_eq!(report["signal"], 15); // SIGTERM
    assert!(report.get("status").is_none());

    let _ = child.wait();
}

#[test]
fn stdout_is_forwarded() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["echo", "hello from child"], None, None, false),
    );

    let (out, _exit) = drain_until_exit(&mut stdout);
    assert_eq!(String::from_utf8_lossy(&out).trim(), "hello from child");

    let _ = child.wait();
}

#[test]
fn output_is_drained_before_exit_frame() {
    // A fast-exiting child races its own output pump: the waiter can reap
    // the child before the pump has read the pipe. The shim must drain
    // output before writing EXIT, or the frames it does write arrive
    // after the terminator and are dropped by the consumer. Repeat to
    // give the race room to show up.
    for _ in 0..20 {
        let mut child = start_shim();
        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();

        write_frame(
            &mut stdin,
            TAG_SPAWN,
            &spawn_payload(&["sh", "-c", "echo raced"], None, None, false),
        );

        let (out, exit_frame) = drain_until_exit(&mut stdout);
        assert!(exit_frame.is_some(), "expected an EXIT frame");
        assert_eq!(
            String::from_utf8_lossy(&out).trim(),
            "raced",
            "stdout written just before exit was lost to the EXIT frame race"
        );

        let _ = child.wait();
    }
}

#[test]
fn group_kill_reaches_grandchild() {
    // The direct child forks a grandchild ("sleep 60 &") and then sleeps
    // forever itself. A correct group kill takes down both; a kill that
    // only signals the direct child leaves the grandchild sleeping.
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    let script = "sleep 60 & echo GRANDCHILD_PID=$!; sleep 60";
    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["sh", "-c", script], None, Some(200), false),
    );

    // Read the grandchild pid line off stdout before killing.
    let mut stdout_bytes = Vec::new();
    let mut grandchild_pid: Option<i32> = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while grandchild_pid.is_none() && Instant::now() < deadline {
        match read_frame(&mut stdout) {
            Some(f) if f.tag == TAG_STDOUT => {
                stdout_bytes.extend_from_slice(&f.payload);
                let text = String::from_utf8_lossy(&stdout_bytes);
                if let Some(line) = text.lines().find(|l| l.starts_with("GRANDCHILD_PID=")) {
                    grandchild_pid = line
                        .trim_start_matches("GRANDCHILD_PID=")
                        .trim()
                        .parse()
                        .ok();
                }
            }
            Some(_) => {}
            None => break,
        }
    }
    let grandchild_pid = grandchild_pid.expect("did not observe grandchild pid");

    // Kill the whole group now.
    write_frame(&mut stdin, TAG_KILL, &[]);

    let (_out, exit_frame) = drain_until_exit(&mut stdout);
    assert!(exit_frame.is_some(), "expected an EXIT frame after KILL");

    // Confirm the grandchild is actually dead: signal 0 to its pid should
    // fail with ESRCH once it's reaped (or at least once it's no longer
    // signalable).
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut alive = true;
    while Instant::now() < deadline {
        alive = unsafe { libc_kill(grandchild_pid, 0) == 0 };
        if !alive {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!alive, "grandchild pid {grandchild_pid} is still alive");

    let _ = child.wait();
}

// Minimal signal-0 probe without pulling in a dependency just for the
// test suite: call the libc `kill` symbol directly via `extern "C"`.
extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}
unsafe fn libc_kill(pid: i32, sig: i32) -> i32 {
    kill(pid, sig)
}

#[test]
fn sigterm_ignoring_child_is_sigkilled_after_grace() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    // trap SIGTERM and ignore it; only SIGKILL can end this one. The
    // child prints READY after installing the trap so the test can wait
    // for the handler to actually be in place; a fixed sleep races shell
    // startup and loses often enough to flake (issue #19). Processes the
    // shell forks after the trap inherit SIG_IGN, so `sleep` ignores
    // SIGTERM too.
    let script = "trap '' TERM; echo READY; sleep 60";
    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["sh", "-c", script], None, Some(300), false),
    );

    // Wait for the readiness marker before starting the kill sequence.
    let mut stdout_bytes = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !String::from_utf8_lossy(&stdout_bytes).contains("READY") {
        assert!(
            Instant::now() < deadline,
            "child never signaled readiness after installing the trap"
        );
        match read_frame(&mut stdout) {
            Some(f) if f.tag == TAG_STDOUT => stdout_bytes.extend_from_slice(&f.payload),
            Some(_) => {}
            None => panic!("shim stdout closed before the readiness marker"),
        }
    }

    let start = Instant::now();
    write_frame(&mut stdin, TAG_KILL, &[]);

    let (_out, exit_frame) = drain_until_exit(&mut stdout);
    let elapsed = start.elapsed();

    assert!(exit_frame.is_some(), "expected an EXIT frame after KILL");
    // Must have waited at least the grace period before the SIGKILL could
    // have landed.
    assert!(
        elapsed >= Duration::from_millis(280),
        "child died before the grace period elapsed: {elapsed:?}"
    );
    // But not indefinitely -- SIGKILL should have landed well within a
    // few seconds of the grace period expiring.
    assert!(
        elapsed < Duration::from_secs(5),
        "child took too long to die: {elapsed:?}"
    );

    let _ = child.wait();
}

#[test]
fn stdin_eof_kills_the_group() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    let script = "sleep 60 & echo GRANDCHILD_PID=$!; sleep 60";
    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["sh", "-c", script], None, Some(200), false),
    );

    let mut stdout_bytes = Vec::new();
    let mut grandchild_pid: Option<i32> = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while grandchild_pid.is_none() && Instant::now() < deadline {
        match read_frame(&mut stdout) {
            Some(f) if f.tag == TAG_STDOUT => {
                stdout_bytes.extend_from_slice(&f.payload);
                let text = String::from_utf8_lossy(&stdout_bytes);
                if let Some(line) = text.lines().find(|l| l.starts_with("GRANDCHILD_PID=")) {
                    grandchild_pid = line
                        .trim_start_matches("GRANDCHILD_PID=")
                        .trim()
                        .parse()
                        .ok();
                }
            }
            Some(_) => {}
            None => break,
        }
    }
    let grandchild_pid = grandchild_pid.expect("did not observe grandchild pid");

    // Simulate BEAM death: drop stdin, closing the pipe with no EOF
    // frame and no KILL frame -- just a raw close.
    drop(stdin);

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut alive = true;
    while Instant::now() < deadline {
        alive = unsafe { libc_kill(grandchild_pid, 0) == 0 };
        if !alive {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!alive, "grandchild pid {grandchild_pid} survived stdin EOF");

    let _ = child.wait();
}

#[test]
fn timeout_kills_group_and_reports_timed_out() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["sleep", "60"], Some(200), Some(200), false),
    );

    let start = Instant::now();
    let (_out, exit_frame) = drain_until_exit(&mut stdout);
    let elapsed = start.elapsed();

    let exit_frame = exit_frame.expect("expected an EXIT frame");
    let report: serde_json::Value = serde_json::from_slice(&exit_frame.payload).unwrap();
    assert_eq!(report["timed_out"], true);
    assert!(
        elapsed < Duration::from_secs(5),
        "timeout took too long to take effect: {elapsed:?}"
    );

    let _ = child.wait();
}

#[test]
fn spawn_failure_reports_error_frame() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(&["/no/such/binary/forcola-test"], None, None, false),
    );

    let frame = read_frame(&mut stdout).expect("expected a frame");
    assert_eq!(frame.tag, TAG_ERROR);

    let _ = child.wait();
}

#[test]
fn merge_stderr_routes_into_stdout_stream() {
    let mut child = start_shim();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    write_frame(
        &mut stdin,
        TAG_SPAWN,
        &spawn_payload(
            &["sh", "-c", "echo to-stderr 1>&2"],
            None,
            None,
            true, // merge_stderr
        ),
    );

    let (out, _exit) = drain_until_exit(&mut stdout);
    assert_eq!(String::from_utf8_lossy(&out).trim(), "to-stderr");

    let _ = child.wait();
}
