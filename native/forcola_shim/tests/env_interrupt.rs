//! Environment removal and cancellation through the real wire protocol.

use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const SPAWN: u8 = 0x01;
const EOF: u8 = 0x03;
const KILL: u8 = 0x04;
const HELLO: u8 = 0x07;
const STDOUT: u8 = 0x11;
const STDERR: u8 = 0x12;
const EXIT: u8 = 0x13;
const ERROR: u8 = 0x14;
const CAPABILITIES: u8 = 0x15;
const SPAWNED: u8 = 0x16;

struct Shim {
    child: Child,
    stdin: Option<ChildStdin>,
    frames: Receiver<(u8, Vec<u8>)>,
    reader: Option<JoinHandle<()>>,
    pgid: Option<i32>,
}

impl Shim {
    fn new(features: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_forcola_shim"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            // Only this shim inherits the fixtures; other parallel tests do not.
            .env("FORCOLA_ENV_REMOVE", "inherited")
            .env("FORCOLA_ENV_KEEP", "inherited")
            .env("FORCOLA_ENV_SET", "old")
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let (tx, frames) = mpsc::channel();
        let reader = thread::spawn(move || loop {
            let mut size = [0; 4];
            if stdout.read_exact(&mut size).is_err() {
                break;
            }
            let mut frame = vec![0; u32::from_be_bytes(size) as usize];
            if stdout.read_exact(&mut frame).is_err()
                || tx.send((frame[0], frame[1..].to_vec())).is_err()
            {
                break;
            }
        });
        let mut shim = Self {
            stdin: child.stdin.take(),
            child,
            frames,
            reader: Some(reader),
            pgid: None,
        };
        shim.send(
            HELLO,
            &serde_json::to_vec(&json!({"features":features})).unwrap(),
        );
        let (tag, bytes) = shim.recv().unwrap();
        assert_eq!(tag, CAPABILITIES);
        let report: Value = serde_json::from_slice(&bytes).unwrap();
        for feature in features {
            assert!(report["features"]
                .as_array()
                .unwrap()
                .contains(&json!(feature)));
        }
        shim
    }

    fn send(&mut self, tag: u8, payload: &[u8]) {
        let stdin = self.stdin.as_mut().unwrap();
        stdin
            .write_all(&((payload.len() + 1) as u32).to_be_bytes())
            .unwrap();
        stdin.write_all(&[tag]).unwrap();
        stdin.write_all(payload).unwrap();
        stdin.flush().unwrap();
    }

    fn spawn(&mut self, request: Value) {
        self.send(SPAWN, &serde_json::to_vec(&request).unwrap());
    }

    fn recv(&mut self) -> Option<(u8, Vec<u8>)> {
        match self.frames.recv_timeout(Duration::from_secs(10)) {
            Ok((SPAWNED, bytes)) => {
                let report: Value = serde_json::from_slice(&bytes).unwrap();
                self.pgid = Some(report["pgid"].as_i64().unwrap() as i32);
                Some((SPAWNED, bytes))
            }
            Ok(frame) => Some(frame),
            Err(mpsc::RecvTimeoutError::Disconnected) => None,
            Err(mpsc::RecvTimeoutError::Timeout) => panic!("shim response timed out"),
        }
    }

    fn finish(&mut self) -> (Vec<u8>, Option<Value>) {
        let mut stdout = Vec::new();
        let mut exit = None;
        while let Some((tag, bytes)) = self.recv() {
            match tag {
                STDOUT => stdout.extend(bytes),
                STDERR | SPAWNED => {}
                EXIT => {
                    assert!(exit.is_none(), "duplicate EXIT");
                    exit = Some(serde_json::from_slice(&bytes).unwrap());
                }
                ERROR => panic!("unexpected ERROR: {}", String::from_utf8_lossy(&bytes)),
                _ => panic!("unexpected tag {tag:#x}"),
            }
        }
        assert!(self.child.wait().unwrap().success());
        self.pgid = None;
        (stdout, exit)
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        self.stdin.take();
        // Failure cleanup must not leave the processes this test is measuring.
        if self.child.try_wait().ok().flatten().is_none() {
            if let Some(pgid) = self.pgid {
                unsafe { libc::kill(-pgid, libc::SIGKILL) };
            }
            let deadline = Instant::now() + Duration::from_secs(2);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            if self.child.try_wait().ok().flatten().is_none() {
                let _ = self.child.kill();
            }
        }
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "forcola-env-interrupt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::write(path.join("child.py"), SIGNAL_CHILD).unwrap();
        Self(path)
    }

    fn ready(&self, label: &str) -> i32 {
        let path = self.0.join(format!("{label}.ready"));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(bytes) = fs::read(&path) {
                if let Ok(pid) = serde_json::from_slice(&bytes) {
                    return pid;
                }
            }
            assert!(
                Instant::now() < deadline,
                "child {label} never became ready"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn signals(&self, label: &str) -> Vec<Value> {
        fs::read_to_string(self.0.join(format!("{label}.signals")))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

const SIGNAL_CHILD: &str = r#"
import json, os, signal, sys, time
directory, label, mode = sys.argv[1:]
def record(name):
    with open(os.path.join(directory, label + '.signals'), 'a') as stream:
        stream.write(json.dumps({'signal': name, 'time': time.monotonic()}) + '\n')
def interrupt(*_):
    record('INT')
    if mode == 'cooperate':
        sys.exit(42)
def terminate(*_):
    record('TERM')
signal.signal(signal.SIGINT, interrupt)
signal.signal(signal.SIGTERM, terminate)
with open(os.path.join(directory, label + '.ready'), 'w') as stream:
    json.dump(os.getpid(), stream)
while True:
    signal.pause()
"#;

fn assert_gone(pid: i32) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if unsafe { libc::kill(pid, 0) } < 0
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return;
        }
        if Instant::now() >= deadline {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            panic!("child {pid} survived cleanup");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn env_null_removes_inherited_variables_in_pipe_and_pty_modes() {
    for mode in ["inherit", "replace"] {
        for pty in [false, true] {
            let mut shim = Shim::new(&["env_unset_v1", "bounded_run_v1"]);
            shim.spawn(json!({
                "argv":["/usr/bin/env"], "env_mode":mode, "pty":pty,
                "env":{
                    "FORCOLA_ENV_REMOVE":null, "FORCOLA_ENV_ABSENT":null,
                    "FORCOLA_ENV_SET":"new", "FORCOLA_ENV_EMPTY":"", "FORCOLA_ENV_LITERAL":"nil"
                },
                "timeout_ms":5_000, "kill_grace_ms":100
            }));
            let (bytes, exit) = shim.finish();
            let output = String::from_utf8(bytes).unwrap();
            let mut entries: Vec<_> = output.lines().collect();
            entries.sort_unstable();
            assert!(!entries
                .iter()
                .any(|entry| entry.starts_with("FORCOLA_ENV_REMOVE=")));
            assert!(!entries
                .iter()
                .any(|entry| entry.starts_with("FORCOLA_ENV_ABSENT=")));
            assert!(entries.contains(&"FORCOLA_ENV_SET=new"));
            assert!(entries.contains(&"FORCOLA_ENV_EMPTY="));
            assert!(entries.contains(&"FORCOLA_ENV_LITERAL=nil"));
            if mode == "replace" {
                assert_eq!(
                    entries,
                    [
                        "FORCOLA_ENV_EMPTY=",
                        "FORCOLA_ENV_LITERAL=nil",
                        "FORCOLA_ENV_SET=new"
                    ]
                );
            } else {
                assert!(entries.contains(&"FORCOLA_ENV_KEEP=inherited"));
            }
            assert_eq!(exit.unwrap()["status"], 0);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Trigger {
    Kill,
    OwnerEof,
    Timeout,
}

fn signal_case(trigger: Trigger, pty: bool, cooperate: bool) {
    let dir = TestDir::new();
    let mut shim = Shim::new(&["bounded_run_v1", "spawn_identity_v1"]);
    shim.spawn(json!({
        "argv":["python3",dir.0.join("child.py"),dir.0,"main",if cooperate {"cooperate"} else {"stubborn"}],
        "pty":pty, "interrupt_signal":"int", "interrupt_grace_ms":200,
        "kill_grace_ms":200, "timeout_ms":if trigger == Trigger::Timeout {1_000} else {5_000}
    }));
    assert_eq!(shim.recv().unwrap().0, SPAWNED);
    let pid = dir.ready("main");
    let started = Instant::now();
    match trigger {
        Trigger::Kill => shim.send(KILL, &[]),
        Trigger::OwnerEof => drop(shim.stdin.take()),
        Trigger::Timeout => {}
    }
    let (_, exit) = shim.finish();
    let elapsed = started.elapsed();
    let signals = dir.signals("main");
    assert_eq!(signals[0]["signal"], "INT");
    if cooperate {
        assert_eq!(signals.len(), 1, "cooperative INT must not reach TERM");
    } else {
        assert_eq!(signals.len(), 2);
        assert_eq!(signals[1]["signal"], "TERM");
        let interrupt_elapsed =
            signals[1]["time"].as_f64().unwrap() - signals[0]["time"].as_f64().unwrap();
        assert!(
            interrupt_elapsed >= 0.18,
            "INT grace was skipped: {interrupt_elapsed}"
        );
        assert!(
            elapsed >= Duration::from_millis(380),
            "graces were skipped: {elapsed:?}"
        );
    }
    if trigger == Trigger::OwnerEof {
        assert!(
            exit.is_none(),
            "transport EOF must not emit EXIT to a dead owner"
        );
    } else {
        let exit = exit.unwrap();
        assert_eq!(exit["timed_out"], trigger == Trigger::Timeout);
        assert_eq!(exit["confirmed"], true);
        if cooperate {
            assert_eq!(exit["status"], 42);
        } else {
            assert_eq!(exit["signal"], libc::SIGKILL);
        }
    }
    assert_gone(pid);
}

#[test]
fn kill_uses_interrupt_then_term_then_kill_in_pipe_and_pty_modes() {
    for pty in [false, true] {
        signal_case(Trigger::Kill, pty, false);
    }
}

#[test]
fn owner_eof_uses_interrupt_then_term_then_kill_in_pipe_and_pty_modes() {
    for pty in [false, true] {
        signal_case(Trigger::OwnerEof, pty, false);
    }
}

#[test]
fn timeout_uses_interrupt_then_term_then_kill_in_pipe_and_pty_modes() {
    for pty in [false, true] {
        signal_case(Trigger::Timeout, pty, false);
    }
}

#[test]
fn cooperative_interrupt_stops_before_term_for_every_trigger() {
    for trigger in [Trigger::Kill, Trigger::OwnerEof, Trigger::Timeout] {
        for pty in [false, true] {
            signal_case(trigger, pty, true);
        }
    }
}

#[test]
fn protocol_eof_only_closes_child_stdin_with_interrupt_configured() {
    let mut shim = Shim::new(&["bounded_run_v1"]);
    shim.spawn(json!({
        "argv":["/bin/cat"], "interrupt_signal":"int", "interrupt_grace_ms":200,
        "kill_grace_ms":100, "timeout_ms":5_000
    }));
    shim.send(0x02, b"read-until-eof\n");
    shim.send(EOF, &[]);
    let (output, exit) = shim.finish();
    assert_eq!(output, b"read-until-eof\n");
    assert_eq!(exit.unwrap()["status"], 0);
}

#[test]
fn interrupt_reaches_both_children_in_a_shell_pipeline() {
    for pty in [false, true] {
        let dir = TestDir::new();
        let mut shim = Shim::new(&["bounded_run_v1", "spawn_identity_v1"]);
        shim.spawn(json!({
            "argv":["/bin/sh","-c","python3 \"$1\" \"$2\" left cooperate | python3 \"$1\" \"$2\" right cooperate","forcola-pipeline",dir.0.join("child.py"),dir.0],
            "pty":pty, "interrupt_signal":"int", "interrupt_grace_ms":500,
            "kill_grace_ms":100, "timeout_ms":5_000
        }));
        assert_eq!(shim.recv().unwrap().0, SPAWNED);
        let left = dir.ready("left");
        let right = dir.ready("right");
        shim.send(KILL, &[]);
        let (_, exit) = shim.finish();
        assert_eq!(exit.unwrap()["confirmed"], true);
        for (label, pid) in [("left", left), ("right", right)] {
            let signals = dir.signals(label);
            assert_eq!(signals.len(), 1);
            assert_eq!(signals[0]["signal"], "INT");
            assert_gone(pid);
        }
    }
}
