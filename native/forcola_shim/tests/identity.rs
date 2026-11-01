//! Negotiated spawn identity, independently checked against OS observations.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

const SPAWN: u8 = 0x01;
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
    stdout: ChildStdout,
}

impl Shim {
    fn new() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_forcola_shim"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Self {
            stdin: child.stdin.take(),
            stdout: child.stdout.take().unwrap(),
            child,
        }
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

    fn recv(&mut self) -> Option<(u8, Vec<u8>)> {
        let mut len = [0; 4];
        if self.stdout.read_exact(&mut len).is_err() {
            return None;
        }
        let mut body = vec![0; u32::from_be_bytes(len) as usize];
        self.stdout.read_exact(&mut body).unwrap();
        Some((body[0], body[1..].to_vec()))
    }

    fn negotiate(&mut self, payload: &[u8]) {
        self.send(HELLO, payload);
        let (tag, payload) = self.recv().unwrap();
        assert_eq!(tag, CAPABILITIES);
        let report: Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(report["protocol"], 1);
        let features = report["features"].as_array().unwrap();
        assert!(features.contains(&json!("bounded_run_v1")));
        assert!(features.contains(&json!("spawn_identity_v1")));
    }

    fn select_identity(&mut self) {
        self.negotiate(br#"{"features":["spawn_identity_v1"]}"#);
    }

    fn spawn(&mut self, argv: &[&str], pty: bool) {
        self.send(
            SPAWN,
            &serde_json::to_vec(&json!({
                "argv": argv,
                "pty": pty,
                "timeout_ms": 5_000,
                "kill_grace_ms": 0
            }))
            .unwrap(),
        );
    }

    fn spawned(&mut self) -> Value {
        let (tag, payload) = self.recv().unwrap();
        assert_eq!(tag, SPAWNED, "{}", String::from_utf8_lossy(&payload));
        serde_json::from_slice(&payload).unwrap()
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        // EOF lets the shim clean up its process group even when an assertion
        // fails. The short timeout is a second backstop for each test child.
        self.stdin.take();
        let _ = self.child.wait();
    }
}

#[test]
fn identity_precedes_pipe_and_pty_output_and_matches_ps() {
    for pty in [false, true] {
        let mut shim = Shim::new();
        shim.select_identity();
        shim.spawn(&["sh", "-c", "echo first; echo error >&2; sleep 30"], pty);
        let identity = shim.spawned();
        let pid = identity["pid"].as_u64().unwrap();
        assert_eq!(identity["pgid"], pid);

        let group = ps(pid, "pgid=");
        assert_eq!(group.trim().parse::<u64>().unwrap(), pid);
        assert_eq!(ps(pid, "lstart=").trim(), format_start_time(&identity));
        check_native_start_time(pid, &identity["start_time"]);

        let mut output = Vec::new();
        while !String::from_utf8_lossy(&output).contains("first")
            || !String::from_utf8_lossy(&output).contains("error")
        {
            let (tag, payload) = shim.recv().expect("missing output");
            assert!(matches!(tag, STDOUT | STDERR), "unexpected frame {tag:#x}");
            output.extend(payload);
        }
        shim.send(KILL, b"");
        loop {
            let (tag, payload) = shim.recv().expect("missing EXIT");
            match tag {
                STDOUT | STDERR => output.extend(payload),
                EXIT => break,
                _ => panic!("unexpected frame {tag:#x}"),
            }
        }
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("first"));
        assert!(output.contains("error"));
    }
}

#[test]
fn identity_survives_immediate_child_exit() {
    for _ in 0..20 {
        let mut shim = Shim::new();
        shim.select_identity();
        shim.spawn(&["true"], false);
        let identity = shim.spawned();
        assert_eq!(identity["pid"], identity["pgid"]);
        let (tag, payload) = shim.recv().unwrap();
        assert_eq!(tag, EXIT);
        assert_eq!(
            serde_json::from_slice::<Value>(&payload).unwrap()["status"],
            0
        );
    }
}

#[test]
fn spawn_failure_never_reports_identity() {
    for pty in [false, true] {
        let mut shim = Shim::new();
        shim.select_identity();
        shim.spawn(&["/no/such/forcola-identity-command"], pty);
        assert_eq!(shim.recv().unwrap().0, ERROR);
        assert!(shim.recv().is_none());
    }
}

#[test]
fn unselected_sessions_keep_the_original_frames() {
    for hello in [
        None,
        Some(b"".as_slice()),
        Some(br#"{"features":[]}"#.as_slice()),
        Some(br#"{"features":["bounded_run_v1"]}"#.as_slice()),
    ] {
        let mut shim = Shim::new();
        if let Some(payload) = hello {
            shim.negotiate(payload);
        }
        shim.spawn(&["echo", "legacy"], false);
        assert_eq!(shim.recv().unwrap().0, STDOUT);
        assert_eq!(shim.recv().unwrap().0, EXIT);
        assert!(shim.recv().is_none());
    }
}

#[test]
fn invalid_feature_selection_fails_before_spawn() {
    for hello in [
        b"bad json".as_slice(),
        br#"{"features":true}"#,
        br#"{"features":[123]}"#,
        br#"{"features":["not_supported"]}"#,
    ] {
        let mut shim = Shim::new();
        shim.send(HELLO, hello);
        assert_eq!(shim.recv().unwrap().0, ERROR);
        assert!(shim.recv().is_none());
    }
}

fn ps(pid: u64, field: &str) -> String {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", field])
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

fn format_start_time(identity: &Value) -> String {
    let epoch = start_seconds(&identity["start_time"]) as libc::time_t;
    let mut time = std::mem::MaybeUninit::<libc::tm>::uninit();
    let mut formatted = [0u8; 64];
    // SAFETY: both C functions receive valid, correctly sized buffers;
    // gmtime_r initializes tm before strftime reads it.
    let count = unsafe {
        assert!(!libc::gmtime_r(&epoch, time.as_mut_ptr()).is_null());
        libc::strftime(
            formatted.as_mut_ptr().cast(),
            formatted.len(),
            c"%a %b %e %T %Y".as_ptr(),
            time.as_ptr(),
        )
    };
    assert!(count > 0);
    String::from_utf8(formatted[..count].to_vec()).unwrap()
}

#[cfg(target_os = "linux")]
fn start_seconds(start: &Value) -> u64 {
    let stat = std::fs::read_to_string("/proc/stat").unwrap();
    let boot = stat
        .lines()
        .find_map(|line| line.strip_prefix("btime "))
        .unwrap()
        .parse::<u64>()
        .unwrap();
    // SAFETY: sysconf reads a process-independent numeric system setting.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    assert!(hz > 0);
    boot + start["ticks"].as_u64().unwrap() / hz as u64
}

#[cfg(target_os = "macos")]
fn start_seconds(start: &Value) -> u64 {
    start["seconds"].as_u64().unwrap()
}

#[cfg(target_os = "linux")]
fn check_native_start_time(pid: u64, start: &Value) {
    assert_eq!(start["kind"], "linux");
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let ticks = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(start["ticks"], ticks);
    assert_eq!(
        start["boot_id"],
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap()
            .trim()
    );
}

#[cfg(target_os = "macos")]
fn check_native_start_time(_pid: u64, start: &Value) {
    assert_eq!(start["kind"], "macos");
    assert!(start["microseconds"].as_u64().unwrap() < 1_000_000);
}
