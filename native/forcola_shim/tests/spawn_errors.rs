//! Structured errors from real child-side failures, with legacy compatibility.

use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

const SPAWN: u8 = 1;
const HELLO: u8 = 7;
const STDOUT: u8 = 0x11;
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
    fn new(features: Option<&[&str]>) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_forcola_shim"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut shim = Self {
            stdin: child.stdin.take(),
            stdout: child.stdout.take().unwrap(),
            child,
        };
        if let Some(features) = features {
            let hello = if features.is_empty() {
                Vec::new()
            } else {
                serde_json::to_vec(&json!({"features": features})).unwrap()
            };
            shim.send(HELLO, &hello);
            let (tag, payload) = shim.recv().unwrap();
            assert_eq!(tag, CAPABILITIES);
            let report: Value = serde_json::from_slice(&payload).unwrap();
            assert!(report["features"]
                .as_array()
                .unwrap()
                .contains(&json!("structured_spawn_errors_v1")));
        }
        shim
    }

    fn send(&mut self, tag: u8, payload: &[u8]) {
        let input = self.stdin.as_mut().unwrap();
        input
            .write_all(&((payload.len() + 1) as u32).to_be_bytes())
            .unwrap();
        input.write_all(&[tag]).unwrap();
        input.write_all(payload).unwrap();
        input.flush().unwrap();
    }

    fn spawn(&mut self, mut request: Value) {
        request["timeout_ms"] = json!(5_000);
        request["kill_grace_ms"] = json!(0);
        self.send(SPAWN, &serde_json::to_vec(&request).unwrap());
    }

    fn recv(&mut self) -> Option<(u8, Vec<u8>)> {
        let mut size = [0; 4];
        if self.stdout.read_exact(&mut size).is_err() {
            return None;
        }
        let mut frame = vec![0; u32::from_be_bytes(size) as usize];
        self.stdout.read_exact(&mut frame).unwrap();
        Some((frame[0], frame[1..].to_vec()))
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        self.stdin.take();
        let _ = self.child.wait();
    }
}

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "forcola-spawn-errors-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn failed(request: Value, features: Option<&[&str]>) -> Value {
    let mut shim = Shim::new(features);
    shim.spawn(request);
    let (tag, payload) = shim.recv().unwrap();
    assert_eq!(tag, ERROR, "{}", String::from_utf8_lossy(&payload));
    assert!(
        shim.recv().is_none(),
        "a failed spawn must emit no identity or output"
    );
    serde_json::from_slice(&payload).unwrap()
}

#[test]
fn cwd_exec_and_privdrop_failures_have_typed_reasons_on_pipes_and_ptys() {
    let dir = TestDir::new();
    let missing = dir.0.join("missing");
    let plain_file = dir.0.join("not-executable");
    fs::write(&plain_file, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&plain_file, fs::Permissions::from_mode(0o600)).unwrap();

    let cases = [
        (
            json!({"argv":["/bin/echo","never"],"cd":missing}),
            "cwd",
            "enoent",
            Some(libc::ENOENT),
            Some(missing.clone()),
        ),
        (
            json!({"argv":["/bin/echo","never"],"cd":plain_file}),
            "cwd",
            "enotdir",
            Some(libc::ENOTDIR),
            Some(plain_file.clone()),
        ),
        (
            json!({"argv":[missing]}),
            "exec",
            "enoent",
            Some(libc::ENOENT),
            Some(missing.clone()),
        ),
        (
            json!({"argv":[plain_file]}),
            "exec",
            "eacces",
            Some(libc::EACCES),
            Some(plain_file.clone()),
        ),
        (
            json!({"argv":["/bin/echo","never"],"user":"forcola-no-such-account-103"}),
            "privdrop",
            "not_found",
            None,
            None,
        ),
    ];
    for pty in [false, true] {
        for (request, stage, reason, errno, path) in &cases {
            let mut request = request.clone();
            request["pty"] = json!(pty);
            let legacy = failed(request.clone(), None);
            let report = failed(
                request,
                Some(&["structured_spawn_errors_v1", "spawn_identity_v1"]),
            );
            assert_eq!(report["reason"], legacy["reason"]);
            assert_eq!(report["spawn_error"]["stage"], *stage);
            assert_eq!(report["spawn_error"]["reason"], *reason);
            assert_eq!(report["spawn_error"]["errno"], json!(errno));
            assert_eq!(report["spawn_error"]["path"], json!(path));
        }
    }
}

#[test]
fn nul_cwd_preserves_parent_setup_failure_precedence_on_pipes_and_ptys() {
    let cwd = "nul\0directory";
    for pty in [false, true] {
        for (extra, stage, reason, path) in [
            (json!({}), "cwd", "invalid_input", Some(cwd)),
            (
                json!({"user":"forcola-no-such-account-103"}),
                "privdrop",
                "not_found",
                None,
            ),
            (
                json!({"user":"forcola-no-such-account-103","cgroup":"required"}),
                "privdrop",
                "not_found",
                None,
            ),
        ] {
            let mut request = json!({"argv":["/bin/echo","never"],"cd":cwd,"pty":pty});
            request
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let legacy = failed(request.clone(), None);
            let report = failed(request, Some(&["structured_spawn_errors_v1"]));
            assert_eq!(report["reason"], legacy["reason"]);
            assert_eq!(report["spawn_error"]["stage"], stage);
            assert_eq!(report["spawn_error"]["reason"], reason);
            assert!(report["spawn_error"]["errno"].is_null());
            assert_eq!(report["spawn_error"]["path"], json!(path));
        }

        let request = json!({"argv":["/bin/echo","never"],"cd":cwd,"pty":pty,"cgroup":"required"});
        let legacy = failed(request.clone(), None);
        let report = failed(request, Some(&["structured_spawn_errors_v1"]));
        assert_eq!(report["reason"], legacy["reason"]);
        // Required cgroups always fail in parent setup off Linux. On Linux a
        // delegated hierarchy can prepare successfully, reaching the NUL cwd.
        #[cfg(not(target_os = "linux"))]
        assert_eq!(report["spawn_error"]["stage"], "cgroup");
        #[cfg(target_os = "linux")]
        assert!(matches!(
            report["spawn_error"]["stage"].as_str(),
            Some("cgroup" | "cwd")
        ));
    }
}

#[test]
fn actual_child_privilege_drop_failure_is_attributed() {
    for pty in [false, true] {
        // gid_t(-1) is invalid for setgid, including when the tests run as root.
        // Non-root setgroups can fail first with EPERM. Both are real child
        // credential syscalls, separate from parent account-resolution errors.
        let report = failed(
            json!({"argv":["/bin/echo","never"],"group":u32::MAX,"pty":pty}),
            Some(&["structured_spawn_errors_v1"]),
        );
        assert_eq!(report["spawn_error"]["stage"], "privdrop");
        assert!(matches!(
            report["spawn_error"]["reason"].as_str(),
            Some("einval" | "eperm")
        ));
        assert!(report["spawn_error"]["errno"].is_i64());
        assert!(report["spawn_error"]["path"].is_null());
    }
}

#[test]
fn old_sessions_keep_reason_only_even_after_hello() {
    let request = json!({"argv":["/forcola/no/such/command"]});
    for features in [
        None,
        Some([].as_slice()),
        Some(["bounded_run_v1"].as_slice()),
        Some(["spawn_identity_v1"].as_slice()),
    ] {
        let report = failed(request.clone(), features);
        assert_eq!(report.as_object().unwrap().len(), 1);
        assert!(report["reason"]
            .as_str()
            .unwrap()
            .starts_with("spawn failed: "));
    }
}

#[test]
fn failure_before_child_hook_is_spawn_not_exec_and_does_not_block() {
    let report = failed(
        json!({"argv":["/bin/echo","nul\0argument"]}),
        Some(&["structured_spawn_errors_v1"]),
    );
    assert_eq!(report["spawn_error"]["stage"], "spawn");
    assert_eq!(report["spawn_error"]["reason"], "invalid_input");
    assert!(report["spawn_error"]["errno"].is_null());
    assert!(report["spawn_error"]["path"].is_null());
}

fn successful(request: Value, features: Option<&[&str]>) -> (Vec<u8>, Vec<u8>) {
    let mut shim = Shim::new(features);
    shim.spawn(request);
    let mut tags = Vec::new();
    let mut output = Vec::new();
    loop {
        let (tag, bytes) = shim.recv().unwrap();
        tags.push(tag);
        match tag {
            STDOUT => output.extend(bytes),
            SPAWNED => {}
            EXIT => {
                assert_eq!(
                    serde_json::from_slice::<Value>(&bytes).unwrap()["status"],
                    0
                );
                break;
            }
            _ => panic!(
                "unexpected frame {tag:#x}: {}",
                String::from_utf8_lossy(&bytes)
            ),
        }
    }
    (output, tags)
}

#[test]
fn relative_executable_and_path_use_requested_cwd_including_replaced_env_and_pty() {
    let dir = TestDir::new();
    let child = dir.0.join("child");
    fs::write(&child, b"#!/bin/sh\nprintf relative-ok\n").unwrap();
    fs::set_permissions(&child, fs::Permissions::from_mode(0o700)).unwrap();
    for argv in ["./child", "child"] {
        for mode in ["inherit", "replace"] {
            for pty in [false, true] {
                let (output, tags) = successful(
                    json!({"argv":[argv],"cd":dir.0,"env":{"PATH":"."},"env_mode":mode,"pty":pty}),
                    Some(&[
                        "structured_spawn_errors_v1",
                        "spawn_identity_v1",
                        "bounded_run_v1",
                    ]),
                );
                assert_eq!(output, b"relative-ok");
                assert_eq!(tags.first(), Some(&SPAWNED));
                assert_eq!(tags.last(), Some(&EXIT));
            }
        }
    }
}

#[test]
fn attribution_pipe_descriptors_are_not_inherited_by_the_executable() {
    // Runners may intentionally pass inheritable descriptors to the shim.
    // Keep one open ourselves so this regression also exercises that case
    // locally. The feature must add no fds to the unchanged legacy baseline.
    let inherited = fs::File::open("/dev/null").unwrap();
    rustix::io::fcntl_setfd(&inherited, rustix::io::FdFlags::empty()).unwrap();
    let limit = (inherited.as_raw_fd() + 1).max(512).to_string();
    let request = json!({"argv":["python3","-c","import os,sys\nopened=[]\nfor fd in range(3,int(sys.argv[1])):\n try: os.fstat(fd); opened.append(fd)\n except OSError: pass\nprint(opened)",limit]});
    let (legacy, _) = successful(request.clone(), None);
    let (output, tags) = successful(request, Some(&["structured_spawn_errors_v1"]));
    let legacy: Vec<i32> = serde_json::from_slice(&legacy).unwrap();
    let observed: Vec<i32> = serde_json::from_slice(&output).unwrap();
    assert!(legacy.contains(&inherited.as_raw_fd()));
    assert_eq!(observed, legacy);
    assert!(!tags.contains(&SPAWNED));
}

#[test]
fn required_cgroup_failure_retains_attribution_or_executes_only_when_available() {
    let mut shim = Shim::new(Some(&["structured_spawn_errors_v1"]));
    shim.spawn(json!({"argv":["/bin/true"],"cgroup":"required"}));
    let (tag, bytes) = shim.recv().unwrap();
    let report: Value = serde_json::from_slice(&bytes).unwrap();
    if tag == ERROR {
        assert_eq!(report["spawn_error"]["stage"], "cgroup");
        assert!(report["spawn_error"]["reason"].is_string());
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(report["spawn_error"]["reason"], "unsupported");
            assert!(report["spawn_error"]["errno"].is_null());
        }
    } else {
        assert_eq!(tag, EXIT);
        assert_eq!(report["contained"], true);
        assert_eq!(report["status"], 0);
    }
}
