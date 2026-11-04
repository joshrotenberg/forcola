//! `forcola_shim groups`: one JSON request line in, one JSON result line out.

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::process::{Command, ExitStatus, Stdio};

fn run(input: &[u8]) -> (Value, ExitStatus) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_forcola_shim"))
        .arg("groups")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let mut output = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    let status = child.wait().unwrap();
    assert_eq!(output.matches('\n').count(), 1, "one line: {output:?}");
    (serde_json::from_str(&output).unwrap(), status)
}

fn request(value: Value) -> Vec<u8> {
    let mut line = serde_json::to_vec(&value).unwrap();
    line.push(b'\n');
    line
}

fn foreign_start_time() -> Value {
    if cfg!(target_os = "linux") {
        json!({"kind": "macos", "seconds": 1, "microseconds": 0})
    } else {
        json!({"kind": "linux", "boot_id": "x", "ticks": 1})
    }
}

#[test]
fn malformed_requests_are_rejected_with_status_2() {
    let cases: [(&[u8], &str); 5] = [
        (b"not json\n", "invalid_request"),
        (b"{\"op\":\"check\",\"groups\":[]}", "unterminated_request"),
        (b"{\"op\":\"wait\",\"groups\":[]}\n", "invalid_request"),
        (
            b"{\"op\":\"stop\",\"groups\":[],\"interrupt_signal\":\"int\"}\n",
            "invalid_interrupt",
        ),
        (
            b"{\"op\":\"stop\",\"groups\":[],\"kill_grace_ms\":4294967296}\n",
            "invalid_kill_grace",
        ),
    ];
    for (input, reason) in cases {
        let (result, status) = run(input);
        assert_eq!(result, json!({"error": reason}));
        assert_eq!(status.code(), Some(2));
    }
}

#[test]
fn results_follow_request_order_with_reasons_only_for_unknown() {
    let foreign = json!({"pid": 10, "pgid": 10, "start_time": foreign_start_time()});
    let invalid = json!({"pid": 10});
    let earlier_boot = if cfg!(target_os = "linux") {
        let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
        assert_ne!(boot_id.trim(), "00000000-0000-0000-0000-000000000000");
        json!({"pid": 1, "pgid": 1, "start_time":
            {"kind": "linux", "boot_id": "00000000-0000-0000-0000-000000000000", "ticks": 1}})
    } else {
        json!({"pid": 1, "pgid": 1, "start_time":
            {"kind": "macos", "seconds": 1, "microseconds": 0}})
    };

    let (result, status) = run(&request(json!({
        "op": "check",
        "groups": [foreign, earlier_boot, invalid],
    })));
    assert_eq!(status.code(), Some(0));
    assert_eq!(
        result,
        json!({
            "groups": [
                {"state": "unknown", "reason": "foreign_identity"},
                {"state": "gone", "reason": null},
                {"state": "unknown", "reason": "invalid_identity"},
            ],
            "confirmed": false,
        })
    );
}

#[test]
fn stop_with_nothing_present_is_confirmed_without_waiting() {
    let earlier_boot = if cfg!(target_os = "linux") {
        json!({"kind": "linux", "boot_id": "00000000-0000-0000-0000-000000000000", "ticks": 1})
    } else {
        json!({"kind": "macos", "seconds": 1, "microseconds": 0})
    };
    let started = std::time::Instant::now();
    let (result, status) = run(&request(json!({
        "op": "stop",
        "groups": [{"pid": 1, "pgid": 1, "start_time": earlier_boot}],
        "interrupt_signal": "int",
        "interrupt_grace_ms": 10_000,
        "kill_grace_ms": 10_000,
    })));
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(status.code(), Some(0));
    assert_eq!(
        result,
        json!({"groups": [{"state": "gone", "reason": null}], "confirmed": true})
    );
}

#[test]
fn an_empty_request_is_confirmed() {
    let (result, status) = run(&request(json!({"op": "check", "groups": []})));
    assert_eq!(status.code(), Some(0));
    assert_eq!(result, json!({"groups": [], "confirmed": true}));
}
