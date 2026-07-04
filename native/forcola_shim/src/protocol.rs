//! JSON payload shapes for the SPAWN, EXIT, and ERROR frames.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Payload of an inbound SPAWN frame.
#[derive(Debug, Clone, Deserialize)]
pub struct SpawnRequest {
    pub argv: Vec<String>,
    #[serde(default)]
    pub cd: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub merge_stderr: bool,
    /// Milliseconds before the child is killed for running too long. `None`
    /// (or absent) means no timeout is enforced by the shim.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Milliseconds to wait after SIGTERM before escalating to SIGKILL.
    #[serde(default = "default_kill_grace_ms")]
    pub kill_grace_ms: u64,
    /// Run the child under a pseudo-terminal. When true, the child's
    /// stdin/stdout/stderr all connect to the pty slave and its output is
    /// framed back as STDOUT (0x11); a pty merges stderr into the terminal,
    /// so there is no separate STDERR stream in pty mode. Default false
    /// leaves the pipe path unchanged.
    #[serde(default)]
    pub pty: bool,
    /// Initial pty window height in rows. Applied only in pty mode.
    #[serde(default)]
    pub pty_rows: Option<u16>,
    /// Initial pty window width in columns. Applied only in pty mode.
    #[serde(default)]
    pub pty_cols: Option<u16>,
}

fn default_kill_grace_ms() -> u64 {
    5_000
}

/// Payload of an outbound EXIT frame.
#[derive(Debug, Clone, Serialize)]
pub struct ExitReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    pub timed_out: bool,
}

/// Payload of an outbound ERROR frame.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorReport<'a> {
    pub reason: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_request_defaults() {
        let json = r#"{"argv": ["echo", "hi"]}"#;
        let req: SpawnRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.argv, vec!["echo".to_string(), "hi".to_string()]);
        assert_eq!(req.cd, None);
        assert!(req.env.is_empty());
        assert!(!req.merge_stderr);
        assert_eq!(req.timeout_ms, None);
        assert_eq!(req.kill_grace_ms, 5_000);
        assert!(!req.pty);
        assert_eq!(req.pty_rows, None);
        assert_eq!(req.pty_cols, None);
    }

    #[test]
    fn spawn_request_pty_fields() {
        let json = r#"{"argv": ["sh"], "pty": true, "pty_rows": 24, "pty_cols": 80}"#;
        let req: SpawnRequest = serde_json::from_str(json).unwrap();
        assert!(req.pty);
        assert_eq!(req.pty_rows, Some(24));
        assert_eq!(req.pty_cols, Some(80));
    }

    #[test]
    fn spawn_request_full() {
        let json = r#"{
            "argv": ["sleep", "1"],
            "cd": "/tmp",
            "env": {"FOO": "bar"},
            "merge_stderr": true,
            "timeout_ms": 100,
            "kill_grace_ms": 50
        }"#;
        let req: SpawnRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.cd.as_deref(), Some("/tmp"));
        assert_eq!(req.env.get("FOO"), Some(&"bar".to_string()));
        assert!(req.merge_stderr);
        assert_eq!(req.timeout_ms, Some(100));
        assert_eq!(req.kill_grace_ms, 50);
    }

    #[test]
    fn exit_report_omits_absent_fields() {
        let report = ExitReport {
            status: Some(0),
            signal: None,
            timed_out: false,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"status\":0"));
        assert!(!json.contains("signal"));
    }
}
