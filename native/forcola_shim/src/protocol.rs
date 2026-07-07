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
    /// Run the child as this user. A JSON string is a username to resolve
    /// against the passwd database; a JSON number is a numeric uid used
    /// directly. The user's primary gid and supplementary groups are taken
    /// from the passwd/group database unless `group` overrides the gid.
    /// Absent leaves the child running as the shim's own user.
    #[serde(default)]
    pub user: Option<UserSpec>,
    /// Run the child with this group as its primary gid. A JSON string is a
    /// group name to resolve; a JSON number is a numeric gid used directly.
    /// When given without `user`, the supplementary group list is cleared to
    /// just this gid. When given with `user`, it overrides the user's primary
    /// gid. Absent leaves the gid derived from `user`, or unchanged.
    #[serde(default)]
    pub group: Option<GroupSpec>,
    /// Opt in to Linux cgroup v2 containment. When true and a delegated cgroup
    /// v2 subtree is available, the child is placed in a dedicated child cgroup
    /// before exec so descendants that escape the process group (deliberate
    /// daemonizers) are still reaped via `cgroup.kill`. Linux-only; on other
    /// platforms, on non-cgroup-v2 systems, or when the subtree is not
    /// delegated, it degrades to process-group kill with a warning. The EXIT
    /// report's `contained` field says which mechanism was used. Default false
    /// leaves the kill path unchanged.
    #[serde(default)]
    pub cgroup: bool,
    /// Opt in to demand-driven backpressure on the child's stdout. When
    /// present, the stdout pump reads the child only while the BEAM has
    /// granted read credit via CREDIT frames; when the credit is exhausted
    /// the pump stops reading, the OS pipe fills, and the child's next write
    /// blocks. The value is the BEAM's window size in bytes; it is
    /// informational on the shim side, since the BEAM controls the actual
    /// credit. Absent leaves the eager pump unchanged. STDERR is never gated.
    #[serde(default)]
    pub window_bytes: Option<u64>,
}

/// A user identity in a SPAWN payload: either a name to resolve or a raw
/// numeric uid.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum UserSpec {
    /// Numeric uid, used directly with no passwd lookup.
    Id(u32),
    /// Username, resolved against the passwd database in the parent.
    Name(String),
}

/// A group identity in a SPAWN payload: either a name to resolve or a raw
/// numeric gid.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum GroupSpec {
    /// Numeric gid, used directly with no group lookup.
    Id(u32),
    /// Group name, resolved against the group database in the parent.
    Name(String),
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
    /// Whether Linux cgroup v2 containment was actually active for this run.
    /// `true` only when `cgroup: true` was requested and a delegated cgroup v2
    /// subtree was available; `false` on the default path, on fallback, and on
    /// non-Linux platforms. Reports which kill mechanism was used.
    pub contained: bool,
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
        assert_eq!(req.user, None);
        assert_eq!(req.group, None);
        assert!(!req.cgroup);
        assert_eq!(req.window_bytes, None);
    }

    #[test]
    fn spawn_request_window_bytes_opt_in() {
        let json = r#"{"argv": ["cat"], "window_bytes": 4096}"#;
        let req: SpawnRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.window_bytes, Some(4096));
    }

    #[test]
    fn spawn_request_cgroup_opt_in() {
        let json = r#"{"argv": ["sleep", "1"], "cgroup": true}"#;
        let req: SpawnRequest = serde_json::from_str(json).unwrap();
        assert!(req.cgroup);
    }

    #[test]
    fn spawn_request_user_and_group_names() {
        let json = r#"{"argv": ["id"], "user": "nobody", "group": "staff"}"#;
        let req: SpawnRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.user, Some(UserSpec::Name("nobody".to_string())));
        assert_eq!(req.group, Some(GroupSpec::Name("staff".to_string())));
    }

    #[test]
    fn spawn_request_user_and_group_ids() {
        let json = r#"{"argv": ["id"], "user": 1000, "group": 20}"#;
        let req: SpawnRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.user, Some(UserSpec::Id(1000)));
        assert_eq!(req.group, Some(GroupSpec::Id(20)));
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
            contained: false,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"status\":0"));
        assert!(!json.contains("signal"));
        assert!(json.contains("\"contained\":false"));
    }
}
