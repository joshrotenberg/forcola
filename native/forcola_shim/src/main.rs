//! forcola_shim: the process-group supervisor between the BEAM and a child.
//!
//! The BEAM opens this binary as a port. The shim forks the real command,
//! calls `setsid` in the child before exec so the child leads a new process
//! group, and then supervises it:
//!
//! - stdout/stderr from the child are framed back to the BEAM over the
//!   shim's stdout. When the SPAWN payload requests a pty, the child runs
//!   under a pseudo-terminal instead of pipes: its stdin/stdout/stderr all
//!   attach to the pty slave, it claims that slave as its controlling
//!   terminal (`TIOCSCTTY` after `setsid`), and all of its output comes back
//!   as STDOUT frames since a pty merges stderr onto the one terminal.
//! - a timeout (or an explicit kill frame) triggers SIGTERM to the whole
//!   group (`kill(-pgid, SIGTERM)`), escalating to SIGKILL after the grace.
//!   An optional SIGINT and its own grace precede this sequence.
//! - a normally exiting leader triggers the same cleanup for any descendants
//!   that remain in its process group
//! - EOF on the shim's stdin means the BEAM died; the shim kills the group
//!   and exits. Tracked mode independently detects a closed protocol output
//!   peer even when child input blocks the reader. The untracked synchronous
//!   input path can delay EOF handling while its child write is blocked.
//!
//! Wire protocol (BEAM <-> shim), legacy framing with optional v1 handshake:
//!
//! Frames are `{:packet, 4}`-style: a 4-byte big-endian length prefix, then
//! a 1-byte tag, then the payload.
//!
//! BEAM -> shim:
//!   0x01 SPAWN   payload: JSON {argv, cd, env, merge_stderr, timeout_ms,
//!                               kill_grace_ms, pty, pty_rows, pty_cols,
//!                               user, group, cgroup, window_bytes,
//!                               stderr_window_bytes, strict_output, env_mode,
//!                               interrupt_signal, interrupt_grace_ms}
//!                env values are strings to set or null to remove a variable.
//!                env_mode replace clears inheritance before applying env.
//!   0x02 STDIN   payload: bytes for the child's stdin (duplex mode)
//!   0x03 EOF     close the child's stdin
//!   0x04 KILL    kill the group now
//!   0x05 CREDIT  payload: 8-byte big-endian byte count; grants the stdout
//!                pump that many more bytes of read budget under backpressure
//!                (only when SPAWN carried window_bytes)
//!   0x06 STDERR_CREDIT  same payload, for the separately gated stderr pump
//!
//!   0x07 HELLO   optional first frame, before SPAWN; no child runs yet
//!                Empty payload queries capabilities without selecting new
//!                frames. JSON {features: ["spawn_identity_v1"]} selects
//!                features explicitly; unsupported selections fail closed.
//!                structured_spawn_errors_v1 independently selects typed
//!                spawn-error details; both extensions can be selected together.
//!
//! shim -> BEAM:
//!   0x15 HELLO   JSON {protocol: 1, features: ["bounded_run_v1", ...]}
//!                Confirms strict credit drain, env replacement and INT grace.
//!                env_unset_v1 additionally confirms null-valued env entries.
//!                These are capability requirements, with no extra frames.
//!                An old shim rejects HELLO rather than ignoring SPAWN keys.
//!                spawn_identity_v1 is advertised only after a native
//!                self-identity probe succeeds when HELLO is handled, including
//!                an empty HELLO. Access to a child after a
//!                privilege change can still fail; that failure kills and
//!                reaps the child before ERROR, without sending SPAWNED.
//!   0x11 STDOUT  payload: bytes from the child's stdout
//!   0x12 STDERR  payload: bytes from the child's stderr
//!   0x13 EXIT    payload: JSON {status | signal, timed_out, confirmed,
//!                               contained, output_truncated}
//!   0x14 ERROR   payload: JSON {reason} (spawn failure etc.). With
//!                structured_spawn_errors_v1 selected, spawn failures also
//!                carry spawn_error: {stage, reason, errno, path}. Stages are
//!                cwd, exec, privdrop, cgroup, spawn, or pty. Nested reason is
//!                a lowercase POSIX errno name or static I/O error-kind name;
//!                errno is the native integer or null, path a string or null.
//!                Top-level reason retains the original human-readable text.
//!                exec path is requested argv[0], not a resolved PATH candidate.
//!                Child-side attribution comes from failed syscalls, without
//!                path preflight. Generic pre-hook failures use stage spawn.
//!   0x16 SPAWNED JSON {pid, pgid, start_time}, before any output and only
//!                with spawn_identity_v1 selected. start_time is tagged:
//!                Linux {kind: "linux", boot_id: string, ticks: integer}
//!                macOS {kind: "macos", seconds: integer, microseconds: integer}
//!                Linux ticks are /proc/<pid>/stat field 22 since boot;
//!                macOS uses the native process-start timeval. Compare every
//!                field when checking identity, including the Linux boot id.
//!   0x17 TRACKED Same identity payload, selected by track_descendants_v1.
//!                The root group is first, before output; newly discovered
//!                descendant group leaders follow. Tracking adds EXIT scope
//!                tree_contained (Linux subreaper) or tree_sampled (macOS).

mod cgroup;
mod credit;
mod frame;
mod identity;
mod privdrop;
mod protocol;
mod spawn_error;
mod supervisor;
mod tracking;

use std::io;
use std::os::fd::AsRawFd;
use std::process::ExitCode;

fn main() -> ExitCode {
    let stdin = io::stdin();
    let stdout = io::stdout();

    let protocol_fd = stdout.as_raw_fd();
    match supervisor::run(stdin, stdout, Some(protocol_fd)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("forcola_shim: fatal: {err}");
            ExitCode::FAILURE
        }
    }
}
