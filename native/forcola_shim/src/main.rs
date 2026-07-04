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
//!   group (`kill(-pgid, SIGTERM)`), escalating to SIGKILL after the grace
//! - EOF on the shim's stdin means the BEAM died; the shim kills the group
//!   and exits
//!
//! Wire protocol (BEAM <-> shim), v0 draft:
//!
//! Frames are `{:packet, 4}`-style: a 4-byte big-endian length prefix, then
//! a 1-byte tag, then the payload.
//!
//! BEAM -> shim:
//!   0x01 SPAWN   payload: JSON {argv, cd, env, merge_stderr, timeout_ms,
//!                               kill_grace_ms, pty, pty_rows, pty_cols}
//!   0x02 STDIN   payload: bytes for the child's stdin (duplex mode)
//!   0x03 EOF     close the child's stdin
//!   0x04 KILL    kill the group now
//!
//! shim -> BEAM:
//!   0x11 STDOUT  payload: bytes from the child's stdout
//!   0x12 STDERR  payload: bytes from the child's stderr
//!   0x13 EXIT    payload: JSON {status | signal, timed_out}
//!   0x14 ERROR   payload: JSON {reason} (spawn failure etc.)

mod frame;
mod protocol;
mod supervisor;

use std::io;
use std::process::ExitCode;

fn main() -> ExitCode {
    let stdin = io::stdin();
    let stdout = io::stdout();

    match supervisor::run(stdin, stdout) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("forcola_shim: fatal: {err}");
            ExitCode::FAILURE
        }
    }
}
