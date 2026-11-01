//! Native process identity shared by SPAWNED and TRACKED reports.
//!
//! Read before reaping a child: even a child that exits immediately retains
//! its kernel identity until wait(2), and its pid cannot be reused meanwhile.

use serde::Serialize;
use std::io;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub pgid: u32,
    pub start_time: StartTime,
}

/// Compare the complete tagged value when checking for pid reuse. Linux ticks
/// are relative to boot, so a boot id is part of the identity. macOS exposes
/// the native timeval without reducing its precision to ps's whole seconds.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StartTime {
    #[cfg(target_os = "linux")]
    Linux { boot_id: String, ticks: u64 },
    #[cfg(target_os = "macos")]
    Macos { seconds: u64, microseconds: u64 },
}

#[derive(Debug, Clone)]
pub struct ProcessRecord {
    pub identity: ProcessIdentity,
    pub ppid: u32,
    pub exited: bool,
}

impl ProcessIdentity {
    /// A process may change groups while retaining the same native identity.
    pub fn same_process(&self, other: &Self) -> bool {
        self.pid == other.pid && self.start_time == other.start_time
    }
}

pub fn read(pid: u32) -> io::Result<ProcessIdentity> {
    read_process(pid).map(|record| record.identity)
}

#[cfg(target_os = "linux")]
pub fn read_process(pid: u32) -> io::Result<ProcessRecord> {
    let stat = std::fs::read(format!("/proc/{pid}/stat"))?;
    let (ppid, pgid, ticks, exited) = parse_stat(&stat)?;
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    if boot_id.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty boot id"));
    }
    Ok(ProcessRecord {
        identity: ProcessIdentity {
            pid,
            pgid,
            start_time: StartTime::Linux { boot_id, ticks },
        },
        ppid,
        exited,
    })
}

#[cfg(target_os = "linux")]
fn parse_stat(stat: &[u8]) -> io::Result<(u32, u32, u64, bool)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid process stat");
    // comm (field 2) can contain arbitrary bytes, spaces and parentheses,
    // including ')'. Only the suffix containing numeric fields must be UTF-8.
    let end = stat
        .iter()
        .rposition(|&byte| byte == b')')
        .ok_or_else(invalid)?;
    let fields = std::str::from_utf8(&stat[end + 1..]).map_err(|_| invalid())?;
    let mut fields = fields.split_whitespace();
    // The suffix starts at field 3 (state), followed by ppid and pgrp.
    let exited = matches!(fields.next().ok_or_else(invalid)?, "Z" | "X" | "x");
    let ppid = fields
        .next()
        .ok_or_else(invalid)?
        .parse::<u32>()
        .map_err(|_| invalid())?;
    let pgid = fields
        .next()
        .ok_or_else(invalid)?
        .parse::<u32>()
        .map_err(|_| invalid())?;
    // starttime is field 22, seventeen fields after pgrp.
    let ticks = fields
        .nth(16)
        .ok_or_else(invalid)?
        .parse::<u64>()
        .map_err(|_| invalid())?;
    if pgid == 0 {
        return Err(invalid());
    }
    Ok((ppid, pgid, ticks, exited))
}

#[cfg(target_os = "macos")]
pub fn read_process(pid: u32) -> io::Result<ProcessRecord> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: proc_pidinfo writes at most size bytes to this correctly sized,
    // aligned buffer. We inspect it only after a complete result. TBSDINFO's
    // nonzero arg includes unreaped zombies as well as running processes;
    // arg=0 would return ESRCH for a child that exited before this lookup.
    let result = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            1,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    if result != size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short process info",
        ));
    }
    // SAFETY: the successful full-size result initialized the entire struct.
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != pid || info.pbi_pgid == 0 || info.pbi_start_tvusec >= 1_000_000 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid process info",
        ));
    }
    Ok(ProcessRecord {
        identity: ProcessIdentity {
            pid,
            pgid: info.pbi_pgid,
            // These are p_start.tv_sec/usec, the same kernel timeval exported as
            // kp_proc.p_starttime by sysctl. libproc supplies a typed, stable ABI.
            start_time: StartTime::Macos {
                seconds: info.pbi_start_tvsec,
                microseconds: info.pbi_start_tvusec,
            },
        },
        ppid: info.pbi_ppid,
        exited: info.pbi_status == 5, // SZOMB in sys/proc.h
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn read_process(_pid: u32) -> io::Result<ProcessRecord> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process identity is unavailable",
    ))
}

#[cfg(test)]
mod tests {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn native_identity_is_stable() {
        let pid = std::process::id();
        let first = super::read(pid).unwrap();
        assert_eq!(first.pid, pid);
        assert!(first.pgid > 0);
        assert_eq!(first, super::read(pid).unwrap());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_identity_survives_an_unreaped_child_exit() {
        use std::io;
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};

        // cat stays alive on its open stdin while we capture the original
        // identity. A separate process group makes the expected pgid explicit.
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        let running = super::read(pid);
        drop(child.stdin.take());

        let mut status = std::mem::MaybeUninit::<libc::siginfo_t>::uninit();
        let waited = loop {
            // SAFETY: waitid writes to a correctly sized/aligned status buffer.
            // WNOWAIT leaves this exited child unreaped and its pid reserved.
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    pid,
                    status.as_mut_ptr(),
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if result == 0 {
                break Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                break Err(error);
            }
        };
        let exited = super::read(pid);
        // Reap before any assertions, including when either identity lookup or
        // waitid failed, so a failing regression cannot leave a zombie behind.
        let reaped = child.wait();

        waited.unwrap();
        assert!(reaped.unwrap().success());
        let running = running.unwrap();
        assert_eq!(running.pid, pid);
        assert_eq!(running.pgid, pid);
        assert_eq!(exited.unwrap(), running);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stat_parser_handles_spaces_and_parentheses_in_comm() {
        let stat = b"123 (a tricky ) name\xff) S 1 123 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 98765 0";
        assert_eq!(super::parse_stat(stat).unwrap(), (1, 123, 98765, false));
        for bad in [
            "",
            "123 (name) S 1 0",
            "123 (name) S 1 x",
            "123 name S 1 123",
        ] {
            assert!(super::parse_stat(bad.as_bytes()).is_err());
        }
        assert!(super::parse_stat(b"123 (name) S 1 123 \xff").is_err());
    }
}
