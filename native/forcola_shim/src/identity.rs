//! Native process identity shared by SPAWNED and future TRACKED reports.
//!
//! Read before reaping a child: even a child that exits immediately retains
//! its kernel identity until wait(2), and its pid cannot be reused meanwhile.

use serde::Serialize;
use std::io;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub pgid: u32,
    pub start_time: StartTime,
}

/// Compare the complete tagged value when checking for pid reuse. Linux ticks
/// are relative to boot, so a boot id is part of the identity. macOS exposes
/// the native timeval without reducing its precision to ps's whole seconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StartTime {
    #[cfg(target_os = "linux")]
    Linux { boot_id: String, ticks: u64 },
    #[cfg(target_os = "macos")]
    Macos { seconds: u64, microseconds: u64 },
}

#[cfg(target_os = "linux")]
pub fn read(pid: u32) -> io::Result<ProcessIdentity> {
    let stat = std::fs::read(format!("/proc/{pid}/stat"))?;
    let (pgid, ticks) = parse_stat(&stat)?;
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    if boot_id.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty boot id"));
    }
    Ok(ProcessIdentity {
        pid,
        pgid,
        start_time: StartTime::Linux { boot_id, ticks },
    })
}

#[cfg(target_os = "linux")]
fn parse_stat(stat: &[u8]) -> io::Result<(u32, u64)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid process stat");
    // comm (field 2) can contain arbitrary bytes, spaces and parentheses,
    // including ')'. Only the suffix containing numeric fields must be UTF-8.
    let end = stat
        .iter()
        .rposition(|&byte| byte == b')')
        .ok_or_else(invalid)?;
    let fields = std::str::from_utf8(&stat[end + 1..]).map_err(|_| invalid())?;
    let mut fields = fields.split_whitespace();
    // The suffix starts at field 3 (state); pgrp is field 5.
    let pgid = fields
        .nth(2)
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
    Ok((pgid, ticks))
}

#[cfg(target_os = "macos")]
pub fn read(pid: u32) -> io::Result<ProcessIdentity> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: proc_pidinfo writes at most size bytes to this correctly sized,
    // aligned buffer. We inspect it only after a complete result. TBSDINFO
    // supports unreaped zombies as well as running processes.
    let result = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
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
    Ok(ProcessIdentity {
        pid,
        pgid: info.pbi_pgid,
        // These are p_start.tv_sec/usec, the same kernel timeval exported as
        // kp_proc.p_starttime by sysctl. libproc supplies a typed, stable ABI.
        start_time: StartTime::Macos {
            seconds: info.pbi_start_tvsec,
            microseconds: info.pbi_start_tvusec,
        },
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn read(_pid: u32) -> io::Result<ProcessIdentity> {
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

    #[cfg(target_os = "linux")]
    #[test]
    fn stat_parser_handles_spaces_and_parentheses_in_comm() {
        let stat = b"123 (a tricky ) name\xff) S 1 123 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 98765 0";
        assert_eq!(super::parse_stat(stat).unwrap(), (123, 98765));
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
