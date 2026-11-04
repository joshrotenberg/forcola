//! `forcola_shim lock <path>`: hold a whole-file POSIX `fcntl` write lock.
//!
//! The BEAM cannot take `fcntl` record locks itself, so this mode holds one
//! on its behalf for as long as the shim lives:
//!
//! - opens or creates `<path>` (`O_RDWR | O_CREAT | O_CLOEXEC`, mode 0600
//!   before umask). The file is never truncated, removed, or replaced.
//! - calls `fcntl(F_SETLK)` with `F_WRLCK` over the whole file (start 0,
//!   len 0). It never waits for a conflicting lock.
//! - writes exactly one line on stdout, then either exits or, on success,
//!   blocks reading stdin. Stdin EOF, or any byte on stdin, ends the process
//!   with status 0, which releases the lock. If the BEAM dies, stdin closes
//!   and the lock is released the same way.
//!
//! | Outcome                  | Line                       | Exit status |
//! |--------------------------|----------------------------|-------------|
//! | lock acquired            | `ok`                       | 0 (release) |
//! | held by another process  | `held <pid>`               | 3           |
//! | bad arguments            | `error usage`              | 2           |
//! | open failed              | `error open <errno-name>`  | 4           |
//! | lock call failed         | `error lock <errno-name>`  | 5           |
//!
//! `<pid>` comes from `F_GETLK` and is `0` when the holder could not be
//! identified (for example, it released the lock between the two calls on
//! every retry). Error lines carry only a stage and a stable errno name.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::process::ExitCode;

use crate::spawn_error::error_reason;

const EXIT_RELEASED: u8 = 0;
const EXIT_USAGE: u8 = 2;
const EXIT_HELD: u8 = 3;
const EXIT_OPEN: u8 = 4;
const EXIT_LOCK: u8 = 5;

/// Attempts at `F_SETLK` when `F_GETLK` reports the conflicting lock is gone.
const ATTEMPTS: usize = 3;

enum Attempt {
    Acquired,
    Held(libc::pid_t),
}

/// Runs lock mode with the arguments that follow the `lock` subcommand.
pub fn run(args: &[OsString]) -> ExitCode {
    let mut stdout = io::stdout();
    let path = match args {
        [path] if !path.is_empty() => path,
        _ => return finish(&mut stdout, "error usage", EXIT_USAGE),
    };

    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC)
        .open(path)
    {
        Ok(file) => file,
        Err(err) => {
            let line = format!("error open {}", error_reason(&err));
            return finish(&mut stdout, &line, EXIT_OPEN);
        }
    };

    match acquire(&file) {
        Ok(Attempt::Acquired) => {}
        Ok(Attempt::Held(pid)) => {
            let line = format!("held {pid}");
            return finish(&mut stdout, &line, EXIT_HELD);
        }
        Err(err) => {
            let line = format!("error lock {}", error_reason(&err));
            return finish(&mut stdout, &line, EXIT_LOCK);
        }
    }

    if writeln!(stdout, "ok")
        .and_then(|()| stdout.flush())
        .is_err()
    {
        // Nobody is listening; dropping the file releases the lock.
        return ExitCode::from(EXIT_RELEASED);
    }

    wait_for_release();
    // The lock belongs to this process; closing the descriptor (and process
    // exit) releases it without touching the file's contents or its name.
    drop(file);
    ExitCode::from(EXIT_RELEASED)
}

fn finish(stdout: &mut io::Stdout, line: &str, status: u8) -> ExitCode {
    let _ = writeln!(stdout, "{line}").and_then(|()| stdout.flush());
    ExitCode::from(status)
}

/// Blocks until stdin reaches EOF, delivers any byte, or fails.
fn wait_for_release() {
    let mut stdin = io::stdin();
    let mut buf = [0u8; 64];
    loop {
        match stdin.read(&mut buf) {
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            _ => return,
        }
    }
}

/// A write-lock request (or F_GETLK probe) covering the whole file.
fn whole_file() -> libc::flock {
    // SAFETY: flock is a plain C struct; all-zero is a valid value and every
    // field the kernel reads is set explicitly below.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    lock.l_start = 0;
    lock.l_len = 0;
    lock
}

fn acquire(file: &File) -> io::Result<Attempt> {
    let fd = file.as_raw_fd();
    for _ in 0..ATTEMPTS {
        let lock = whole_file();
        // SAFETY: fd is an open descriptor owned by `file`; lock is a valid
        // flock that outlives the call.
        if unsafe { libc::fcntl(fd, libc::F_SETLK, &lock) } == 0 {
            return Ok(Attempt::Acquired);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(code) if code == libc::EACCES || code == libc::EAGAIN => {}
            _ => return Err(err),
        }

        let mut probe = whole_file();
        // SAFETY: as above; F_GETLK writes the conflicting lock into probe.
        if unsafe { libc::fcntl(fd, libc::F_GETLK, &mut probe) } != 0 {
            return Ok(Attempt::Held(0));
        }
        // F_UNLCK is c_short on some platforms and c_int on others.
        if i64::from(probe.l_type) != i64::from(libc::F_UNLCK) {
            return Ok(Attempt::Held(probe.l_pid));
        }
        // The holder released between F_SETLK and F_GETLK; try again.
    }
    Ok(Attempt::Held(0))
}
