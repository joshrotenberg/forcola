//! Spawn failures retain the original display text and a separate attribution.
//!
//! A CLOEXEC pipe carries one fixed-size child report. The parent does not
//! infer a failure from filesystem preflight checks: it reads either the
//! actual failing child operation or an explicit marker immediately before
//! std's exec. No marker means a parent/stdio/fork failure, not an exec error.

use serde::Serialize;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, OwnedFd};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Stage {
    Spawn = 1,
    Cwd = 2,
    Exec = 3,
    Privdrop = 4,
    Cgroup = 5,
    Pty = 6,
}

#[derive(Debug)]
pub struct SpawnFailure {
    pub stage: Stage,
    pub error: io::Error,
    pub path: Option<String>,
    message: String,
}

impl SpawnFailure {
    pub fn new(stage: Stage, error: io::Error, path: Option<String>) -> Self {
        let message = error.to_string();
        Self {
            stage,
            error,
            path,
            message,
        }
    }

    #[cfg(target_os = "linux")]
    pub fn with_message(
        stage: Stage,
        error: io::Error,
        path: Option<String>,
        message: String,
    ) -> Self {
        Self {
            stage,
            error,
            path,
            message,
        }
    }

    pub fn report(&self) -> SpawnErrorReport<'_> {
        SpawnErrorReport {
            stage: self.stage,
            reason: error_reason(&self.error),
            errno: self.error.raw_os_error(),
            path: self.path.as_deref(),
        }
    }
}

impl fmt::Display for SpawnFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SpawnErrorReport<'a> {
    pub stage: Stage,
    pub reason: &'static str,
    pub errno: Option<i32>,
    pub path: Option<&'a str>,
}

/// Stable names avoid interpreting platform-specific errno numbers in Elixir.
/// Unknown errno values retain their numeric value plus a static io kind.
pub(crate) fn error_reason(error: &io::Error) -> &'static str {
    match error.raw_os_error() {
        Some(libc::ENOENT) => "enoent",
        Some(libc::ENOTDIR) => "enotdir",
        Some(libc::EACCES) => "eacces",
        Some(libc::EPERM) => "eperm",
        Some(libc::EINVAL) => "einval",
        Some(libc::ENOEXEC) => "enoexec",
        Some(libc::E2BIG) => "e2big",
        Some(libc::ELOOP) => "eloop",
        Some(libc::ENAMETOOLONG) => "enametoolong",
        Some(libc::ENOMEM) => "enomem",
        Some(libc::EMFILE) => "emfile",
        Some(libc::ENFILE) => "enfile",
        Some(libc::EIO) => "eio",
        Some(libc::EBADF) => "ebadf",
        Some(libc::EROFS) => "erofs",
        Some(libc::ENOSPC) => "enospc",
        Some(libc::EBUSY) => "ebusy",
        Some(libc::ETXTBSY) => "etxtbsy",
        Some(libc::EAGAIN) => "eagain",
        Some(libc::EINTR) => "eintr",
        Some(libc::EPIPE) => "epipe",
        Some(libc::ESRCH) => "esrch",
        Some(libc::EEXIST) => "eexist",
        Some(libc::ENOSYS) => "enosys",
        Some(libc::ENOTSUP) => "enotsup",
        _ => match error.kind() {
            io::ErrorKind::NotFound => "not_found",
            io::ErrorKind::PermissionDenied => "permission_denied",
            io::ErrorKind::InvalidInput => "invalid_input",
            io::ErrorKind::InvalidData => "invalid_data",
            io::ErrorKind::Unsupported => "unsupported",
            io::ErrorKind::AlreadyExists => "already_exists",
            io::ErrorKind::WouldBlock => "would_block",
            io::ErrorKind::Interrupted => "interrupted",
            io::ErrorKind::TimedOut => "timed_out",
            io::ErrorKind::BrokenPipe => "broken_pipe",
            io::ErrorKind::WriteZero => "write_zero",
            io::ErrorKind::UnexpectedEof => "unexpected_eof",
            io::ErrorKind::OutOfMemory => "out_of_memory",
            _ => "other",
        },
    }
}

pub struct ChildReporter(OwnedFd);
pub struct ReportReader(File);

pub fn channel() -> io::Result<(ReportReader, ChildReporter)> {
    let (read, write) = rustix::pipe::pipe()?;
    // Duplicating also keeps both ends above stdio even if an embedding caller
    // closed fd 0/1/2. All fd setup happens in the parent before fork.
    let read = rustix::io::fcntl_dupfd_cloexec(&read, 3)?;
    let write = rustix::io::fcntl_dupfd_cloexec(&write, 3)?;
    Ok((ReportReader(File::from(read)), ChildReporter(write)))
}

impl ChildReporter {
    /// Async-signal-safe: fixed stack bytes and write(2), without formatting,
    /// allocation, locks or child-side destructor work. A single short report
    /// fits in an empty pipe even while Command::spawn waits for exec.
    fn write(&self, stage: Stage, errno: i32) -> io::Result<()> {
        let errno = errno.to_be_bytes();
        let bytes = [
            b'F',
            b'S',
            stage as u8,
            0,
            errno[0],
            errno[1],
            errno[2],
            errno[3],
        ];
        loop {
            // SAFETY: this fd stays open across fork, and bytes is a valid
            // eight-byte stack buffer. CLOEXEC closes the fd on successful exec.
            let count =
                unsafe { libc::write(self.0.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
            if count == bytes.len() as isize {
                return Ok(());
            }
            if count < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(error);
            }
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
    }

    pub fn check(&self, stage: Stage, result: io::Result<()>) -> io::Result<()> {
        if let Err(error) = &result {
            // Preserve the syscall error even if the report pipe itself fails.
            // The parent treats a missing/partial record as unknown spawn setup.
            let _ = self.write(stage, error.raw_os_error().unwrap_or(libc::EINVAL));
        }
        result
    }

    pub fn ready_for_exec(&self) -> io::Result<()> {
        self.write(Stage::Exec, 0)
    }
}

impl ReportReader {
    /// Call only after dropping Command and all parent write-end owners.
    /// std has already reaped the failed child before returning its error.
    pub fn failure(
        mut self,
        error: io::Error,
        argv0: &str,
        cwd: Option<&str>,
        cgroup: Option<&str>,
    ) -> SpawnFailure {
        let mut bytes = [0; 8];
        let stage =
            if self.0.read_exact(&mut bytes).is_ok() && bytes[..2] == *b"FS" && bytes[3] == 0 {
                let reported_errno = i32::from_be_bytes(bytes[4..].try_into().unwrap());
                match (bytes[2], reported_errno) {
                    (2, n) if n > 0 && error.raw_os_error() == Some(n) => Stage::Cwd,
                    (3, 0) => Stage::Exec,
                    (4, n) if n > 0 && error.raw_os_error() == Some(n) => Stage::Privdrop,
                    (5, n) if n > 0 && error.raw_os_error() == Some(n) => Stage::Cgroup,
                    (6, n) if n > 0 && error.raw_os_error() == Some(n) => Stage::Pty,
                    _ => Stage::Spawn,
                }
            } else {
                Stage::Spawn
            };
        let path = match stage {
            Stage::Cwd => cwd,
            Stage::Exec => Some(argv0),
            Stage::Cgroup => cgroup,
            _ => None,
        }
        .map(str::to_owned);
        SpawnFailure::new(stage, error, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_report_pipe_ends_are_close_on_exec() {
        let (reader, writer) = channel().unwrap();
        assert!(rustix::io::fcntl_getfd(&reader.0)
            .unwrap()
            .contains(rustix::io::FdFlags::CLOEXEC));
        assert!(rustix::io::fcntl_getfd(&writer.0)
            .unwrap()
            .contains(rustix::io::FdFlags::CLOEXEC));
    }

    #[test]
    fn only_an_explicit_exec_marker_attributes_exec() {
        let (reader, writer) = channel().unwrap();
        drop(writer);
        assert_eq!(
            reader
                .failure(
                    io::Error::from_raw_os_error(libc::ENOENT),
                    "missing",
                    None,
                    None
                )
                .stage,
            Stage::Spawn
        );

        let (reader, writer) = channel().unwrap();
        writer.ready_for_exec().unwrap();
        drop(writer);
        let failure = reader.failure(
            io::Error::from_raw_os_error(libc::ENOENT),
            "missing",
            None,
            None,
        );
        assert_eq!(failure.stage, Stage::Exec);
        assert_eq!(failure.path.as_deref(), Some("missing"));
    }

    #[test]
    fn child_failure_preserves_stage_errno_and_path() {
        let (reader, writer) = channel().unwrap();
        let error = writer
            .check(Stage::Cwd, Err(io::Error::from_raw_os_error(libc::ENOTDIR)))
            .unwrap_err();
        drop(writer);
        let failure = reader.failure(error, "echo", Some("a-file"), None);
        assert_eq!(failure.stage, Stage::Cwd);
        assert_eq!(failure.report().reason, "enotdir");
        assert_eq!(failure.report().errno, Some(libc::ENOTDIR));
        assert_eq!(failure.path.as_deref(), Some("a-file"));
    }

    #[test]
    fn mismatched_errno_does_not_claim_an_operation() {
        let (reader, writer) = channel().unwrap();
        writer
            .check(Stage::Cwd, Err(io::Error::from_raw_os_error(libc::EACCES)))
            .unwrap_err();
        drop(writer);
        let failure = reader.failure(
            io::Error::from_raw_os_error(libc::ENOENT),
            "echo",
            Some("cwd"),
            None,
        );
        assert_eq!(failure.stage, Stage::Spawn);
    }
}
