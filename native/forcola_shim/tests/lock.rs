//! `forcola_shim lock <path>`: whole-file fcntl write lock held until stdin closes.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "forcola-lock-test-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn spawn(args: &[&std::ffi::OsStr]) -> (Child, BufReader<ChildStdout>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_forcola_shim"))
        .arg("lock")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    (child, stdout)
}

fn lock(path: &Path) -> (Child, String) {
    let (child, mut stdout) = spawn(&[path.as_os_str()]);
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    (child, line.trim_end().to_string())
}

/// Runs a lock attempt expected to exit on its own; returns its line and status.
fn run_to_exit(args: &[&std::ffi::OsStr]) -> (String, ExitStatus) {
    let (mut child, mut stdout) = spawn(args);
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let mut rest = String::new();
    std::io::Read::read_to_string(&mut stdout, &mut rest).unwrap();
    assert!(
        rest.is_empty(),
        "lock mode wrote more than one line: {rest:?}"
    );
    let status = child.wait().unwrap();
    (line.trim_end().to_string(), status)
}

fn lock_fails(path: &Path) -> (String, ExitStatus) {
    run_to_exit(&[path.as_os_str()])
}

fn release(mut child: Child) -> ExitStatus {
    drop(child.stdin.take());
    child.wait().unwrap()
}

#[test]
fn second_lock_reports_the_holder_pid() {
    let dir = TempDir::new();
    let path = dir.join("layer.lock");

    let (first, line) = lock(&path);
    assert_eq!(line, "ok");

    let (line, status) = lock_fails(&path);
    assert_eq!(line, format!("held {}", first.id()));
    assert_eq!(status.code(), Some(3));

    assert_eq!(release(first).code(), Some(0));
}

#[test]
fn release_on_stdin_eof_allows_reacquire() {
    let dir = TempDir::new();
    let path = dir.join("layer.lock");

    let (first, line) = lock(&path);
    assert_eq!(line, "ok");
    assert_eq!(release(first).code(), Some(0));

    let (second, line) = lock(&path);
    assert_eq!(line, "ok");
    assert_eq!(release(second).code(), Some(0));
}

#[test]
fn any_byte_on_stdin_releases() {
    let dir = TempDir::new();
    let path = dir.join("layer.lock");

    let (mut first, line) = lock(&path);
    assert_eq!(line, "ok");
    // Keep stdin open: the byte alone must end the hold.
    let mut stdin = first.stdin.take().unwrap();
    stdin.write_all(b"release\n").unwrap();
    assert_eq!(first.wait().unwrap().code(), Some(0));
    drop(stdin);

    let (second, line) = lock(&path);
    assert_eq!(line, "ok");
    release(second);
}

#[test]
fn conflicts_with_an_fcntl_lock_held_by_another_program() {
    let dir = TempDir::new();
    let path = dir.join("layer.lock");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .unwrap();

    // SAFETY: zeroed flock is valid; the fd is open for writing.
    let mut lock_req: libc::flock = unsafe { std::mem::zeroed() };
    lock_req.l_type = libc::F_WRLCK as _;
    lock_req.l_whence = libc::SEEK_SET as _;
    assert_eq!(
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock_req) },
        0
    );

    let (line, status) = lock_fails(&path);
    assert_eq!(line, format!("held {}", std::process::id()));
    assert_eq!(status.code(), Some(3));

    drop(file);
    let (holder, line) = lock(&path);
    assert_eq!(line, "ok");
    release(holder);
}

#[test]
fn lock_file_is_created_and_never_modified() {
    let dir = TempDir::new();
    let fresh = dir.join("fresh.lock");
    let (holder, line) = lock(&fresh);
    assert_eq!(line, "ok");
    assert!(fresh.exists());
    release(holder);
    assert!(fresh.exists(), "lock file was removed on release");
    assert_eq!(fs::read(&fresh).unwrap(), b"");

    let existing = dir.join("existing.lock");
    fs::write(&existing, b"pid 123\nkeep me\n").unwrap();
    let before = fs::metadata(&existing).unwrap();

    let (holder, line) = lock(&existing);
    assert_eq!(line, "ok");
    let (line, _status) = lock_fails(&existing);
    assert!(line.starts_with("held "));
    release(holder);

    let after = fs::metadata(&existing).unwrap();
    assert_eq!(fs::read(&existing).unwrap(), b"pid 123\nkeep me\n");
    assert_eq!(
        std::os::unix::fs::MetadataExt::ino(&before),
        std::os::unix::fs::MetadataExt::ino(&after)
    );
}

#[test]
fn open_failure_reports_errno_name() {
    let dir = TempDir::new();
    let path = dir.join("missing-dir").join("layer.lock");
    let (line, status) = lock_fails(&path);
    assert_eq!(line, "error open enoent");
    assert_eq!(status.code(), Some(4));
    assert!(!path.exists());
}

#[test]
fn bad_usage_is_reported() {
    let empty = std::ffi::OsStr::new("");
    let a = std::ffi::OsStr::new("a");
    for args in [&[][..], &[empty][..], &[a, a][..]] {
        let (line, status) = run_to_exit(args);
        assert_eq!(line, "error usage");
        assert_eq!(status.code(), Some(2));
    }
}
