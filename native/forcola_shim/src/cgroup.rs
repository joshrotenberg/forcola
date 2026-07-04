//! Opt-in Linux cgroup v2 containment.
//!
//! The process-group kill (`kill(-pgid, ...)`) reaches every descendant that
//! stays in the child's process group, but a target that deliberately
//! daemonizes (double-fork plus `setsid`) leaves the group and survives. On
//! Linux with cgroup v2 this module adds a backstop: the child is placed in a
//! dedicated child cgroup before exec, so every descendant it forks inherits
//! the cgroup regardless of process-group games, and on kill the shim writes
//! `cgroup.kill` to SIGKILL the whole subtree at once.
//!
//! The work splits, like [`crate::privdrop`], into a parent half and a child
//! half with a hard rule between them:
//!
//! 1. **Parent, before fork** ([`prepare`]): detect cgroup v2 (unified mount
//!    with a readable `cgroup.controllers`), read the shim's own cgroup from
//!    `/proc/self/cgroup`, verify the subtree is delegated (a child dir can be
//!    created), create `forcola-<pid>-<n>`, and open its `cgroup.procs` for
//!    writing. All of this does allocation and fallible I/O, which is fine in
//!    the parent. Any failure degrades cleanly to `None` (process-group kill
//!    only) with a warning; it is never an error.
//!
//! 2. **Child, after fork, inside `pre_exec`** ([`Placement::join_in_child`]):
//!    write the child's own pid to the parent-opened `cgroup.procs` fd. This is
//!    the only cgroup work done in the child, and it is async-signal-safe: no
//!    heap allocation, the pid is formatted into a stack buffer by hand, and
//!    the write goes to an already-open fd. Moving the child in from the parent
//!    after spawn would leave a window a fast double-fork could escape, so the
//!    child moves itself before it can fork anything.
//!
//! On kill the supervisor calls [`Cgroup::kill`] to write `cgroup.kill`, then
//! [`Cgroup::drained`] to confirm `cgroup.procs` is empty, then [`Cgroup::remove`]
//! to `rmdir` the now-empty cgroup. These are layered on top of the existing
//! process-group kill, never in place of it.
//!
//! Everything here is Linux-only. On other platforms [`prepare`] is a no-op
//! that returns `None`, so `cgroup: true` degrades to process-group kill.

#[cfg(target_os = "linux")]
mod imp {
    use std::fs::{File, OpenOptions};
    use std::io::{self, Read, Write};
    use std::os::fd::{AsRawFd, OwnedFd, RawFd};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// The unified cgroup v2 mount point. A cgroup v2 system mounts the unified
    /// hierarchy here and exposes `cgroup.controllers` at its root.
    const CGROUP_ROOT: &str = "/sys/fs/cgroup";

    /// A prepared child cgroup: the directory the shim created plus the open
    /// `cgroup.procs` fd the child writes its pid to. Held by the supervisor
    /// for the child's lifetime; dropping it (without [`Cgroup::remove`]) leaves
    /// the directory behind, so the supervisor always removes it after drain.
    pub struct Cgroup {
        dir: PathBuf,
        /// Open write fd for `<dir>/cgroup.procs`. The child writes its own pid
        /// here inside `pre_exec`; the supervisor keeps it open so the fd is
        /// valid across the fork.
        procs: OwnedFd,
    }

    /// The child-side handle: just the raw `cgroup.procs` fd, moved into the
    /// `pre_exec` closure. Kept separate from [`Cgroup`] so the closure captures
    /// a plain `RawFd` and does no allocation or drop work.
    #[derive(Clone, Copy)]
    pub struct Placement {
        procs_fd: RawFd,
    }

    impl Cgroup {
        /// The child-side placement handle to move into `pre_exec`.
        pub fn placement(&self) -> Placement {
            Placement {
                procs_fd: self.procs.as_raw_fd(),
            }
        }

        /// Writes `1` to `cgroup.kill`, SIGKILLing every process in the subtree
        /// at once (Linux 5.14+). Best-effort: an error (for example an older
        /// kernel without `cgroup.kill`) is ignored, because the process-group
        /// kill has already run and is the primary mechanism.
        pub fn kill(&self) {
            let path = self.dir.join("cgroup.kill");
            if let Ok(mut f) = OpenOptions::new().write(true).open(path) {
                let _ = f.write_all(b"1");
            }
        }

        /// Is the cgroup drained (no pids left in `cgroup.procs`)? Used, in
        /// addition to the process-group death probe, to confirm the contained
        /// subtree is gone before the EXIT frame. A read error is treated as
        /// "not drained" so the caller keeps waiting up to its deadline rather
        /// than removing a live cgroup.
        pub fn drained(&self) -> bool {
            match std::fs::read_to_string(self.dir.join("cgroup.procs")) {
                Ok(contents) => contents.split_whitespace().next().is_none(),
                Err(_) => false,
            }
        }

        /// `rmdir`s the (now-empty) cgroup directory. Best-effort: a non-empty
        /// or already-removed directory just leaves cleanup to the parent
        /// delegation owner. Called after [`Self::drained`] reports empty.
        pub fn remove(&self) {
            let _ = std::fs::remove_dir(&self.dir);
        }
    }

    impl Placement {
        /// Writes the current pid to the open `cgroup.procs` fd, moving this
        /// process (and therefore every descendant it later forks) into the
        /// cgroup.
        ///
        /// # Safety
        ///
        /// Runs after fork, inside `pre_exec`, before exec. It is
        /// async-signal-safe: it allocates nothing, formats the pid into a
        /// stack buffer with [`format_pid`], and issues a single `write(2)` to
        /// an fd the parent already opened. A write failure returns `Err`,
        /// aborting exec, so a child that could not be contained never runs
        /// (fail-closed, matching the privdrop drop).
        pub fn join_in_child(&self) -> io::Result<()> {
            // `getpid()` is async-signal-safe. Format it without allocating.
            let pid = unsafe { libc::getpid() };
            let mut buf = [0u8; MAX_PID_DIGITS];
            let bytes = format_pid(pid, &mut buf);

            // A single write of the pid decimal string to cgroup.procs.
            let n = unsafe {
                libc::write(
                    self.procs_fd,
                    bytes.as_ptr() as *const libc::c_void,
                    bytes.len(),
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    /// Widest decimal form of a 32-bit pid ("-2147483648") is 11 bytes.
    const MAX_PID_DIGITS: usize = 11;

    /// Formats a pid into `buf` as decimal ASCII with no heap allocation,
    /// returning the filled slice. Async-signal-safe: no allocation, no locks,
    /// no library formatting machinery. pids are non-negative in practice, but
    /// the sign is handled so the function is total over `pid_t`.
    fn format_pid(pid: libc::pid_t, buf: &mut [u8; MAX_PID_DIGITS]) -> &[u8] {
        // Work in the widest unsigned form to handle pid_t::MIN without
        // overflow when negating.
        let negative = pid < 0;
        let mut value = if negative {
            (pid as i64).unsigned_abs()
        } else {
            pid as u64
        };

        // Fill from the end of the buffer backward.
        let mut i = buf.len();
        if value == 0 {
            i -= 1;
            buf[i] = b'0';
        } else {
            while value > 0 {
                i -= 1;
                buf[i] = b'0' + (value % 10) as u8;
                value /= 10;
            }
        }
        if negative {
            i -= 1;
            buf[i] = b'-';
        }
        &buf[i..]
    }

    /// Monotonic counter making each shim's child cgroup name unique even if
    /// one shim (hypothetically) prepared more than one.
    static SEQ: AtomicU32 = AtomicU32::new(0);

    /// Parent-side setup. Returns `Some(Cgroup)` when a delegated cgroup v2
    /// subtree is available and a child cgroup was created; `None` (with a
    /// warning on stderr) otherwise, so the caller falls back to process-group
    /// kill. Never errors.
    pub fn prepare() -> Option<Cgroup> {
        match try_prepare() {
            Ok(cg) => Some(cg),
            Err(reason) => {
                eprintln!(
                    "forcola_shim: cgroup containment unavailable ({reason}); \
                     falling back to process-group kill"
                );
                None
            }
        }
    }

    /// The fallible core of [`prepare`], separated so each failure carries a
    /// reason for the warning.
    fn try_prepare() -> Result<Cgroup, String> {
        if !is_cgroup2() {
            return Err("no cgroup v2 unified hierarchy".into());
        }

        let own = own_cgroup_dir().map_err(|e| format!("cannot read own cgroup: {e}"))?;

        // Delegation / writability check plus the actual child-dir creation are
        // the same operation: if we can mkdir a child here, the subtree is
        // delegated to us. A failure (EACCES/EROFS/EPERM) means no delegation.
        let dir = create_child_dir(&own).map_err(|e| format!("subtree not delegated: {e}"))?;

        // Open cgroup.procs for writing now, in the parent, so the fd is valid
        // across the fork and the child writes its pid to an already-open fd.
        let procs = OpenOptions::new()
            .write(true)
            .open(dir.join("cgroup.procs"))
            .map_err(|e| {
                // Roll back the directory we just made so a half-prepared
                // cgroup is not left behind.
                let _ = std::fs::remove_dir(&dir);
                format!("cannot open cgroup.procs: {e}")
            })?;

        Ok(Cgroup {
            dir,
            procs: procs.into(),
        })
    }

    /// Detects a cgroup v2 unified hierarchy: the root mount exposes a
    /// `cgroup.controllers` file, which exists only under cgroup v2. (A
    /// v1/hybrid system has controller-specific subdirectories instead.)
    fn is_cgroup2() -> bool {
        Path::new(CGROUP_ROOT).join("cgroup.controllers").exists()
    }

    /// Resolves the shim's own cgroup directory by parsing `/proc/self/cgroup`
    /// and joining the path onto the unified mount root.
    fn own_cgroup_dir() -> io::Result<PathBuf> {
        let mut contents = String::new();
        File::open("/proc/self/cgroup")?.read_to_string(&mut contents)?;
        let rel = parse_own_cgroup(&contents)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no cgroup v2 line"))?;
        // The parsed path is absolute-from-the-mount ("/user.slice/..."); strip
        // the leading slash and join onto the mount root.
        Ok(Path::new(CGROUP_ROOT).join(rel.trim_start_matches('/')))
    }

    /// Creates a uniquely named child cgroup directory under `parent`. The name
    /// combines the shim pid and a monotonic counter so concurrent shims (and a
    /// single shim preparing more than once) never collide.
    fn create_child_dir(parent: &Path) -> io::Result<PathBuf> {
        let pid = std::process::id();
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = parent.join(format!("forcola-{pid}-{seq}"));
        std::fs::create_dir(&dir)?;
        Ok(dir)
    }

    /// Parses the cgroup v2 line out of `/proc/self/cgroup`. In the unified
    /// hierarchy the v2 entry has an empty controller field, so the line looks
    /// like `0::/user.slice/...`; the returned value is the path after `0::`.
    ///
    /// Split out from the I/O so it can be unit-tested against fixed input.
    pub(super) fn parse_own_cgroup(contents: &str) -> Option<String> {
        for line in contents.lines() {
            // Format: hierarchy-ID:controller-list:cgroup-path. The v2 line has
            // hierarchy-ID 0 and an empty controller list.
            let mut parts = line.splitn(3, ':');
            let hid = parts.next()?;
            let controllers = parts.next()?;
            let path = parts.next()?;
            if hid == "0" && controllers.is_empty() {
                return Some(path.to_string());
            }
        }
        None
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn format_pid_typical() {
            let mut buf = [0u8; MAX_PID_DIGITS];
            assert_eq!(format_pid(1, &mut buf), b"1");
            assert_eq!(format_pid(42, &mut buf), b"42");
            assert_eq!(format_pid(99999, &mut buf), b"99999");
        }

        #[test]
        fn format_pid_zero() {
            let mut buf = [0u8; MAX_PID_DIGITS];
            assert_eq!(format_pid(0, &mut buf), b"0");
        }

        #[test]
        fn format_pid_max_i32() {
            let mut buf = [0u8; MAX_PID_DIGITS];
            assert_eq!(format_pid(i32::MAX as libc::pid_t, &mut buf), b"2147483647");
        }

        #[test]
        fn format_pid_matches_std_formatting() {
            // Cross-check the hand-rolled formatter against std for a spread of
            // values, since the child path cannot use std formatting.
            for pid in [1, 2, 7, 10, 100, 1000, 32768, 100_000, 1_000_000] {
                let mut buf = [0u8; MAX_PID_DIGITS];
                let got = format_pid(pid as libc::pid_t, &mut buf);
                assert_eq!(got, pid.to_string().as_bytes(), "pid {pid}");
            }
        }

        #[test]
        fn parse_own_cgroup_unified() {
            // Pure cgroup v2: a single 0:: line.
            let contents = "0::/user.slice/user-1000.slice/session-3.scope\n";
            assert_eq!(
                parse_own_cgroup(contents).as_deref(),
                Some("/user.slice/user-1000.slice/session-3.scope")
            );
        }

        #[test]
        fn parse_own_cgroup_hybrid_picks_v2_line() {
            // A hybrid system lists v1 controllers first, then the v2 0:: line.
            let contents = "12:pids:/user.slice\n\
                            1:name=systemd:/user.slice/session-3.scope\n\
                            0::/user.slice/user-1000.slice/session-3.scope\n";
            assert_eq!(
                parse_own_cgroup(contents).as_deref(),
                Some("/user.slice/user-1000.slice/session-3.scope")
            );
        }

        #[test]
        fn parse_own_cgroup_root() {
            assert_eq!(parse_own_cgroup("0::/\n").as_deref(), Some("/"));
        }

        #[test]
        fn parse_own_cgroup_none_when_only_v1() {
            // cgroup v1 only: no 0:: line, so there is nothing to contain in.
            let contents = "12:pids:/user.slice\n1:name=systemd:/user.slice\n";
            assert_eq!(parse_own_cgroup(contents), None);
        }

        #[test]
        fn parse_own_cgroup_empty() {
            assert_eq!(parse_own_cgroup(""), None);
        }
    }
}

// --- Public surface, uniform across platforms ----------------------------
//
// On Linux the real implementation is used. On every other platform the type
// is an uninhabited stand-in and `prepare` always returns `None`, so
// `cgroup: true` degrades to the process-group kill with no cfg noise at the
// call sites.

#[cfg(target_os = "linux")]
pub use imp::{Cgroup, Placement};

/// Parent-side setup for cgroup containment. On Linux, attempts to create a
/// delegated child cgroup and returns `Some(Cgroup)` on success or `None` (with
/// a warning) on any failure. On non-Linux platforms this is always `None`.
#[cfg(target_os = "linux")]
pub fn prepare() -> Option<Cgroup> {
    imp::prepare()
}

#[cfg(not(target_os = "linux"))]
mod stub {
    /// Uninhabited on non-Linux: containment is never active, so no method of
    /// this type is ever called. It exists only to give the supervisor a
    /// uniform `Option<Cgroup>` to hold.
    pub enum Cgroup {}

    impl Cgroup {
        /// The child-side placement handle. Unreachable: a `Cgroup` value never
        /// exists on non-Linux.
        pub fn placement(&self) -> Placement {
            match *self {}
        }

        /// Unreachable on non-Linux.
        pub fn kill(&self) {
            match *self {}
        }

        /// Unreachable on non-Linux.
        pub fn drained(&self) -> bool {
            match *self {}
        }

        /// Unreachable on non-Linux.
        pub fn remove(&self) {
            match *self {}
        }
    }

    /// Uninhabited child-side handle, mirroring the Linux `Placement`.
    #[derive(Clone, Copy)]
    pub enum Placement {}

    impl Placement {
        /// Unreachable on non-Linux.
        pub fn join_in_child(&self) -> std::io::Result<()> {
            match *self {}
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub use stub::{Cgroup, Placement};

/// Non-Linux stand-in: containment is never available, so this always returns
/// `None` and `cgroup: true` falls back to the process-group kill.
#[cfg(not(target_os = "linux"))]
pub fn prepare() -> Option<Cgroup> {
    None
}
