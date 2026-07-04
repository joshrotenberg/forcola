//! Dropping the child to a different user and/or group before exec.
//!
//! The work splits into two halves with a hard rule between them:
//!
//! 1. **Parent, before fork** ([`resolve`]): turn the `user`/`group` names in
//!    a SPAWN payload into a numeric [`Credentials`] (uid, gid, and the
//!    supplementary gid list). This calls `getpwnam_r`/`getgrnam_r`/
//!    `getgrouplist`, which are **not** async-signal-safe and would deadlock
//!    if called after fork in a multithreaded process (a lock could be held by
//!    another thread at fork time). Resolution failures fail closed: the child
//!    is never spawned.
//!
//! 2. **Child, after fork, inside `pre_exec`** ([`Credentials::apply`]): call
//!    only the numeric syscalls `setgroups`, `setgid`, `setuid`, in that fixed
//!    order. Each is fail-closed: any error returns `Err` from the `pre_exec`
//!    closure, aborting exec so the child never runs as the wrong user. Groups
//!    and gid are dropped before uid because after `setuid` the process may
//!    lack the privilege to change them.
//!
//! POSIX-only. The whole feature is compiled for Unix; a `user`/`group` field
//! on a non-Unix target is rejected before we get here.

use crate::protocol::{GroupSpec, UserSpec};
use std::ffi::CString;
use std::io;

/// The numeric identity to install in the child, resolved in the parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    /// Target real/effective uid, or `None` to leave the uid unchanged
    /// (a `group`-only request).
    pub uid: Option<u32>,
    /// Target real/effective gid.
    pub gid: u32,
    /// Supplementary group list to install via `setgroups`. For a `user`
    /// request this is what `getgrouplist` returned; for a `group`-only
    /// request it is just `[gid]`.
    pub groups: Vec<u32>,
}

/// The subset of syscalls the child must actually run, computed in the parent
/// by diffing [`Credentials`] against the shim's current identity. A field is
/// `None` when the current process already satisfies it, so the syscall is
/// skipped: this is what makes a no-op drop to the current user succeed even
/// as a non-root process (where `setgroups`/`setgid`/`setuid` to the *same*
/// identity would still fail with EPERM). A genuinely different target keeps
/// its `Some`, so the syscall runs and fails closed when unprivileged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropPlan {
    groups: Option<Vec<u32>>,
    gid: Option<u32>,
    uid: Option<u32>,
}

/// Resolves the optional `user`/`group` of a SPAWN request into a
/// [`DropPlan`], or `Ok(None)` when neither was given (the default path).
///
/// Runs in the parent, before fork: it resolves names to numeric ids and then
/// diffs them against the shim's current identity so a no-op request skips the
/// privileged syscalls. Any lookup failure is returned as an `Err` so the
/// caller can fail closed and never spawn.
pub fn resolve(user: Option<&UserSpec>, group: Option<&GroupSpec>) -> io::Result<Option<DropPlan>> {
    match resolve_credentials(user, group)? {
        None => Ok(None),
        Some(creds) => Ok(Some(plan_from_current(creds))),
    }
}

/// Resolves `user`/`group` to numeric [`Credentials`] without diffing against
/// the current identity. Split out so it can be unit-tested directly.
fn resolve_credentials(
    user: Option<&UserSpec>,
    group: Option<&GroupSpec>,
) -> io::Result<Option<Credentials>> {
    match (user, group) {
        (None, None) => Ok(None),

        // group only: set the gid (and clear supplementary groups to just
        // that gid) without changing the uid.
        (None, Some(group)) => {
            let gid = resolve_gid(group)?;
            Ok(Some(Credentials {
                uid: None,
                gid,
                groups: vec![gid],
            }))
        }

        // user given: derive uid, primary gid, and the supplementary list
        // from the passwd/group database. A `group` overrides the primary
        // gid but leaves the supplementary list as the user's.
        (Some(user), group) => {
            let resolved = resolve_user(user)?;
            let gid = match group {
                Some(group) => resolve_gid(group)?,
                None => resolved.primary_gid,
            };
            let groups = supplementary_groups(&resolved, gid);
            Ok(Some(Credentials {
                uid: Some(resolved.uid),
                gid,
                groups,
            }))
        }
    }
}

/// Diffs `creds` against the shim's current uid and gid, producing a
/// [`DropPlan`] that omits any syscall the process already satisfies. This runs
/// in the parent so the child only ever runs the numeric setters.
///
/// The key case is a no-op drop to the current user: when the request changes
/// neither the uid nor the gid, there is nothing to drop, so all three setters
/// (including `setgroups`) are skipped. This matters because `setgroups`
/// requires privilege even to install the *same* set, and because the group
/// list resolved from the passwd/group database can legitimately differ from
/// the process's live `getgroups()` set (notably on macOS, where directory
/// membership is broader than the kernel's capped supplementary set): forcing
/// it on a no-op would fail closed for no reason.
///
/// When the request *does* change the uid or gid, a real drop was asked for, so
/// `setgroups` is kept; it runs and fails closed if the shim is unprivileged.
fn plan_from_current(creds: Credentials) -> DropPlan {
    let cur_uid = unsafe { libc::getuid() } as u32;
    let cur_gid = unsafe { libc::getgid() } as u32;

    let uid = creds.uid.filter(|u| *u != cur_uid);
    let gid = Some(creds.gid).filter(|g| *g != cur_gid);

    // A genuine drop changes the uid or the gid. If neither changes, the whole
    // request is a no-op: skip setgroups too.
    let groups = if uid.is_some() || gid.is_some() {
        Some(creds.groups)
    } else {
        None
    };

    DropPlan { groups, gid, uid }
}

/// A user resolved from the passwd database, plus the supplementary group
/// list `getgrouplist` returns for that user's name and primary gid.
struct ResolvedUser {
    uid: u32,
    primary_gid: u32,
    supplementary: Vec<u32>,
}

/// Builds the final supplementary group list for the child. When a `group`
/// override replaces the primary gid, that gid is folded into the list so it
/// is present as a supplementary group too (matching how a login would see
/// the primary gid in its group set).
fn supplementary_groups(user: &ResolvedUser, effective_gid: u32) -> Vec<u32> {
    let mut groups = user.supplementary.clone();
    if !groups.contains(&effective_gid) {
        groups.push(effective_gid);
    }
    groups
}

/// Resolves a [`UserSpec`] to a uid, primary gid, and supplementary group
/// list. A numeric spec still goes through the passwd database so its primary
/// gid and groups can be filled in; if the uid is not in the database we fall
/// back to using the uid as its own gid with no supplementary groups, so a
/// bare numeric uid remains usable.
fn resolve_user(user: &UserSpec) -> io::Result<ResolvedUser> {
    match user {
        UserSpec::Name(name) => {
            let cname = cstring(name)?;
            let (uid, primary_gid) = getpwnam(&cname)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("unknown user: {name}"))
            })?;
            let supplementary = getgrouplist(&cname, primary_gid)?;
            Ok(ResolvedUser {
                uid,
                primary_gid,
                supplementary,
            })
        }
        UserSpec::Id(uid) => match getpwuid(*uid)? {
            Some((name, primary_gid)) => {
                let supplementary = getgrouplist(&name, primary_gid)?;
                Ok(ResolvedUser {
                    uid: *uid,
                    primary_gid,
                    supplementary,
                })
            }
            None => Ok(ResolvedUser {
                uid: *uid,
                primary_gid: *uid,
                supplementary: Vec::new(),
            }),
        },
    }
}

/// Resolves a [`GroupSpec`] to a numeric gid.
fn resolve_gid(group: &GroupSpec) -> io::Result<u32> {
    match group {
        GroupSpec::Name(name) => {
            let cname = cstring(name)?;
            getgrnam(&cname)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("unknown group: {name}"))
            })
        }
        GroupSpec::Id(gid) => Ok(*gid),
    }
}

fn cstring(s: &str) -> io::Result<CString> {
    CString::new(s).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "user/group name contains an interior NUL byte",
        )
    })
}

impl DropPlan {
    /// Installs the planned identity in the current process. Called inside
    /// `pre_exec`, so it touches only the numeric, async-signal-safe syscalls
    /// `setgroups`, `setgid`, `setuid`, in that fixed order. Groups and gid are
    /// dropped before uid because after `setuid` the process may lack the
    /// privilege to change them. Each is fail-closed: the first error returns
    /// `Err`, aborting exec so the child never runs as the wrong user.
    ///
    /// A field the parent found already satisfied is `None` and its syscall is
    /// skipped, which is what lets a no-op drop to the current user succeed
    /// without privilege.
    ///
    /// # Safety
    ///
    /// Must run after fork and before exec (the `pre_exec` contract). All ids
    /// were resolved in the parent; this call allocates nothing beyond reading
    /// the moved plan and does no non-signal-safe work.
    pub fn apply(&self) -> io::Result<()> {
        // 1. Supplementary groups first: must precede setuid, since after
        //    setuid the process may lack the privilege to call setgroups.
        if let Some(groups) = &self.groups {
            let gids: Vec<libc::gid_t> = groups.iter().map(|g| *g as libc::gid_t).collect();
            // Safety: gids points at a live, correctly-sized slice.
            if unsafe { libc::setgroups(gids.len() as _, gids.as_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }

        // 2. Primary gid, again before setuid.
        if let Some(gid) = self.gid {
            // Safety: plain numeric syscall.
            if unsafe { libc::setgid(gid as libc::gid_t) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }

        // 3. uid last: after this the process runs as the target user and can
        //    no longer regain group privileges.
        if let Some(uid) = self.uid {
            // Safety: plain numeric syscall.
            if unsafe { libc::setuid(uid as libc::uid_t) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(())
    }
}

// --- passwd/group database lookups (parent side only) --------------------

/// `getpwnam_r`: username -> (uid, primary gid). `Ok(None)` if the user does
/// not exist; `Err` on a lookup error.
fn getpwnam(name: &CString) -> io::Result<Option<(u32, u32)>> {
    with_pwd(|pwd, buf, len, result| unsafe {
        libc::getpwnam_r(name.as_ptr(), pwd, buf, len, result)
    })
}

/// `getpwuid_r`: uid -> (username, primary gid). `Ok(None)` if the uid is not
/// in the database.
fn getpwuid(uid: u32) -> io::Result<Option<(CString, u32)>> {
    let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut buf = vec![0 as libc::c_char; pwd_buf_size()];
    let mut result: *mut libc::passwd = std::ptr::null_mut();

    let rc = unsafe {
        libc::getpwuid_r(
            uid as libc::uid_t,
            &mut pwd,
            buf.as_mut_ptr(),
            buf.len(),
            &mut result,
        )
    };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    if result.is_null() {
        return Ok(None);
    }
    let name = unsafe { CString::from(std::ffi::CStr::from_ptr(pwd.pw_name)) };
    Ok(Some((name, pwd.pw_gid as u32)))
}

/// Shared `getpwnam_r`-shaped call: runs `call`, growing the buffer on
/// ERANGE, and maps the result to `(uid, gid)`.
fn with_pwd<F>(mut call: F) -> io::Result<Option<(u32, u32)>>
where
    F: FnMut(
        *mut libc::passwd,
        *mut libc::c_char,
        libc::size_t,
        *mut *mut libc::passwd,
    ) -> libc::c_int,
{
    let mut size = pwd_buf_size();
    loop {
        let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut buf = vec![0 as libc::c_char; size];
        let mut result: *mut libc::passwd = std::ptr::null_mut();

        let rc = call(&mut pwd, buf.as_mut_ptr(), buf.len(), &mut result);
        if rc == libc::ERANGE && size < 1 << 20 {
            size *= 2;
            continue;
        }
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        if result.is_null() {
            return Ok(None);
        }
        return Ok(Some((pwd.pw_uid as u32, pwd.pw_gid as u32)));
    }
}

/// `getgrnam_r`: group name -> gid. `Ok(None)` if the group does not exist.
fn getgrnam(name: &CString) -> io::Result<Option<u32>> {
    let mut size = grp_buf_size();
    loop {
        let mut grp: libc::group = unsafe { std::mem::zeroed() };
        let mut buf = vec![0 as libc::c_char; size];
        let mut result: *mut libc::group = std::ptr::null_mut();

        let rc = unsafe {
            libc::getgrnam_r(
                name.as_ptr(),
                &mut grp,
                buf.as_mut_ptr(),
                buf.len(),
                &mut result,
            )
        };
        if rc == libc::ERANGE && size < 1 << 20 {
            size *= 2;
            continue;
        }
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        if result.is_null() {
            return Ok(None);
        }
        return Ok(Some(grp.gr_gid as u32));
    }
}

/// `getgrouplist`: the supplementary gid list for a user name and primary
/// gid. An empty user name (a bare numeric uid not in the database) yields an
/// empty list rather than a lookup.
fn getgrouplist(name: &CString, primary_gid: u32) -> io::Result<Vec<u32>> {
    if name.as_bytes().is_empty() {
        return Ok(Vec::new());
    }

    // getgrouplist takes an in/out count and fills a caller-sized array. Start
    // generous and grow on truncation (a negative return, or the count coming
    // back larger than the buffer).
    let mut ngroups: libc::c_int = 32;
    loop {
        let mut gids = vec![0 as GetGroupListGid; ngroups as usize];
        let mut count = ngroups;
        let rc = unsafe {
            libc::getgrouplist(
                name.as_ptr(),
                primary_gid as GetGroupListGid,
                gids.as_mut_ptr(),
                &mut count,
            )
        };
        if rc < 0 || count > ngroups {
            // Truncated: grow to the reported count (or double) and retry.
            ngroups = count.max(ngroups.saturating_mul(2));
            if ngroups as usize > (1 << 20) {
                return Err(io::Error::other("supplementary group list too large"));
            }
            continue;
        }
        gids.truncate(count as usize);
        // The cast is a no-op on Linux (GetGroupListGid is u32) but a real
        // c_int -> u32 conversion on macOS/BSD, so it must stay.
        #[allow(clippy::unnecessary_cast)]
        return Ok(gids.into_iter().map(|g| g as u32).collect());
    }
}

/// `getgrouplist`'s group-id array element type differs by platform: `gid_t`
/// on Linux, `c_int` on macOS/BSD.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
type GetGroupListGid = libc::c_int;
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
type GetGroupListGid = libc::gid_t;

/// Initial passwd buffer size, from `sysconf(_SC_GETPW_R_SIZE_MAX)` with a
/// sane fallback when it is unavailable or reports no limit.
fn pwd_buf_size() -> usize {
    sysconf_size(libc::_SC_GETPW_R_SIZE_MAX, 16 * 1024)
}

/// Initial group buffer size, from `sysconf(_SC_GETGR_R_SIZE_MAX)`.
fn grp_buf_size() -> usize {
    sysconf_size(libc::_SC_GETGR_R_SIZE_MAX, 16 * 1024)
}

fn sysconf_size(name: libc::c_int, fallback: usize) -> usize {
    let v = unsafe { libc::sysconf(name) };
    if v <= 0 {
        fallback
    } else {
        v as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_none_is_default_path() {
        assert_eq!(resolve_credentials(None, None).unwrap(), None);
        assert_eq!(resolve(None, None).unwrap(), None);
    }

    #[test]
    fn numeric_group_only_sets_gid_and_clears_supplementary() {
        let creds = resolve_credentials(None, Some(&GroupSpec::Id(20)))
            .unwrap()
            .unwrap();
        assert_eq!(creds.uid, None);
        assert_eq!(creds.gid, 20);
        assert_eq!(creds.groups, vec![20]);
    }

    #[test]
    fn numeric_user_and_group_override() {
        // A numeric user not in the database falls back to uid==gid, and the
        // group override replaces the gid and forms the supplementary list.
        let creds = resolve_credentials(Some(&UserSpec::Id(4242)), Some(&GroupSpec::Id(99)))
            .unwrap()
            .unwrap();
        assert_eq!(creds.uid, Some(4242));
        assert_eq!(creds.gid, 99);
        assert_eq!(creds.groups, vec![99]);
    }

    #[test]
    fn unknown_user_name_fails_closed() {
        let err = resolve_credentials(
            Some(&UserSpec::Name("forcola-no-such-user-xyz".to_string())),
            None,
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn unknown_group_name_fails_closed() {
        let err = resolve_credentials(
            None,
            Some(&GroupSpec::Name("forcola-no-such-group-xyz".to_string())),
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn root_user_name_resolves_to_uid_zero() {
        // "root" exists on every POSIX system with uid 0.
        let creds = resolve_credentials(Some(&UserSpec::Name("root".to_string())), None)
            .unwrap()
            .unwrap();
        assert_eq!(creds.uid, Some(0));
        // root's primary gid is 0 on Linux and macOS; either way the gid is
        // in the supplementary list.
        assert!(creds.groups.contains(&creds.gid));
    }

    #[test]
    fn plan_is_a_full_noop_for_the_current_identity() {
        // Requesting the current uid and gid changes nothing, so every setter
        // (including setgroups) is skipped: a no-op drop must succeed even as
        // a non-root process, where setgroups to the same set would EPERM.
        let cur_uid = unsafe { libc::getuid() } as u32;
        let cur_gid = unsafe { libc::getgid() } as u32;
        let plan = plan_from_current(Credentials {
            uid: Some(cur_uid),
            gid: cur_gid,
            groups: vec![cur_gid, 999_999],
        });
        assert_eq!(plan.uid, None, "uid to the current user must be skipped");
        assert_eq!(plan.gid, None, "gid to the current group must be skipped");
        assert_eq!(
            plan.groups, None,
            "a no-op drop must skip setgroups entirely, whatever the resolved set"
        );
    }

    #[test]
    fn plan_keeps_all_setters_for_a_genuinely_different_uid() {
        // A different uid is a real drop: setuid is kept, and so is setgroups
        // so the supplementary set is actually installed. Both run and fail
        // closed when unprivileged.
        let cur_uid = unsafe { libc::getuid() } as u32;
        let other = cur_uid.wrapping_add(1);
        let plan = plan_from_current(Credentials {
            uid: Some(other),
            gid: unsafe { libc::getgid() } as u32,
            groups: vec![10, 20],
        });
        assert_eq!(
            plan.uid,
            Some(other),
            "a different uid must be kept so setuid runs and fails closed"
        );
        assert_eq!(
            plan.groups,
            Some(vec![10, 20]),
            "a real drop must keep setgroups so the supplementary set is installed"
        );
    }

    #[test]
    fn plan_keeps_setgroups_when_only_the_gid_changes() {
        // A group-only change (uid unchanged) is still a real drop, so
        // setgroups and setgid are kept.
        let cur_gid = unsafe { libc::getgid() } as u32;
        let other_gid = cur_gid.wrapping_add(1);
        let plan = plan_from_current(Credentials {
            uid: None,
            gid: other_gid,
            groups: vec![other_gid],
        });
        assert_eq!(plan.uid, None);
        assert_eq!(plan.gid, Some(other_gid));
        assert_eq!(plan.groups, Some(vec![other_gid]));
    }
}
