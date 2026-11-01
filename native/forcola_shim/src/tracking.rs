//! Opt-in descendant discovery, checked signalling, and bounded escalation.
//!
//! Process anchors survive reparenting and group changes. Group identities are
//! separate: a missing or replaced leader never authorizes a group signal.
//! Linux adopts orphans before discovery; macOS can only retain sampled trees.

use crate::identity::{self, ProcessIdentity, ProcessRecord};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::time::{Duration, Instant};

pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(50);
pub const MAX_GROUPS: usize = 4096;
pub const MAX_PROCESSES: usize = 65_536;
const CONFIRM_GRACE: Duration = Duration::from_millis(500);

pub fn scope() -> &'static str {
    if cfg!(target_os = "linux") {
        "tree_contained"
    } else {
        "tree_sampled"
    }
}

pub fn available() -> bool {
    platform::probe().is_ok()
        && identity::read(std::process::id()).is_ok()
        && platform::children(std::process::id()).is_ok()
}

pub fn prepare() -> io::Result<ProcessIdentity> {
    platform::prepare()?;
    identity::read(std::process::id())
}

trait Source {
    fn read(&mut self, pid: u32) -> io::Result<Option<ProcessRecord>>;
    fn children(&mut self, pid: u32) -> io::Result<Vec<u32>>;
    fn signal(&mut self, target: i32, signal: i32) -> io::Result<()>;
    fn reap(&mut self, pid: u32) -> io::Result<()>;
    fn now(&self, reference: Instant) -> Instant {
        Instant::now().max(reference)
    }
}

pub struct Native;

fn disappeared(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::ESRCH | libc::ENOENT))
}

impl Source for Native {
    fn read(&mut self, pid: u32) -> io::Result<Option<ProcessRecord>> {
        match identity::read_process(pid) {
            Ok(record) => Ok(Some(record)),
            Err(error) if disappeared(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn children(&mut self, pid: u32) -> io::Result<Vec<u32>> {
        platform::children(pid)
    }

    fn signal(&mut self, target: i32, signal: i32) -> io::Result<()> {
        // SAFETY: callers validate positive pid bounds and current identities;
        // negative targets denote only independently verified group leaders.
        if unsafe { libc::kill(target, signal) } == 0 {
            Ok(())
        } else {
            let error = io::Error::last_os_error();
            if disappeared(&error) {
                Ok(())
            } else {
                Err(error)
            }
        }
    }

    fn reap(&mut self, pid: u32) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let mut status = 0;
            loop {
                // Only this exact adopted child, never the root waiter's pid.
                let result = unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) };
                if result >= 0 {
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ECHILD) {
                    return Ok(());
                }
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Interrupt,
    Terminate,
    Kill,
}

#[derive(Clone, Copy)]
struct Sequence {
    phase: Phase,
    next: Instant,
}

struct Cleanup {
    interrupt: Option<Duration>,
    term: Duration,
    deadline: Instant,
    empty_scans: u8,
}

pub struct Progress {
    pub done: bool,
    pub confirmed: bool,
}

pub struct Tracker<S = Native> {
    source: S,
    shim: ProcessIdentity,
    root_pid: u32,
    processes: HashMap<u32, ProcessRecord>,
    groups: HashMap<ProcessIdentity, Option<Sequence>>,
    members: HashMap<ProcessIdentity, Sequence>,
    pending: Vec<ProcessIdentity>,
    cleanup: Option<Cleanup>,
    uncertain: bool,
    group_limit: usize,
    process_limit: usize,
    adopt_orphans: bool,
}

impl Tracker<Native> {
    pub fn new(shim: ProcessIdentity, root: ProcessIdentity) -> Self {
        Self::with_source(Native, shim, root)
    }

    pub fn failed_start(shim: ProcessIdentity, root_pid: u32) -> Self {
        Self::empty(Native, shim, root_pid)
    }
}

#[allow(private_bounds)]
impl<S: Source> Tracker<S> {
    fn with_source(source: S, shim: ProcessIdentity, root: ProcessIdentity) -> Self {
        let mut tracker = Self::empty(source, shim, root.pid);
        tracker.processes.insert(
            root.pid,
            ProcessRecord {
                identity: root.clone(),
                ppid: tracker.shim.pid,
                exited: false,
            },
        );
        tracker.groups.insert(root.clone(), None);
        tracker.pending.push(root);
        tracker
    }

    fn empty(source: S, shim: ProcessIdentity, root_pid: u32) -> Self {
        Self {
            source,
            root_pid,
            processes: HashMap::new(),
            shim,
            groups: HashMap::new(),
            members: HashMap::new(),
            pending: Vec::new(),
            cleanup: None,
            uncertain: false,
            group_limit: MAX_GROUPS,
            process_limit: MAX_PROCESSES,
            adopt_orphans: cfg!(target_os = "linux"),
        }
    }

    pub fn uncertain(&self) -> bool {
        self.uncertain
    }
    pub fn root_known(&self) -> bool {
        self.processes.contains_key(&self.root_pid)
    }
    pub fn fail(&mut self) {
        self.uncertain = true;
    }
    pub fn take_new_groups(&mut self) -> Vec<ProcessIdentity> {
        std::mem::take(&mut self.pending)
    }
    pub fn cleaning(&self) -> bool {
        self.cleanup.is_some()
    }
    pub fn expired(&self, now: Instant) -> bool {
        self.cleanup
            .as_ref()
            .is_some_and(|cleanup| now >= cleanup.deadline)
    }

    pub fn finish_scan(&mut self) -> bool {
        self.scan();
        let live: Vec<_> = self
            .processes
            .values()
            .filter(|p| !p.exited)
            .map(|p| p.identity.clone())
            .collect();
        if !live.is_empty() {
            self.fail();
            // A late adoption found after the final confirmation cannot be
            // reported as complete. Make a last checked kill within the bound.
            for member in live {
                self.send_checked(&member, false, libc::SIGKILL);
            }
        }
        !self.uncertain
    }

    pub fn begin_cleanup(&mut self, now: Instant, interrupt: Option<Duration>, term: Duration) {
        if self.cleanup.is_none() {
            self.cleanup = Some(Cleanup {
                interrupt,
                term,
                deadline: now + interrupt.unwrap_or_default() + term + CONFIRM_GRACE,
                empty_scans: 0,
            });
        }
    }

    /// Refresh retained anchors, then traverse every verified child edge. A
    /// failed read never silently turns an unknown subtree into an empty one.
    pub fn scan(&mut self) {
        let mut queue: VecDeque<_> = self
            .processes
            .values()
            .map(|p| p.identity.clone())
            .collect();
        if self.adopt_orphans {
            queue.push_front(self.shim.clone());
        }
        let mut visited = HashMap::new();
        let mut attempts = 0;
        while let Some(expected) = queue.pop_front() {
            if visited.get(&expected.pid) == Some(&expected.start_time) {
                continue;
            }
            attempts += 1;
            if attempts > self.process_limit.saturating_mul(2) + 1 {
                self.fail();
                break;
            }
            let current = match self.source.read(expected.pid) {
                Ok(Some(current)) if current.identity.same_process(&expected) => current,
                Ok(_) => {
                    // A freshly discovered replacement may already be queued.
                    // A stale anchor must not erase that newer observation.
                    if self
                        .processes
                        .get(&expected.pid)
                        .is_some_and(|p| p.identity.same_process(&expected))
                    {
                        self.processes.remove(&expected.pid);
                    }
                    continue;
                }
                Err(_) => {
                    self.fail();
                    continue;
                }
            };
            visited.insert(expected.pid, current.identity.start_time.clone());
            if expected.pid != self.shim.pid {
                self.processes.insert(expected.pid, current.clone());
            }
            let children = match self.source.children(expected.pid) {
                Ok(children) => children,
                Err(error) if disappeared(&error) => continue,
                Err(_) => {
                    self.fail();
                    continue;
                }
            };
            // The parent may have exited/reused its pid during enumeration.
            match self.source.read(expected.pid) {
                Ok(Some(parent)) if parent.identity.same_process(&expected) => {}
                Ok(_) => continue,
                Err(_) => {
                    self.fail();
                    continue;
                }
            }
            for pid in children {
                if pid == self.shim.pid {
                    continue;
                }
                let child = match self.source.read(pid) {
                    Ok(Some(child)) if child.ppid == expected.pid => child,
                    Ok(_) => continue,
                    Err(_) => {
                        self.fail();
                        continue;
                    }
                };
                if visited.get(&pid) == Some(&child.identity.start_time) {
                    continue;
                }
                if !self.processes.contains_key(&pid) && self.processes.len() >= self.process_limit
                {
                    self.fail();
                    break;
                }
                queue.push_back(child.identity.clone());
                self.processes.insert(pid, child);
            }
        }

        let records: Vec<_> = self.processes.values().cloned().collect();
        for record in records {
            let id = &record.identity;
            if id.pid == id.pgid && !self.groups.contains_key(id) {
                if self.groups.len() >= self.group_limit {
                    self.fail();
                } else {
                    self.groups.insert(id.clone(), None);
                    self.pending.push(id.clone());
                }
            }
            // Root is exclusively owned by Child::wait in the waiter thread.
            // Adopted zombies need no further signals and are reaped promptly.
            if self.adopt_orphans
                && id.pid != self.root_pid
                && record.ppid == self.shim.pid
                && record.exited
            {
                if self.source.reap(id.pid).is_err() {
                    self.fail();
                } else {
                    self.processes.remove(&id.pid);
                }
            }
        }
        self.members.retain(|id, _| {
            self.processes
                .get(&id.pid)
                .is_some_and(|p| !p.exited && p.identity == *id)
        });
    }

    /// Check identity immediately before signalling. There is no portable
    /// atomic start-time-and-kill operation; detected mismatches are skipped.
    fn send_checked(&mut self, expected: &ProcessIdentity, group: bool, signal: i32) -> bool {
        let current = match self.source.read(expected.pid) {
            Ok(Some(current)) => current,
            Ok(None) => return false,
            Err(_) => {
                self.fail();
                return false;
            }
        };
        if !current.identity.same_process(expected)
            || (group && (current.identity != *expected || expected.pid != expected.pgid))
        {
            return false;
        }
        if !group && current.exited {
            return false;
        }
        if expected.pid == 0 || expected.pid > i32::MAX as u32 {
            self.fail();
            return false;
        }
        let target = if group {
            -(expected.pid as i32)
        } else {
            expected.pid as i32
        };
        if self.source.signal(target, signal).is_err() {
            self.fail();
        }
        true
    }

    fn next_signal(
        &self,
        sequence: Option<Sequence>,
        now: Instant,
        force: bool,
    ) -> (Sequence, Option<i32>, bool) {
        let cleanup = self.cleanup.as_ref().unwrap();
        let original = sequence;
        let mut sequence = sequence.unwrap_or_else(|| {
            let phase = if cleanup.interrupt.is_some() {
                Phase::Interrupt
            } else {
                Phase::Terminate
            };
            Sequence { phase, next: now }
        });
        if force {
            return (
                Sequence {
                    phase: Phase::Kill,
                    next: now,
                },
                Some(libc::SIGKILL),
                original.is_none_or(|s| s.phase != Phase::Kill),
            );
        }
        if original.is_some() && now < sequence.next {
            return (sequence, None, false);
        }
        if original.is_some() {
            sequence.phase = match sequence.phase {
                Phase::Interrupt => Phase::Terminate,
                Phase::Terminate | Phase::Kill => Phase::Kill,
            };
        }
        let (signal, grace) = match sequence.phase {
            Phase::Interrupt => (libc::SIGINT, cleanup.interrupt.unwrap()),
            Phase::Terminate => (libc::SIGTERM, cleanup.term),
            Phase::Kill => (libc::SIGKILL, SAMPLE_INTERVAL),
        };
        sequence.next = now + grace;
        (sequence, Some(signal), false)
    }

    fn after_signal(&self, mut sequence: Sequence, now: Instant) -> Sequence {
        let cleanup = self.cleanup.as_ref().unwrap();
        let grace = match sequence.phase {
            Phase::Interrupt => cleanup.interrupt.unwrap(),
            Phase::Terminate => cleanup.term,
            Phase::Kill => SAMPLE_INTERVAL,
        };
        // Identity reads and earlier targets in a large batch may be slow.
        // Each target receives its entire grace after its own signal syscall.
        sequence.next = self.source.now(now) + grace;
        sequence
    }

    /// Called after each scan. Every due target advances in this pass, so the
    /// total grace is independent of group count. Newly found groups start at
    /// INT, but the overall bound may force early KILL with negative evidence.
    pub fn advance(&mut self, now: Instant) -> Progress {
        let Some(cleanup) = &self.cleanup else {
            return Progress {
                done: false,
                confirmed: false,
            };
        };
        let force = now >= cleanup.deadline;
        let live: Vec<_> = self
            .processes
            .values()
            .filter(|p| !p.exited)
            .map(|p| p.identity.clone())
            .collect();
        let mut covered = HashSet::new();
        let groups: Vec<_> = self
            .groups
            .iter()
            .map(|(id, seq)| (id.clone(), *seq))
            .collect();
        for (group, previous) in groups {
            if !live.iter().any(|id| id.pgid == group.pgid) {
                continue;
            }
            let target_now = self.source.now(now);
            let (sequence, signal, shortened) =
                self.next_signal(previous, target_now, self.expired(target_now));
            // Even between stages validate the leader before excluding members
            // from their fallback path. Signal 0 is only an identity-safe probe.
            if self.send_checked(&group, true, signal.unwrap_or(0)) {
                let sequence = if signal.is_some() {
                    self.after_signal(sequence, target_now)
                } else {
                    sequence
                };
                self.groups.insert(group.clone(), Some(sequence));
                if shortened {
                    self.fail();
                }
                for member in live.iter().filter(|id| id.pgid == group.pgid) {
                    covered.insert(member.pid);
                    self.members.insert(member.clone(), sequence);
                }
            }
        }
        for member in live.iter().filter(|id| !covered.contains(&id.pid)) {
            let previous = self.members.get(member).copied();
            let target_now = self.source.now(now);
            let (sequence, signal, shortened) =
                self.next_signal(previous, target_now, self.expired(target_now));
            if let Some(signal) = signal {
                if self.send_checked(member, false, signal) {
                    let sequence = self.after_signal(sequence, target_now);
                    self.members.insert(member.clone(), sequence);
                    if shortened {
                        self.fail();
                    }
                }
            }
        }
        let cleanup = self.cleanup.as_mut().unwrap();
        cleanup.empty_scans = if live.is_empty() {
            cleanup.empty_scans.saturating_add(1)
        } else {
            0
        };
        if force && !live.is_empty() {
            self.uncertain = true;
        }
        let done = cleanup.empty_scans >= 2 || force;
        Progress {
            done,
            confirmed: done && live.is_empty() && !self.uncertain,
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    use std::fs;

    pub fn probe() -> io::Result<()> {
        let mut enabled: libc::c_int = 0;
        // Reapply the existing value to verify prctl permission without changing
        // legacy sessions that only query capabilities.
        if unsafe { libc::prctl(libc::PR_GET_CHILD_SUBREAPER, &mut enabled, 0, 0, 0) } != 0
            || unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, enabled, 0, 0, 0) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    pub fn prepare() -> io::Result<()> {
        if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    pub fn children(pid: u32) -> io::Result<Vec<u32>> {
        let mut children = HashSet::new();
        let tasks = match fs::read_dir(format!("/proc/{pid}/task")) {
            Ok(tasks) => tasks,
            Err(error) if disappeared(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        for (index, task) in tasks.enumerate() {
            if index >= MAX_PROCESSES {
                return Err(io::Error::other("tracking thread inventory overflow"));
            }
            let path = task?.path();
            let bytes = match fs::read(path.join("children")) {
                Ok(bytes) => bytes,
                Err(error) if disappeared(&error) && !path.exists() => continue,
                Err(error) => return Err(error),
            };
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| io::Error::other("invalid children list"))?;
            for child in text.split_whitespace() {
                let child = child
                    .parse()
                    .map_err(|_| io::Error::other("invalid child pid"))?;
                children.insert(child);
                if children.len() > MAX_PROCESSES {
                    return Err(io::Error::other("tracking process inventory overflow"));
                }
            }
        }
        Ok(children.into_iter().collect())
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use crate::identity::StartTime;

    #[derive(Default)]
    struct Fake {
        records: HashMap<u32, ProcessRecord>,
        denied: HashSet<u32>,
        fail_children: bool,
        fail_signals: bool,
        signals: Vec<(i32, i32)>,
        reaped: Vec<u32>,
        read_delay: Duration,
        elapsed: Duration,
    }

    impl Source for Fake {
        fn read(&mut self, pid: u32) -> io::Result<Option<ProcessRecord>> {
            self.elapsed += self.read_delay;
            if self.denied.contains(&pid) {
                return Err(io::Error::from_raw_os_error(libc::EPERM));
            }
            Ok(self.records.get(&pid).cloned())
        }
        fn children(&mut self, pid: u32) -> io::Result<Vec<u32>> {
            if self.fail_children {
                return Err(io::Error::other("inventory unavailable"));
            }
            let mut children: Vec<_> = self
                .records
                .values()
                .filter(|p| p.ppid == pid)
                .map(|p| p.identity.pid)
                .collect();
            children.sort_unstable();
            Ok(children)
        }
        fn signal(&mut self, target: i32, signal: i32) -> io::Result<()> {
            if self.fail_signals {
                return Err(io::Error::from_raw_os_error(libc::EPERM));
            }
            if signal != 0 {
                self.signals.push((target, signal));
            }
            Ok(())
        }
        fn reap(&mut self, pid: u32) -> io::Result<()> {
            self.reaped.push(pid);
            self.records.remove(&pid);
            Ok(())
        }
        fn now(&self, reference: Instant) -> Instant {
            reference + self.elapsed
        }
    }

    fn record(pid: u32, pgid: u32, ppid: u32) -> ProcessRecord {
        #[cfg(target_os = "linux")]
        let start_time = StartTime::Linux {
            boot_id: "test-boot".into(),
            ticks: u64::from(pid),
        };
        #[cfg(target_os = "macos")]
        let start_time = StartTime::Macos {
            seconds: u64::from(pid),
            microseconds: 123,
        };
        ProcessRecord {
            identity: ProcessIdentity {
                pid,
                pgid,
                start_time,
            },
            ppid,
            exited: false,
        }
    }

    fn replacement(mut process: ProcessRecord) -> ProcessRecord {
        match &mut process.identity.start_time {
            #[cfg(target_os = "linux")]
            StartTime::Linux { ticks, .. } => *ticks += 1,
            #[cfg(target_os = "macos")]
            StartTime::Macos { seconds, .. } => *seconds += 1,
        }
        process
    }

    fn tracker() -> Tracker<Fake> {
        let shim = record(1, 1, 0);
        let root = record(10, 10, 1);
        let source = Fake {
            records: HashMap::from([(1, shim.clone()), (10, root.clone())]),
            ..Fake::default()
        };
        let mut tracker = Tracker::with_source(source, shim.identity, root.identity);
        tracker.adopt_orphans = false;
        tracker
    }

    fn begin(tracker: &mut Tracker<Fake>, now: Instant) {
        tracker.begin_cleanup(
            now,
            Some(Duration::from_millis(100)),
            Duration::from_millis(200),
        );
    }

    #[test]
    fn root_first_deduplicated_groups_and_mutable_process_pgid() {
        let mut tracker = tracker();
        assert_eq!(tracker.take_new_groups(), vec![record(10, 10, 1).identity]);
        tracker.source.records.insert(20, record(20, 20, 10));
        tracker.source.records.insert(21, record(21, 20, 20));
        tracker.scan();
        assert_eq!(tracker.take_new_groups(), vec![record(20, 20, 10).identity]);
        tracker.scan();
        assert!(tracker.take_new_groups().is_empty());
        tracker.source.records.get_mut(&21).unwrap().identity.pgid = 21;
        tracker.scan();
        assert_eq!(tracker.take_new_groups(), vec![record(21, 21, 20).identity]);
        assert!(!tracker.uncertain());
    }

    #[test]
    fn reused_group_leader_is_never_signalled_but_verified_members_are() {
        let mut tracker = tracker();
        tracker.source.records.insert(20, record(20, 20, 10));
        tracker.source.records.insert(21, record(21, 20, 20));
        tracker.scan();
        tracker
            .source
            .records
            .insert(20, replacement(record(20, 20, 1)));
        let now = Instant::now();
        begin(&mut tracker, now);
        tracker.advance(now);
        assert!(!tracker
            .source
            .signals
            .iter()
            .any(|(pid, _)| pid.abs() == 20));
        assert!(tracker.source.signals.contains(&(21, libc::SIGINT)));
    }

    #[test]
    fn missing_leader_falls_back_without_restarting_member_grace() {
        let mut tracker = tracker();
        tracker.source.records.insert(20, record(20, 20, 10));
        tracker.source.records.insert(21, record(21, 20, 20));
        tracker.scan();
        let now = Instant::now();
        begin(&mut tracker, now);
        tracker.advance(now);
        tracker.source.records.remove(&20);
        tracker.source.records.get_mut(&21).unwrap().ppid = 1;
        tracker.scan();
        tracker.advance(now + Duration::from_millis(100));
        assert!(tracker.source.signals.contains(&(-20, libc::SIGINT)));
        assert!(tracker.source.signals.contains(&(21, libc::SIGTERM)));
        assert!(!tracker.source.signals.contains(&(21, libc::SIGINT)));
    }

    #[test]
    fn leaderless_group_seen_for_first_time_uses_member_identity() {
        let mut tracker = tracker();
        tracker.source.records.insert(21, record(21, 20, 10));
        tracker.scan();
        let now = Instant::now();
        begin(&mut tracker, now);
        tracker.advance(now);
        assert!(tracker.source.signals.contains(&(21, libc::SIGINT)));
        assert!(!tracker.source.signals.iter().any(|(pid, _)| *pid == -20));
    }

    #[test]
    fn reused_process_anchor_does_not_adopt_its_new_children() {
        let mut tracker = tracker();
        tracker.source.records.insert(20, record(20, 20, 10));
        tracker.scan();
        tracker
            .source
            .records
            .insert(20, replacement(record(20, 20, 99)));
        tracker.source.records.insert(21, record(21, 21, 20));
        tracker.scan();
        assert!(!tracker.processes.contains_key(&20));
        assert!(!tracker.processes.contains_key(&21));
    }

    #[test]
    fn stale_queued_anchor_cannot_erase_a_newly_discovered_identity() {
        let mut tracker = tracker();
        tracker.adopt_orphans = true;
        tracker.source.records.insert(20, record(20, 20, 10));
        tracker.scan();
        let new = replacement(record(20, 20, 1));
        tracker.source.records.insert(20, new.clone());
        tracker.source.records.insert(21, record(21, 21, 20));
        tracker.scan();
        assert_eq!(tracker.processes.get(&20).unwrap().identity, new.identity);
        assert!(tracker.processes.contains_key(&21));
    }

    #[test]
    fn subreaper_finds_pre_sample_orphans_and_reaps_only_adopted_zombies() {
        let mut tracker = tracker();
        tracker.adopt_orphans = true;
        tracker.source.records.get_mut(&10).unwrap().exited = true;
        let mut adopted = record(20, 20, 1);
        adopted.exited = true;
        tracker.source.records.insert(20, adopted);
        tracker.source.records.insert(21, record(21, 21, 1));
        tracker.scan();
        assert_eq!(tracker.source.reaped, vec![20]);
        assert!(tracker.source.records.contains_key(&10));
        assert!(tracker.processes.contains_key(&21));
    }

    #[test]
    fn sampled_mode_does_not_claim_to_find_an_unseen_reparented_branch() {
        let mut tracker = tracker();
        tracker.source.records.remove(&10);
        tracker.source.records.insert(20, record(20, 20, 1));
        tracker.scan();
        assert!(tracker.processes.is_empty());
        assert_eq!(tracker.groups.len(), 1);
    }

    #[test]
    fn inventory_metadata_and_signal_errors_remain_unconfirmed_after_recovery() {
        for failure in 0..3 {
            let mut tracker = tracker();
            match failure {
                0 => {
                    tracker.source.denied.insert(10);
                }
                1 => tracker.source.fail_children = true,
                _ => tracker.source.fail_signals = true,
            }
            tracker.scan();
            let now = Instant::now();
            begin(&mut tracker, now);
            tracker.advance(now);
            assert!(tracker.uncertain());
            tracker.source.denied.clear();
            tracker.source.fail_children = false;
            tracker.source.records.remove(&10);
            tracker.scan();
            tracker.advance(now + SAMPLE_INTERVAL);
            tracker.scan();
            let result = tracker.advance(now + SAMPLE_INTERVAL * 2);
            assert!(result.done);
            assert!(!result.confirmed);
        }
    }

    #[test]
    fn inventory_bounds_are_sticky_and_group_overflow_retains_member_fallback() {
        for groups in [false, true] {
            let mut tracker = tracker();
            if groups {
                tracker.group_limit = 1;
            } else {
                tracker.process_limit = 1;
            }
            tracker.source.records.insert(20, record(20, 20, 10));
            tracker.scan();
            assert!(tracker.uncertain());
            let now = Instant::now();
            begin(&mut tracker, now);
            tracker.advance(now);
            if groups {
                assert!(tracker.source.signals.contains(&(20, libc::SIGINT)));
            } else {
                assert_eq!(tracker.processes.len(), 1);
            }
        }
    }

    #[test]
    fn stable_groups_receive_parallel_full_signal_graces() {
        let mut tracker = tracker();
        tracker.source.records.insert(20, record(20, 20, 10));
        tracker.scan();
        let now = Instant::now();
        begin(&mut tracker, now);
        for millis in [0, 99, 100, 299, 300] {
            tracker.advance(now + Duration::from_millis(millis));
        }
        for group in [-10, -20] {
            let signals: Vec<_> = tracker
                .source
                .signals
                .iter()
                .filter(|(pid, _)| *pid == group)
                .map(|(_, signal)| *signal)
                .collect();
            assert_eq!(signals, [libc::SIGINT, libc::SIGTERM, libc::SIGKILL]);
        }
    }

    #[test]
    fn late_group_starts_at_interrupt_but_overall_bound_forces_unconfirmed_kill() {
        let mut tracker = tracker();
        let now = Instant::now();
        begin(&mut tracker, now);
        for millis in [0, 100, 300] {
            tracker.advance(now + Duration::from_millis(millis));
        }
        tracker.source.records.insert(20, record(20, 20, 10));
        tracker.scan();
        tracker.advance(now + Duration::from_millis(750));
        assert!(tracker.source.signals.contains(&(-20, libc::SIGINT)));
        let result = tracker.advance(now + Duration::from_millis(800));
        assert!(result.done);
        assert!(!result.confirmed);
        assert!(tracker.source.signals.contains(&(-20, libc::SIGKILL)));
        assert!(!tracker.source.signals.contains(&(-20, libc::SIGTERM)));
    }

    #[test]
    fn slow_identity_reads_do_not_shorten_signal_graces() {
        let mut tracker = tracker();
        let now = Instant::now();
        begin(&mut tracker, now);
        tracker.source.read_delay = Duration::from_millis(40);
        tracker.advance(now);
        let sequence = tracker
            .groups
            .get(&record(10, 10, 1).identity)
            .unwrap()
            .unwrap();
        assert_eq!(sequence.phase, Phase::Interrupt);
        assert_eq!(sequence.next, now + Duration::from_millis(140));
        tracker.source.read_delay = Duration::ZERO;
        tracker.source.elapsed = Duration::ZERO;
        tracker.advance(now + Duration::from_millis(100));
        assert_eq!(tracker.source.signals, vec![(-10, libc::SIGINT)]);
        tracker.advance(now + Duration::from_millis(140));
        assert_eq!(
            tracker.source.signals,
            vec![(-10, libc::SIGINT), (-10, libc::SIGTERM)]
        );
    }

    #[test]
    fn final_scan_late_adoption_is_killed_and_never_confirmed() {
        let mut tracker = tracker();
        tracker.adopt_orphans = true;
        tracker.source.records.remove(&10);
        tracker.scan();
        tracker.source.records.insert(20, record(20, 20, 1));
        assert!(!tracker.finish_scan());
        assert!(tracker.source.signals.contains(&(20, libc::SIGKILL)));
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    pub fn probe() -> io::Result<()> {
        Ok(())
    }
    pub fn prepare() -> io::Result<()> {
        children(std::process::id()).map(|_| ())
    }
    pub fn children(pid: u32) -> io::Result<Vec<u32>> {
        let mut capacity = 64;
        loop {
            let mut pids = vec![0_i32; capacity];
            // proc_listchildpids returns a PID count, unlike proc_listpids's
            // byte count. Clear errno to distinguish no children from failure.
            let count = unsafe {
                *libc::__error() = 0;
                libc::proc_listchildpids(
                    pid as i32,
                    pids.as_mut_ptr().cast(),
                    (pids.len() * std::mem::size_of::<i32>()) as i32,
                )
            };
            if count <= 0 {
                let error = io::Error::last_os_error();
                if count == 0 && error.raw_os_error() == Some(0) {
                    return Ok(Vec::new());
                }
                return Err(error);
            }
            if (count as usize) < capacity {
                pids.truncate(count as usize);
                return Ok(pids
                    .into_iter()
                    .filter(|pid| *pid > 0)
                    .map(|pid| pid as u32)
                    .collect());
            }
            if capacity > MAX_PROCESSES {
                return Err(io::Error::other("tracking process inventory overflow"));
            }
            capacity = (capacity * 2).min(MAX_PROCESSES + 1);
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use super::*;
    pub fn probe() -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descendant tracking unavailable",
        ))
    }
    pub fn prepare() -> io::Result<()> {
        probe()
    }
    pub fn children(_pid: u32) -> io::Result<Vec<u32>> {
        probe().map(|_| Vec::new())
    }
}
