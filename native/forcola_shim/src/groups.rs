//! `forcola_shim groups`: check or stop process groups recorded earlier.
//!
//! A short-lived invocation that needs no live session. It reads one JSON
//! request line on stdin, writes one JSON result line on stdout, and exits 0.
//! A malformed request writes `{"error": "<reason>"}` and exits 2.
//!
//! Request:
//!   {"op": "check" | "stop",
//!    "groups": [{"pid", "pgid", "start_time"}],   (SPAWNED/TRACKED shape)
//!    "interrupt_signal": "int" | null, "interrupt_grace_ms", "kill_grace_ms"}
//!
//! Result:
//!   {"groups": [{"state": "gone" | "present" | "unknown", "reason"}],
//!    "confirmed": bool}
//!
//! One result per request entry, in order; `reason` is set only for unknown.
//! `confirmed` is true only when every group is gone.
//!
//! A group is present while its recorded leader is alive with the recorded
//! start time, or, once the leader is gone, while any non-zombie process
//! remains in the recorded process group. A leader pid now held by another
//! process proves the group is gone: a pid is not reused while a process
//! group with that id exists. An identity from an earlier boot is gone.
//!
//! `stop` sends optional INT, then TERM, then KILL to every present group,
//! each followed by its grace (KILL by a 500 ms confirmation window). A stage
//! ends early once no group is present. Every signal is preceded by a fresh
//! identity read: a verified leader receives `kill(-pgid)`, members of a
//! leaderless group receive individual `kill(pid)` after their own reads, and
//! a group whose leader identity no longer matches is never signalled. Reads
//! and signals are not atomic, as in live tracking. A group seen gone stays
//! gone for the rest of the request, so a later group reusing its pgid is not
//! signalled.

use crate::identity::{self, ProcessIdentity, ProcessRecord, StartTime};
use crate::spawn_error::error_reason;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Read, Write};
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

const EXIT_OK: u8 = 0;
const EXIT_BAD_REQUEST: u8 = 2;
const MAX_GROUPS: usize = crate::tracking::MAX_GROUPS;
const MAX_REQUEST_BYTES: u64 = 16 * 1024 * 1024;
const POLL: Duration = Duration::from_millis(20);
const CONFIRM_WINDOW: Duration = Duration::from_millis(500);
const MAX_GRACE_MS: u64 = u32::MAX as u64;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Op {
    Check,
    Stop,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Interrupt {
    Int,
}

#[derive(Deserialize)]
struct Request {
    op: Op,
    groups: Vec<Value>,
    #[serde(default)]
    interrupt_signal: Option<Interrupt>,
    #[serde(default)]
    interrupt_grace_ms: Option<u64>,
    #[serde(default = "default_kill_grace_ms")]
    kill_grace_ms: u64,
}

fn default_kill_grace_ms() -> u64 {
    5_000
}

#[derive(Serialize)]
struct Report {
    groups: Vec<StateReport>,
    confirmed: bool,
}

#[derive(Serialize)]
struct StateReport {
    state: &'static str,
    reason: Option<&'static str>,
}

/// A recorded group as the shim understands it, or why it cannot.
enum Recorded {
    Group(ProcessIdentity),
    Invalid(&'static str),
}

/// One observation of a recorded group.
#[derive(Debug)]
enum State {
    Gone,
    /// The recorded leader is alive and still leads the group.
    Led,
    /// The leader is gone (or left the group); these members remain.
    Leaderless(Vec<ProcessIdentity>),
    Unknown(&'static str),
}

impl State {
    fn present(&self) -> bool {
        matches!(self, State::Led | State::Leaderless(_))
    }

    fn report(&self) -> StateReport {
        let (state, reason) = match self {
            State::Gone => ("gone", None),
            State::Led | State::Leaderless(_) => ("present", None),
            State::Unknown(reason) => ("unknown", Some(*reason)),
        };
        StateReport { state, reason }
    }
}

/// Runs groups mode. Arguments after `groups` are not accepted.
pub fn run(args: &[std::ffi::OsString]) -> ExitCode {
    let mut stdout = io::stdout();
    if !args.is_empty() {
        return reject(&mut stdout, "usage");
    }
    let mut line = String::new();
    let read = io::stdin()
        .lock()
        .take(MAX_REQUEST_BYTES)
        .read_line(&mut line);
    if read.is_err() || !line.ends_with('\n') {
        return reject(&mut stdout, "unterminated_request");
    }
    let request: Request = match serde_json::from_str(&line) {
        Ok(request) => request,
        Err(_) => return reject(&mut stdout, "invalid_request"),
    };
    if request.groups.len() > MAX_GROUPS {
        return reject(&mut stdout, "too_many_groups");
    }
    let interrupt = match (&request.interrupt_signal, request.interrupt_grace_ms) {
        (None, None) => None,
        (Some(Interrupt::Int), Some(ms)) if ms > 0 && ms <= MAX_GRACE_MS => {
            Some(Duration::from_millis(ms))
        }
        _ => return reject(&mut stdout, "invalid_interrupt"),
    };
    if request.kill_grace_ms > MAX_GRACE_MS {
        return reject(&mut stdout, "invalid_kill_grace");
    }

    let recorded: Vec<Recorded> = request.groups.iter().map(parse_identity).collect();
    let states = match request.op {
        Op::Check => recorded.iter().map(observe).collect(),
        Op::Stop => stop(
            &recorded,
            interrupt,
            Duration::from_millis(request.kill_grace_ms),
        ),
    };
    let report = Report {
        confirmed: states.iter().all(|state| matches!(state, State::Gone)),
        groups: states.iter().map(State::report).collect(),
    };
    let encoded = serde_json::to_string(&report).expect("report serializes");
    let _ = writeln!(stdout, "{encoded}").and_then(|()| stdout.flush());
    ExitCode::from(EXIT_OK)
}

fn reject(stdout: &mut io::Stdout, reason: &str) -> ExitCode {
    let line = serde_json::json!({ "error": reason });
    let _ = writeln!(stdout, "{line}").and_then(|()| stdout.flush());
    ExitCode::from(EXIT_BAD_REQUEST)
}

fn parse_identity(value: &Value) -> Recorded {
    let positive = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_u64)
            .filter(|n| *n > 0 && *n <= i32::MAX as u64)
            .map(|n| n as u32)
    };
    let (Some(pid), Some(pgid)) = (positive("pid"), positive("pgid")) else {
        return Recorded::Invalid("invalid_identity");
    };
    let Some(start) = value.get("start_time") else {
        return Recorded::Invalid("invalid_identity");
    };
    let start_time = match parse_start_time(start) {
        Ok(start_time) => start_time,
        Err(reason) => return Recorded::Invalid(reason),
    };
    if pid != pgid {
        return Recorded::Invalid("not_a_group_leader");
    }
    Recorded::Group(ProcessIdentity {
        pid,
        pgid,
        start_time,
    })
}

fn parse_start_time(value: &Value) -> Result<StartTime, &'static str> {
    let kind = value.get("kind").and_then(Value::as_str);
    match kind {
        #[cfg(target_os = "linux")]
        Some("linux") => {
            let boot_id = value.get("boot_id").and_then(Value::as_str);
            let ticks = value.get("ticks").and_then(Value::as_u64);
            match (boot_id, ticks) {
                (Some(boot_id), Some(ticks)) if !boot_id.is_empty() => Ok(StartTime::Linux {
                    boot_id: boot_id.to_owned(),
                    ticks,
                }),
                _ => Err("invalid_identity"),
            }
        }
        #[cfg(target_os = "macos")]
        Some("macos") => {
            let seconds = value.get("seconds").and_then(Value::as_u64);
            let microseconds = value
                .get("microseconds")
                .and_then(Value::as_u64)
                .filter(|us| *us < 1_000_000);
            match (seconds, microseconds) {
                (Some(seconds), Some(microseconds)) => Ok(StartTime::Macos {
                    seconds,
                    microseconds,
                }),
                _ => Err("invalid_identity"),
            }
        }
        #[cfg(target_os = "linux")]
        Some("macos") => Err("foreign_identity"),
        #[cfg(target_os = "macos")]
        Some("linux") => Err("foreign_identity"),
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        Some("linux" | "macos") => Err("foreign_identity"),
        _ => Err("invalid_identity"),
    }
}

fn disappeared(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::ESRCH | libc::ENOENT))
}

fn read(pid: u32) -> io::Result<Option<ProcessRecord>> {
    match identity::read_process(pid) {
        Ok(record) => Ok(Some(record)),
        Err(error) if disappeared(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Observes a recorded group once, without signalling it.
fn observe(recorded: &Recorded) -> State {
    let expected = match recorded {
        Recorded::Group(expected) => expected,
        Recorded::Invalid(reason) => return State::Unknown(reason),
    };
    match platform::from_earlier_boot(&expected.start_time) {
        Ok(true) => return State::Gone,
        Ok(false) => {}
        Err(error) => return State::Unknown(error_reason(&error)),
    }
    match read(expected.pid) {
        Ok(Some(leader)) if !leader.identity.same_process(expected) => State::Gone,
        Ok(Some(leader)) if leader.identity.pgid == expected.pgid && !leader.exited => State::Led,
        // The leader exited (unreaped) or moved to another group. Its pid is
        // still held, so this pgid still names the recorded group.
        Ok(Some(_)) | Ok(None) => members(expected.pgid),
        Err(error) => State::Unknown(error_reason(&error)),
    }
}

fn members(pgid: u32) -> State {
    let pids = match platform::group_members(pgid) {
        Ok(pids) => pids,
        Err(error) => return State::Unknown(error_reason(&error)),
    };
    let mut live = Vec::new();
    for pid in pids {
        match read(pid) {
            Ok(Some(record)) if record.identity.pgid == pgid && !record.exited => {
                live.push(record.identity)
            }
            Ok(_) => {}
            Err(error) => return State::Unknown(error_reason(&error)),
        }
    }
    if live.is_empty() {
        State::Gone
    } else {
        State::Leaderless(live)
    }
}

/// Signals a present group after re-verifying it. Returns nothing: the next
/// observation decides the group's state.
fn signal(expected: &ProcessIdentity, state: &State, signal: i32) {
    match state {
        State::Led => {
            let verified = matches!(
                read(expected.pid),
                Ok(Some(leader)) if leader.identity == *expected && !leader.exited
            );
            if verified {
                // SAFETY: pid is in 1..=i32::MAX (checked when parsed) and was
                // just verified as the recorded, live group leader.
                let result = unsafe { libc::kill(-(expected.pgid as i32), signal) };
                if result == 0 {
                    return;
                }
                // macOS reports EPERM for a group with only zombie members;
                // other users' members also refuse. Fall through to members.
            }
            if let State::Leaderless(live) = members(expected.pgid) {
                signal_members(expected.pgid, &live, signal);
            }
        }
        State::Leaderless(live) => signal_members(expected.pgid, live, signal),
        State::Gone | State::Unknown(_) => {}
    }
}

fn signal_members(pgid: u32, live: &[ProcessIdentity], signal: i32) {
    for member in live {
        let verified = matches!(
            read(member.pid),
            Ok(Some(current))
                if current.identity.same_process(member)
                    && current.identity.pgid == pgid
                    && !current.exited
        );
        if verified && member.pid <= i32::MAX as u32 {
            // SAFETY: the member was just re-read with the recorded start time
            // and process group. Errors leave it present for the next read.
            unsafe { libc::kill(member.pid as i32, signal) };
        }
    }
}

fn stop(recorded: &[Recorded], interrupt: Option<Duration>, term: Duration) -> Vec<State> {
    let mut states: Vec<State> = recorded.iter().map(observe).collect();
    let mut stages = Vec::new();
    if let Some(grace) = interrupt {
        stages.push((libc::SIGINT, grace));
    }
    stages.push((libc::SIGTERM, term));
    stages.push((libc::SIGKILL, CONFIRM_WINDOW));

    for (number, grace) in stages {
        if !states.iter().any(State::present) {
            break;
        }
        for (recorded, state) in recorded.iter().zip(&states) {
            if let (Recorded::Group(expected), true) = (recorded, state.present()) {
                signal(expected, state, number);
            }
        }
        let deadline = Instant::now() + grace;
        loop {
            refresh(recorded, &mut states);
            if !states.iter().any(State::present) || Instant::now() >= deadline {
                break;
            }
            thread::sleep(POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }
    states
}

/// Re-observes every group except those already seen gone, which stay gone
/// so a later group that reuses the pgid is never treated as the recorded one.
fn refresh(recorded: &[Recorded], states: &mut [State]) {
    for (recorded, state) in recorded.iter().zip(states.iter_mut()) {
        if !matches!(state, State::Gone) {
            *state = observe(recorded);
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;
    use crate::tracking::MAX_PROCESSES;

    pub fn from_earlier_boot(start_time: &StartTime) -> io::Result<bool> {
        let StartTime::Linux { boot_id, .. } = start_time;
        let current = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        Ok(current.trim() != boot_id)
    }

    /// Every visible process whose process group is `pgid`.
    pub fn group_members(pgid: u32) -> io::Result<Vec<u32>> {
        let mut members = Vec::new();
        for (index, entry) in std::fs::read_dir("/proc")?.enumerate() {
            if index > MAX_PROCESSES * 2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "process inventory overflow",
                ));
            }
            let entry = entry?;
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            match read(pid) {
                Ok(Some(record)) if record.identity.pgid == pgid => members.push(pid),
                Ok(_) => {}
                // Processes hidden by hidepid or racing exit cannot be ours to
                // signal; a permission failure is a real inventory gap.
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {}
                Err(error) => return Err(error),
            }
        }
        Ok(members)
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    pub fn from_earlier_boot(start_time: &StartTime) -> io::Result<bool> {
        let StartTime::Macos {
            seconds,
            microseconds,
        } = start_time;
        let mut boot = libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        let mut size = std::mem::size_of::<libc::timeval>();
        let mut mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];
        // SAFETY: mib names kern.boottime, whose value is one timeval; size is
        // that buffer's size and is updated with the bytes written.
        let result = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                2,
                (&mut boot as *mut libc::timeval).cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        let boot = (boot.tv_sec as u64, boot.tv_usec as u64);
        Ok((*seconds, *microseconds) < boot)
    }

    pub fn group_members(pgid: u32) -> io::Result<Vec<u32>> {
        crate::tracking::group_members(pgid)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use super::*;

    pub fn from_earlier_boot(_start_time: &StartTime) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "process identity is unavailable",
        ))
    }

    pub fn group_members(_pgid: u32) -> io::Result<Vec<u32>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "group inventory unavailable",
        ))
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::io::BufReader;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};

    fn group(script: &str) -> Child {
        Command::new("/bin/sh")
            .args(["-c", script])
            .process_group(0)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn recorded(child: &Child) -> Recorded {
        Recorded::Group(identity::read(child.id()).unwrap())
    }

    fn first_line(child: &mut Child) -> String {
        let mut line = String::new();
        BufReader::new(child.stdout.as_mut().unwrap())
            .read_line(&mut line)
            .unwrap();
        line.trim().to_owned()
    }

    fn alive(pid: u32) -> bool {
        matches!(read(pid), Ok(Some(record)) if !record.exited)
    }

    fn wait_gone(pid: u32) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if !alive(pid) {
                return true;
            }
            thread::sleep(POLL);
        }
        false
    }

    fn with_start_time_shifted(recorded: &Recorded) -> Recorded {
        let Recorded::Group(id) = recorded else {
            unreachable!()
        };
        let start_time = match id.start_time.clone() {
            #[cfg(target_os = "linux")]
            StartTime::Linux { boot_id, ticks } => StartTime::Linux {
                boot_id,
                ticks: ticks + 1,
            },
            #[cfg(target_os = "macos")]
            StartTime::Macos {
                seconds,
                microseconds,
            } => StartTime::Macos {
                seconds: seconds + 1,
                microseconds,
            },
        };
        Recorded::Group(ProcessIdentity {
            start_time,
            ..id.clone()
        })
    }

    #[test]
    fn a_live_leader_is_present() {
        let mut child = group("echo ready; exec sleep 30");
        assert_eq!(first_line(&mut child), "ready");
        assert!(matches!(observe(&recorded(&child)), State::Led));
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn an_exited_group_is_gone() {
        let mut child = group("exit 0");
        let id = recorded(&child);
        child.wait().unwrap();
        assert!(matches!(observe(&id), State::Gone));
    }

    #[test]
    fn a_reused_leader_pid_is_gone_and_never_signalled() {
        let mut child = group("echo ready; exec sleep 30");
        assert_eq!(first_line(&mut child), "ready");
        let stale = with_start_time_shifted(&recorded(&child));
        assert!(matches!(observe(&stale), State::Gone));

        let states = stop(&[stale], None, Duration::from_millis(50));
        assert!(matches!(states[..], [State::Gone]));
        assert!(
            alive(child.id()),
            "a process with another start time was signalled"
        );
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn a_leaderless_group_with_a_live_member_is_present_until_stopped() {
        let mut child = group("sleep 30 & echo $!; exit 0");
        let member: u32 = first_line(&mut child).parse().unwrap();
        let id = recorded(&child);
        child.wait().unwrap();
        assert!(alive(member));

        let State::Leaderless(live) = observe(&id) else {
            panic!("expected a leaderless group");
        };
        assert_eq!(live.iter().map(|m| m.pid).collect::<Vec<_>>(), [member]);

        let states = stop(&[id], None, Duration::from_secs(2));
        assert!(matches!(states[..], [State::Gone]));
        assert!(wait_gone(member));
    }

    #[test]
    fn stop_escalates_to_kill_for_a_group_that_ignores_int_and_term() {
        let mut child = group("trap '' INT TERM; sleep 30 & echo $!; wait");
        let member: u32 = first_line(&mut child).parse().unwrap();
        let id = recorded(&child);

        let started = Instant::now();
        let states = stop(
            &[id],
            Some(Duration::from_millis(100)),
            Duration::from_millis(100),
        );
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(matches!(states[..], [State::Gone]), "{states:?}");
        child.wait().unwrap();
        assert!(wait_gone(member));
    }

    #[test]
    fn invalid_and_foreign_identities_are_unknown() {
        let foreign = if cfg!(target_os = "linux") {
            serde_json::json!({"kind": "macos", "seconds": 1, "microseconds": 0})
        } else {
            serde_json::json!({"kind": "linux", "boot_id": "x", "ticks": 1})
        };
        let cases = [
            (
                serde_json::json!({"pid": 10, "pgid": 10, "start_time": foreign}),
                "foreign_identity",
            ),
            (
                serde_json::json!({"pid": 10, "pgid": 11, "start_time": foreign}),
                "foreign_identity",
            ),
            (
                serde_json::json!({"pid": 0, "pgid": 0, "start_time": {}}),
                "invalid_identity",
            ),
            (serde_json::json!({"pid": 10}), "invalid_identity"),
        ];
        for (value, reason) in cases {
            assert!(
                matches!(observe(&parse_identity(&value)), State::Unknown(r) if r == reason),
                "{value}"
            );
        }

        let own = identity::read(std::process::id()).unwrap();
        let mut value = serde_json::to_value(&own).unwrap();
        value["pgid"] = serde_json::json!(own.pid + 1);
        assert!(matches!(
            observe(&parse_identity(&value)),
            State::Unknown("not_a_group_leader")
        ));
    }

    #[test]
    fn an_identity_from_an_earlier_boot_is_gone() {
        let mut child = group("echo ready; exec sleep 30");
        assert_eq!(first_line(&mut child), "ready");
        let Recorded::Group(id) = recorded(&child) else {
            unreachable!()
        };
        let start_time = match id.start_time.clone() {
            #[cfg(target_os = "linux")]
            StartTime::Linux { ticks, .. } => StartTime::Linux {
                boot_id: "00000000-0000-0000-0000-000000000000".into(),
                ticks,
            },
            #[cfg(target_os = "macos")]
            StartTime::Macos { .. } => StartTime::Macos {
                seconds: 1,
                microseconds: 0,
            },
        };
        let earlier = Recorded::Group(ProcessIdentity { start_time, ..id });
        assert!(matches!(observe(&earlier), State::Gone));
        child.kill().unwrap();
        child.wait().unwrap();
    }
}
