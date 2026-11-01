//! Descendant tracking through the public wire protocol and OS observations.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use serde_json::{json, Value};
use std::cell::RefCell;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const SPAWN: u8 = 0x01;
const STDIN: u8 = 0x02;
const KILL: u8 = 0x04;
const HELLO: u8 = 0x07;
const STDOUT: u8 = 0x11;
const STDERR: u8 = 0x12;
const EXIT: u8 = 0x13;
const ERROR: u8 = 0x14;
const CAPABILITIES: u8 = 0x15;
const SPAWNED: u8 = 0x16;
const TRACKED: u8 = 0x17;

struct Shim {
    child: Child,
    stdin: Option<ChildStdin>,
    frames: Receiver<(u8, Vec<u8>)>,
    reader: Option<JoinHandle<()>>,
    tracked: Vec<Value>,
}

impl Shim {
    fn new(features: Option<&[&str]>) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_forcola_shim"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let (tx, frames) = mpsc::channel();
        let reader = thread::spawn(move || loop {
            let mut size = [0; 4];
            if stdout.read_exact(&mut size).is_err() {
                break;
            }
            let mut frame = vec![0; u32::from_be_bytes(size) as usize];
            if stdout.read_exact(&mut frame).is_err()
                || tx.send((frame[0], frame[1..].to_vec())).is_err()
            {
                break;
            }
        });
        let mut shim = Self {
            stdin: child.stdin.take(),
            child,
            frames,
            reader: Some(reader),
            tracked: Vec::new(),
        };
        if let Some(features) = features {
            shim.send(
                HELLO,
                &serde_json::to_vec(&json!({"features":features})).unwrap(),
            );
            let (tag, bytes) = shim.recv().unwrap();
            assert_eq!(tag, CAPABILITIES);
            let report: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(report["protocol"], 1);
            for feature in features {
                assert!(report["features"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(feature)));
            }
        }
        shim
    }

    fn send(&mut self, tag: u8, payload: &[u8]) {
        let stdin = self.stdin.as_mut().unwrap();
        stdin
            .write_all(&((payload.len() + 1) as u32).to_be_bytes())
            .unwrap();
        stdin.write_all(&[tag]).unwrap();
        stdin.write_all(payload).unwrap();
        stdin.flush().unwrap();
    }

    fn spawn(&mut self, request: Value) {
        self.send(SPAWN, &serde_json::to_vec(&request).unwrap());
    }

    fn recv(&mut self) -> Option<(u8, Vec<u8>)> {
        let frame = match self.frames.recv_timeout(Duration::from_secs(10)) {
            Ok(frame) => frame,
            Err(mpsc::RecvTimeoutError::Disconnected) => return None,
            Err(mpsc::RecvTimeoutError::Timeout) => panic!("shim response timed out"),
        };
        if frame.0 == TRACKED {
            let identity: Value = serde_json::from_slice(&frame.1).unwrap();
            assert_identity_shape(&identity);
            assert!(
                !self.tracked.contains(&identity),
                "duplicate TRACKED: {identity}"
            );
            self.tracked.push(identity);
        }
        Some(frame)
    }

    fn root_tracked(&mut self) -> Value {
        let (tag, bytes) = self.recv().expect("missing root TRACKED");
        assert_eq!(tag, TRACKED, "{}", String::from_utf8_lossy(&bytes));
        let identity: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(identity["pid"], identity["pgid"]);
        identity
    }

    fn wait_tracked(&mut self, pids: &[i32]) {
        while !pids
            .iter()
            .all(|pid| self.tracked.iter().any(|id| id["pid"] == *pid))
        {
            let (tag, bytes) = self.recv().expect("shim exited before descendant TRACKED");
            assert!(
                matches!(tag, TRACKED | STDOUT | STDERR),
                "unexpected {tag:#x}: {}",
                String::from_utf8_lossy(&bytes)
            );
        }
    }

    fn finish(&mut self) -> Option<Value> {
        let mut exit = None;
        while let Some((tag, bytes)) = self.recv() {
            assert!(exit.is_none(), "frame arrived after EXIT: {tag:#x}");
            match tag {
                STDOUT | STDERR | TRACKED => {}
                EXIT => exit = Some(serde_json::from_slice(&bytes).unwrap()),
                ERROR => panic!("unexpected ERROR: {}", String::from_utf8_lossy(&bytes)),
                _ => panic!("unexpected tag {tag:#x}"),
            }
        }
        assert!(self.child.wait().unwrap().success());
        exit
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        self.stdin.take();
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if self.child.try_wait().ok().flatten().is_none() {
            for identity in &self.tracked {
                let pid = identity["pid"].as_i64().unwrap() as i32;
                if native_identity(pid).as_ref() == Some(identity) {
                    unsafe { libc::kill(-pid, libc::SIGKILL) };
                }
            }
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

struct Fixture {
    path: PathBuf,
    identities: RefCell<Vec<Value>>,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "forcola-tracking-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::write(path.join("child.py"), CHILD).unwrap();
        Self {
            path,
            identities: RefCell::new(Vec::new()),
        }
    }

    fn request(&self, mode: &str, pty: bool) -> Value {
        json!({
            "argv":["python3",self.path.join("child.py"),self.path,mode],
            "pty":pty, "timeout_ms":5_000, "kill_grace_ms":100
        })
    }

    fn ready(&self, label: &str) -> i32 {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(bytes) = fs::read(self.path.join(format!("{label}.ready"))) {
                if let Ok(pid) = serde_json::from_slice::<i32>(&bytes) {
                    if let Some(identity) = native_identity(pid) {
                        self.identities.borrow_mut().push(identity);
                    }
                    return pid;
                }
            }
            assert!(
                Instant::now() < deadline,
                "fixture {label} never became ready"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn trigger(&self, name: &str) {
        fs::write(self.path.join(name), b"go").unwrap();
    }

    fn signals(&self, label: &str) -> Vec<Value> {
        fs::read_to_string(self.path.join(format!("{label}.signals")))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Preserve the complete start identity in failure cleanup too.
        for identity in self.identities.borrow().iter() {
            let pid = identity["pid"].as_i64().unwrap() as i32;
            if native_identity(pid).as_ref() == Some(identity) {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        }
        let _ = fs::remove_dir_all(&self.path);
    }
}

const CHILD: &str = r#"
import json, os, signal, sys, threading, time, tty
directory, mode = sys.argv[1:]
signal.signal(signal.SIGHUP, signal.SIG_IGN)
def ready(label):
    path = os.path.join(directory, label + '.ready')
    with open(path + '.tmp', 'w') as stream:
        json.dump(os.getpid(), stream)
    os.replace(path + '.tmp', path)
def exists(name):
    return os.path.exists(os.path.join(directory, name))
def install(label):
    def record(name):
        with open(os.path.join(directory, label + '.signals'), 'a') as stream:
            stream.write(json.dumps({'signal': name, 'time': time.monotonic()}) + '\n')
    signal.signal(signal.SIGINT, lambda *_: record('INT'))
    signal.signal(signal.SIGTERM, lambda *_: record('TERM'))
    signal.alarm(15)
def stay(label):
    install(label)
    ready(label)
    while not exists(label + '.exit'):
        time.sleep(0.005)
    os._exit(23)
def fork_group(label, session):
    pid = os.fork()
    if pid == 0:
        if session:
            os.setsid()
        else:
            os.setpgid(0, 0)
        stay(label)
    return pid
install('root')
if mode in ('groups', 'blocked_groups'):
    if mode == 'blocked_groups' and os.isatty(0):
        tty.setraw(0)
    fork_group('session', True)
    fork_group('group', False)
elif mode == 'late':
    if os.isatty(0):
        tty.setraw(0)
    ready('root')
    while not exists('create'):
        time.sleep(0.005)
    fork_group('late', True)
elif mode == 'orphan' or mode == 'adopted':
    middle = os.fork()
    if middle == 0:
        os.setsid()
        if os.fork() == 0:
            os.setsid()
            stay('orphan')
        os._exit(0)
    os.waitpid(middle, 0)
    if mode == 'orphan':
        while not exists('orphan.ready'):
            time.sleep(0.001)
        ready('root')
        os._exit(23)
elif mode == 'leaderless':
    leader = os.fork()
    if leader == 0:
        os.setsid()
        if os.fork() == 0:
            stay('member')
        stay('leader')
    ready('root')
    os.waitpid(leader, 0)
    ready('reaped')
elif mode == 'thread':
    # Keep the creating thread alive so its per-thread children file owns
    # the fork. The child exec avoids Python signal restrictions on threads.
    def launch():
        if os.fork() == 0:
            os.setsid()
            os.execl(sys.executable, sys.executable, __file__, directory, 'thread_child')
        while not exists('root.exit'):
            time.sleep(0.005)
    threading.Thread(target=launch, daemon=True).start()
elif mode == 'thread_child':
    stay('thread_child')
ready('root')
print('ready', flush=True)
while not exists('root.exit'):
    time.sleep(0.005)
os._exit(23)
"#;

fn assert_identity_shape(identity: &Value) {
    assert_eq!(identity.as_object().unwrap().len(), 3);
    assert!(identity["pid"].as_u64().is_some_and(|pid| pid > 0));
    assert_eq!(identity["pid"], identity["pgid"]);
    let start = &identity["start_time"];
    assert_eq!(start.as_object().unwrap().len(), 3);
    if cfg!(target_os = "linux") {
        assert_eq!(start["kind"], "linux");
        assert!(!start["boot_id"].as_str().unwrap().is_empty());
        assert!(start["ticks"].as_u64().is_some());
    } else {
        assert_eq!(start["kind"], "macos");
        assert!(start["seconds"].as_u64().is_some());
        assert!(start["microseconds"].as_u64().unwrap() < 1_000_000);
    }
}

#[cfg(target_os = "linux")]
fn native_identity(pid: i32) -> Option<Value> {
    let stat = fs::read(format!("/proc/{pid}/stat")).ok()?;
    let end = stat.iter().rposition(|byte| *byte == b')')?;
    let fields: Vec<_> = std::str::from_utf8(&stat[end + 1..])
        .ok()?
        .split_whitespace()
        .collect();
    Some(
        json!({"pid":pid,"pgid":fields[2].parse::<u32>().ok()?,"start_time":{
            "kind":"linux", "boot_id":fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?.trim(),
            "ticks":fields[19].parse::<u64>().ok()?
        }}),
    )
}

#[cfg(target_os = "macos")]
fn native_identity(pid: i32) -> Option<Value> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let result = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            1,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if result != size {
        return None;
    }
    let info = unsafe { info.assume_init() };
    Some(
        json!({"pid":info.pbi_pid,"pgid":info.pbi_pgid,"start_time":{
            "kind":"macos","seconds":info.pbi_start_tvsec,"microseconds":info.pbi_start_tvusec
        }}),
    )
}

fn assert_gone(pid: i32) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if unsafe { libc::kill(pid, 0) } < 0
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "process {pid} survived or remained a zombie"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_tracking_exit(exit: &Value) {
    assert_eq!(
        exit["scope"],
        if cfg!(target_os = "linux") {
            "tree_contained"
        } else {
            "tree_sampled"
        }
    );
    assert_eq!(exit["confirmed"], true, "{exit}");
    assert_eq!(exit["contained"], false, "tracking does not imply a cgroup");
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Trigger {
    Kill,
    NormalExit,
    RootKill,
    OwnerEof,
    Timeout,
}

fn cleanup_case(trigger: Trigger, pty: bool) {
    let fixture = Fixture::new();
    let mut shim = Shim::new(Some(&["track_descendants_v1"]));
    let mut request = fixture.request("groups", pty);
    if trigger == Trigger::Timeout {
        request["timeout_ms"] = json!(2_000);
    }
    shim.spawn(request);
    let identity = shim.root_tracked();
    let root = fixture.ready("root");
    let session = fixture.ready("session");
    let group = fixture.ready("group");
    assert_eq!(identity, native_identity(root).unwrap());
    shim.wait_tracked(&[session, group]);
    assert_eq!(shim.tracked.len(), 3);
    for pid in [session, group] {
        assert_eq!(
            unsafe { libc::getpgid(pid) },
            pid,
            "fixture did not escape root group"
        );
        assert!(shim.tracked.contains(&native_identity(pid).unwrap()));
    }
    match trigger {
        Trigger::Kill => shim.send(KILL, &[]),
        Trigger::NormalExit => fixture.trigger("root.exit"),
        Trigger::RootKill => assert_eq!(unsafe { libc::kill(root, libc::SIGKILL) }, 0),
        Trigger::OwnerEof => drop(shim.stdin.take()),
        Trigger::Timeout => {}
    }
    let exit = shim.finish();
    if trigger == Trigger::OwnerEof {
        assert!(
            exit.is_none(),
            "transport EOF must not report to a dead owner"
        );
    } else {
        let exit = exit.unwrap();
        assert_tracking_exit(&exit);
        assert_eq!(exit["timed_out"], trigger == Trigger::Timeout);
        if trigger == Trigger::NormalExit {
            assert_eq!(exit["status"], 23);
        }
        if trigger == Trigger::RootKill {
            assert_eq!(exit["signal"], libc::SIGKILL);
        }
    }
    for pid in [root, session, group] {
        assert_gone(pid);
    }
}

#[test]
fn kill_reaches_escaped_sessions_and_groups_on_pipes_and_ptys() {
    for pty in [false, true] {
        cleanup_case(Trigger::Kill, pty);
    }
}

#[test]
fn normal_root_exit_reaps_escaped_descendants_on_pipes_and_ptys() {
    for pty in [false, true] {
        cleanup_case(Trigger::NormalExit, pty);
    }
}

#[test]
fn externally_killed_root_reaps_escaped_descendants_on_pipes_and_ptys() {
    for pty in [false, true] {
        cleanup_case(Trigger::RootKill, pty);
    }
}

#[test]
fn transport_eof_reaps_escaped_descendants_on_pipes_and_ptys() {
    for pty in [false, true] {
        cleanup_case(Trigger::OwnerEof, pty);
    }
}

#[test]
fn timeout_reaps_escaped_descendants_on_pipes_and_ptys() {
    for pty in [false, true] {
        cleanup_case(Trigger::Timeout, pty);
    }
}

#[test]
fn interrupt_graces_advance_concurrently_for_all_tracked_groups() {
    for pty in [false, true] {
        let fixture = Fixture::new();
        let mut shim = Shim::new(Some(&["track_descendants_v1", "bounded_run_v1"]));
        let mut request = fixture.request("groups", pty);
        request["interrupt_signal"] = json!("int");
        request["interrupt_grace_ms"] = json!(200);
        request["kill_grace_ms"] = json!(200);
        shim.spawn(request);
        shim.root_tracked();
        let root = fixture.ready("root");
        let session = fixture.ready("session");
        let group = fixture.ready("group");
        shim.wait_tracked(&[session, group]);
        let started = Instant::now();
        shim.send(KILL, &[]);
        let exit = shim.finish().unwrap();
        assert_tracking_exit(&exit);
        assert_eq!(exit["signal"], libc::SIGKILL);
        assert!(
            started.elapsed() >= Duration::from_millis(380),
            "configured signal graces were skipped"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "graces multiplied by group count"
        );
        for label in ["root", "session", "group"] {
            let signals = fixture.signals(label);
            assert_eq!(
                signals.len(),
                2,
                "unexpected signal sequence for {label}: {signals:?}"
            );
            assert_eq!(signals[0]["signal"], "INT");
            assert_eq!(signals[1]["signal"], "TERM");
        }
        for pid in [root, session, group] {
            assert_gone(pid);
        }
    }
}

#[test]
fn tracking_composes_with_spawn_identity_and_precedes_output() {
    for pty in [false, true] {
        let mut shim = Shim::new(Some(&[
            "spawn_identity_v1",
            "track_descendants_v1",
            "structured_spawn_errors_v1",
        ]));
        shim.spawn(json!({"argv":["sh","-c","echo first; sleep 30"],"pty":pty,"timeout_ms":5_000,"kill_grace_ms":0}));
        let (tag, bytes) = shim.recv().unwrap();
        assert_eq!(tag, SPAWNED);
        let spawned: Value = serde_json::from_slice(&bytes).unwrap();
        let tracked = shim.root_tracked();
        assert_eq!(spawned, tracked);
        assert_eq!(shim.recv().unwrap().0, STDOUT);
        shim.send(KILL, &[]);
        assert_tracking_exit(&shim.finish().unwrap());
    }
}

#[test]
fn sessions_without_selection_do_not_emit_tracking_frames_or_scope() {
    for features in [
        None,
        Some([].as_slice()),
        Some(["bounded_run_v1"].as_slice()),
        Some(["spawn_identity_v1"].as_slice()),
    ] {
        let mut shim = Shim::new(features);
        shim.spawn(json!({"argv":["echo","legacy"],"timeout_ms":5_000}));
        if features.is_some_and(|features| features.contains(&"spawn_identity_v1")) {
            assert_eq!(shim.recv().unwrap().0, SPAWNED);
        }
        assert_eq!(shim.recv().unwrap().0, STDOUT);
        let exit = shim.finish().unwrap();
        assert!(shim.tracked.is_empty());
        assert!(exit.get("scope").is_none());
    }
}

#[test]
fn retained_members_are_cleaned_after_their_group_leader_is_reaped() {
    let fixture = Fixture::new();
    let mut shim = Shim::new(Some(&["track_descendants_v1"]));
    shim.spawn(fixture.request("leaderless", false));
    shim.root_tracked();
    let root = fixture.ready("root");
    let leader = fixture.ready("leader");
    let member = fixture.ready("member");
    shim.wait_tracked(&[leader]);
    assert_eq!(unsafe { libc::getpgid(member) }, leader);
    // TRACKED describes the group leader, not its members. Give the 50 ms
    // inventory loop two more samples while the member is still reachable.
    thread::sleep(Duration::from_millis(150));
    fixture.trigger("leader.exit");
    assert_eq!(fixture.ready("reaped"), root);
    assert_gone(leader);
    assert_eq!(unsafe { libc::getpgid(member) }, leader);
    shim.send(KILL, &[]);
    assert_tracking_exit(&shim.finish().unwrap());
    assert_gone(member);
    assert_gone(root);
}

#[test]
fn scanning_and_cancellation_continue_when_the_child_does_not_read_stdin() {
    for pty in [false, true] {
        let fixture = Fixture::new();
        let mut shim = Shim::new(Some(&["track_descendants_v1"]));
        shim.spawn(fixture.request("late", pty));
        shim.root_tracked();
        let root = fixture.ready("root");
        // This exceeds both pipe and raw terminal input capacity. The child
        // never reads it, so a write on the supervision loop would prevent
        // discovery of the group created after this frame was accepted.
        shim.send(STDIN, &vec![b'x'; 1024 * 1024]);
        thread::sleep(Duration::from_millis(100));
        fixture.trigger("create");
        let late = fixture.ready("late");
        shim.wait_tracked(&[late]);
        shim.send(KILL, &[]);
        assert_tracking_exit(&shim.finish().unwrap());
        assert_gone(root);
        assert_gone(late);
    }
}

#[test]
fn closed_protocol_output_cleans_all_groups_when_stdin_reader_is_blocked_without_timer() {
    fn read_protocol(reader: &mut impl Read) -> (u8, Vec<u8>) {
        let mut size = [0; 4];
        reader.read_exact(&mut size).unwrap();
        let mut frame = vec![0; u32::from_be_bytes(size) as usize];
        reader.read_exact(&mut frame).unwrap();
        (frame[0], frame[1..].to_vec())
    }

    for pty in [false, true] {
        let fixture = Fixture::new();
        let mut child = Command::new(env!("CARGO_BIN_EXE_forcola_shim"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut output = child.stdout.take().unwrap();
        let (_sender, frames) = mpsc::channel();
        let mut shim = Shim {
            stdin: child.stdin.take(),
            child,
            frames,
            reader: None,
            tracked: Vec::new(),
        };
        shim.send(HELLO, br#"{"features":["track_descendants_v1"]}"#);
        assert_eq!(read_protocol(&mut output).0, CAPABILITIES);
        let mut request = fixture.request("blocked_groups", pty);
        request.as_object_mut().unwrap().remove("timeout_ms");
        // Leave output pumps parked on credit. No output write failure or run
        // timer can rescue a reader blocked on forwarding child input.
        request["window_bytes"] = json!(4096);
        request["strict_output"] = json!(true);
        shim.spawn(request);
        let root = fixture.ready("root");
        let session = fixture.ready("session");
        let group = fixture.ready("group");
        while shim.tracked.len() < 3 {
            let (tag, payload) = read_protocol(&mut output);
            assert_eq!(tag, TRACKED);
            shim.tracked.push(serde_json::from_slice(&payload).unwrap());
        }
        assert!(shim.tracked.iter().any(|identity| identity["pid"] == root));
        assert!(shim
            .tracked
            .iter()
            .any(|identity| identity["pid"] == session));
        assert!(shim.tracked.iter().any(|identity| identity["pid"] == group));
        let mut input = shim.stdin.take().unwrap();
        let (written, completion) = mpsc::channel();
        let writer = thread::spawn(move || {
            let payload = vec![b'x'; 1024 * 1024];
            let result = (|| -> std::io::Result<()> {
                for _ in 0..20 {
                    input.write_all(&((payload.len() + 1) as u32).to_be_bytes())?;
                    input.write_all(&[STDIN])?;
                    input.write_all(&payload)?;
                }
                Ok(())
            })();
            let _ = written.send(result);
        });
        assert!(matches!(
            completion.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(shim.child.try_wait().unwrap().is_none());
        // Close only the peer read end. The blocked producer still owns the
        // protocol input fd, so neither a KILL frame nor StdinClosed can arrive.
        drop(output);
        for pid in [root, session, group] {
            assert_gone(pid);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while shim.child.try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "shim lingered after peer closure"
            );
            thread::sleep(Duration::from_millis(10));
        }
        completion
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .ok();
        writer.join().unwrap();
    }
}

#[cfg(target_os = "linux")]
#[test]
fn subreaper_finds_immediately_orphaned_descendants_without_a_live_root() {
    for _ in 0..5 {
        let fixture = Fixture::new();
        let mut shim = Shim::new(Some(&["track_descendants_v1"]));
        shim.spawn(fixture.request("orphan", false));
        shim.root_tracked();
        let orphan = fixture.ready("orphan");
        let exit = shim.finish().unwrap();
        assert_tracking_exit(&exit);
        assert_eq!(exit["status"], 23);
        assert!(shim
            .tracked
            .iter()
            .any(|identity| identity["pid"] == orphan));
        assert_gone(orphan);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn adopted_zombies_are_reaped_while_root_is_still_running() {
    let fixture = Fixture::new();
    let mut shim = Shim::new(Some(&["track_descendants_v1"]));
    shim.spawn(fixture.request("adopted", false));
    shim.root_tracked();
    let root = fixture.ready("root");
    let orphan = fixture.ready("orphan");
    shim.wait_tracked(&[orphan]);
    fixture.trigger("orphan.exit");
    assert_gone(orphan);
    assert_eq!(unsafe { libc::kill(root, 0) }, 0);
    assert!(shim.child.try_wait().unwrap().is_none());
    shim.send(KILL, &[]);
    assert_tracking_exit(&shim.finish().unwrap());
    assert_gone(root);
}

#[cfg(target_os = "linux")]
#[test]
fn tracking_discovers_forks_owned_by_nonleader_threads() {
    let fixture = Fixture::new();
    let mut shim = Shim::new(Some(&["track_descendants_v1"]));
    shim.spawn(fixture.request("thread", false));
    shim.root_tracked();
    let root = fixture.ready("root");
    let child = fixture.ready("thread_child");
    shim.wait_tracked(&[child]);
    shim.send(KILL, &[]);
    assert_tracking_exit(&shim.finish().unwrap());
    assert_gone(child);
    assert_gone(root);
}

#[test]
fn natural_exit_keeps_its_outcome_and_output_when_descendant_cleanup_crosses_deadline() {
    for cancel_after_exit in [false, true] {
        let fixture = Fixture::new();
        let script = r#"
import os, signal, sys, time
path = sys.argv[1]
pid = os.fork()
if pid == 0:
    os.setsid()
    signal.signal(signal.SIGTERM, lambda *_: None)
    with open(path + '/session.ready', 'w') as stream:
        stream.write(str(os.getpid()))
    time.sleep(30)
    os._exit(0)
while not os.path.exists(path + '/root.exit'):
    time.sleep(.005)
print('complete trailing output', flush=True)
os._exit(0)
"#;
        let mut shim = Shim::new(Some(&["track_descendants_v1"]));
        let started = Instant::now();
        shim.spawn(json!({
            "argv":["python3", "-c", script, fixture.path],
            "timeout_ms": 2000, "kill_grace_ms": 2500, "strict_output": true
        }));
        let root = shim.root_tracked()["pid"].as_i64().unwrap() as i32;
        let descendant = fixture.ready("session");
        shim.wait_tracked(&[descendant]);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "fixture startup used the root's deadline"
        );
        fixture.trigger("root.exit");
        // The root is an unreaped zombie during descendant cleanup. Confirm
        // from outside that its normal exit occurred before the run deadline.
        let deadline = Instant::now() + Duration::from_millis(500);
        while !process_exited(root) {
            assert!(
                Instant::now() < deadline,
                "root did not exit before its deadline"
            );
            thread::sleep(Duration::from_millis(5));
        }
        if cancel_after_exit {
            // Let the waiter event arrive before this late cancellation.
            thread::sleep(Duration::from_millis(100));
            shim.send(KILL, &[]);
        }
        let mut output = Vec::new();
        let exit = loop {
            let (tag, bytes) = shim.recv().expect("missing EXIT");
            match tag {
                STDOUT => output.extend(bytes),
                STDERR | TRACKED => {}
                EXIT => break serde_json::from_slice::<Value>(&bytes).unwrap(),
                _ => panic!("unexpected frame {tag:#x}"),
            }
        };
        assert!(started.elapsed() >= Duration::from_millis(2500));
        assert_tracking_exit(&exit);
        assert_eq!(exit["status"], 0);
        assert_eq!(exit["timed_out"], false);
        assert_eq!(exit["output_truncated"], false);
        assert_eq!(output, b"complete trailing output\n");
        assert!(shim.finish().is_none());
        assert_gone(root);
        assert_gone(descendant);
    }
}

#[cfg(target_os = "macos")]
fn process_exited(pid: i32) -> bool {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
    let result = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            1,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    result == size && unsafe { info.assume_init() }.pbi_status == 5
}

#[cfg(target_os = "linux")]
fn process_exited(pid: i32) -> bool {
    fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .is_some_and(|stat| {
            stat.rsplit_once(')')
                .is_some_and(|(_, fields)| fields.trim_start().starts_with('Z'))
        })
}

#[cfg(target_os = "macos")]
#[test]
fn immediately_exiting_shell_keeps_unsampled_root_group_members_signalable() {
    for _ in 0..5 {
        let fixture = Fixture::new();
        let mut shim = Shim::new(Some(&["track_descendants_v1"]));
        shim.spawn(json!({
            "argv":["sh", "-c", "sleep 60 & echo $! > \"$1/session.ready\"; exit 0", "sh", fixture.path],
            "timeout_ms": 5000, "kill_grace_ms": 100
        }));
        let root = shim.root_tracked()["pid"].as_i64().unwrap() as i32;
        let member = fixture.ready("session");
        let exit = shim.finish().unwrap();
        assert_tracking_exit(&exit);
        assert_eq!(exit["status"], 0);
        assert_gone(root);
        assert_gone(member);
    }
}

#[test]
fn large_stdin_uses_backpressure_without_stopping_a_slow_child() {
    const CHUNKS: usize = 1280;
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut shim = Shim::new(Some(&["track_descendants_v1"]));
    let script = "import sys,time\ntime.sleep(.2)\nsize=0\nwhile True:\n data=sys.stdin.buffer.read(65536)\n if not data: break\n size+=len(data)\n time.sleep(.001)\nprint(size,flush=True)\n";
    shim.spawn(json!({"argv":["python3", "-c", script], "timeout_ms":15000, "kill_grace_ms":100}));
    let root = shim.root_tracked()["pid"].as_i64().unwrap() as i32;
    let bytes = vec![b'x'; CHUNK_SIZE];
    for _ in 0..CHUNKS {
        shim.send(STDIN, &bytes);
    }
    shim.send(0x03, &[]);
    let mut output = Vec::new();
    let exit = loop {
        let (tag, bytes) = shim.recv().expect("missing EXIT");
        match tag {
            STDOUT => output.extend(bytes),
            STDERR | TRACKED => {}
            EXIT => break serde_json::from_slice::<Value>(&bytes).unwrap(),
            _ => panic!("unexpected frame {tag:#x}"),
        }
    };
    assert_tracking_exit(&exit);
    assert_eq!(exit["status"], 0);
    assert_eq!(exit["timed_out"], false);
    assert_eq!(
        String::from_utf8(output).unwrap().trim(),
        (CHUNKS * CHUNK_SIZE).to_string()
    );
    assert!(shim.finish().is_none());
    assert_gone(root);
}
