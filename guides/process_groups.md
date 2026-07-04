# Process groups and cleanup

This is the reference for Forcola's process-lifecycle semantics: how the kill
mechanism works, what it guarantees, and what it cannot reach.

## The kill mechanism

The shim calls `setsid` before `exec`, so the child leads a new process group.
Every process the child forks inherits that group unless it deliberately
leaves. When the shim kills the group it uses the negative pgid:

```text
kill(-pgid, SIGTERM)   then, after the kill grace:   kill(-pgid, SIGKILL)
```

`kill(-pgid, sig)` signals every process in the group at once: the CLI and
everything it forked. The default grace between SIGTERM and SIGKILL is 5000 ms
(`:kill_grace_ms`). A process that installs a SIGTERM handler and ignores it is
killed by the SIGKILL that follows.

## When the kill fires

The group is killed on any of:

- A bounded run hitting `:timeout_ms`.
- An early halt of a `Forcola.Stream.lines/2` stream.
- Termination of a `Forcola.Daemon` GenServer, including supervisor shutdown
  and owner crash.
- A `Forcola.Duplex.close/1` call, or death of the session's owner.
- BEAM death, including `kill -9` of the VM.

## Mandatory timeout on bounded runs

`Forcola.run/2` and `Forcola.Stream.lines/2` require `:timeout_ms`. There is no
default and no way to opt out: a bounded run without a bound is the leak this
library exists to close. On expiry the group is killed and the caller receives
`{:error, {:timeout, partial_result}}` (or, for a stream, a raised
`Forcola.Stream.Error`) carrying output captured so far.

`:timeout_ms` bounds the whole run, not the gap between lines.
`Forcola.Stream.lines/2` also takes an optional `:idle_timeout_ms` that bounds
the gap between output frames: if no output (stdout or stderr) arrives within
the interval the producer is treated as stalled, the group is killed, and
`Forcola.Stream.Error` is raised with `idle_timed_out: true`. It is independent
of and composable with `:timeout_ms`; whichever bound fires first wins.

`Forcola.Daemon` and `Forcola.Duplex` take no `:timeout_ms`; passing one raises
`ArgumentError`. Their bound is the supervisor and the owner process
respectively.

## BEAM death as stdin EOF

The shim is a port program, a separate OS process, not a NIF. The BEAM holds
the write end of the shim's stdin pipe. When the BEAM dies, that pipe closes,
and the shim reads EOF on stdin. The shim treats stdin EOF as the signal to
kill the group and exit.

This path does not depend on the BEAM running any cleanup code, so it covers
`kill -9` of the VM, where no `terminate/2` callback or `after` block would
run. It also covers a `Forcola.Stream`, `Forcola.Daemon`, or `Forcola.Duplex`
process being killed brutally: the port closes, the shim sees EOF, and the
group dies.

## Group death confirmed before the call returns

The shim confirms the group is dead before it reports back. Concretely: after
the kill sequence, the shim waits for the group to be reaped, then sends its
EXIT frame. The Elixir side blocks on that frame. So when a bounded run returns
a timeout, when an early stream halt returns, when a daemon's `terminate/2`
finishes, or when `Forcola.Duplex.close/1` returns, the group is already dead.
`{:error, :timeout}` means the process is gone, not that it may still be
running.

### The backstop exception

The confirmation guarantee has one exception. The Elixir side arms a backstop
deadline (`timeout_ms + kill_grace_ms` plus a margin) in case the shim never
reports back at all, for example if the shim is wedged or the BEAM-to-shim pipe
is stuck. If that deadline fires first, the result's status is
`{:signal, :unconfirmed}`: death was requested but not confirmed. Closing the
port is the remaining kill lever (the shim treats stdin EOF as BEAM death).

`{:signal, :unconfirmed}` also appears when the shim exits without sending an
EXIT or ERROR frame (for example it crashed, or was itself SIGKILLed). A
SIGKILLed shim gets no chance to kill the group, so the child may survive,
reparented to pid 1. Treat `{:signal, :unconfirmed}` as leaked and investigate;
see `Forcola.Result`.

## What group kill cannot reach

A process-group escape audit
([#9](https://github.com/joshrotenberg/forcola/issues/9)) tested the target CLI
set on macOS: agent CLIs with stdio MCP servers, git with hooks and the
fsmonitor daemon, make, cargo, npm, aws, gcloud, ffmpeg, redis-server in
foreground mode, and shell constructs like `nohup` and `disown`. The method was
to snapshot the process tree during a live run and flag any descendant whose
`pgid` differs from the CLI's. All of them keep their entire tree in the
child's process group and die to the group kill.

The escapes fall into three classes, and no client-side mechanism closes them:

### Deliberate daemonizers

A child that deliberately daemonizes, by double-forking plus `setsid` or via a
flag like `redis-server --daemonize yes`, leaves the process group and survives
the kill. It has explicitly asked to outlive its parent.

Run servers in foreground mode under `Forcola.Daemon`. Foreground operation is
the same contract every process supervisor (systemd, runit, foreman) imposes:
the supervisor owns the process lifecycle, so the process must not fork away
from it.

On Linux, the opt-in `cgroup: true` option contains deliberate daemonizers.
When it is set and the shim runs under a delegated cgroup v2 subtree, the child
is placed in a dedicated cgroup before exec, so every descendant it forks
inherits the cgroup regardless of process-group games. On kill the shim writes
`cgroup.kill`, which SIGKILLs the whole subtree at once (Linux 5.14+), reaping
the escapee the process-group kill could not reach. This is layered on top of
the process-group kill, not in place of it: the SIGTERM-then-SIGKILL group
sequence still runs, and `cgroup.kill` is the backstop for anything that left
the group.

`cgroup: true` requires cgroup delegation. The BEAM must run inside a
delegatable unit: under systemd, set `Delegate=yes` on the service, or wrap the
run in `systemd-run --user --scope`. Without a delegated, writable subtree the
shim cannot create a child cgroup.

It is Linux-only and always degrades gracefully, never erroring. On macOS, on
non-cgroup-v2 systems, and when the subtree is not delegated, `cgroup: true`
falls back to the ordinary process-group kill and logs a warning; ordinary
in-group grandchildren still die exactly as before. The EXIT report carries a
`contained` flag reporting which mechanism was used, surfaced as a
`Logger.debug` line when containment was active. See `Forcola.run/2` for the
option.

### Client/daemon control channels

Client/daemon CLIs such as docker: the CLI is only a control channel. The
container or build runs under the docker daemon, not under the CLI. Killing the
client never stops that work, and no client-side mechanism, process group or
cgroup, can, because the work was never a descendant of the client. Use the
tool's own teardown semantics (`docker run --rm`, `docker kill`) on top of
Forcola.

### Scheduler-owned work

Work handed to system schedulers (`git maintenance` background jobs, launchd or
systemd timers) was never a child of the CLI at all. The CLI registered a job
with the scheduler and returned; the scheduler runs it later, independently.
This is out of scope for any process-based mechanism.

## Platform residual

On Linux, the opt-in cgroup v2 layer
([#15](https://github.com/joshrotenberg/forcola/issues/15)) contains deliberate
daemonizers by placing the child's whole subtree in a cgroup and writing
`cgroup.kill`, rather than relying on the process group. It requires a delegated
cgroup v2 subtree; see ["Deliberate daemonizers"](#deliberate-daemonizers)
above. On macOS nothing can contain a deliberate daemonizer, which is equally
true of erlexec and MuonTrap, so `cgroup: true` falls back to the process-group
kill there. Windows support is tracked separately in
[#34](https://github.com/joshrotenberg/forcola/issues/34).
