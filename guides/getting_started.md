# Getting started

Forcola runs OS processes through a small Rust shim that puts each child in
its own process group and kills the whole group, SIGTERM then SIGKILL, when
the run times out or the BEAM dies. This guide covers installation and the
four execution modes.

## Installation

Add `forcola` to your dependencies:

```elixir
def deps do
  [
    {:forcola, "~> 0.1"}
  ]
end
```

Requires Elixir 1.18+ and OTP 27+. No Rust toolchain is needed on the five
precompiled targets (macOS arm64 and x86-64, Linux x86-64 and arm64 glibc,
x86-64 musl): the shim binary is downloaded from the matching GitHub Release
and verified against a SHA256 checksum at compile time. On other targets, or
to opt out of the download, set `FORCOLA_BUILD=1` to build the shim from
source with cargo.

## A first run

`Forcola.run/2` runs a command to completion under the shim. The argument is
`[binary | args]`, and `:timeout_ms` is required.

```elixir
{:ok, %Forcola.Result{status: 0, stdout: out}} =
  Forcola.run(["echo", "hello"], timeout_ms: 5_000)
```

Any exit status is `{:ok, %Forcola.Result{}}`; callers branch on `:status`. A
non-zero exit is a result, not an error:

```elixir
{:ok, %Forcola.Result{status: 1}} =
  Forcola.run(["false"], timeout_ms: 5_000)
```

`:status` is the exit code for a normal exit, or `{:signal, number}` when the
process died from a signal (for example `{:signal, 15}` for SIGTERM). See
`Forcola.Result`.

## Execution modes

Four shapes, matching what CLI wrapper libraries need:

| Mode | API | Use |
|---|---|---|
| Bounded run | `Forcola.run/2` | One-shot command with mandatory timeout |
| Line stream | `Forcola.Stream.lines/2` | Line output consumed as an `Enumerable` |
| Daemon | `Forcola.Daemon` | Long-running server under a supervision tree |
| Duplex | `Forcola.Duplex` | Bidirectional stdin/stdout session |

### Bounded run: `Forcola.run/2`

A one-shot command with a mandatory timeout.

```elixir
case Forcola.run(["git", "clone", url, dir], timeout_ms: 60_000) do
  {:ok, %Forcola.Result{status: 0}} -> :ok
  {:ok, %Forcola.Result{status: code}} -> {:error, {:exit, code}}
  {:error, {:timeout, %Forcola.Result{stdout: partial}}} -> {:error, :timeout}
  {:error, {:spawn, reason}} -> {:error, {:spawn, reason}}
end
```

Options:

- `:timeout_ms` (required): on expiry the child's process group is killed
  (SIGTERM, then SIGKILL after the kill grace) and
  `{:error, {:timeout, partial_result}}` is returned with output captured so
  far. The group is confirmed dead before the call returns, with one
  exception described in the [process groups guide](process_groups.html).
- `:kill_grace_ms`: SIGTERM-to-SIGKILL grace in milliseconds, default
  `5_000`.
- `:cd`: working directory.
- `:env`: list of `{name, value}` strings.
- `:merge_stderr`: route stderr into stdout, default `false`.
- `:user`: run the child as this user, a string username or an integer uid.
- `:group`: run the child with this group as its primary gid, a string group
  name or an integer gid.
- `:cgroup`: opt in to Linux cgroup v2 containment, default `false`. See
  "cgroup containment" below.

Return shapes:

- `{:ok, %Forcola.Result{status: status, stdout: out, stderr: err}}` for any
  completed run. `status` is an exit code or `{:signal, n}`.
- `{:error, {:timeout, %Forcola.Result{}}}` on timeout, carrying output
  captured so far.
- `{:error, {:spawn, reason}}` where `reason` is `:shim_not_found`, a string
  reported by the shim, or `{:shim_exited, %Forcola.Result{}}`.

#### Running as a different user

`:user` and `:group` make the shim drop privileges before executing the child.
They work the same way in every mode (`run/2`, `Forcola.Stream`,
`Forcola.Daemon`, `Forcola.Duplex`).

```elixir
Forcola.run(["id", "-un"], timeout_ms: 5_000, user: "nobody")
Forcola.run(["some-tool"], timeout_ms: 5_000, user: 1001, group: "builders")
```

The shim resolves the user/group in the parent process, then in the child
(after `setsid`, before exec) calls `setgroups`, `setgid`, and `setuid` in that
order. Key properties:

- POSIX-only. Windows is out of scope; a `:user`/`:group` on a platform without
  these semantics fails with a clear error.
- One-way drop. The shim process must already run with enough privilege to drop
  (root, or `CAP_SETUID`/`CAP_SETGID` on Linux). Requesting the user the shim
  already runs as is a no-op and always succeeds.
- Fail-closed. If the user/group cannot be resolved, or the shim lacks the
  privilege to drop, the child is never executed. The failure surfaces as the
  mode's normal spawn error (`{:error, {:spawn, reason}}` for `run/2`,
  `{:forcola_exit, session, {:spawn_error, reason}}` for `Forcola.Duplex`). The
  command never runs as the shim's own user when a different user was requested.

#### cgroup containment

The process-group kill reaches every descendant that stays in the child's
process group, but a target that deliberately daemonizes (double-fork plus
`setsid`, or a `--daemon` flag) leaves the group and survives. On Linux,
`cgroup: true` adds a backstop: the child runs in a dedicated cgroup v2 cgroup,
so descendants it forks inherit the cgroup, and on kill the shim writes
`cgroup.kill` to SIGKILL the whole subtree at once. It is layered on top of the
process-group kill, never in place of it, and works the same in every mode.

```elixir
Forcola.run(["some-daemonizing-tool"], timeout_ms: 5_000, cgroup: true)
```

Key properties:

- Linux only. On macOS and other platforms `cgroup: true` is a no-op that falls
  back to the process-group kill.
- Requires cgroup delegation. The BEAM must run inside a delegatable unit: under
  systemd, `Delegate=yes` on the service, or wrapping the run in
  `systemd-run --user --scope`. Without a delegated, writable cgroup v2 subtree
  the shim cannot create a child cgroup.
- Graceful fallback, never an error. On macOS, on non-cgroup-v2 systems, or when
  the subtree is not delegated, it degrades to the process-group kill and logs a
  warning; ordinary in-group grandchildren still die exactly as before. A
  `Logger.debug` line is emitted when containment was actually active.

See the [process groups guide](process_groups.html#deliberate-daemonizers) for
the mechanism.

### Line stream: `Forcola.Stream.lines/2`

Stdout as a lazy stream of lines, for CLIs that emit NDJSON or line-oriented
progress. Lines arrive without their trailing newline.

```elixir
Forcola.Stream.lines(["claude", "-p", prompt], timeout_ms: 300_000)
|> Stream.map(&:json.decode/1)
|> Enum.to_list()
```

Options: the same as `Forcola.run/2`. `:timeout_ms` is required and bounds the
whole run, not the gap between lines.

`:idle_timeout_ms` (optional) bounds the gap between output frames instead: if no
STDOUT or STDERR data arrives within the interval the producer is treated as
stalled, the group is killed, and `Forcola.Stream.Error` is raised with
`idle_timed_out: true`. It is independent of and composable with `:timeout_ms`;
whichever bound fires first wins. The idle deadline resets on any output (stdout
or stderr), not only newline-terminated lines. This suits a long-lived follow
(agent stream-json, `docker events`) that may legitimately run for hours but
should die if the producer hangs.

Termination:

- A zero exit ends the stream cleanly.
- A non-zero exit, death by signal, timeout, or spawn failure raises
  `Forcola.Stream.Error` after every line produced before death has been
  emitted. Stderr captured during the run rides in the exception.
- Halting the stream early (`Enum.take/2`, `Stream.take_while/2`, an exception
  downstream) kills the process group and blocks until the shim confirms the
  group is dead.

### Daemon: `Forcola.Daemon`

A long-running server under a supervision tree. When the GenServer terminates
for any reason, including supervisor shutdown and owner crash, the shim kills
the group and the terminate blocks until the group is confirmed dead.

```elixir
children = [
  {Forcola.Daemon,
   argv: ["redis-server", "--port", "6399"],
   name: MyApp.Redis,
   ready: fn -> match?({:ok, _}, :gen_tcp.connect(~c"localhost", 6399, [])) end}
]
```

A daemon has no `:timeout_ms`; its bound is its supervisor. Passing
`:timeout_ms` raises `ArgumentError`.

Options:

- `:argv` (required): `[binary | args]` as in `Forcola.run/2`.
- `:name`: optional GenServer registration name.
- `:cd`, `:env`, `:merge_stderr`, `:user`, `:group`, `:cgroup`: as in `Forcola.run/2`.
- `:kill_grace_ms`: SIGTERM-to-SIGKILL grace, default `5_000`.
- `:output`: where child output goes, default `:logger`.
- `:log_output`: `Logger` level for `output: :logger`, default `:info`.
- `:log_prefix`: string prepended to each logged line, default `""`.
- `:ready`: optional zero-arity readiness check.
- `:ready_timeout_ms`: readiness deadline, default `5_000`.
- `:ready_poll_ms`: readiness poll interval, default `100`.

Output routing:

- `output: :logger` (default): stdout and stderr are logged line by line at
  `:log_output`, prefixed with `:log_prefix`.
- `output: fun`: a 2-arity function called as `fun.(:stdout | :stderr, chunk)`
  with raw chunks, from the daemon process.
- `output: {:send, pid}`: `pid` receives
  `{Forcola.Daemon, daemon_pid, {:stdout | :stderr, chunk}}` messages.

Readiness: with `ready: fun`, `init` polls `fun.()` (every `:ready_poll_ms`)
until it returns a truthy value, so `start_link` and supervisor startup block
until the server accepts connections. If the check does not pass within
`:ready_timeout_ms`, or the child exits first, the group is killed and
`start_link` returns `{:error, :ready_timeout}` or
`{:error, {:exited_before_ready, reason}}`.

Exit and restart: when the child exits on its own the daemon stops. Status 0
stops `:normal`, a non-zero status stops `{:exit_status, n}`, and death by
signal stops `{:exit_signal, n}`. Under `restart: :permanent` any of these
restarts the daemon; under `:transient` only the abnormal ones do.

### Duplex: `Forcola.Duplex`

A bidirectional stdin/stdout session, for interactive CLIs driven over stdin.
The caller that opens the session is its owner and receives its messages.

```elixir
{:ok, session} =
  Forcola.Duplex.open(["claude", "--input-format", "stream-json"], [])

:ok = Forcola.Duplex.send_line(session, json)

receive do
  {:forcola_line, ^session, line} -> line
end

:ok = Forcola.Duplex.close(session)
```

There is no `:timeout_ms`; the session is bounded by its owner process and
`close/1`. Passing `:timeout_ms` raises `ArgumentError`.

Options:

- `:cd`, `:env`, `:merge_stderr`, `:user`, `:group`, `:cgroup`: as in `Forcola.run/2`.
- `:kill_grace_ms`: SIGTERM-to-SIGKILL grace, default `5_000`.
- `:pty`: run the child under a pseudo-terminal (default `false`), for CLIs
  that behave differently when they detect a tty. See
  [pseudo-terminal](#pseudo-terminal) below.
- `:pty_rows`, `:pty_cols`: initial pty window size, applied only under
  `pty: true`.

API:

- `send_line/2`: writes a line to the child's stdin (a newline is appended).
  Returns `{:error, :closed}` once the session is over or stdin was closed
  with `send_eof/1`.
- `send_eof/1`: closes the child's stdin without killing the group, for CLIs
  that exit when input ends. The child's own exit then arrives as a
  `:forcola_exit` message.
- `close/1`: kills the child's process group and blocks until the shim
  confirms the group is dead. Idempotent.

Messages to the owner:

- `{:forcola_line, session, line}`: a stdout line, without its trailing
  newline.
- `{:forcola_stderr, session, line}`: a stderr line, unless `merge_stderr:
  true` routed stderr into `:forcola_line`.
- `{:forcola_exit, session, status}`: the child exited on its own. `status`
  is the exit code, `{:signal, n}` for death by signal,
  `{:spawn_error, reason}` if it never started, or `:shim_exited` if the shim
  died without reporting. The session is over; `close/1` is not required.

A spawn failure is asynchronous: `open/2` still returns `{:ok, session}` and
the failure arrives as `{:forcola_exit, session, {:spawn_error, reason}}`.

#### Pseudo-terminal

`pty: true` runs the child under a pseudo-terminal instead of pipes. CLIs
that detect a tty then behave as they do in a real terminal: line buffering,
color output, progress rendering, and interactive prompts (password entry,
pagers, REPLs, TUIs).

```elixir
{:ok, session} =
  Forcola.Duplex.open(["/bin/sh", "-c", "test -t 0 && echo tty || echo pipe"],
    pty: true,
    pty_rows: 24,
    pty_cols: 80
  )

receive do
  {:forcola_line, ^session, "tty"} -> :ok
end
```

A terminal carries one stream, so a pty merges the child's stdout and stderr:
all output arrives as `{:forcola_line, ...}` and no `:forcola_stderr` messages
are produced. Passing `merge_stderr: false` with `pty: true` raises
`ArgumentError`. Group-kill, `send_eof/1`, and owner-death cleanup work the
same as in pipe mode. There is no dynamic resize yet; `:pty_rows`/`:pty_cols`
set the initial size only.

## Next steps

- The [process groups guide](process_groups.html) covers the kill mechanism
  and its guarantees in depth.
- The [adoption guide](adopting_forcola.html) covers slotting Forcola into an
  existing CLI wrapper library.
- The [alternatives guide](alternatives.html) compares Forcola with other
  external-process libraries for the BEAM.
