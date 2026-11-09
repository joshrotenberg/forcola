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
    {:forcola, "~> 0.4"}
  ]
end
```

Requires Elixir 1.18+ and OTP 27+. No Rust toolchain is needed on the five
precompiled targets (macOS arm64 and x86-64, Linux x86-64 and arm64 glibc,
x86-64 musl): the shim binary is downloaded from the matching GitHub Release
and verified against a SHA256 checksum at compile time. On other targets, or
to opt out of the download, set `FORCOLA_BUILD=1` to build the shim from
source with cargo.

### Mix escripts

A native executable inside an escript archive cannot be launched directly.
Include the shim when building your CLI:

```elixir
# In project/0 in the consumer's mix.exs:
escript: [main_module: MyCLI, embed_elixir: true, include_priv_for: [:forcola]]
```

The consumer owns extraction and passes the resulting **trusted absolute
filesystem path** as `:shim_path`. This option is supported by all four APIs:

```elixir
Forcola.run(argv, timeout_ms: 5_000, shim_path: path)
Forcola.Stream.lines(argv, timeout_ms: 5_000, shim_path: path) |> Enum.to_list()
Forcola.Duplex.open(argv, shim_path: path)
Forcola.Daemon.start_link(argv: argv, shim_path: path)
```

Without the option, Mix and releases continue to use the installed
`priv/forcola_shim`. Forcola does not read an environment variable to replace
the shim, extract an archive automatically, or fall back to `System.cmd`.
The selected shim still implements the same timeout, cancellation, and
process-group cleanup protocol.

This consumer helper extracts only the bundled shim into a new directory
for each invocation. `private_parent` must already be an absolute path to
an application-owned directory whose contents and ancestors cannot be
replaced by untrusted users. Mix documents `:escript.extract/2` for reading
[included priv files](https://hexdocs.pm/mix/Mix.Tasks.Escript.Build.html).

```elixir
defmodule MyCLI.Shim do
  def with_shim(private_parent, fun) do
    {:ok, sections} = :escript.extract(:escript.script_name(), [])
    archive = Keyword.fetch!(sections, :archive)
    entry = ~c"forcola/priv/forcola_shim"
    {:ok, [{^entry, binary}]} = :zip.extract(archive, [:memory, {:file_list, [entry]}])

    unique = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
    directory = Path.join(private_parent, "forcola-" <> unique)
    File.mkdir!(directory)

    try do
      File.chmod!(directory, 0o700)
      shim = Path.join(directory, "forcola_shim")
      File.write!(shim, binary, [:binary, :exclusive])
      File.chmod!(shim, 0o500)
      fun.(shim)
    after
      File.rm_rf!(directory)
    end
  end
end

MyCLI.Shim.with_shim(private_parent, fn path ->
  Forcola.run(["git", "--version"], timeout_ms: 5_000, shim_path: path)
end)
```

Each directory is created exclusively, and the path is handed to Forcola
only after the entire file is written and made executable. Concurrent CLI
invocations never reuse a cache file. The executable comes from that
escript's own Forcola build, so versions and architectures cannot share a
stale cached binary. The escript must be built for the host's OS and CPU;
embedding Elixir does not make the native shim portable between targets.

Keep the directory until all streams have been consumed and all duplex
sessions and daemons have stopped. The helper's `after` block removes it
on ordinary completion or an exception; abrupt VM death can leave it
behind, so the consumer's installation policy should clean up abandoned
directories when no invocation uses them. Long-lived supervised daemons
can instead use an executable installed by the application's packaging
step and retained for its lifetime.

The override is trusted executable code running with the BEAM's privileges,
before any child `:user`/`:group` drop. Do not obtain it from untrusted
command input or allow other users to replace the file, symlink target,
or parent directories. Forcola validates the path's basic shape and file
permissions, but does not authenticate a supplied executable or prevent
replacement between validation and launch. If using a persistent cache,
the consumer must additionally isolate versions/targets, verify its contents
against the bundled binary, and publish files atomically in a protected
directory.

A missing default shim keeps the existing `:shim_not_found` error.
An invalid override reports `{:invalid_shim_path, reason}` (for example
`:not_absolute`, `:enoent`, or `:not_executable`); a synchronous OS failure
to open the port reports `{:shim_start_failed, reason}`. These appear in
the mode's normal startup-error shape: `{:error, {:spawn, reason}}` for
bounded runs and daemons, `Forcola.Stream.Error.reason` when consuming a stream, and
`{:error, reason}` from `Forcola.Duplex.open/2`.
If the port opens but its executable then fails, the mode reports its
existing shim-death error instead; duplex and daemon sessions can report
this after startup.

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
  far. The group is normally confirmed dead before the call returns; an
  unconfirmed cleanup is surfaced as `status: {:signal, :unconfirmed}` as
  described in the [process groups guide](process_groups.html).
- `:kill_grace_ms`: SIGTERM-to-SIGKILL grace in milliseconds, default
  `5_000`.
- `:cd`: working directory.
- `:env`: list of `{name, value}` strings.
- `:merge_stderr`: route stderr into stdout, default `false`.
- `:shim_path`: trusted absolute path to an installed shim; see
  [Mix escripts](#mix-escripts). Omit to use the normal `priv` location.
- `:user`: run the child as this user, a string username or an integer uid.
- `:group`: run the child with this group as its primary gid, a string group
  name or an integer gid.
- `:cgroup`: opt in to Linux cgroup v2 containment with `true`, or require it
  before exec with `:required`; default `false`. See
  "cgroup containment" below.

Return shapes:

- `{:ok, %Forcola.Result{status: status, stdout: out, stderr: err}}` for any
  completed run. `status` is an exit code or `{:signal, n}`.
- `{:error, {:timeout, %Forcola.Result{}}}` on timeout, carrying output
  captured so far.
- `{:error, {:spawn, reason}}` where `reason` is `:shim_not_found`, a shim
  path/startup error described above, a string reported by the shim, or
  `{:shim_exited, %Forcola.Result{}}`.

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
  already runs as, without `:group`, is a no-op and always succeeds. An
  explicit `:group` always replaces supplementary groups, even when it resolves
  to the current primary gid, so it still requires permission to call
  `setgroups`.
- Fail-closed. If the user/group cannot be resolved, or the shim lacks the
  privilege to drop, the child is never executed. The failure surfaces as the
  mode's normal spawn error (`{:error, {:spawn, reason}}` for `run/2`,
  `{:forcola_exit, session, {:spawn_error, reason}}` for `Forcola.Duplex`). The
  command never runs as the shim's own user when a different user was requested.

#### cgroup containment

The process-group kill reaches every descendant that stays in the child's
process group. Without descendant tracking, a target that deliberately daemonizes (double-fork plus
`setsid`, or a `--daemon` flag) leaves the group and survives. On Linux,
`cgroup: true` can add a backstop: when placement succeeds, the child runs in a
dedicated cgroup v2 cgroup. Descendants it forks inherit the cgroup, and on
kill the shim writes `cgroup.kill` to SIGKILL the whole subtree at once. It is
layered on top of the process-group kill and works the same in every mode.

```elixir
Forcola.run(["some-daemonizing-tool"], timeout_ms: 5_000, cgroup: true)

# Refuse to run if Linux cgroup v2 containment cannot be established:
Forcola.run(["some-daemonizing-tool"], timeout_ms: 5_000, cgroup: :required)
```

Key properties:

- Linux only. On macOS and other platforms `cgroup: true` is a no-op that falls
  back to the process-group kill.
- Requires cgroup delegation. The BEAM must run inside a delegatable unit: under
  systemd, `Delegate=yes` on the service, or wrapping the run in
  `systemd-run --user --scope`. Without a delegated, writable cgroup v2 subtree
  the shim cannot create a child cgroup.
- With `cgroup: true`, graceful fallback is never an error. On macOS, on non-cgroup-v2 systems, or when
  the subtree is not delegated, it degrades to the process-group kill and logs a
  warning; ordinary in-group grandchildren still die exactly as before. A
  `Logger.debug` line is emitted when containment was actually active; this
  line comes from `run/2` and `Forcola.Daemon` (`Forcola.Stream` and
  `Forcola.Duplex` report their exit through messages or a raise and do not
  emit it). When an EXIT report arrives, `Forcola.Duplex.Terminal.scope`
  reports `:active_cgroup` when placement was active or `:process_group` on
  fallback when tracking is disabled. With tracking enabled, scope instead
  identifies `:tree_contained` on Linux or `:tree_sampled` on macOS; the native
  `contained` flag separately records active cgroup placement. With `cgroup: :required`, the shim instead verifies placement and
  writable `cgroup.kill` before starting the child; failure takes the mode's
  normal spawn-error path, and the command never executes. Required mode is
  Linux-only. It cannot contain work that a separate daemon or scheduler owns.

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
should die if the producer hangs. The error carries the child status from the
shim's confirming EXIT frame; it uses `{:signal, :unconfirmed}` only when that
confirmation is missing, malformed, or itself reports unconfirmed cleanup.

`:window_bytes` (optional) opts into demand-driven backpressure. By default the
shim forwards output as fast as the pipes allow, so a slow consumer can grow
BEAM memory without bound. With a window, the shim reads stdout only while the
stream grants credit. Stderr has a separate finite budget, including with
`merge_stderr: true`. `backpressure: true` is shorthand for a 64 KiB stdout
window. Bounded mode defaults to `max_line_bytes: 65_536` for any
unterminated line and `max_stderr_bytes: 1_048_576` for total stderr; both can
be set to another positive integer. The options require backpressure.

The BEAM buffering bound is approximately one stdout window, the stderr budget
plus one detection byte, one partial line, and protocol frames. Each OS pipe
may hold additional bytes outside BEAM memory. If a limit is crossed, Stream
kills the process group and raises `Forcola.Stream.Error` with a typed `:limit`
and `output_truncated: true`; it does not report a complete stream.

```elixir
# Stream gigabytes to a slow consumer with bounded memory.
Forcola.Stream.lines(["produce-huge-output"], timeout_ms: 600_000, window_bytes: 65_536)
|> Stream.each(&slowly_handle/1)
|> Stream.run()
```

A consumer-driven pause does not trip `:idle_timeout_ms` (the idle clock is reset
each time the stream grants credit); a genuinely hung producer still does, and
`:timeout_ms` still bounds a consumer that never consumes.

Termination:

- A zero exit ends the stream cleanly.
- A non-zero exit, death by signal, timeout, or spawn failure raises
  `Forcola.Stream.Error` after every line produced before death has been
  emitted. Stderr captured during the run rides in the exception.
- Halting the stream early (`Enum.take/2`, `Stream.take_while/2`, an exception
  downstream) kills the process group and blocks for the shim's bounded
  confirmation. It logs a warning if teardown remains unconfirmed.

### Daemon: `Forcola.Daemon`

A long-running server under a supervision tree. When the GenServer terminates
for any reason, including supervisor shutdown and owner crash, the shim kills
the group and the terminate callback blocks for bounded confirmation. It logs
a warning if teardown remains unconfirmed when that backstop expires.

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
- `:cd`, `:env`, `:merge_stderr`, `:user`, `:group`, `:cgroup`, `:shim_path`: as in `Forcola.run/2`.
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

- `:cd`, `:env`, `:merge_stderr`, `:user`, `:group`, `:cgroup`, `:shim_path`: as in `Forcola.run/2`.
- `:kill_grace_ms`: SIGTERM-to-SIGKILL grace, default `5_000`.
- `:terminal_recipient`: optional process that receives
  `{:forcola_terminal, session, terminal}` even if the owner dies. This is
  useful when another process supervises the attempt.
- `:delivery`: `:messages` (default) or `:pull`. Pull mode sends no line
  messages and requires `recv/2` calls to grant output credit.
- `:max_line_bytes`, `:max_output_bytes`, `:max_pending_bytes`: positive
  bounds for pull mode, defaulting to 64 KiB, 16 MiB, and one line plus its
  newline per stream. The pending bound must exceed the line bound.
- `:pty`: run the child under a pseudo-terminal (default `false`), for CLIs
  that behave differently when they detect a tty. See
  [pseudo-terminal](#pseudo-terminal) below.
- `:pty_rows`, `:pty_cols`: initial pty window size, applied only under
  `pty: true`.

API:

- `send_line/3`: writes a line to the child's stdin (a newline is appended)
  and returns once the shim has accepted it. Under `track_descendants: true` a
  child that stopped reading makes the line wait, in order, up to the optional
  timeout (default 5000 ms); on expiry it is not sent and `{:error, :busy}` is
  returned. Returns `{:error, :closed}` once the session is over or stdin was
  closed with `send_eof/2`.
- `send_eof/2`: closes the child's stdin without killing the group, for CLIs
  that exit when input ends. The child's own exit then arrives as a
  `:forcola_exit` message. The EOF queues behind unsent lines under the same
  bound; `{:error, :busy}` means stdin is still open.
- `close/1`: kills the child's process group, waits for its bounded report,
  and returns `:ok` for compatibility. It releases retained terminal evidence.
- `shutdown/1`: closes the session and returns `{:ok, terminal}` with the
  native status and cleanup confirmation kept separate. Repeated calls return
  the same result.
- `await_terminal/2`: waits for a natural exit or another caller's shutdown,
  then returns the retained terminal result. A wait timeout returns
  `{:error, :timeout}` and says nothing about cleanup.
- `forget_terminal/1`: releases the retained result after the owner has
  accounted for it. Otherwise it remains available until the owner exits.
- `recv/2`: in pull mode, demands one `{:stdout, line}` or `{:stderr, line}`.
  It returns `{:done, terminal}` after all output, or typed output-limit
  evidence if a limit stops the run.

For example, a caller that must account for uncertain cleanup can use:

```elixir
{:ok, terminal} = Forcola.Duplex.shutdown(session)

case terminal.confirmation do
  :confirmed -> :ok
  other -> {:cleanup_uncertain, other, terminal.status}
end
```

`terminal.scope` is `:process_group` or `:active_cgroup` when the native
EXIT report identifies the active mechanism, and `:unknown` without a report.
It describes what was observed, not proof that a descendant outside that
mechanism was confined. An abrupt shim loss is `:transport_lost`; a shim
that does not answer before the backstop is `:timeout`. The legacy
`{:forcola_exit, session, status}` message remains unchanged.

For a bounded consumer, use pull delivery and keep calling `recv/2` until
it reports a terminal result:

```elixir
{:ok, session} =
  Forcola.Duplex.open(argv,
    delivery: :pull,
    max_line_bytes: 65_536,
    max_output_bytes: 16_777_216
  )

case Forcola.Duplex.recv(session, 5_000) do
  {:ok, {:stdout, line}} -> handle_stdout(line)
  {:ok, {:stderr, line}} -> handle_stderr(line)
  {:done, terminal} -> handle_terminal(terminal)
  {:error, {:output_limit, terminal}} -> handle_limit(terminal)
  {:error, :timeout} -> Forcola.Duplex.shutdown(session)
end
```

Both native pumps start without credit and stop reading when the consumer
stops calling `recv/2`; the child's pipes then back up. The pending credit
limit applies independently to stdout and stderr, including an unterminated
line. A returned line is the only owner delivery outstanding; no line
messages accumulate in the owner's mailbox. A line or total-output limit
kills the group and records the limit in `terminal.output`, without masking
the observed child status or cleanup confirmation. Shutting down before
draining all output marks it `:truncated`. On natural child exit, continue
calling `recv/2` to drain buffered output before awaiting terminal evidence.
Pull mode rejects `merge_stderr: true` for pipes; a pty still yields a single
merged stdout stream.

Messages to the owner:

- `{:forcola_line, session, line}`: a stdout line, without its trailing
  newline.
- `{:forcola_stderr, session, line}`: a stderr line, unless `merge_stderr:
  true` routed stderr into `:forcola_line`.
- `{:forcola_exit, session, status}`: the child exited on its own. `status`
  is the exit code, `{:signal, n}` for death by signal,
  `{:spawn_error, reason}` if it never started, or `:shim_exited` if the shim
  died without reporting. The session is over; `close/1` is not required.

A child spawn failure is asynchronous: `open/2` still returns `{:ok, session}`
and the failure arrives as `{:forcola_exit, session, {:spawn_error, reason}}`.
A missing or invalid shim fails synchronously with `{:error, reason}`.

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
`ArgumentError`. Group-kill, `send_eof/2`, and owner-death cleanup work the
same as in pipe mode. There is no dynamic resize yet; `:pty_rows`/`:pty_cols`
set the initial size only.

## Distinguish spawn failures

All four modes accept `structured_spawn_errors: true`. It selects
`structured_spawn_errors_v1` during HELLO before SPAWN. An older shim that does
not advertise the feature returns `:incompatible_shim` without executing the
child. This option composes with identity reporting and bounded runs.

```elixir
case Forcola.run(["provider", "--version"],
       cd: "/work/session",
       timeout_ms: 5_000,
       structured_spawn_errors: true
     ) do
  {:error, {:spawn, %Forcola.SpawnError{stage: :cwd, reason: :enoent}}} ->
    {:error, :cwd_missing}

  {:error, {:spawn, %Forcola.SpawnError{stage: :exec, reason: :enoent}}} ->
    {:error, :provider_unavailable}

  result ->
    result
end
```

The struct has `stage`, `reason`, `errno`, `path`, and `message` fields. The
original error string remains available as `message`. `errno` is the native
integer when available; errors from name lookup or unsupported facilities may
have no native errno. Known reason codes are atoms. Unknown reason codes remain
strings, so decoding never creates atoms from wire data.

| Failure | Stage | Typical reason | Path |
| --- | --- | --- | --- |
| Missing working directory | `:cwd` | `:enoent` | Requested `cd` |
| Working directory is a file | `:cwd` | `:enotdir` | Requested `cd` |
| Missing executable | `:exec` | `:enoent` | Requested `argv[0]` |
| Executable lacks execute permission | `:exec` | `:eacces` | Requested `argv[0]` |
| User/group lookup or credential change fails | `:privdrop` | `:not_found` or `:eperm` | `nil` |
| Cgroup setup or placement fails | `:cgroup` | Native error or an unsupported-facility reason | Failed cgroup path when available |

Other spawn setup failures use a setup stage rather than being guessed to be
executable errors. Paths describe the operation attempted; `argv[0]` is not a
resolved PATH search result. An executable with a missing interpreter can also
fail at `:exec` with `:enoent`. No filesystem precheck is used to infer the stage:
working-directory and child setup failures are reported from the actual child
operations before exec. Cgroup best-effort fallback remains unchanged;
`cgroup: :required` reports unavailable containment as a spawn failure.

Each mode keeps its existing error envelope:

- Run returns `{:error, {:spawn, error}}`. Bounded collection retains its
  `{:error, {:spawn, {error, partial_result}}}` form.
- Duplex sends `{:forcola_exit, session, {:spawn_error, error}}` in message mode.
  Both message and pull delivery retain `error` as `terminal.spawn_error`,
  available through `await_terminal/2`, including shutdown drains.
- Daemon terminates with `{:spawn, error}`. During readiness polling,
  `start_link/1` returns `{:error, {:exited_before_ready, {:spawn, error}}}`.
- Stream raises `Forcola.Stream.Error` with the struct in its `reason` field.

Without the option, callers continue to receive the existing error string and
the shim sends the original ERROR payload. Negotiated payloads retain that
string alongside the structured details. Protocol and handshake errors remain
separate from typed spawn failures. Malformed structured details fall back to a
valid legacy reason. Unreadable ERROR frames or invalid top-level reasons
return `:invalid_error`. No SPAWNED frame is emitted for a failed spawn.

## Record process identity

Identity reporting is opt-in and requires a shim advertising
`spawn_identity_v1`. With `spawn_identity: true`, `Forcola.run/2` populates
`result.pid`, `result.pgid`, and `result.start_time`, including on partial
results after timeout. These fields are `nil` when identity is not requested.
Duplex notifies the process that opened it before delivering any child output:

```elixir
{:ok, session} =
  Forcola.Duplex.open(["/bin/cat"], spawn_identity: true)

try do
  receive do
    {:forcola_spawned, ^session, identity} ->
      # Persist identity here and wait for the durable write to finish.
      :ok = RecoveryStore.record(identity)
      {:ok, ^identity} = Forcola.Duplex.spawned(session)
      :ok = Forcola.Duplex.send_line(session, "first input")

    {:forcola_exit, ^session, reason} ->
      raise "child did not start: #{inspect(reason)}"
  after
    5_000 -> raise "identity was not received"
  end
after
  Forcola.Duplex.close(session)
end
```

`RecoveryStore` stands for the application's durable store. The notification
does not pause the executable: the child may already be running or may have
exited. Waiting before `send_line/3` only gates input controlled by the caller.
The identity is observation, not a liveness or cleanup guarantee.

`spawned/1` returns `{:ok, identity}` once observed, `{:error, :pending}` while
waiting, `{:error, :not_requested}` for a live session with reporting disabled,
or `{:error, :closed}` when unavailable after shutdown. Duplex retains its
identity alongside terminal evidence after natural exit until `close/1`,
`forget_terminal/1`, or owner exit releases the record. Daemon queries require
a live daemon. `Forcola.Daemon.spawned/2` accepts a query timeout in milliseconds,
defaulting to 5,000. Any server that cannot reply within that timeout also returns
`{:error, :pending}`, whether or not identity is enabled, including while a named
daemon is still polling readiness.

Daemon sends identity notifications only to an explicit
`spawn_observer: {local_pid, reference}`. The observer enables identity reporting
and receives `{reference, {:spawned, identity}}` before child output is routed,
including during readiness polling. Configure a process that can retain the
identity, rather than sending notifications to the supervisor:

```elixir
run_ref = make_ref()

child =
  {Forcola.Daemon,
   argv: ["redis-server", "--port", "6399"],
   spawn_observer: {recovery_pid, run_ref}}

Supervisor.start_link([child], strategy: :one_for_one)
```

Here `recovery_pid` is the application's recovery process. It receives
`{run_ref, {:spawned, identity}}` on each start or restart. With
`spawn_identity: true` alone, Daemon records identity for `spawned/1` polling
and sends no notification to its starter or supervisor.

For a line stream, supply `spawn_observer: {local_pid, reference}`. This
enables identity reporting and sends `{reference, {:spawned, identity}}`
before the first yielded line, including when backpressure is enabled.
Stream rejects `spawn_identity: true` without a non-nil observer because
there would be no way to retrieve the identity. Run and Duplex reject
`:spawn_observer`; their identity interfaces are the result and owner message.
Observer delivery is unacknowledged; the observer must persist the value
itself. Streams close stdin immediately and do not wait for that persistence.

The identity map has atom keys:

- `pid`: the child's OS process ID.
- `pgid`: its process group ID at capture, equal to `pid` after `setsid`.
- `start_time`: on Linux, `%{kind: :linux, boot_id: string, ticks: integer}`,
  using `/proc/sys/kernel/random/boot_id` and field 22 of `/proc/<pid>/stat`.
  Ticks are native clock ticks since boot, not milliseconds or wall time.
  On macOS, `%{kind: :macos, seconds: integer, microseconds: integer}` is the
  kernel process start timestamp, equivalent to `kp_proc.p_starttime`, with
  seconds since the Unix epoch and a microsecond fraction.

Keep the full native value when recording identity. Formatted `ps` output
loses precision. A later recovery process must reread the leader's native
identity and compare it before signalling; the stored PID alone can be
reused, and Linux records must also match the boot ID. Identity values are
for the same host and PID namespace. This API does not implement recovery
or make checking identity and sending a signal atomic.

Without either identity option, existing wire behavior is unchanged. A shim
without the capability rejects the request before the child executes.
The shim probes its own native identity when HELLO is handled, including an
empty HELLO, so inaccessible process metadata disables the feature before SPAWN.
The probe does not guarantee later access to a child after a privilege change
or a change in permissions. If child identity capture then fails, the shim
kills and reaps the child, which may already have executed.
`SPAWNED` uses tag `0x16`. `TRACKED` uses `0x17` with the same identity payload.
Tracking is not enabled by selecting spawn identity.

## Track descendant groups

Every mode accepts `track_descendants: true`. It selects the independent
`track_descendants_v1` capability before SPAWN; a shim without working native
identity and descendant discovery support rejects the request before executing
the command. The option composes with spawn identity, structured spawn errors,
environment replacement, cgroups and the cooperative interrupt sequence.
Defaults continue to clean the original process group only.

```elixir
ref = make_ref()
{:ok, session} = Forcola.Duplex.open(["some-tool"],
  track_observer: {self(), ref},
  interrupt_signal: :int,
  interrupt_grace_ms: 1_000,
  kill_grace_ms: 1_000)

receive do
  {^ref, {:tracked, identity}} -> RecoveryStore.record(identity)
end

{:ok, groups} = Forcola.Duplex.tracked(session)
terminal = Forcola.Duplex.shutdown(session)
```

`track_observer: {local_pid, reference}` enables tracking, even if
`track_descendants: false` was supplied. Notifications are unacknowledged:
`{reference, {:tracked, identity}}` reports each retained distinct identity
once, starting with the root group before output delivery. The identity has the same native
`pid`, `pgid`, and `start_time` shape as SPAWNED, with `pid == pgid`.
A process that changes groups may produce another distinct group identity. The
native shim can report an identity again after its group was pruned and later
reappears; Elixir deduplicates identities within its retained history.
Receiving a notification does not pause the executable. Persist it and its
full start time if recovery code needs it later.

When the Elixir process survives to report completion, the observer receives
one terminal event,
`{reference, {:tracking_exit, evidence}}`. The evidence map contains `scope`,
`confirmation`, `contained` and `tracked_groups_truncated`.
It also carries two diagnostics. `cleanup_trigger` is what first started
cleanup: `:child_exit`, `:kill`, `:timeout`, `:peer_disconnected`,
`:transport_closed` or `:tracker_uncertain`. `uncertain_reason` is set only for
an unconfirmed cleanup and names the first cause, such as `:incomplete_scans`,
`:signal_failed`, `:cleanup_deadline` or `:group_limit`. Either is `nil` when
the shim did not report a recognized value, including on paths with no EXIT
report. They explain a result and never change it: `confirmation` alone says
whether cleanup was confirmed. `Forcola.Duplex.Terminal` carries the same two
fields.
`:tracker_uncertain` means tracking uncertainty started cleanup before anything
else did. It does not show that the child was still running or that a signal
reached it.
Native EOF cleanup still runs after brutal consumer/server or BEAM death, but
that path cannot promise a final observer event. A Duplex session surviving
its owner's death can still report to a surviving explicit observer. This
event reports cleanup; queued pull output can keep `Duplex.await_terminal/2`
pending until consumption or shutdown.
Successful Stream and Daemon execution can be observed this way without
changing line or output delivery. Daemon never implicitly messages its
supervisor. Tracking without an observer still performs cleanup.
`Forcola.Result.tracked_groups`, `Forcola.Duplex.Terminal.tracked_groups` and
`Forcola.Stream.Error.tracked_groups` retain the first 4,096 distinct group
identities. `tracked_groups_truncated: true` records later history overflow or
a native observation queue overflow. Discarded malformed observations or missing
root history also mark the retained history incomplete. History truncation neither
stops a healthy command nor changes cleanup confirmation: that comes from the
validated native EXIT report for the live set.
The observer receives at most the retained history; persist those observations
if they are needed after the session is released.
Duplex also sends `{:forcola_tracked, session, identity}` to its owner.
`Duplex.tracked/1` retains history after natural exit until release;
`Daemon.tracked/2` queries a live daemon, with a default 5,000 ms timeout.
Disabled tracking returns `{:error, :not_requested}`, a closed record returns
`{:error, :closed}`, and a daemon that cannot reply in time returns
`{:error, :pending}`. An enabled session with no observations yet returns
`{:ok, []}`. A paused stream consumer delays observer delivery, but native
tracking continues independently.

On Linux the shim enables `PR_SET_CHILD_SUBREAPER` before spawning, walks
children of every thread, retains verified descendants, and discovers
orphans adopted by the shim. It reaps adopted zombies while the command runs.
On macOS it samples children through `proc_listchildpids`, retaining verified
observations after reparenting. Both use a 50 ms polling interval and scan at
the start of cleanup; inventory work adds to that interval. A macOS descendant
that is reparented before any sample can escape; `:tree_sampled` never proves whole-tree containment. Platforms
without these mechanisms reject the negotiated feature.

The shim observes root exit without reaping it until tracked cleanup finishes.
The retained root identity keeps its original group signalable. On macOS,
`proc_listpids` inventories that pinned group after root exit, including ordinary
same-group descendants that were never individually sampled. This
preserves original-group cleanup on macOS; descendants that escape the group
and are reparented before a sample remain outside the guarantee.

Normal child exit, child SIGKILL, timeout, idle/output bounds, stream halt,
Duplex close, Daemon shutdown/readiness failure, owner death and BEAM EOF all
use tracking cleanup. For a Duplex session whose child has stopped reading
stdin, close and owner death reach it only after the cleanup budget and port
abort described below. Every verified target receives the configured interrupt
and TERM/KILL sequence concurrently, within a shared deadline rather than one
grace per group. A newly found target can shorten the available grace; the
report is unconfirmed if the full sequence could not be completed. Before
signalling a group, the shim rereads its leader's start time and group. A
missing or mismatched leader is never used to signal that numeric group;
retained members are checked and signalled individually. Checking identity
and sending a POSIX signal are separate operations, so a narrow race remains.

A valid tracking EXIT reports `:tree_contained` on Linux or `:tree_sampled` on
macOS. Confirmation covers every tracked live group/member, including cleanup
after the root has already exited. `cgroup: true` remains the stronger Linux
backstop; `cgroup: :required` still requires placement before exec. Cgroups do
not remove the need to reap adopted children. The native tracker retains at most
4,096 current groups and 65,536 process records, pruning dead groups and processes
so repeated short-lived groups do not consume a lifetime quota. A bounded stdin
channel applies backpressure to the sender while the supervision loop continues
sampling. A KILL frame behind blocked input waits for that input. Tracking checks
protocol-output hang-up independently on every scan, so closing the port or
losing the BEAM still starts cleanup even when the stdin reader is blocked. The
native run timer also remains independent. Final guarded-port closure discards
queued protocol input so Erlang does not keep the OS pipes open to flush it;
the shim still performs the process kills. Strict output drain and post-EXIT
linger check hang-up without waiting for the reader's EOF event.

Backpressure reaches the Duplex owner as waiting `send_line/3` and
`send_eof/2` calls, which return `{:error, :busy}` when their bound elapses.
The session itself never suspends on the port: output, `close/1`, `shutdown/1`
and owner death are handled while input waits. Closing such a session, or
losing its owner, cannot deliver KILL past the unread input; the session waits
out its cleanup budget, reports `confirmation: :timeout`, and aborts the port,
after which the shim's hang-up detection kills the group.

Untracked execution queues child stdin in the shim without bound, so its
supervision loop never waits on a child that refuses input: KILL, timer and
EOF handling are not delayed, and the owner controls how much it sends. The
synchronous write described in
[#111](https://github.com/joshrotenberg/forcola/issues/111) was removed in
0.9.0.

A single transient metadata or child-inventory read error is retried at the next
scan. Three consecutive incomplete scans start bounded cleanup. Invalid metadata,
unsupported discovery, live inventory capacity exhaustion, reaping failures and
signal failures also prevent positive confirmation and trigger cleanup where it
has not already begun. Transport loss still triggers cleanup. History truncation
alone does not stop the command or prevent cleanup confirmation. A valid native
EXIT confirms the live set independently of the retained observation history;
missing/malformed EXIT evidence or an unknown scope cannot confirm cleanup. Observer terminal confirmation is
`:confirmed` or `:unconfirmed`, with `:not_started` for startup failure,
`:timeout` for a missing report at the backstop, and `:transport_lost` for a
lost shim. Missing scope is `:unknown`.

Neither tracking nor cgroups reach work owned by a separate daemon or scheduler.
See the [process groups guide](process_groups.html) for the remaining limits.

## Next steps

- The [process groups guide](process_groups.html) covers the kill mechanism
  and its guarantees in depth.
- The [adoption guide](adopting_forcola.html) covers slotting Forcola into an
  existing CLI wrapper library.
- The [alternatives guide](alternatives.html) compares Forcola with other
  external-process libraries for the BEAM.

## One-shot output and idle bounds

`Forcola.run(argv, timeout_ms: whole, idle_timeout_ms: idle,
max_output_bytes: total)` enforces independent whole-run and idle deadlines
and a cumulative stdout-plus-stderr byte limit. New integer options must be
positive and at most `4_294_967_295`. The exact limit succeeds; the next byte
returns `{:error, {:max_output_bytes, partial_result}}`. Idle expiry returns
`{:error, {:idle_timeout, partial_result}}`. Output resets only the idle clock.
Finite 8 KiB credits on each pipe bound pending output before accumulation and
observer notification, including unterminated lines and the final drain.
Without a cumulative byte option, capture can still grow until the time bound.

## Environment and interrupt options in every mode

Use `env_mode: :replace` to clear inherited variables before applying `env:`;
`:inherit` remains the default. All four modes accept these options. With
strict Run options, or explicit `env_mode` or interrupt options in other modes,
`env` must be a list with binary names and binary, `nil`, or `false` values.
Names must be nonempty without `=` or NUL, and string values must have no NUL.
In every mode, `nil` and `false` remove the named variable, and all other
non-binary values raise `ArgumentError` before opening the shim. Plain calls
retain `env: nil`, maps, and atom names.

For example, strip parent session markers from an interactive child:

```elixir
{:ok, session} = Forcola.Duplex.open(["/absolute/path/to/cli"],
  env: [{"PARENT_SESSION", nil}, {"ERL_FLAGS", false}],
  interrupt_signal: :int,
  interrupt_grace_ms: 500
)
:ok = Forcola.Duplex.close(session)
```

An unset is JSON `null` on the wire and calls `env_remove` in the shim.
It requires `env_unset_v1`, independently of the `bounded_run_v1` requirement
for explicit `env_mode` or interrupt options. An incompatible shim fails before
SPAWN. Removals alone keep Run's legacy collector and error envelopes.

Use `interrupt_signal: :int, interrupt_grace_ms: 500` for cooperative SIGINT
before TERM/KILL on every native termination path, including owner/VM death.
These two options must be supplied together; the grace is a positive integer
at most `4_294_967_295`. All modes use the existing INT, TERM, KILL sequence.
Duplex close/shutdown, Stream halt or consumer failure, owner death, and BEAM
death through observed protocol EOF all use it. Tracking additionally detects
hang-up independently when stdin is blocked; a tracked Duplex child that has
stopped reading delays close and owner death by the cleanup budget described
in the tracking section. Stream whole-run and idle timeouts,
Stream/Duplex output limits, and Daemon readiness timeout also use it. Duplex
`recv/2` timeout only stops waiting, and `send_eof/2` only closes child stdin.

Each cleanup wait includes interrupt grace, kill grace and a five-second
confirmation backstop; closing the guard has its own five-second bound.
Daemon's generated child spec allows those bounds plus one scheduling second
(default shutdown: 16 seconds). `Supervisor.child_spec/2` can override it;
a shorter bound may interrupt confirmation, while native EOF cleanup continues.
A blocking user output or readiness callback can delay the start of cleanup.

This reaches a separate-group child only if its parent cooperates; it does not
contain escaped descendants.

## One-shot negotiation and result evidence

New Run options perform a capability handshake before
SPAWN. A mismatch returns `{:error, {:spawn, :incompatible_shim}}` without
executing a child. A stalled exchange uses `:handshake_timeout`, malformed
replies use `:invalid_handshake`, and transport exit before SPAWN uses
`{:shim_exited, status}` in the same spawn-reason tuple. With new options,
`timeout_ms` must be an integer in `0..4_294_967_295`; opening and negotiation
consume the same execution deadline. Cleanup has additional bounded time.
`cgroup: :required` alone keeps its existing enforcement and legacy collector
without negotiation. Stream also counts environment and interrupt negotiation
toward its execution timeout. Duplex and Daemon use a five-second handshake
bound and keep their existing error shapes. All capability requirements compose
with spawn identity and structured spawn errors. Stream and Daemon reject
`max_output_bytes`; Duplex retains its existing support with `delivery: :pull`.
Duplex and Daemon reject `idle_timeout_ms`; Stream retains its existing idle
option. These existing mode options keep their own validation and semantics.
Spawn errors after SPAWN with new options carry partial output in
`{:error, {:spawn, {reason, partial_result}}}`. Malformed EXIT uses
`{:error, {:spawn, {:invalid_exit, partial_result}}}`; missing EXIT uses the
existing `{:error, {:spawn, {:shim_exited, partial_result}}}` shape. A timeout
or limit already in progress retains its reason across later transport loss.

Every returned `Forcola.Result` has an evidence map. Inspect its raw
`child_status`/`child_signal`, `termination`, `reason`, `output_truncated`,
`cleanup`, `scope`, and `guard_close` separately. Missing/malformed cleanup
information is unknown. Confirmed process-group cleanup says nothing about a
child that escaped the group. An early JSON response is never terminal proof.
Strict drain waits for pipe closure after root exit. An escaped child holding
a pipe can leave `child_status: 0` alongside `reason: :timeout` or
`:idle_timeout`; consume both facts independently.
