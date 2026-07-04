# Adopting Forcola in a wrapper library

This guide is for libraries that wrap a CLI (git, docker, redis-server, agent
CLIs) and shell out with `System.cmd/3` today. It shows how such a library
adopts Forcola for leak-free process control without making Forcola a
mandatory dependency for its existing consumers.

## The starting point

Most wrapper libraries implement timeouts with some variant of:

```elixir
task = Task.async(fn -> System.cmd(binary, args) end)

case Task.yield(task, timeout) || Task.shutdown(task) do
  {:ok, result} -> result
  nil -> {:error, :timeout}
end
```

`Task.shutdown/1` kills the BEAM task, which closes the Erlang port. Closing
a port closes pipes; it sends no signal. The external process keeps running,
and any children it spawned are never signaled at all. The README describes
the full mechanism. Forcola closes the leak by running the command in its own
process group and killing the whole group, SIGTERM then SIGKILL, on timeout
or BEAM death.

## The Runner behaviour pattern

Making Forcola a hard dependency of a wrapper library would impose it on
every consumer, including those who accept the current behavior. Instead the
wrapper defines a small behaviour for "run this command, return the shapes I
already use", keeps its current `System.cmd/3` path as the default
implementation, and accepts a Forcola-backed implementation through
configuration.

Three pieces:

1. A behaviour whose callback returns exactly what the wrapper's call sites
   already consume, so parsing code does not change.
2. A default implementation wrapping the existing `System.cmd/3` call.
   Existing consumers see no change in behavior and gain no new mandatory
   dependencies.
3. A Forcola-backed implementation, selected via application config, with
   Forcola declared optional:

```elixir
# mix.exs of the wrapper library
defp deps do
  [
    {:forcola, "~> 0.1", optional: true}
  ]
end
```

Consumers who want leak-free execution add `{:forcola, "~> 0.1"}` to their
own deps and set one config line. Everyone else is untouched.

## Worked example: git_wrapper_ex

`Git.Command.run/3` in git_wrapper_ex (`lib/git/command.ex`) is the smallest
real case: one function that builds an argument list, executes git, and hands
the output to a parser. Its current core is the leaky pattern above:

```elixir
def run(mod, command, %Config{} = config) do
  all_args = Config.base_args(config) ++ mod.args(command)
  opts = Config.cmd_opts(config)

  task =
    Task.async(fn ->
      System.cmd(config.binary, all_args, opts)
    end)

  case Task.yield(task, config.timeout) || Task.shutdown(task) do
    {:ok, {stdout, exit_code}} ->
      mod.parse_output(stdout, exit_code)

    nil ->
      {:error, :timeout}
  end
end
```

On timeout the caller gets `{:error, :timeout}` while git, and anything git
spawned (hooks, credential helpers), may still be running.

### Step 1: define the behaviour

The callback returns what the call site already consumes: `System.cmd/3`'s
`{stdout, exit_code}` on completion, `{:error, :timeout}` on timeout.

```elixir
defmodule Git.Runner do
  @moduledoc """
  How git commands are executed.

  The default is `Git.Runner.Port`, the `System.cmd/3` path. For leak-free
  execution add `:forcola` to your deps and configure:

      config :git_wrapper_ex, runner: Git.Runner.Forcola
  """

  @callback run(binary :: String.t(), args :: [String.t()], opts :: keyword()) ::
              {:ok, {stdout :: String.t(), exit_code :: non_neg_integer()}}
              | {:error, term()}

  def impl do
    Application.get_env(:git_wrapper_ex, :runner, Git.Runner.Port)
  end
end
```

### Step 2: the default implementation is the current code, moved

```elixir
defmodule Git.Runner.Port do
  @behaviour Git.Runner

  @impl true
  def run(binary, args, opts) do
    {timeout, cmd_opts} = Keyword.pop!(opts, :timeout)

    task = Task.async(fn -> System.cmd(binary, args, cmd_opts) end)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, result} -> {:ok, result}
      nil -> {:error, :timeout}
    end
  end
end
```

### Step 3: the Forcola-backed implementation

```elixir
if Code.ensure_loaded?(Forcola) do
  defmodule Git.Runner.Forcola do
    @behaviour Git.Runner

    @impl true
    def run(binary, args, opts) do
      {timeout, cmd_opts} = Keyword.pop!(opts, :timeout)

      forcola_opts =
        [timeout_ms: timeout, merge_stderr: true] ++
          Keyword.take(cmd_opts, [:cd, :env])

      case Forcola.run([binary | args], forcola_opts) do
        {:ok, %Forcola.Result{status: status, stdout: stdout}} when is_integer(status) ->
          {:ok, {stdout, status}}

        {:ok, %Forcola.Result{status: {:signal, signal}}} ->
          {:error, {:signal, signal}}

        {:error, {:timeout, _partial}} ->
          {:error, :timeout}

        {:error, {:spawn, reason}} ->
          {:error, {:spawn, reason}}
      end
    end
  end
end
```

The `Code.ensure_loaded?/1` guard keeps the module from compiling when
Forcola is not present, so the optional dependency stays optional.

Mapping notes, each verified against `Forcola.run/2`:

- `:timeout_ms` is mandatory. The wrapper's existing timeout value maps onto
  it directly. On expiry Forcola returns `{:error, {:timeout, partial}}`
  only after the child's process group is confirmed dead, so
  `{:error, :timeout}` now means "git is gone", not "git may still be
  running".
- A non-zero exit is `{:ok, %Forcola.Result{}}`, matching `System.cmd/3`, so
  `parse_output/2` keeps receiving the exit code and decides what it means.
- `merge_stderr: true` is the equivalent of `stderr_to_stdout: true`; use it
  when the wrapper's current `System.cmd/3` options do.
- `:cd` and `:env` carry over. `:env` is a list of `{name, value}` string
  tuples, the same shape `System.cmd/3` takes.
- `{:signal, _}` in `:status` has no `System.cmd/3` equivalent (the child
  died from a signal); surface it as an error rather than inventing an exit
  code.

### Step 4: dispatch through the behaviour

```elixir
def run(mod, command, %Config{} = config) do
  all_args = Config.base_args(config) ++ mod.args(command)
  opts = Keyword.put(Config.cmd_opts(config), :timeout, config.timeout)

  case Git.Runner.impl().run(config.binary, all_args, opts) do
    {:ok, {stdout, exit_code}} -> mod.parse_output(stdout, exit_code)
    {:error, reason} -> {:error, reason}
  end
end
```

That is the whole migration: one behaviour, the old code as the default, an
adapter, and a config switch.

## Mode mapping for the wrapper family

Each wrapper's call shapes map onto one of Forcola's four modes:

| Wrapper call shape | Example | Forcola mode |
|---|---|---|
| Bounded subcommand | git subcommands; `claude -p` / `codex exec` one-shot; docker sync paths (`ps`, `build`) | `Forcola.run/2` |
| Line/NDJSON streaming | claude/codex stream-json output; `docker logs -f`, `docker events` | `Forcola.Stream.lines/2` |
| Managed server | redis_server_wrapper managed mode | `Forcola.Daemon` |
| Interactive session | claude duplex stream-json over stdin | `Forcola.Duplex` |

Per-mode notes:

- `Forcola.run/2`: `:timeout_ms` is mandatory; a non-zero exit is
  `{:ok, %Forcola.Result{}}`, not an error.
- `Forcola.Stream.lines/2`: `:timeout_ms` is mandatory and bounds the whole
  run, not the gap between lines. A non-zero exit, death by signal, timeout,
  or spawn failure raises `Forcola.Stream.Error` after every line produced
  before death has been emitted; halting the stream early kills the process
  group and blocks until it is confirmed dead.
- `Forcola.Daemon`: no `:timeout_ms` (passing one raises `ArgumentError`);
  the daemon's bound is its supervisor. Supports a `:ready` check so
  `start_link` blocks until the server accepts connections, and `:output`
  routing for logs. Run the server in foreground mode; a daemonize flag
  escapes the process group (see "What group kill cannot reach" in the
  README).
- `Forcola.Duplex`: no `:timeout_ms` (passing one raises `ArgumentError`);
  the session is bounded by its owner process and `close/1`. Lines go in
  with `send_line/2`, arrive as `{:forcola_line, session, line}` messages,
  and `send_eof/1` closes stdin for CLIs that exit when input ends.

One caveat for docker-shaped wrappers: the docker CLI is a control channel
for a daemon. Forcola kills the client reliably, but that never stops the
container or build running under the daemon; pair Forcola with the tool's
own teardown (`docker run --rm`, `docker kill`). The README section "What
group kill cannot reach" covers this class.

## Alternatives and tradeoffs

Forcola is not the only way to run external processes from the BEAM. The
table below reflects each option's published source and tracker as of
mid-2026; rows marked "tested" were verified empirically on macOS with
Elixir 1.20 / OTP 29.

| Option | Architecture | BEAM-death cleanup | Grandchild kill | Install footprint | Maintenance (mid-2026) |
|---|---|---|---|---|---|
| System.cmd / Port / :os.cmd | BEAM port | None; no signal on port close (tested) | No | None | OTP/Elixir stdlib |
| [erlexec](https://github.com/saleyn/erlexec) | One C++ port program for all commands | Yes, incl. kill -9: SIGTERM then SIGKILL in 6 s | Opt-in (`{group, GID}` + `kill_group`) | C++ toolchain + rebar3 at dep compile, source-only package | Active (2.3.4, June 2026) |
| [MuonTrap](https://github.com/fhunleth/muontrap) | C wrapper per command | Yes, incl. kill -9 (tested) | Linux cgroups: full tree; macOS: direct child only (tested) | C compiler (elixir_make) | Active (1.8.0 May 2026, 2.0 rc June 2026) |
| [Porcelain](https://github.com/alco/porcelain) + goon | Go middleman, manual download | Closes child stdin and waits; never kills | No | goon fetched by hand; last goon release 2014 | Unmaintained (last release 2016, last commit 2020) |
| [Rambo](https://github.com/jayjun/rambo) | Rust shim per call | SIGKILLs direct child on stdin EOF | No | Bundled x86-64 binaries only; broken out of the box on Apple Silicon (tested) | Dormant (last release March 2021) |
| [exile](https://github.com/akash-akya/exile) | NIF IO + spawner that execs into the command | Normal exits yes; kill -9 of BEAM orphans the child (tested) | No | C compiler (elixir_make) | Maintained, single author (0.14.0, Feb 2026) |
| forcola | Rust shim per command | stdin EOF kills the process group, covers kill -9; death confirmed before EXIT | Yes (setsid + kill(-pgid), TERM then KILL) | None on 5 precompiled targets; cargo elsewhere | New (v0.1.0) |

erlexec is the most capable and most mature option: a single C++ port
program with pty support, user switching, and opt-in process-group kill,
actively maintained since 2003. Its costs are a C++ toolchain at dependency
compile time and a larger API surface. If you need a pty or run-as-user,
choose erlexec; forcola does not do either.

MuonTrap solves the same core problem as forcola with a per-command C
wrapper, and on Linux adds cgroup containment that kills entire process
trees, including deliberate daemonizers. Without cgroups it kills the direct
child only, so grandchildren escape; forcola's group kill covers ordinary
grandchildren everywhere but cannot contain a deliberate daemonizer on
macOS either. On Nerves or embedded Linux, MuonTrap is the native choice.

exile takes a different shape: NIF-based demand-driven IO with real
backpressure, ideal when a slow consumer must stream huge output without
buffering. The tradeoff is cleanup: with no middleman process, a kill -9 of
the BEAM orphans the child (in testing on macOS, exile's child survived
where forcola's and MuonTrap's shims cleaned up).

Porcelain and Rambo are effectively frozen. Porcelain has had no release
since 2016 and its goon driver's last release is from 2014; the released
goon never kills the child, it only closes stdin and waits. Rambo is a tidy
one-shot design but has had no release since March 2021, ships x86-64-only
binaries, and in testing on an Apple Silicon Mac it failed out of the box.

Choose something else when:

- You need a pty or run-as-user: erlexec.
- You need Linux cgroup containment of daemonizers today: MuonTrap (or
  systemd-run). forcola tracks an optional cgroup layer in
  [#15](https://github.com/joshrotenberg/forcola/issues/15).
- You need backpressure-first streaming and accept the kill -9 orphan risk:
  exile.
- You need Windows: Rambo's bundled binary or plain System.cmd.
- You cannot ship native binaries at all: System.cmd/ports, with the
  orphan-on-death leak documented and accepted.
- forcola is new (v0.1.0). If that is a blocker, erlexec and MuonTrap are
  the mature, actively maintained alternatives that cover the closest
  ground.

## Migrating from erlexec

For a wrapper that uses erlexec today for the same contract (group kill on
timeout, cleanup on BEAM death), the migration is mechanical:

1. Add `{:forcola, "~> 0.1"}` and swap the erlexec calls for their Forcola
   counterparts; bounded runs become `Forcola.run/2` with `:timeout_ms`.
2. Run your existing test suite; it is the acceptance bar. Forcola's own
   suite covers the group-kill contract cases, including the
   SIGTERM-ignoring child.
3. Drop the erlexec dependency. This removes the C++ compile erlexec runs on
   each consumer's machine; Forcola ships precompiled shim binaries with
   checksum verification instead.
