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
and any children it spawned are never signaled at all. The [process groups
guide](process_groups.html) describes the full mechanism. Forcola closes the
leak by running the command in its own process group and killing the whole
group, SIGTERM then SIGKILL, on timeout or BEAM death.

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
    {:forcola, "~> 0.3", optional: true}
  ]
end
```

Consumers who want leak-free execution add `{:forcola, "~> 0.3"}` to their
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
  [process groups guide](process_groups.html)).
- `Forcola.Duplex`: no `:timeout_ms` (passing one raises `ArgumentError`);
  the session is bounded by its owner process and `close/1`. Lines go in
  with `send_line/2`, arrive as `{:forcola_line, session, line}` messages,
  and `send_eof/1` closes stdin for CLIs that exit when input ends.

One caveat for docker-shaped wrappers: the docker CLI is a control channel
for a daemon. Forcola kills the client reliably, but that never stops the
container or build running under the daemon; pair Forcola with the tool's
own teardown (`docker run --rm`, `docker kill`). The [process groups
guide](process_groups.html) section "What group kill cannot reach" covers
this class.

The [alternatives guide](alternatives.html) compares Forcola with erlexec,
MuonTrap, exile, Porcelain, Rambo, and plain `System.cmd/3`, and lists when
to choose each.

## Migrating from erlexec

For a wrapper that uses erlexec today for the same contract (group kill on
timeout, cleanup on BEAM death), the migration is mechanical:

1. Add `{:forcola, "~> 0.3"}` and swap the erlexec calls for their Forcola
   counterparts; bounded runs become `Forcola.run/2` with `:timeout_ms`.
2. Run your existing test suite; it is the acceptance bar. Forcola's own
   suite covers the group-kill contract cases, including the
   SIGTERM-ignoring child.
3. Drop the erlexec dependency. This removes the C++ compile erlexec runs on
   each consumer's machine; Forcola ships precompiled shim binaries with
   checksum verification instead.
