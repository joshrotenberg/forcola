defmodule Forcola.Daemon do
  @moduledoc """
  A long-running external server process under a supervision tree.

  For processes that are supposed to outlive a single call (redis-server,
  a dev proxy) but must never outlive their supervisor. The child runs in
  its own process group; when this GenServer terminates for any reason,
  including supervisor shutdown and owner crash, the shim kills the group
  (SIGTERM, then SIGKILL after the kill grace). When the interrupt pair is
  enabled, SIGINT and its grace come first. The terminate callback blocks
  for the shim's bounded confirmation. If teardown cannot be confirmed,
  termination returns after the backstop and logs a warning. If the daemon
  process is killed brutally (no terminate), the port closes, the shim sees
  stdin EOF, and the group is killed anyway; the same path covers BEAM death.

      children = [
        {Forcola.Daemon,
         argv: ["redis-server", "--port", "6399"],
         name: MyApp.Redis}
      ]

  Unlike bounded runs, a daemon has no `:timeout_ms`; its bound is its
  supervisor. Passing `:timeout_ms` raises `ArgumentError`.
  The one-shot options `:idle_timeout_ms` and `:max_output_bytes` also raise `ArgumentError`
  before the GenServer or shim is started.

  The generated `child_spec/1` allows both signal graces, the confirmation
  and guard-close bounds, and a scheduling margin before the supervisor forces
  termination. Its default shutdown budget is 16 seconds. See `child_spec/1`
  for an explicit `Supervisor.child_spec/2` override.

  ## Options

    * `:argv` - required, `[binary | args]` as in `Forcola.run/2`.
    * `:name` - optional GenServer registration name.
    * `:cd`, `:merge_stderr` - as in `Forcola.run/2`.
    * `:env` - environment entries as in `Forcola.run/2`. Binary values set
      variables; `nil` or `false` removes them and requires `env_unset_v1`.
      Other non-binary values raise `ArgumentError` before opening the shim.
    * `:env_mode` - `:inherit` (default) or `:replace`; replacement clears
      inherited variables before applying `:env`. Explicit `:env_mode` or
      interrupt options require a list of binary environment names without
      `=` or NUL and binary values without NUL, or `nil`/`false` unsets.
      Explicit `:env_mode` requires `bounded_run_v1` negotiation.
    * `:interrupt_signal`, `:interrupt_grace_ms` - optional pair, `:int` and
      a positive integer grace at most `4_294_967_295`. Requires
      `bounded_run_v1`; the shim sends SIGINT before TERM/KILL on shutdown,
      owner death, or BEAM EOF. Cleanup waits include both grace intervals.
      These capabilities compose with identity and structured errors, and
      negotiate within five seconds. Incompatible shims fail before SPAWN.
    * `:shim_path` - trusted absolute shim path, as in `Forcola.run/2`.
    * `:structured_spawn_errors` - negotiate native `Forcola.SpawnError`
      reasons, default `false`. Spawn failures still stop the daemon with
      `{:spawn, reason}`, or return
      `{:error, {:exited_before_ready, {:spawn, reason}}}` during readiness
      polling. The struct retains the original string in `:message`.
      Requires `structured_spawn_errors_v1` with a five-second negotiation
      bound; incompatible shims fail before SPAWN.
    * `:track_descendants` - opt in to lifetime descendant-group tracking
      and cleanup, default `false`. Requires `track_descendants_v1` before
      SPAWN; `tracked/2` returns the live daemon's history, root group first.
      Histories are bounded at 4,096 group identities. Linux reports
      `:tree_contained` through subreaper adoption; macOS reports
      `:tree_sampled`. A descendant reparented before its first macOS sample
      is outside that guarantee. Incomplete observations mark history truncated,
      independently of native cleanup confirmation. Unknown scope or malformed
      EXIT cannot confirm cleanup.
    * `:track_observer` - optional `{local_pid, reference}` receiving
      `{reference, {:tracked, identity}}` and one terminal
      `{reference, {:tracking_exit, evidence}}`
      on startup failure, exit, readiness failure, or shutdown when the daemon
      survives to report it. A non-nil observer enables tracking even with
      `track_descendants: false`. Only this explicit observer is notified;
      the supervisor receives no implicit tracking messages. See
      `Forcola.run/2` for confirmation values and `Forcola.Result` for the
      identity shape. Tracking works independently of `:spawn_identity`.
    * `:spawn_identity` - request the child's native identity for `spawned/1`,
      default `false`. This sends no notification by itself. Requires
      `spawn_identity_v1` negotiation with a five-second handshake bound;
      an incompatible shim fails before starting the child.
    * `:spawn_observer` - optional `{local_pid, reference}` receiving
      `{reference, {:spawned, identity}}` before output is routed, including
      during readiness polling. Only the explicit observer receives this
      notification; the starting process or supervisor is not implicitly
      notified. The identity is `%{pid: integer, pgid: integer, start_time: map}`;
      see `Forcola.Result` for the platform-specific start-time fields.
      A non-nil observer enables identity even with `spawn_identity: false`.
      Notifications are unacknowledged, and a dead observer is harmless.
    * `:user`, `:group` - run the child as a different user/group, as in
      `Forcola.run/2`. POSIX-only, a one-way drop, and requires a
      privileged shim; failures fail closed and surface as the daemon's
      normal spawn-error exit.
    * `:cgroup` - opt in to Linux cgroup v2 containment, as in
      `Forcola.run/2`. Linux only, requires a delegated cgroup v2 subtree
      (systemd `Delegate=yes` or `systemd-run --scope`), and falls back to
      the process-group kill with a warning elsewhere. A daemon is the
      typical place to want this: a long-running server that might spawn a
      helper which daemonizes away from the process group is reaped anyway
      on shutdown. Set `:required` to fail before exec if containment is
      unavailable. Default `false`.
    * `:kill_grace_ms` - SIGTERM-to-SIGKILL grace, default `5_000`.
    * `:output` - where child output goes; see below. Default `:logger`.
    * `:log_output` - `Logger` level for `output: :logger`, default `:info`.
    * `:log_prefix` - string prepended to each logged line, default `""`.
    * `:ready` - optional zero-arity readiness check; see below.
    * `:ready_timeout_ms` - readiness deadline, default `5_000`.
    * `:ready_poll_ms` - readiness poll interval, default `100`.

  ## Output handling

    * `output: :logger` (default) - stdout and stderr are logged line by
      line at the `:log_output` level, prefixed with `:log_prefix`. A
      partial line held across frames is logged once its newline arrives
      (or at termination).
    * `output: fun` - a 2-arity function called as
      `fun.(:stdout | :stderr, chunk)` with raw chunks, from the daemon
      process.
    * `output: {:send, pid}` - `pid` receives
      `{Forcola.Daemon, daemon_pid, {:stdout | :stderr, chunk}}` messages.

  With `merge_stderr: true` everything arrives as `:stdout`.

  ## Readiness

  With `ready: fun`, `init` polls `fun.()` (every `:ready_poll_ms`) until
  it returns a truthy value, so `start_link` and supervisor startup block
  until the server accepts connections:

      {Forcola.Daemon,
       argv: ["redis-server", "--port", "6399"],
       ready: fn -> match?({:ok, _}, :gen_tcp.connect(~c"localhost", 6399, [])) end}

  If the check does not pass within `:ready_timeout_ms`, or the child
  exits first, the group is killed and `start_link` returns
  `{:error, :ready_timeout}` or `{:error, {:exited_before_ready, reason}}`.

  Without `:ready`, `start_link` returns as soon as the spawn request is
  written to the shim: it succeeds without any proof that the shim or the
  child is alive, and a failed spawn only surfaces later as a daemon
  exit. Use `:ready` for any daemon whose availability matters.

  ## Exit and restart behavior

  When the child exits on its own, the daemon stops: status 0 stops
  `:normal`, a non-zero status stops `{:exit_status, n}`, and death by
  signal stops `{:exit_signal, n}`. An unconfirmed teardown stops
  `{:exit_signal, :unconfirmed}`. Under `restart: :permanent` any of
  these restarts the daemon; under `:transient` only the abnormal ones do.
  """

  use GenServer

  require Logger

  alias Forcola.{Shim, Tracking}

  @default_kill_grace_ms 5_000
  @default_ready_timeout_ms 5_000
  @default_ready_poll_ms 100

  @typedoc "Where child output is routed."
  @type output ::
          :logger
          | (:stdout | :stderr, binary() -> any())
          | {:send, pid()}

  @doc """
  Builds a supervisor child specification with time for bounded cleanup.

  The shutdown budget includes the interrupt grace, kill grace, five seconds
  for the native confirmation, five seconds for the port guard, and one second
  of scheduling margin. With the default options this is 16,000 milliseconds.
  When the sum exceeds the supervisor's maximum timeout of `4_294_967_295`
  milliseconds, the child specification uses `:infinity`; the daemon still
  enforces its own bounded cleanup deadline. This avoids truncating an accepted
  interrupt grace or passing an invalid timeout to the supervisor.
  Override the generated value with `Supervisor.child_spec/2` when needed:

      Supervisor.child_spec({Forcola.Daemon, argv: ["my-server"]}, shutdown: 30_000)

  A shorter override can stop waiting before cleanup finishes. Output and
  readiness callbacks should return promptly; their duration is not included
  in this cleanup budget.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    Shim.validate_execution_options!(opts)

    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: Shim.shutdown_timeout(opts)
    }
  end

  @doc """
  Starts a daemon running `opts[:argv]` under the shim.

  See the module docs for options. Raises `ArgumentError` on invalid
  options (missing `:argv`, a `:timeout_ms`, a bad `:output` shape).
  Shim path validation and synchronous port startup failures return
  `{:error, {:spawn, reason}}`, including `{:invalid_shim_path, reason}`
  for a bad override.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    opts = Tracking.validate_opts!(opts)
    validate_opts!(opts)
    opts = if opts[:spawn_observer], do: Keyword.put(opts, :spawn_identity, true), else: opts
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Queries the live daemon's recorded child identity.

  Returns `{:error, :pending}` before an enabled daemon receives the frame,
  `{:error, :not_requested}` when disabled, and `{:error, :closed}` after
  the daemon exits. The query timeout defaults to 5,000 milliseconds.
  `{:error, :pending}` is also returned for any server that cannot reply within
  the timeout, whether or not identity is enabled, for example during readiness
  polling or while an output callback is blocked. A query timeout does not mean
  that the daemon has closed.
  An explicit `:spawn_observer` can retain its notification after the daemon
  exits. An observed identity is not proof that the process remains alive.
  """
  @spec spawned(GenServer.server(), timeout()) ::
          {:ok, Shim.identity()} | {:error, :pending | :not_requested | :closed}
  def spawned(daemon, timeout \\ 5_000) do
    GenServer.call(daemon, :spawned, timeout)
  catch
    :exit, {:timeout, _call} -> {:error, :pending}
    :exit, _ -> {:error, :closed}
  end

  @doc """
  Returns the live daemon's cleanup-group history in first-observed order.

  Enabled tracking starts with `{:ok, []}` and records the root group before
  descendants. Entries are observations, not proof of current liveness.
  Disabled daemons return `{:error, :not_requested}`; a query timeout returns
  `{:error, :pending}`, and an exited daemon returns `{:error, :closed}`.
  Use `:track_observer` to retain observations after the daemon exits.
  """
  @spec tracked(GenServer.server(), timeout()) ::
          {:ok, [Shim.identity()]} | {:error, :not_requested | :pending | :closed}
  def tracked(daemon, timeout \\ 5_000) do
    GenServer.call(daemon, :tracked, timeout)
  catch
    :exit, {:timeout, _call} -> {:error, :pending}
    :exit, _ -> {:error, :closed}
  end

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 runs on supervisor shutdown and on owner
    # (parent) crash; both must kill the process group.
    Process.flag(:trap_exit, true)

    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, @default_kill_grace_ms)

    case Shim.open_with_features(opts) do
      {:ok, port} ->
        argv = Keyword.fetch!(opts, :argv)
        payload = Shim.encode_spawn(argv, Keyword.put(opts, :kill_grace_ms, kill_grace_ms))
        Shim.send_frame(port, Shim.tag_spawn(), payload)

        state = %{
          port: port,
          spawn_observer: opts[:spawn_observer],
          identity_enabled: Shim.identity_requested?(opts),
          structured_spawn_errors: Keyword.get(opts, :structured_spawn_errors, false),
          identity: nil,
          tracking: Tracking.new(opts),
          output: Keyword.get(opts, :output, :logger),
          log_level: Keyword.get(opts, :log_output, :info),
          log_prefix: Keyword.get(opts, :log_prefix, ""),
          cleanup_timeout_ms: Shim.cleanup_timeout(opts),
          buffers: %{stdout: "", stderr: ""},
          exit: nil
        }

        case await_ready(state, opts) do
          {:ok, state} ->
            {:ok, state}

          {:error, reason, state} ->
            shutdown(state)
            {:stop, reason}
        end

      {:error, :not_found} ->
        Tracking.startup_failed(opts)
        {:stop, {:spawn, :shim_not_found}}

      {:error, reason} ->
        Tracking.startup_failed(opts)
        {:stop, {:spawn, reason}}
    end
  end

  @impl true
  def handle_call(:spawned, _from, %{identity_enabled: false} = state),
    do: {:reply, {:error, :not_requested}, state}

  def handle_call(:spawned, _from, %{identity: nil} = state),
    do: {:reply, {:error, :pending}, state}

  def handle_call(:spawned, _from, state), do: {:reply, {:ok, state.identity}, state}

  def handle_call(:tracked, _from, %{tracking: %{enabled: false}} = state),
    do: {:reply, {:error, :not_requested}, state}

  def handle_call(:tracked, _from, state),
    do: {:reply, {:ok, Tracking.groups(state.tracking)}, state}

  @impl true
  def handle_info({port, {:data, <<tag, payload::binary>>}}, %{port: port} = state) do
    state = handle_frame(state, tag, payload)

    case state.exit do
      nil -> {:noreply, state}
      exit -> {:stop, stop_reason(exit), state}
    end
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    # The shim exited without sending EXIT/ERROR (e.g. it crashed). Its
    # death killed the group by the shim's own on-drop rule, but that
    # cannot be confirmed from here.
    state = finish_tracking(state, :unknown, :transport_lost)
    {:stop, {:shim_exited, status}, %{state | exit: {:shim_exited, status}}}
  end

  def handle_info({:EXIT, port, _reason}, %{port: port} = state) do
    # Port link notification; the paired :exit_status message drives the
    # actual stop.
    {:noreply, state}
  end

  def handle_info({:EXIT, pid, reason}, state) when is_pid(pid) do
    # A non-parent linked process died (the parent case is handled by
    # gen_server itself and goes straight to terminate/2).
    {:stop, reason, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    flush_buffers(state)
    shutdown(state)
  end

  ## Option validation

  defp validate_opts!(opts) do
    Shim.validate_spawn_identity!(opts)
    Shim.validate_structured_spawn_errors!(opts)
    Shim.validate_execution_options!(opts)
    Shim.validate_spawn_observer!(opts)
    Shim.reject_run_options!(opts, __MODULE__)

    if Keyword.has_key?(opts, :timeout_ms) do
      raise ArgumentError,
            "Forcola.Daemon takes no :timeout_ms; a daemon's bound is its supervisor"
    end

    validate_argv!(opts)
    validate_output!(opts)
    validate_ready!(opts)
  end

  defp validate_argv!(opts) do
    case Keyword.fetch(opts, :argv) do
      {:ok, [binary | rest]} when is_binary(binary) ->
        if Enum.all?(rest, &is_binary/1) do
          :ok
        else
          raise ArgumentError,
                ":argv must be a non-empty list of binaries, got: #{inspect([binary | rest])}"
        end

      {:ok, other} ->
        raise ArgumentError, ":argv must be a non-empty list of binaries, got: #{inspect(other)}"

      :error ->
        raise ArgumentError, "Forcola.Daemon requires :argv"
    end
  end

  defp validate_output!(opts) do
    case Keyword.get(opts, :output, :logger) do
      :logger ->
        :ok

      fun when is_function(fun, 2) ->
        :ok

      {:send, pid} when is_pid(pid) ->
        :ok

      other ->
        raise ArgumentError,
              ":output must be :logger, a 2-arity function, or {:send, pid}, " <>
                "got: #{inspect(other)}"
    end
  end

  defp validate_ready!(opts) do
    case Keyword.get(opts, :ready) do
      nil ->
        :ok

      fun when is_function(fun, 0) ->
        :ok

      other ->
        raise ArgumentError, ":ready must be a zero-arity function, got: #{inspect(other)}"
    end
  end

  ## Readiness

  defp await_ready(state, opts) do
    case Keyword.get(opts, :ready) do
      nil ->
        {:ok, state}

      fun ->
        timeout_ms = Keyword.get(opts, :ready_timeout_ms, @default_ready_timeout_ms)
        poll_ms = Keyword.get(opts, :ready_poll_ms, @default_ready_poll_ms)
        deadline = System.monotonic_time(:millisecond) + timeout_ms
        poll_ready(state, fun, deadline, poll_ms)
    end
  end

  defp poll_ready(%{exit: exit} = state, _fun, _deadline, _poll_ms) when not is_nil(exit) do
    {:error, {:exited_before_ready, stop_reason(exit)}, state}
  end

  defp poll_ready(state, fun, deadline, poll_ms) do
    cond do
      fun.() ->
        {:ok, state}

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :ready_timeout, state}

      true ->
        # Absorb port frames while waiting so output keeps routing and an
        # early child exit is noticed.
        state = absorb_frames(state, poll_ms)
        poll_ready(state, fun, deadline, poll_ms)
    end
  end

  defp absorb_frames(%{port: port} = state, wait_ms) do
    receive do
      {^port, {:data, <<tag, payload::binary>>}} ->
        state
        |> handle_frame(tag, payload)
        |> absorb_frames(0)

      {^port, {:exit_status, status}} ->
        %{state | exit: {:shim_exited, status}}
    after
      wait_ms -> state
    end
  end

  ## Frame handling

  defp handle_frame(state, tag, payload) do
    cond do
      tag == Shim.tag_stdout() ->
        emit(state, :stdout, payload)

      tag == Shim.tag_stderr() ->
        emit(state, :stderr, payload)

      tag == Shim.tag_spawned() ->
        record_identity(state, payload)

      tag == Shim.tag_tracked() ->
        record_tracked(state, payload)

      tag == Shim.tag_exit() ->
        handle_exit(state, payload)

      tag == Shim.tag_error() ->
        state
        |> finish_tracking(:unknown, :not_started)
        |> Map.put(
          :exit,
          {:spawn_error, Shim.decode_error(payload, state.structured_spawn_errors)}
        )

      true ->
        state
    end
  end

  defp record_tracked(state, payload) do
    {tracking, identity} = Tracking.record(state.tracking, payload)
    state = %{state | tracking: tracking}
    Tracking.notify_record(tracking, identity)
    state
  end

  defp finish_tracking(state, scope, confirmation, contained \\ :unknown, truncated \\ false),
    do: %{
      state
      | tracking: Tracking.finish(state.tracking, scope, confirmation, contained, truncated)
    }

  defp record_identity(%{identity_enabled: true, identity: nil} = state, payload) do
    case Shim.decode_identity(payload) do
      {:ok, identity} ->
        if state.spawn_observer do
          {pid, ref} = state.spawn_observer
          send(pid, {ref, {:spawned, identity}})
        end

        %{state | identity: identity}

      {:error, _reason} ->
        state
    end
  end

  defp record_identity(state, _payload), do: state

  defp handle_exit(state, payload) do
    report = Tracking.report(state.tracking, payload)

    if report.contained == true do
      Logger.debug("forcola: daemon ran under Linux cgroup v2 containment")
    end

    state
    |> finish_tracking(
      report.scope,
      Tracking.confirmation(report),
      report.contained,
      report.tracked_groups_truncated
    )
    |> Map.put(:exit, {:exit, {Tracking.status(report), report.timed_out}})
  end

  defp stop_reason({:exit, {0, _timed_out}}), do: :normal

  defp stop_reason({:exit, {status, _timed_out}}) when is_integer(status),
    do: {:exit_status, status}

  defp stop_reason({:exit, {{:signal, signal}, _timed_out}}), do: {:exit_signal, signal}
  defp stop_reason({:spawn_error, reason}), do: {:spawn, reason}
  defp stop_reason({:shim_exited, status}), do: {:shim_exited, status}

  ## Output routing

  defp emit(%{output: :logger} = state, stream, chunk) do
    {lines, rest} = split_lines(state.buffers[stream] <> chunk)
    Enum.each(lines, &Logger.log(state.log_level, state.log_prefix <> &1))
    %{state | buffers: Map.put(state.buffers, stream, rest)}
  end

  defp emit(%{output: fun} = state, stream, chunk) when is_function(fun, 2) do
    fun.(stream, chunk)
    state
  end

  defp emit(%{output: {:send, pid}} = state, stream, chunk) do
    send(pid, {__MODULE__, self(), {stream, chunk}})
    state
  end

  defp split_lines(data) do
    parts = :binary.split(data, "\n", [:global])
    {lines, [rest]} = Enum.split(parts, length(parts) - 1)
    {lines, rest}
  end

  # Log partial lines still buffered when the daemon stops.
  defp flush_buffers(%{output: :logger} = state) do
    for {_stream, buffer} <- state.buffers, buffer != "" do
      Logger.log(state.log_level, state.log_prefix <> buffer)
    end

    :ok
  end

  defp flush_buffers(_state), do: :ok

  ## Kill discipline

  # The child may still be running: kill the group and wait for the
  # shim's EXIT frame (the shim confirms group death before sending it).
  defp shutdown(%{port: port, exit: nil, cleanup_timeout_ms: cleanup_timeout_ms} = state) do
    {confirmation, state} =
      try do
        Shim.send_frame(port, Shim.tag_kill())
        await_exit(state, cleanup_timeout_ms)
      catch
        # The port raced us shut (shim already gone); its death killed the
        # group by the shim's stdin-EOF rule, so there is nothing to wait on.
        :error, :badarg -> {:transport_lost, state}
      end

    finish_cleanup_tracking(state, confirmation)
    warn_unconfirmed_cleanup(confirmation)

    close_port(port)
  end

  # The shim already accounted for the child; nothing left to kill.
  defp shutdown(%{port: port} = state) do
    confirmation =
      if match?({:spawn_error, _}, state.exit), do: :not_started, else: :transport_lost

    finish_tracking(state, :unknown, confirmation)
    close_port(port)
  end

  defp finish_cleanup_tracking(state, {:exit, report}),
    do:
      finish_tracking(
        state,
        report.scope,
        Tracking.confirmation(report),
        report.contained,
        report.tracked_groups_truncated
      )

  defp finish_cleanup_tracking(state, confirmation),
    do: finish_tracking(state, :unknown, confirmation)

  defp await_exit(state, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_exit(state, deadline)
  end

  defp do_await_exit(%{port: port} = state, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:timeout, state}
    else
      receive do
        {^port, {:data, <<tag, payload::binary>>}} ->
          cond do
            tag == Shim.tag_exit() -> {{:exit, Tracking.report(state.tracking, payload)}, state}
            tag == Shim.tag_error() -> {:not_started, state}
            tag == Shim.tag_spawned() -> do_await_exit(record_identity(state, payload), deadline)
            tag == Shim.tag_tracked() -> do_await_exit(record_tracked(state, payload), deadline)
            true -> do_await_exit(state, deadline)
          end

        {^port, {:exit_status, _status}} ->
          {:transport_lost, state}
      after
        min(remaining, 4_294_967_295) -> do_await_exit(state, deadline)
      end
    end
  end

  defp warn_unconfirmed_cleanup({:exit, report}),
    do: warn_unconfirmed_cleanup(Tracking.confirmation(report))

  defp warn_unconfirmed_cleanup(:unconfirmed) do
    Logger.warning("forcola: daemon cleanup could not confirm process-group death")
  end

  defp warn_unconfirmed_cleanup(:timeout) do
    Logger.warning("forcola: daemon cleanup timed out before shim confirmation")
  end

  defp warn_unconfirmed_cleanup(_confirmed), do: :ok

  defp close_port(port), do: Shim.close(port)
end
