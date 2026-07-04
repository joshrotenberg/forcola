defmodule Forcola.Daemon do
  @moduledoc """
  A long-running external server process under a supervision tree.

  For processes that are supposed to outlive a single call (redis-server,
  a dev proxy) but must never outlive their supervisor. The child runs in
  its own process group; when this GenServer terminates for any reason,
  including supervisor shutdown and owner crash, the shim kills the group
  (SIGTERM, then SIGKILL after the kill grace) and the terminate blocks
  until the shim confirms the group is dead. If the daemon process is
  killed brutally (no terminate), the port closes, the shim sees stdin
  EOF, and the group is killed anyway; the same path covers BEAM death.

      children = [
        {Forcola.Daemon,
         argv: ["redis-server", "--port", "6399"],
         name: MyApp.Redis}
      ]

  Unlike bounded runs, a daemon has no `:timeout_ms`; its bound is its
  supervisor. Passing `:timeout_ms` raises `ArgumentError`.

  ## Options

    * `:argv` - required, `[binary | args]` as in `Forcola.run/2`.
    * `:name` - optional GenServer registration name.
    * `:cd`, `:env`, `:merge_stderr` - as in `Forcola.run/2`.
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
      on shutdown. Default `false`.
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
  signal stops `{:exit_signal, n}`. Under `restart: :permanent` any of
  these restarts the daemon; under `:transient` only the abnormal ones do.
  """

  use GenServer

  require Logger

  alias Forcola.Shim

  @default_kill_grace_ms 5_000
  # Margin on top of kill_grace_ms when waiting for the shim to confirm
  # group death: the shim confirms within kill_grace_ms, so this only
  # fires if the shim itself never reports back.
  @backstop_margin_ms 5_000
  @default_ready_timeout_ms 5_000
  @default_ready_poll_ms 100

  @typedoc "Where child output is routed."
  @type output ::
          :logger
          | (:stdout | :stderr, binary() -> any())
          | {:send, pid()}

  @doc """
  Starts a daemon running `opts[:argv]` under the shim.

  See the module docs for options. Raises `ArgumentError` on invalid
  options (missing `:argv`, a `:timeout_ms`, a bad `:output` shape).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    validate_opts!(opts)
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 runs on supervisor shutdown and on owner
    # (parent) crash; both must kill the process group.
    Process.flag(:trap_exit, true)

    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, @default_kill_grace_ms)

    case Shim.open() do
      {:ok, port} ->
        argv = Keyword.fetch!(opts, :argv)
        payload = Shim.encode_spawn(argv, Keyword.put(opts, :kill_grace_ms, kill_grace_ms))
        Shim.send_frame(port, Shim.tag_spawn(), payload)

        state = %{
          port: port,
          output: Keyword.get(opts, :output, :logger),
          log_level: Keyword.get(opts, :log_output, :info),
          log_prefix: Keyword.get(opts, :log_prefix, ""),
          kill_grace_ms: kill_grace_ms,
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
        {:stop, {:spawn, :shim_not_found}}
    end
  end

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
      tag == Shim.tag_stdout() -> emit(state, :stdout, payload)
      tag == Shim.tag_stderr() -> emit(state, :stderr, payload)
      tag == Shim.tag_exit() -> handle_exit(state, payload)
      tag == Shim.tag_error() -> %{state | exit: {:spawn_error, Shim.decode_error(payload)}}
      true -> state
    end
  end

  defp handle_exit(state, payload) do
    if Shim.decode_contained(payload) do
      Logger.debug("forcola: daemon ran under Linux cgroup v2 containment")
    end

    %{state | exit: {:exit, Shim.decode_exit(payload)}}
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
  defp shutdown(%{port: port, exit: nil, kill_grace_ms: kill_grace_ms}) do
    try do
      Shim.send_frame(port, Shim.tag_kill())
      await_exit(port, kill_grace_ms + @backstop_margin_ms)
    catch
      # The port raced us shut (shim already gone); its death killed the
      # group by the shim's stdin-EOF rule, so there is nothing to wait on.
      :error, :badarg -> :ok
    end

    close_port(port)
  end

  # The shim already accounted for the child; nothing left to kill.
  defp shutdown(%{port: port}) do
    close_port(port)
  end

  defp await_exit(port, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_exit(port, deadline)
  end

  defp do_await_exit(port, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    receive do
      {^port, {:data, <<tag, _payload::binary>>}} ->
        if tag in [Shim.tag_exit(), Shim.tag_error()] do
          :ok
        else
          do_await_exit(port, deadline)
        end

      {^port, {:exit_status, _status}} ->
        :ok
    after
      max(remaining, 0) -> :timeout
    end
  end

  defp close_port(port) do
    if Port.info(port) != nil do
      Port.close(port)
    end
  catch
    :error, :badarg -> :ok
  end
end
