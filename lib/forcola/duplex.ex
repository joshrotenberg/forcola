defmodule Forcola.Duplex do
  @moduledoc """
  A bidirectional stdin/stdout session with an external process.

  For interactive CLIs driven over stdin (agent CLIs in stream-json mode).
  The owner writes lines in and receives lines out; the child runs in its
  own process group and dies with the session.

      {:ok, session} = Forcola.Duplex.open(["claude", "--input-format", "stream-json"], [])
      :ok = Forcola.Duplex.send_line(session, json)
      receive do
        {:forcola_line, ^session, line} -> line
      end
      :ok = Forcola.Duplex.close(session)

  ## Messages

  The process that called `open/2` (the owner) receives:

    * `{:forcola_line, session, line}` - a stdout line, without its
      trailing newline. A partial line held across frames is delivered
      once its newline arrives; a final partial line is delivered before
      the exit message.
    * `{:forcola_stderr, session, line}` - a stderr line, unless
      `merge_stderr: true` routed stderr into `:forcola_line`. Under
      `pty: true` a terminal carries a single stream, so stderr is always
      merged into `:forcola_line` and no `:forcola_stderr` messages arrive.
    * `{:forcola_exit, session, status}` - the child exited on its own;
      `status` is the exit code, `{:signal, n}` for death by signal,
      `{:signal, :unconfirmed}` when bounded teardown could not prove every
      process was gone,
      `{:spawn_error, reason}` if it never started, or `:shim_exited` if
      the shim died without reporting. The session is over; `close/1` is
      not required (but is harmless).

  ## Kill discipline

  `close/1` kills the child's process group (SIGTERM, then SIGKILL after
  the kill grace) and blocks for the shim's confirmation. `shutdown/1` does
  the same and returns a `Forcola.Duplex.Terminal` with the observed status,
  confirmation, and active cleanup scope. `await_terminal/2` retrieves that
  evidence after a spontaneous exit, including after the session process dies.
  The owner may call `forget_terminal/1` when it no longer needs the result;
  otherwise the small terminal record lives until the owner exits.
  `terminal_recipient: pid` also sends `{:forcola_terminal, session, terminal}`
  to another process, so an owner supervisor can retain the evidence if the
  owner dies.
  The session monitors its owner: owner death takes the same path. If
  the session process itself is killed brutally, or the whole BEAM dies,
  the port closes, the shim sees stdin EOF, and the group is killed
  anyway.

  For CLIs that exit when their stdin closes, `send_eof/1` closes the
  child's stdin without killing anything; the child's own exit then
  arrives as a `:forcola_exit` message.

  ## Pseudo-terminal

  `open/2` with `pty: true` runs the child under a pseudo-terminal instead
  of pipes. CLIs that detect a tty behave as they do in a real terminal:
  line buffering rather than block buffering, color output, progress
  rendering, and interactive prompts (password entry, pagers, REPLs, TUIs).

  A terminal carries one bidirectional stream, so a pty merges the child's
  stdout and stderr: all output arrives as `{:forcola_line, ...}` and no
  `:forcola_stderr` messages are produced. `merge_stderr: false` contradicts
  this and raises `ArgumentError`. An initial window size can be set with
  `:pty_rows` and `:pty_cols`; there is no dynamic resize yet.
  """

  use GenServer

  alias Forcola.Shim

  require Logger

  @default_kill_grace_ms 5_000
  # Margin on top of kill_grace_ms when waiting for the shim to confirm
  # group death: the shim confirms within kill_grace_ms, so this only
  # fires if the shim itself never reports back.
  @backstop_margin_ms 5_000

  defmodule Terminal do
    @moduledoc """
    Immutable evidence from a duplex session's terminal path.

    `:status` is the native child exit code or signal, even when cleanup was
    unconfirmed. It is `nil` if no EXIT frame arrived. `:confirmation` is one
    of `:confirmed`, `:unconfirmed`, `:timeout`, `:transport_lost`, or
    `:not_started`. `:scope` is `:process_group`, `:active_cgroup`, or
    `:unknown`; it describes the mechanism the shim reported, not proof that
    an escaped descendant outside that mechanism was confined.
    """

    @enforce_keys [:status, :confirmation, :cause, :scope]
    defstruct [:status, :confirmation, :cause, :scope]

    @type t :: %__MODULE__{
            status: non_neg_integer() | {:signal, non_neg_integer()} | nil,
            confirmation: :confirmed | :unconfirmed | :timeout | :transport_lost | :not_started,
            cause:
              :child_exit
              | :timeout
              | :explicit_close
              | :owner_death
              | :shim_lost
              | :session_lost
              | :spawn_error,
            scope: :process_group | :active_cgroup | :unknown
          }
  end

  @enforce_keys [:pid, :ref, :terminal_table]
  defstruct [:pid, :ref, :terminal_table]

  @typedoc "An open duplex session."
  @opaque session :: %__MODULE__{pid: pid(), ref: reference(), terminal_table: reference()}

  @doc """
  Open a duplex session running `argv`; the caller becomes the owner.

  ## Options

    * `:cd`, `:env`, `:merge_stderr` - as in `Forcola.run/2`.
    * `:shim_path` - trusted absolute shim path, as in `Forcola.run/2`.
    * `:user`, `:group` - run the child as a different user/group, as in
      `Forcola.run/2`. POSIX-only, a one-way drop, and requires a
      privileged shim; failures fail closed and arrive as
      `{:forcola_exit, session, {:spawn_error, reason}}`.
    * `:cgroup` - opt-in Linux cgroup v2 containment of deliberate
      daemonizers, as in `Forcola.run/2`. Linux only, requires a delegated
      cgroup v2 subtree, and falls back to the process-group kill with a
      warning elsewhere. Default `false`.
    * `:kill_grace_ms` - SIGTERM-to-SIGKILL grace, default `5_000`.
    * `:terminal_recipient` - optional process to receive
      `{:forcola_terminal, session, terminal}` on every terminal path,
      including owner death. The recipient must retain the value itself.
    * `:pty` - run the child under a pseudo-terminal (default `false`). In
      pty mode stderr is merged into `:forcola_line` and no `:forcola_stderr`
      messages arrive; passing `merge_stderr: false` raises `ArgumentError`.
    * `:pty_rows`, `:pty_cols` - initial pty window size, applied only when
      `pty: true`.

  There is no `:timeout_ms`; the session is bounded by its owner process
  and `close/1`. Passing `:timeout_ms` raises `ArgumentError`.

  A spawn failure (e.g. a missing binary) is asynchronous: `open/2`
  still returns `{:ok, session}` and the failure arrives as
  `{:forcola_exit, session, {:spawn_error, reason}}`.
  Shim path validation and synchronous port startup failures return
  `{:error, reason}` immediately, including `{:invalid_shim_path, reason}`
  for a bad override. A shim that exits after its port opens is reported
  through `{:forcola_exit, session, :shim_exited}`.
  """
  @spec open([String.t(), ...], keyword()) :: {:ok, session()} | {:error, term()}
  def open([binary | _] = argv, opts) when is_binary(binary) do
    validate_opts!(argv, opts)
    ref = make_ref()
    # The owner owns the table, so the terminal record survives the session
    # process but is reclaimed automatically when the owner exits.
    table = :ets.new(:forcola_duplex_terminal, [:set, :public])

    case GenServer.start(__MODULE__, {self(), ref, table, argv, opts}) do
      {:ok, pid} ->
        {:ok, %__MODULE__{pid: pid, ref: ref, terminal_table: table}}

      {:error, reason} ->
        :ets.delete(table)
        {:error, reason}
    end
  end

  @doc """
  Write a line to the child's stdin.

  A newline is appended. Returns `{:error, :closed}` once the session is
  over or the child's stdin has been closed with `send_eof/1`.
  """
  @spec send_line(session(), iodata()) :: :ok | {:error, term()}
  def send_line(%__MODULE__{pid: pid}, line) do
    GenServer.call(pid, {:send_line, line})
  catch
    :exit, _ -> {:error, :closed}
  end

  @doc """
  Close the child's stdin without killing the group.

  For CLIs that finish and exit when their input ends; the child's exit
  then arrives as a `:forcola_exit` message. Returns `{:error, :closed}`
  if the session is already over.
  """
  @spec send_eof(session()) :: :ok | {:error, term()}
  def send_eof(%__MODULE__{pid: pid}) do
    GenServer.call(pid, :send_eof)
  catch
    :exit, _ -> {:error, :closed}
  end

  @doc """
  Close the session and kill the child's process group.

  Waits for the shim's bounded report and returns `:ok` for compatibility.
  This releases any retained terminal record; use `shutdown/1` to receive
  and retain the evidence. Idempotent when the session is already over.
  """
  @spec close(session()) :: :ok
  def close(%__MODULE__{} = session) do
    stop_session(session.pid)
    forget_terminal(session)
    :ok
  end

  defp stop_session(pid) do
    GenServer.stop(pid, :normal, :infinity)
  catch
    :exit, _ -> :ok
  end

  @doc """
  Close the session and return its terminal evidence. A completed session
  returns the same stored result on repeated calls; the result is never
  inferred from the mere absence of a live session process.
  """
  @spec shutdown(session()) :: {:ok, Terminal.t()} | {:error, :released}
  def shutdown(%__MODULE__{} = session) do
    stop_session(session.pid)
    await_terminal(session, 0)
  end

  @doc """
  Wait for terminal evidence from a naturally exiting or explicitly closed
  session. `timeout` is milliseconds or `:infinity`. A timeout means only that
  no result was available yet; it makes no claim about cleanup.
  """
  @spec await_terminal(session(), timeout()) ::
          {:ok, Terminal.t()} | {:error, :timeout | :released}
  def await_terminal(%__MODULE__{pid: pid} = session, timeout \\ :infinity) do
    case terminal_result(session) do
      {:error, :pending} ->
        monitor = Process.monitor(pid)

        try do
          case terminal_result(session) do
            {:error, :pending} ->
              receive do
                {:DOWN, ^monitor, :process, ^pid, _reason} -> terminal_result(session)
              after
                timeout -> {:error, :timeout}
              end

            result ->
              result
          end
        after
          Process.demonitor(monitor, [:flush])
        end

      result ->
        result
    end
  end

  @doc "Release the retained terminal record after the session has ended."
  @spec forget_terminal(session()) :: :ok | {:error, :active}
  def forget_terminal(%__MODULE__{pid: pid, terminal_table: table}) do
    if Process.alive?(pid) do
      {:error, :active}
    else
      :ets.delete(table)
      :ok
    end
  catch
    :error, :badarg -> :ok
  end

  ## GenServer callbacks

  @impl true
  def init({owner, ref, terminal_table, argv, opts}) do
    # Trap exits so the port's link notification arrives as a message
    # instead of killing the server without terminate/2.
    Process.flag(:trap_exit, true)

    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, @default_kill_grace_ms)

    case Shim.open(opts) do
      {:ok, port} ->
        payload = Shim.encode_spawn(argv, Keyword.put(opts, :kill_grace_ms, kill_grace_ms))
        Shim.send_frame(port, Shim.tag_spawn(), payload)
        Process.monitor(owner)

        {:ok,
         %{
           port: port,
           owner: owner,
           session: %__MODULE__{pid: self(), ref: ref, terminal_table: terminal_table},
           kill_grace_ms: kill_grace_ms,
           buffers: %{stdout: "", stderr: ""},
           stdin_open: true,
           exit: nil,
           terminal: nil,
           terminal_recipient: Keyword.get(opts, :terminal_recipient),
           shutdown_cause: :explicit_close
         }}

      {:error, :not_found} ->
        {:stop, :shim_not_found}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:send_line, _line}, _from, %{exit: exit} = state) when not is_nil(exit) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:send_line, _line}, _from, %{stdin_open: false} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:send_line, line}, _from, state) do
    {:reply, port_command(state.port, Shim.tag_stdin(), [line, "\n"]), state}
  end

  def handle_call(:send_eof, _from, %{exit: exit} = state) when not is_nil(exit) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call(:send_eof, _from, %{stdin_open: false} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call(:send_eof, _from, state) do
    {:reply, port_command(state.port, Shim.tag_eof(), ""), %{state | stdin_open: false}}
  end

  @impl true
  def handle_info({port, {:data, <<tag, payload::binary>>}}, %{port: port} = state) do
    state = handle_frame(state, tag, payload)

    case state.exit do
      nil -> {:noreply, state}
      _exit -> {:stop, :normal, state}
    end
  end

  def handle_info({port, {:exit_status, _status}}, %{port: port} = state) do
    # The shim exited without sending EXIT/ERROR (e.g. it crashed). Its
    # death killed the group by the shim's own on-drop rule, but that
    # cannot be confirmed from here.
    state = flush_buffers(state)
    notify_exit(state, :shim_exited)

    terminal = %Terminal{
      status: nil,
      confirmation: :transport_lost,
      cause: :shim_lost,
      scope: :unknown
    }

    {:stop, :normal, %{state | exit: :shim_exited, terminal: terminal}}
  end

  def handle_info({:EXIT, port, _reason}, %{port: port} = state) do
    # Port link notification; the paired :exit_status message drives the
    # actual stop.
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, owner, _reason}, %{owner: owner} = state) do
    # Owner death; terminate/2 kills the group.
    {:stop, :normal, %{state | shutdown_cause: :owner_death}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    flush_buffers(state)
    terminal = settle(state)
    publish_terminal(state.session.terminal_table, terminal)

    if is_pid(state.terminal_recipient) do
      send(state.terminal_recipient, {:forcola_terminal, state.session, terminal})
    end
  end

  ## Option validation

  defp validate_opts!(argv, opts) do
    if Keyword.has_key?(opts, :timeout_ms) do
      raise ArgumentError,
            "Forcola.Duplex takes no :timeout_ms; a session's bound is its owner and close/1"
    end

    if Keyword.get(opts, :pty, false) and Keyword.get(opts, :merge_stderr, true) == false do
      raise ArgumentError,
            "Forcola.Duplex :pty inherently merges stderr into the terminal; " <>
              "merge_stderr: false is incompatible with pty: true"
    end

    unless Enum.all?(argv, &is_binary/1) do
      raise ArgumentError, "argv must be a non-empty list of binaries, got: #{inspect(argv)}"
    end

    recipient = Keyword.get(opts, :terminal_recipient)

    unless is_nil(recipient) or is_pid(recipient) do
      raise ArgumentError, ":terminal_recipient must be a pid"
    end

    :ok
  end

  ## Frame handling

  defp handle_frame(state, tag, payload) do
    cond do
      tag == Shim.tag_stdout() -> emit(state, :stdout, payload)
      tag == Shim.tag_stderr() -> emit(state, :stderr, payload)
      tag == Shim.tag_exit() -> child_exited(state, payload)
      tag == Shim.tag_error() -> spawn_failed(state, payload)
      true -> state
    end
  end

  defp child_exited(state, payload) do
    # The shim drains the child's pipes before sending EXIT, so every
    # output frame has already been handled; only partial lines remain.
    {status, _timed_out} = Shim.decode_exit(payload)
    terminal = exit_terminal(payload, :child_exit)
    state = flush_buffers(state)
    notify_exit(state, status)
    %{state | exit: status, terminal: terminal}
  end

  defp spawn_failed(state, payload) do
    reason = Shim.decode_error(payload)
    notify_exit(state, {:spawn_error, reason})

    terminal = %Terminal{
      status: nil,
      confirmation: :not_started,
      cause: :spawn_error,
      scope: :unknown
    }

    %{state | exit: {:spawn_error, reason}, terminal: terminal}
  end

  defp notify_exit(state, status) do
    send(state.owner, {:forcola_exit, state.session, status})
  end

  ## Line reassembly and delivery

  defp emit(state, stream, chunk) do
    {lines, rest} = split_lines(state.buffers[stream] <> chunk)
    Enum.each(lines, &deliver(state, stream, &1))
    %{state | buffers: Map.put(state.buffers, stream, rest)}
  end

  defp deliver(state, :stdout, line) do
    send(state.owner, {:forcola_line, state.session, line})
  end

  defp deliver(state, :stderr, line) do
    send(state.owner, {:forcola_stderr, state.session, line})
  end

  defp split_lines(data) do
    parts = :binary.split(data, "\n", [:global])
    {lines, [rest]} = Enum.split(parts, length(parts) - 1)
    {lines, rest}
  end

  # Deliver partial lines still buffered when the session ends.
  defp flush_buffers(state) do
    for {stream, buffer} <- state.buffers, buffer != "" do
      deliver(state, stream, buffer)
    end

    %{state | buffers: %{stdout: "", stderr: ""}}
  end

  ## Port writes

  # A closed port means the shim raced us shut; surface that as the
  # session being over.
  defp port_command(port, tag, payload) do
    case Shim.send_frame(port, tag, payload) do
      true -> :ok
      false -> {:error, :closed}
    end
  end

  ## Kill discipline

  # The child may still be running: kill the group and wait for the native
  # EXIT frame. Preserve its observed status separately from its cleanup
  # confirmation; a missing frame never becomes a confirmed result.
  defp settle(%{port: port, exit: nil, kill_grace_ms: kill_grace_ms} = state) do
    result =
      try do
        Shim.send_frame(port, Shim.tag_kill())
        await_exit(port, kill_grace_ms + @backstop_margin_ms)
      catch
        :error, :badarg -> :transport_lost
      end

    terminal = shutdown_terminal(result, state.shutdown_cause)
    warn_unconfirmed_cleanup(terminal.confirmation)
    close_port(port)
    terminal
  end

  # The shim already accounted for the child; nothing left to kill.
  defp settle(%{port: port, terminal: terminal}) do
    close_port(port)
    terminal
  end

  defp await_exit(port, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_exit(port, deadline)
  end

  defp do_await_exit(port, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    receive do
      {^port, {:data, <<tag, payload::binary>>}} ->
        cond do
          tag == Shim.tag_exit() -> {:exit, payload}
          tag == Shim.tag_error() -> {:spawn_error, payload}
          true -> do_await_exit(port, deadline)
        end

      {^port, {:exit_status, _status}} ->
        :transport_lost
    after
      max(remaining, 0) -> :timeout
    end
  end

  defp exit_terminal(payload, cause) do
    report = Shim.decode_exit_report(payload)

    %Terminal{
      status: report.status,
      confirmation: if(report.confirmed, do: :confirmed, else: :unconfirmed),
      cause: if(report.timed_out, do: :timeout, else: cause),
      scope: if(report.contained, do: :active_cgroup, else: :process_group)
    }
  end

  defp shutdown_terminal({:exit, payload}, cause), do: exit_terminal(payload, cause)

  defp shutdown_terminal({:spawn_error, _payload}, _cause) do
    %Terminal{status: nil, confirmation: :not_started, cause: :spawn_error, scope: :unknown}
  end

  defp shutdown_terminal(:timeout, cause) do
    %Terminal{status: nil, confirmation: :timeout, cause: cause, scope: :unknown}
  end

  defp shutdown_terminal(:transport_lost, cause) do
    %Terminal{status: nil, confirmation: :transport_lost, cause: cause, scope: :unknown}
  end

  defp publish_terminal(table, terminal) do
    :ets.insert_new(table, {:terminal, terminal})
  catch
    # Owner death also deletes its table. Teardown must still complete.
    :error, :badarg -> :ok
  end

  defp terminal_result(%__MODULE__{pid: pid, terminal_table: table}) do
    case :ets.lookup(table, :terminal) do
      [{:terminal, terminal}] ->
        {:ok, terminal}

      [] ->
        if Process.alive?(pid) do
          {:error, :pending}
        else
          terminal = %Terminal{
            status: nil,
            confirmation: :transport_lost,
            cause: :session_lost,
            scope: :unknown
          }

          :ets.insert_new(table, {:terminal, terminal})
          [{:terminal, stored}] = :ets.lookup(table, :terminal)
          {:ok, stored}
        end
    end
  catch
    :error, :badarg -> {:error, :released}
  end

  defp warn_unconfirmed_cleanup(:unconfirmed) do
    Logger.warning("forcola: duplex cleanup could not confirm process-group death")
  end

  defp warn_unconfirmed_cleanup(:timeout) do
    Logger.warning("forcola: duplex cleanup timed out before shim confirmation")
  end

  defp warn_unconfirmed_cleanup(:transport_lost) do
    Logger.warning("forcola: duplex transport closed before cleanup confirmation")
  end

  defp warn_unconfirmed_cleanup(_confirmed), do: :ok

  defp close_port(port), do: Shim.close(port)
end
