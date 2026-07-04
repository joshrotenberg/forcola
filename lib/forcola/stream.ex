defmodule Forcola.Stream do
  @moduledoc """
  Line-by-line output from a shim-supervised process as an `Enumerable`.

  For CLIs that emit NDJSON or line-oriented progress (agent CLIs,
  `docker events`, `git clone --progress`). The child runs in its own
  process group; halting the stream, timing out, or BEAM death all kill
  the whole group.

      Forcola.Stream.lines(["claude", "-p", prompt], timeout_ms: 300_000)
      |> Stream.map(&:json.decode/1)
      |> Enum.to_list()

  ## Termination semantics

    * A zero exit ends the stream cleanly.
    * A non-zero exit, death by signal, timeout, or spawn failure raises
      `Forcola.Stream.Error` after every line produced before death has
      been emitted. The consumers this is built for are NDJSON CLIs where
      mid-stream death matters: silently dropping the exit reproduces the
      leak this library exists to close, and a tagged final element would
      force every `Stream.map(&decode/1)` pipeline to special-case it.
      Stderr captured during the run rides in the exception.
    * Halting the stream early (`Enum.take/2`, `Stream.take_while/2`, an
      exception downstream) kills the process group and blocks until the
      shim confirms the group is dead.
    * If the consuming process dies, the port closes, the shim sees stdin
      EOF, and the group is killed.

  ## Idle timeout

  `:timeout_ms` bounds the whole run. `:idle_timeout_ms` (optional) bounds
  the gap between output frames instead, detecting a stalled producer
  without capping total runtime. The two are independent and composable:
  whichever bound is reached first fires, and the raised
  `Forcola.Stream.Error` marks which one (`:idle_timed_out` vs
  `:timed_out`).

  The idle deadline resets on any child liveness signal: every STDOUT or
  STDERR data frame resets it, not only newline-terminated lines. A child
  that writes bytes without a newline, or writes only to stderr, still
  counts as alive. On idle expiry the same early-halt kill-and-confirm
  path runs (KILL the group, wait for the shim to confirm death, close the
  port) and `Forcola.Stream.Error` is raised with `idle_timed_out: true`
  after any lines already produced have been emitted. Omitting the option
  (the default) leaves behavior identical to a run bounded only by
  `:timeout_ms`.
  """

  alias Forcola.Shim

  defmodule Error do
    @moduledoc """
    Raised when a streamed run does not end with a clean zero exit.

    Fields:

      * `:status` - exit code, `{:signal, n}` for death by signal, or
        `{:signal, :unconfirmed}` when the shim never confirmed death
      * `:timed_out` - whether the run hit the whole-run `:timeout_ms`
      * `:idle_timed_out` - whether the run hit `:idle_timeout_ms` (no
        output arrived within the idle interval); mutually exclusive with
        `:timed_out`
      * `:stderr` - stderr captured before termination (empty when
        `merge_stderr: true` routed it into the line stream)
      * `:reason` - spawn failure reason, `nil` for a run that started
    """

    defexception [:status, :reason, stderr: "", timed_out: false, idle_timed_out: false]

    @type t :: %__MODULE__{
            status: non_neg_integer() | {:signal, atom() | non_neg_integer()} | nil,
            reason: String.t() | atom() | nil,
            stderr: binary(),
            timed_out: boolean(),
            idle_timed_out: boolean()
          }

    @impl true
    def message(%__MODULE__{reason: reason}) when not is_nil(reason) do
      "stream spawn failed: #{inspect(reason)}"
    end

    def message(%__MODULE__{
          status: status,
          timed_out: timed_out,
          idle_timed_out: idle_timed_out,
          stderr: stderr
        }) do
      base =
        cond do
          idle_timed_out ->
            "stream idle timed out; process group killed (#{format_status(status)})"

          timed_out ->
            "stream timed out; process group killed (#{format_status(status)})"

          true ->
            "streamed process exited with #{format_status(status)}"
        end

      case stderr do
        "" -> base
        _ -> base <> "; stderr: " <> stderr
      end
    end

    defp format_status({:signal, signal}), do: "signal #{inspect(signal)}"
    defp format_status(status), do: "status #{inspect(status)}"
  end

  # Same backstop scheme as Forcola.run/2: the shim enforces timeout_ms
  # itself and confirms group death within kill_grace_ms before sending
  # EXIT, so the Elixir-side deadline only fires if the shim never
  # reports back at all.
  @backstop_margin_ms 5_000
  @default_kill_grace_ms 5_000

  @doc """
  Run `argv` and return its stdout as a lazy stream of lines.

  Takes the same options as `Forcola.run/2`, including `:user`/`:group`
  for running the child as a different user (POSIX-only, fail-closed,
  privileged shim required); `:timeout_ms` is required and bounds the
  whole run, not the gap between lines.

  `:idle_timeout_ms` (optional, milliseconds; default `nil` = disabled)
  bounds the gap between output frames: if no STDOUT or STDERR data
  arrives within the interval the producer is treated as stalled, the
  process group is killed, and `Forcola.Stream.Error` is raised with
  `idle_timed_out: true`. It is independent of and composable with
  `:timeout_ms`; whichever bound fires first wins. See the module docs for
  the exact reset semantic.

  Lines are emitted without their trailing newline. A partial line held
  across frame boundaries is emitted once its newline arrives; a final
  partial line with no newline is emitted before the stream terminates.

  See the module docs for how termination is surfaced.
  """
  @spec lines([String.t(), ...], keyword()) :: Enumerable.t()
  def lines([_binary | _] = argv, opts) do
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, @default_kill_grace_ms)
    idle_timeout_ms = Keyword.get(opts, :idle_timeout_ms)

    Stream.resource(
      fn -> start(argv, opts, timeout_ms, kill_grace_ms, idle_timeout_ms) end,
      &next/1,
      &cleanup/1
    )
  end

  defp start(argv, opts, timeout_ms, kill_grace_ms, idle_timeout_ms) do
    case Shim.open() do
      {:ok, port} ->
        payload = Shim.encode_spawn(argv, Keyword.put(opts, :kill_grace_ms, kill_grace_ms))
        Shim.send_frame(port, Shim.tag_spawn(), payload)

        now = System.monotonic_time(:millisecond)
        deadline = now + timeout_ms + kill_grace_ms + @backstop_margin_ms

        %{
          port: port,
          buffer: "",
          stderr: [],
          deadline: deadline,
          idle_timeout_ms: idle_timeout_ms,
          idle_deadline: idle_deadline(idle_timeout_ms, now),
          kill_grace_ms: kill_grace_ms,
          exit: nil
        }

      {:error, :not_found} ->
        raise Error, reason: :shim_not_found
    end
  end

  # nil idle_timeout_ms leaves the idle deadline nil (disabled). Otherwise
  # the deadline is now + interval and is reset on every liveness frame.
  defp idle_deadline(nil, _now), do: nil
  defp idle_deadline(idle_timeout_ms, now), do: now + idle_timeout_ms

  # Terminal conditions never raise directly out of the receive: they set
  # `:exit` in the state and raise on the next call, so cleanup/1 always
  # sees a state that says whether the shim already accounted for the
  # child (and can skip the KILL/confirm handshake when it has).
  defp next(%{exit: nil, port: port, deadline: deadline, idle_deadline: idle_deadline} = state) do
    now = System.monotonic_time(:millisecond)
    run_remaining = deadline - now

    # With idle disabled the wait is just the whole-run backstop. With it
    # enabled the wait is the sooner of the two, and on expiry we attribute
    # to whichever deadline actually elapsed.
    wait =
      case idle_deadline do
        nil -> run_remaining
        _ -> min(run_remaining, idle_deadline - now)
      end

    receive do
      {^port, {:data, <<tag, payload::binary>>}} ->
        handle_frame(tag, payload, state)

      {^port, {:exit_status, _status}} ->
        # The shim exited without sending EXIT/ERROR (e.g. it crashed).
        error = %Error{status: {:signal, :unconfirmed}, stderr: stderr(state)}
        {[], %{state | exit: {:error, error}}}
    after
      max(wait, 0) ->
        {[], %{state | exit: {:error, timeout_error(state)}}}
    end
  end

  defp next(%{exit: :ok} = state), do: {:halt, state}
  defp next(%{exit: {:error, error}}), do: raise(error)

  # Attribute an expired wait. When the idle deadline is the one that
  # elapsed it is an idle timeout: the shim is not timing out on its own
  # (the whole-run bound has not been reached), so we must actively KILL
  # the group and wait for the shim to confirm death before the port is
  # closed in cleanup/1. The whole-run backstop keeps its old behavior:
  # the shim already hit its own timeout plus grace, so closing the port
  # (stdin EOF) in cleanup/1 is the remaining kill lever.
  defp timeout_error(%{idle_deadline: idle_deadline, deadline: deadline} = state)
       when not is_nil(idle_deadline) and idle_deadline <= deadline do
    kill_and_confirm(state)
    %Error{status: {:signal, :unconfirmed}, idle_timed_out: true, stderr: stderr(state)}
  end

  defp timeout_error(state) do
    %Error{status: {:signal, :unconfirmed}, timed_out: true, stderr: stderr(state)}
  end

  # KILL the group and block until the shim confirms death, mirroring the
  # early-halt path in cleanup/1. cleanup/1 will still close and flush the
  # port afterward; the exit is already set, so it takes the non-handshake
  # clause and does not KILL again.
  defp kill_and_confirm(%{port: port, kill_grace_ms: kill_grace_ms}) do
    Shim.send_frame(port, Shim.tag_kill())
    await_exit(port, kill_grace_ms + @backstop_margin_ms)
  catch
    :error, :badarg -> :ok
  end

  defp handle_frame(tag, payload, state) do
    cond do
      tag == Shim.tag_stdout() ->
        {lines, rest} = split_lines(state.buffer <> payload)
        {lines, %{reset_idle(state) | buffer: rest}}

      tag == Shim.tag_stderr() ->
        {[], %{reset_idle(state) | stderr: [state.stderr | payload]}}

      tag == Shim.tag_exit() ->
        handle_exit(payload, state)

      tag == Shim.tag_error() ->
        error = %Error{reason: Shim.decode_error(payload), stderr: stderr(state)}
        {[], %{state | exit: {:error, error}}}

      true ->
        {[], state}
    end
  end

  defp handle_exit(payload, state) do
    {status, timed_out} = Shim.decode_exit(payload)
    final = if state.buffer == "", do: [], else: [state.buffer]

    outcome =
      if status == 0 and not timed_out do
        :ok
      else
        {:error, %Error{status: status, timed_out: timed_out, stderr: stderr(state)}}
      end

    {final, %{state | buffer: "", exit: outcome}}
  end

  # A liveness frame (any STDOUT or STDERR data) pushes the idle deadline
  # forward by the full interval. A no-op when idle timeout is disabled.
  defp reset_idle(%{idle_timeout_ms: nil} = state), do: state

  defp reset_idle(%{idle_timeout_ms: idle_timeout_ms} = state) do
    %{state | idle_deadline: System.monotonic_time(:millisecond) + idle_timeout_ms}
  end

  defp split_lines(data) do
    parts = :binary.split(data, "\n", [:global])
    {lines, [rest]} = Enum.split(parts, length(parts) - 1)
    {lines, rest}
  end

  defp stderr(state), do: IO.iodata_to_binary(state.stderr)

  # Early halt: the child may still be running. Kill the group and wait
  # for the shim's EXIT frame (the shim confirms group death before
  # sending it), so the halt does not return until the group is dead.
  defp cleanup(%{port: port, exit: nil, kill_grace_ms: kill_grace_ms}) do
    try do
      Shim.send_frame(port, Shim.tag_kill())
      await_exit(port, kill_grace_ms + @backstop_margin_ms)
    catch
      # The port raced us shut (shim already gone); its death killed the
      # group by the shim's stdin-EOF rule, so there is nothing to wait on.
      :error, :badarg -> :ok
    end

    close_port(port)
    flush_port(port)
  end

  defp cleanup(%{port: port}) do
    close_port(port)
    flush_port(port)
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

  # Drop any port messages already delivered to the mailbox; the port is
  # closed, so nothing new arrives and stragglers would sit in a
  # long-lived consumer's mailbox forever.
  defp flush_port(port) do
    receive do
      {^port, _} -> flush_port(port)
    after
      0 -> :ok
    end
  end
end
