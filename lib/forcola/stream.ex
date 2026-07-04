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
  """

  alias Forcola.Shim

  defmodule Error do
    @moduledoc """
    Raised when a streamed run does not end with a clean zero exit.

    Fields:

      * `:status` - exit code, `{:signal, n}` for death by signal, or
        `{:signal, :unconfirmed}` when the shim never confirmed death
      * `:timed_out` - whether the run hit `:timeout_ms`
      * `:stderr` - stderr captured before termination (empty when
        `merge_stderr: true` routed it into the line stream)
      * `:reason` - spawn failure reason, `nil` for a run that started
    """

    defexception [:status, :reason, stderr: "", timed_out: false]

    @type t :: %__MODULE__{
            status: non_neg_integer() | {:signal, atom() | non_neg_integer()} | nil,
            reason: String.t() | atom() | nil,
            stderr: binary(),
            timed_out: boolean()
          }

    @impl true
    def message(%__MODULE__{reason: reason}) when not is_nil(reason) do
      "stream spawn failed: #{inspect(reason)}"
    end

    def message(%__MODULE__{status: status, timed_out: timed_out, stderr: stderr}) do
      base =
        if timed_out do
          "stream timed out; process group killed (#{format_status(status)})"
        else
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

  Takes the same options as `Forcola.run/2`; `:timeout_ms` is required
  and bounds the whole run, not the gap between lines (an idle-timeout
  option is tracked in
  [#33](https://github.com/joshrotenberg/forcola/issues/33)).

  Lines are emitted without their trailing newline. A partial line held
  across frame boundaries is emitted once its newline arrives; a final
  partial line with no newline is emitted before the stream terminates.

  See the module docs for how termination is surfaced.
  """
  @spec lines([String.t(), ...], keyword()) :: Enumerable.t()
  def lines([_binary | _] = argv, opts) do
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, @default_kill_grace_ms)

    Stream.resource(
      fn -> start(argv, opts, timeout_ms, kill_grace_ms) end,
      &next/1,
      &cleanup/1
    )
  end

  defp start(argv, opts, timeout_ms, kill_grace_ms) do
    case Shim.open() do
      {:ok, port} ->
        payload = Shim.encode_spawn(argv, Keyword.put(opts, :kill_grace_ms, kill_grace_ms))
        Shim.send_frame(port, Shim.tag_spawn(), payload)

        deadline =
          System.monotonic_time(:millisecond) + timeout_ms + kill_grace_ms + @backstop_margin_ms

        %{
          port: port,
          buffer: "",
          stderr: [],
          deadline: deadline,
          kill_grace_ms: kill_grace_ms,
          exit: nil
        }

      {:error, :not_found} ->
        raise Error, reason: :shim_not_found
    end
  end

  # Terminal conditions never raise directly out of the receive: they set
  # `:exit` in the state and raise on the next call, so cleanup/1 always
  # sees a state that says whether the shim already accounted for the
  # child (and can skip the KILL/confirm handshake when it has).
  defp next(%{exit: nil, port: port, deadline: deadline} = state) do
    remaining = deadline - System.monotonic_time(:millisecond)

    receive do
      {^port, {:data, <<tag, payload::binary>>}} ->
        handle_frame(tag, payload, state)

      {^port, {:exit_status, _status}} ->
        # The shim exited without sending EXIT/ERROR (e.g. it crashed).
        error = %Error{status: {:signal, :unconfirmed}, stderr: stderr(state)}
        {[], %{state | exit: {:error, error}}}
    after
      max(remaining, 0) ->
        # Backstop: the shim never reported back within its own timeout
        # plus grace. Closing the port in cleanup/1 is the remaining
        # kill lever (the shim treats stdin EOF as BEAM death).
        error = %Error{status: {:signal, :unconfirmed}, timed_out: true, stderr: stderr(state)}
        {[], %{state | exit: {:error, error}}}
    end
  end

  defp next(%{exit: :ok} = state), do: {:halt, state}
  defp next(%{exit: {:error, error}}), do: raise(error)

  defp handle_frame(tag, payload, state) do
    cond do
      tag == Shim.tag_stdout() ->
        {lines, rest} = split_lines(state.buffer <> payload)
        {lines, %{state | buffer: rest}}

      tag == Shim.tag_stderr() ->
        {[], %{state | stderr: [state.stderr | payload]}}

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
