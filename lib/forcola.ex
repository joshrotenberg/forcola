defmodule Forcola do
  @moduledoc """
  Leak-free external process execution.

  Every command runs under a small Rust shim (a port program, not a NIF)
  that places the child in its own process group via `setsid` and kills
  the whole group, SIGTERM then SIGKILL, when the run times out or the
  BEAM dies. The shim detects BEAM death as stdin EOF, so cleanup happens
  even on `kill -9` of the VM.

  ## Why not `System.cmd` in a `Task`?

  `Task.shutdown` closes the Erlang port, and closing a port closes pipes;
  it sends no signal. The external process runs on until it next touches a
  closed pipe, and its children are never signaled at all. See the README
  for the full mechanism.

  ## Modes

    * `Forcola.run/2` - bounded one-shot run, mandatory timeout
    * `Forcola.Stream` - line-by-line output as an `Enumerable`
    * `Forcola.Daemon` - long-running server under a supervision tree
    * `Forcola.Duplex` - bidirectional stdin/stdout session

  ## Status

  `Forcola.run/2` and `Forcola.Stream.lines/2` are implemented. Other
  modes still raise `Forcola.NotImplementedError` until they land.
  """

  alias Forcola.{Result, Shim}

  @typedoc "Errors a bounded run can return."
  @type run_error :: {:timeout, Result.t()} | {:spawn, term()}

  # Margin added on top of the shim's own kill_grace_ms when computing
  # the Elixir-side backstop deadline: the shim already enforces
  # timeout_ms and confirms group death within kill_grace_ms before
  # sending EXIT, so this only fires if the shim itself never reports
  # back (e.g. it's wedged or the BEAM<->shim pipe is stuck).
  @backstop_margin_ms 5_000
  @default_kill_grace_ms 5_000

  @doc """
  Run `argv` (`[binary | args]`) to completion under the shim.

  ## Options

    * `:timeout_ms` - required. On expiry the child's process group is
      killed (SIGTERM, then SIGKILL after the kill grace) and
      `{:error, {:timeout, partial_result}}` is returned with output
      captured so far. The group is confirmed dead before the call
      returns.
    * `:cd` - working directory.
    * `:env` - list of `{name, value}` strings.
    * `:merge_stderr` - route stderr into stdout; default `false`.

  Any exit status is `{:ok, %Forcola.Result{}}`; callers branch on
  `:status`. A non-zero exit is a result, not an error.
  """
  @spec run([String.t(), ...], keyword()) :: {:ok, Result.t()} | {:error, run_error()}
  def run([_binary | _] = argv, opts) do
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, @default_kill_grace_ms)

    case Shim.open() do
      {:ok, port} ->
        try do
          spawn_and_collect(port, argv, opts, timeout_ms, kill_grace_ms)
        after
          close_port(port)
        end

      {:error, :not_found} ->
        {:error, {:spawn, :shim_not_found}}
    end
  end

  defp spawn_and_collect(port, argv, opts, timeout_ms, kill_grace_ms) do
    payload = Shim.encode_spawn(argv, Keyword.put(opts, :kill_grace_ms, kill_grace_ms))
    Shim.send_frame(port, Shim.tag_spawn(), payload)

    # Elixir-side backstop only: the shim itself enforces timeout_ms and
    # confirms group death within kill_grace_ms before reporting EXIT, so
    # under normal operation the shim's own EXIT frame arrives first.
    # This deadline exists in case the shim never reports back at all.
    deadline =
      System.monotonic_time(:millisecond) + timeout_ms + kill_grace_ms + @backstop_margin_ms

    collect(port, %{stdout: [], stderr: []}, deadline)
  end

  defp collect(port, acc, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    receive do
      {^port, {:data, <<tag, payload::binary>>}} ->
        handle_frame(port, tag, payload, acc, deadline)

      {^port, {:exit_status, _status}} ->
        # The shim exited without ever sending EXIT/ERROR (e.g. it
        # crashed). Death is implied by the OS reaping the port program
        # itself, but the child's own group state cannot be confirmed
        # from here.
        {:error, {:spawn, {:shim_exited, result(acc, {:signal, :unconfirmed})}}}
    after
      max(remaining, 0) ->
        {:error, {:timeout, result(acc, {:signal, :unconfirmed})}}
    end
  end

  defp handle_frame(port, tag, payload, acc, deadline) do
    cond do
      tag == Shim.tag_stdout() ->
        collect(port, %{acc | stdout: [acc.stdout | payload]}, deadline)

      tag == Shim.tag_stderr() ->
        collect(port, %{acc | stderr: [acc.stderr | payload]}, deadline)

      tag == Shim.tag_exit() ->
        {status, timed_out} = Shim.decode_exit(payload)
        res = result(acc, status)
        if timed_out, do: {:error, {:timeout, res}}, else: {:ok, res}

      tag == Shim.tag_error() ->
        reason = Shim.decode_error(payload)
        {:error, {:spawn, reason}}

      true ->
        collect(port, acc, deadline)
    end
  end

  defp result(acc, status) do
    %Result{
      status: status,
      stdout: IO.iodata_to_binary(acc.stdout),
      stderr: IO.iodata_to_binary(acc.stderr)
    }
  end

  defp close_port(port) do
    if port_open?(port) do
      Port.close(port)
    end
  catch
    :error, :badarg -> :ok
  end

  defp port_open?(port) do
    Port.info(port) != nil
  end
end
