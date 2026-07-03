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

  Scaffold. Functions raise `Forcola.NotImplementedError` until the shim
  lands.
  """

  alias Forcola.Result

  @typedoc "Errors a bounded run can return."
  @type run_error :: {:timeout, Result.t()} | {:spawn, term()}

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
    _ = Keyword.fetch!(opts, :timeout_ms)
    raise Forcola.NotImplementedError, {__MODULE__, :run, [argv, opts]}
  end
end
