defmodule Forcola.Stream do
  @moduledoc """
  Line-by-line output from a shim-supervised process as an `Enumerable`.

  For CLIs that emit NDJSON or line-oriented progress (agent CLIs,
  `docker events`, `git clone --progress`). The child runs in its own
  process group; halting the stream, timing out, or BEAM death all kill
  the whole group.

  Planned API:

      Forcola.Stream.lines(["claude", "-p", prompt], timeout_ms: 300_000)
      |> Stream.map(&Jason.decode!/1)
      |> Enum.to_list()
  """

  @doc """
  Run `argv` and return its stdout as a stream of lines.

  Takes the same options as `Forcola.run/2`. The timeout bounds the whole
  run, not the gap between lines; an idle-timeout option may come later.
  """
  @spec lines([String.t(), ...], keyword()) :: Enumerable.t()
  def lines([_binary | _] = argv, opts) do
    _ = Keyword.fetch!(opts, :timeout_ms)
    raise Forcola.NotImplementedError, {__MODULE__, :lines, [argv, opts]}
  end
end
