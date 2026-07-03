defmodule Forcola.Duplex do
  @moduledoc """
  A bidirectional stdin/stdout session with an external process.

  For interactive CLIs driven over stdin (agent CLIs in stream-json mode).
  The owner writes lines in and receives lines out; the child runs in its
  own process group and dies with the session.

  Planned API:

      {:ok, session} = Forcola.Duplex.open(["claude", "--input-format", "stream-json"], [])
      :ok = Forcola.Duplex.send_line(session, json)
      receive do
        {:forcola_line, ^session, line} -> line
      end
  """

  @typedoc "An open duplex session."
  @opaque session :: reference()

  @doc """
  Open a duplex session running `argv`.

  Takes `:cd` and `:env` as in `Forcola.run/2`. There is no `:timeout_ms`;
  the session is bounded by its owner process, and `close/1`.
  """
  @spec open([String.t(), ...], keyword()) :: {:ok, session()} | {:error, term()}
  def open([_binary | _] = argv, opts) do
    raise Forcola.NotImplementedError, {__MODULE__, :open, [argv, opts]}
  end

  @doc "Write a line to the child's stdin."
  @spec send_line(session(), iodata()) :: :ok | {:error, term()}
  def send_line(session, line) do
    raise Forcola.NotImplementedError, {__MODULE__, :send_line, [session, line]}
  end

  @doc "Close the session and kill the child's process group."
  @spec close(session()) :: :ok
  def close(session) do
    raise Forcola.NotImplementedError, {__MODULE__, :close, [session]}
  end
end
