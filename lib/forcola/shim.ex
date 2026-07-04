defmodule Forcola.Shim do
  @moduledoc """
  Locates and speaks to the `forcola_shim` binary.

  The shim is a Rust port program (source under `native/forcola_shim`).
  Release builds are published per target to GitHub Releases and fetched
  at compile time with SHA256 verification, so consumers need no Rust
  toolchain; a locally built binary in `priv/` takes precedence during
  development.

  Wire protocol (BEAM <-> shim), v0, documented in full in
  `native/forcola_shim/src/main.rs`:

  Frames are `{:packet, 4}`-style: a 4-byte big-endian length prefix
  (handled by the Erlang port itself), then a 1-byte tag, then the
  payload. This module owns the tag constants and the JSON payload
  shapes for SPAWN/EXIT/ERROR; `Forcola.run/2` drives the protocol.
  """

  # Inbound tag: BEAM -> shim.
  @tag_spawn 0x01
  @tag_stdin 0x02
  @tag_eof 0x03
  @tag_kill 0x04

  # Outbound tag: shim -> BEAM.
  @tag_stdout 0x11
  @tag_stderr 0x12
  @tag_exit 0x13
  @tag_error 0x14

  @doc false
  def tag_spawn, do: @tag_spawn
  @doc false
  def tag_stdin, do: @tag_stdin
  @doc false
  def tag_eof, do: @tag_eof
  @doc false
  def tag_kill, do: @tag_kill
  @doc false
  def tag_stdout, do: @tag_stdout
  @doc false
  def tag_stderr, do: @tag_stderr
  @doc false
  def tag_exit, do: @tag_exit
  @doc false
  def tag_error, do: @tag_error

  @doc """
  Absolute path to the shim binary for the current target.

  Returns `{:error, :not_found}` if no binary has been built or
  downloaded yet.
  """
  @spec path() :: {:ok, Path.t()} | {:error, :not_found}
  def path do
    case Application.app_dir(:forcola, "priv/forcola_shim") do
      p when is_binary(p) ->
        if File.exists?(p), do: {:ok, p}, else: {:error, :not_found}
    end
  end

  @doc """
  Opens the shim binary as a port, framed with `{:packet, 4}`.

  The caller owns the returned port: send it SPAWN/STDIN/EOF/KILL
  frames via `send_frame/2` and receive `{port, {:data, <<tag, payload::binary>>}}`
  messages for STDOUT/STDERR/EXIT/ERROR frames.
  """
  @spec open() :: {:ok, port()} | {:error, :not_found}
  def open do
    with {:ok, bin} <- path() do
      port =
        Port.open({:spawn_executable, bin}, [
          {:packet, 4},
          :binary,
          :exit_status,
          :use_stdio,
          :hide,
          args: []
        ])

      {:ok, port}
    end
  end

  @doc "Sends a tagged frame to the shim port."
  @spec send_frame(port(), non_neg_integer(), iodata()) :: true
  def send_frame(port, tag, payload \\ "") do
    Port.command(port, [tag, payload])
  end

  @doc "Encodes a SPAWN frame payload from `Forcola.run/2` options."
  @spec encode_spawn(term(), keyword()) :: binary()
  def encode_spawn(argv, opts) do
    base = %{
      "argv" => argv,
      "merge_stderr" => Keyword.get(opts, :merge_stderr, false)
    }

    base
    |> maybe_put("cd", Keyword.get(opts, :cd))
    |> maybe_put("env", encode_env(Keyword.get(opts, :env)))
    |> maybe_put("timeout_ms", Keyword.get(opts, :timeout_ms))
    |> maybe_put("kill_grace_ms", Keyword.get(opts, :kill_grace_ms))
    |> put_pty(opts)
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  # pty fields are added only when a pty is requested, so the SPAWN payload
  # for non-pty callers is unchanged. The shim defaults pty to false when
  # the key is absent.
  defp put_pty(map, opts) do
    if Keyword.get(opts, :pty, false) do
      map
      |> Map.put("pty", true)
      |> maybe_put("pty_rows", Keyword.get(opts, :pty_rows))
      |> maybe_put("pty_cols", Keyword.get(opts, :pty_cols))
    else
      map
    end
  end

  defp encode_env(nil), do: nil
  defp encode_env(env), do: Map.new(env)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  @doc "Decodes an EXIT frame payload into `{status_or_signal, timed_out}`."
  @spec decode_exit(binary()) ::
          {non_neg_integer() | {:signal, non_neg_integer()}, boolean()}
  def decode_exit(payload) do
    decoded = :json.decode(payload)
    timed_out = Map.get(decoded, "timed_out", false)

    status =
      case decoded do
        %{"status" => status} when is_integer(status) -> status
        %{"signal" => signal} when is_integer(signal) -> {:signal, signal}
      end

    {status, timed_out}
  end

  @doc "Decodes an ERROR frame payload into its reason string."
  @spec decode_error(binary()) :: String.t()
  def decode_error(payload) do
    %{"reason" => reason} = :json.decode(payload)
    reason
  end
end
