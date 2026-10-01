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
  @tag_credit 0x05
  @tag_stderr_credit 0x06

  # Outbound tag: shim -> BEAM.
  @tag_stdout 0x11
  @tag_stderr 0x12
  @tag_exit 0x13
  @tag_error 0x14

  # Port.close is synchronous, but the guard may still be relaying messages.
  # Healthy cleanup ends immediately at its final marker; this bound is only
  # a backstop if the guard itself is killed or cannot finish.
  @guard_close_timeout_ms 5_000

  @doc false
  def tag_spawn, do: @tag_spawn
  @doc false
  def tag_stdin, do: @tag_stdin
  @doc false
  def tag_eof, do: @tag_eof
  @doc false
  def tag_kill, do: @tag_kill
  @doc false
  def tag_credit, do: @tag_credit
  @doc false
  def tag_stderr_credit, do: @tag_stderr_credit
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
  downloaded yet. An explicit `:shim_path` overrides discovery and must
  name an absolute path to a regular file with executable permissions.
  Invalid overrides return `{:error, {:invalid_shim_path, reason}}`.

  The override is trusted executable code. The caller must supply the
  matching Forcola shim for this version and target, protect the file and
  its ancestor directories from replacement, and own its lifetime and
  cleanup. Symlinks are followed and must satisfy the same trust contract.
  Validation does not authenticate the binary or prevent path replacement.
  """
  @spec path(keyword()) ::
          {:ok, Path.t()} | {:error, :not_found | {:invalid_shim_path, atom()}}
  def path(opts \\ []) do
    case Keyword.fetch(opts, :shim_path) do
      {:ok, path} -> validate_path(path)
      :error -> bundled_path()
    end
  end

  defp bundled_path do
    case Application.app_dir(:forcola, "priv/forcola_shim") do
      p when is_binary(p) ->
        if File.exists?(p), do: {:ok, p}, else: {:error, :not_found}
    end
  end

  defp validate_path(path) when is_binary(path) do
    case Path.type(path) do
      :absolute -> validate_file(path, File.stat(path))
      _relative -> {:error, {:invalid_shim_path, :not_absolute}}
    end
  end

  defp validate_path(_path), do: {:error, {:invalid_shim_path, :not_a_binary}}

  defp validate_file(path, {:ok, %File.Stat{type: :regular, mode: mode}}) do
    if Bitwise.band(mode, 0o111) != 0 do
      {:ok, path}
    else
      {:error, {:invalid_shim_path, :not_executable}}
    end
  end

  defp validate_file(_path, {:ok, _stat}), do: {:error, {:invalid_shim_path, :not_regular}}
  defp validate_file(_path, {:error, reason}), do: {:error, {:invalid_shim_path, reason}}

  @doc """
  Opens the shim binary as a port, framed with `{:packet, 4}`.

  The caller controls the returned port: send it SPAWN/STDIN/EOF/KILL
  frames via `send_frame/2` and receive `{port, {:data, <<tag, payload::binary>>}}`
  messages for STDOUT/STDERR/EXIT/ERROR frames.

  A guard process owns the port and relays its messages in order. It monitors
  the caller and closes the port if the caller dies. This retains EOF-driven
  process cleanup without letting a broken shim's port errors (such as
  `:epipe`) kill the caller or change the caller's exit-trapping behavior.
  When finished, the opening process should call `close/1` to close the port
  and drain the guard's remaining messages.

  Accepts the trusted `:shim_path` override documented in `path/1`.
  Synchronous operating-system errors opening the port return
  `{:error, {:shim_start_failed, reason}}`.
  """
  @spec open(keyword()) :: {:ok, port()} | {:error, term()}
  def open(opts \\ []) do
    with {:ok, bin} <- path(opts) do
      open_guarded_port(bin)
    end
  end

  defp open_guarded_port(bin) do
    owner = self()
    request = make_ref()
    {guard, monitor} = spawn_monitor(fn -> guard_open(owner, request, bin) end)

    receive do
      {^request, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^guard, reason} ->
        {:error, {:shim_start_failed, reason}}
    end
  end

  defp guard_open(owner, request, bin) do
    Process.flag(:trap_exit, true)
    monitor = Process.monitor(owner)

    case open_port(bin) do
      {:ok, port} ->
        try do
          send(owner, {request, {:ok, port}})
          guard_port(port, owner, monitor, false)
        after
          send(owner, {port, :guard_closed})
        end

      error ->
        send(owner, {request, error})
    end
  end

  defp guard_port(port, owner, monitor, reported_exit?) do
    receive do
      {^port, {:exit_status, _status}} = message ->
        send(owner, message)
        guard_port(port, owner, monitor, true)

      {^port, _event} = message ->
        send(owner, message)
        guard_port(port, owner, monitor, reported_exit?)

      {:EXIT, ^port, :normal} ->
        :ok

      {:EXIT, ^port, reason} ->
        # Driver failures can close the port without an OS exit_status.
        # All modes already treat an exit without protocol EXIT as unconfirmed.
        # Relaying every event through this guard keeps earlier output and
        # protocol EXIT frames ahead of this terminal notification.
        unless reported_exit?, do: send(owner, {port, {:exit_status, {:shim_error, reason}}})

      {:DOWN, ^monitor, :process, ^owner, _reason} ->
        close_guarded_port(port)
    end
  end

  defp close_guarded_port(port) do
    Port.close(port)
  catch
    :error, :badarg -> :ok
  end

  @doc """
  Closes the port and drains its remaining relayed messages.

  Call once from the process that opened the port, after consuming its result.
  The OS port is closed synchronously; draining normally finishes immediately.
  If the guard itself fails to finish, draining is bounded by five seconds and
  returns `{:error, :guard_close_timeout}`.
  """
  @spec close(port()) :: :ok | {:error, :guard_close_timeout}
  def close(port) do
    close_guarded_port(port)
    deadline = System.monotonic_time(:millisecond) + @guard_close_timeout_ms
    drain_guard(port, deadline)
  end

  defp drain_guard(port, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining > 0 do
      receive do
        {^port, :guard_closed} -> :ok
        {^port, _event} -> drain_guard(port, deadline)
      after
        remaining -> {:error, :guard_close_timeout}
      end
    else
      {:error, :guard_close_timeout}
    end
  end

  defp open_port(bin) do
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
  catch
    :error, reason -> {:error, {:shim_start_failed, reason}}
  end

  @doc """
  Sends a tagged frame to the shim port.

  Returns `false` if the port has already closed. The caller still receives
  the terminal port event and handles it through its ordinary shim-death path.
  """
  @spec send_frame(port(), non_neg_integer(), iodata()) :: boolean()
  def send_frame(port, tag, payload \\ "") do
    Port.command(port, [tag, payload])
  catch
    :error, :badarg ->
      if Port.info(port) == nil, do: false, else: :erlang.error(:badarg)
  end

  @doc """
  Encodes a CREDIT frame payload: an 8-byte big-endian byte count.

  Grants the pump selected by the frame tag that many more bytes of read
  budget. `Forcola.Stream` uses stdout credit; bounded `Forcola.Duplex`
  grants separate stdout and stderr credit.
  """
  @spec encode_credit(non_neg_integer()) :: binary()
  def encode_credit(bytes) when is_integer(bytes) and bytes >= 0 do
    <<bytes::unsigned-big-integer-size(64)>>
  end

  @doc """
  Encodes a SPAWN frame payload from `Forcola.run/2` options.

  Shared by all four modes. Besides `:cd`/`:env`/`:merge_stderr`/`:timeout_ms`/
  `:kill_grace_ms` and the pty options, it threads `:user` and `:group` (each a
  string name or an integer id) through to the shim so the child can be run as a
  different user; see `Forcola.run/2` for the semantics.

  `:cgroup` opts into Linux cgroup v2 containment. `true` allows a warning
  and process-group fallback; `:required` refuses to spawn without placement.
  The required value is encoded as a JSON string so an older shim rejects the
  SPAWN frame rather than treating it as best-effort. The key is absent by
  default, preserving the original payload.

  `:window_bytes` opts into demand-driven backpressure on the child's stdout
  (see `Forcola.Stream.lines/2`). Only added to the payload when present, so
  the default SPAWN payload is unchanged; the shim gates its stdout pump when
  the field is present and reads eagerly otherwise. Duplex pull mode also
  supplies `:stderr_window_bytes` and `:strict_output` so both pumps stay
  gated until output is consumed or explicitly discarded.
  """
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
    |> maybe_put("user", Keyword.get(opts, :user))
    |> maybe_put("group", Keyword.get(opts, :group))
    |> maybe_put("window_bytes", Keyword.get(opts, :window_bytes))
    |> maybe_put("stderr_window_bytes", Keyword.get(opts, :stderr_window_bytes))
    |> maybe_put("strict_output", Keyword.get(opts, :strict_output))
    |> put_cgroup(opts)
    |> put_pty(opts)
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp put_cgroup(map, opts) do
    case Keyword.get(opts, :cgroup, false) do
      value when value in [false, nil] ->
        map

      true ->
        Map.put(map, "cgroup", true)

      :required ->
        Map.put(map, "cgroup", "required")

      other ->
        raise ArgumentError, ":cgroup must be false, true, or :required, got: #{inspect(other)}"
    end
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

  @doc """
  Decodes an EXIT frame payload into `{status_or_signal, timed_out}`.

  A new shim reports `confirmed: false` when its bounded cleanup probes still
  observed a live process group or cgroup. That maps to
  `{:signal, :unconfirmed}` in every execution mode. The field defaults to true
  when absent for compatibility with older shims.
  """
  @spec decode_exit(binary()) ::
          {non_neg_integer() | {:signal, atom() | non_neg_integer()}, boolean()}
  def decode_exit(payload) do
    decoded = :json.decode(payload)
    timed_out = Map.get(decoded, "timed_out", false)

    status =
      if Map.get(decoded, "confirmed", true) do
        case decoded do
          %{"status" => status} when is_integer(status) -> status
          %{"signal" => signal} when is_integer(signal) -> {:signal, signal}
        end
      else
        {:signal, :unconfirmed}
      end

    {status, timed_out}
  end

  @doc """
  Decodes the `contained` flag from an EXIT frame payload.

  `true` when Linux cgroup v2 containment was actually active for the run;
  `false` on the default path, on fallback (macOS, no cgroup v2, or no
  delegated subtree), and whenever the field is absent (older shim). Reports
  which kill mechanism was used.
  """
  @spec decode_contained(binary()) :: boolean()
  def decode_contained(payload) do
    Map.get(:json.decode(payload), "contained", false)
  end

  @doc """
  Decodes the native EXIT report without discarding the observed child status
  when cleanup was unconfirmed. `decode_exit/1` retains its legacy status shape.
  """
  @spec decode_exit_report(binary()) :: map()
  def decode_exit_report(payload) do
    decoded = :json.decode(payload)

    status =
      cond do
        is_integer(decoded["status"]) -> decoded["status"]
        is_integer(decoded["signal"]) -> {:signal, decoded["signal"]}
        true -> nil
      end

    %{
      status: status,
      confirmed: Map.get(decoded, "confirmed", true),
      timed_out: Map.get(decoded, "timed_out", false),
      contained: Map.get(decoded, "contained", false),
      output_truncated: Map.get(decoded, "output_truncated", false)
    }
  end

  @doc "Decodes an ERROR frame payload into its reason string."
  @spec decode_error(binary()) :: String.t()
  def decode_error(payload) do
    %{"reason" => reason} = :json.decode(payload)
    reason
  end
end
