defmodule Forcola.Shim do
  @moduledoc """
  Locates and speaks to the `forcola_shim` binary.

  The shim is a Rust port program (source under `native/forcola_shim`).
  Release builds are published per target to GitHub Releases and fetched
  at compile time with SHA256 verification, so consumers need no Rust
  toolchain; a locally built binary in `priv/` takes precedence during
  development.

  Wire protocol (BEAM <-> shim), legacy framing plus optional version 1
  capability negotiation, documented in full in
  `native/forcola_shim/src/main.rs`:

  Frames are `{:packet, 4}`-style: a 4-byte big-endian length prefix
  (handled by the Erlang port itself), then a 1-byte tag, then the
  payload. This module owns the tag constants and the JSON payload
  shapes for SPAWN/EXIT/ERROR/SPAWNED. Each execution mode drives the protocol.
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
  @tag_spawned 0x16

  @typedoc "Native start identity, stable for the life of a process."
  @type start_time ::
          %{kind: :linux, boot_id: String.t(), ticks: non_neg_integer()}
          | %{kind: :macos, seconds: non_neg_integer(), microseconds: non_neg_integer()}

  @typedoc "The child or process-group leader's native identity."
  @type identity :: %{pid: pos_integer(), pgid: pos_integer(), start_time: start_time()}

  # Port.close is synchronous, but the guard may still be relaying messages.
  # Healthy cleanup ends immediately at its final marker; this bound is only
  # a backstop if the guard itself is killed or cannot finish.
  @guard_close_timeout_ms 5_000
  @cleanup_margin_ms 5_000
  @cgroup_drain_probe_ms 500
  @supervisor_margin_ms 1_000
  @execution_options [:env_mode, :interrupt_signal, :interrupt_grace_ms]

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
  @doc false
  def tag_spawned, do: @tag_spawned

  @doc false
  def negotiate(port, timeout_ms, required_features \\ ["bounded_run_v1"])
      when is_integer(timeout_ms) and timeout_ms >= 0 do
    selected_features =
      Enum.reject(required_features, &(&1 in ["bounded_run_v1", "env_unset_v1"]))

    # bounded_run_v1 and env_unset_v1 only require advertised support.
    # With no outbound extension selected, keep the original empty HELLO.
    payload =
      if selected_features == [],
        do: "",
        else: :json.encode(%{"features" => selected_features})

    send_frame(port, 0x07, payload)

    receive do
      {^port, {:data, <<0x15, payload::binary>>}} ->
        decode_capabilities(payload, required_features)

      {^port, {:data, <<@tag_error, payload::binary>>}} ->
        # Legacy shims explicitly reject HELLO because they expect SPAWN.
        case decode_json(payload) do
          {:ok, %{"reason" => reason}} when is_binary(reason) -> {:error, :incompatible_shim}
          _ -> {:error, :invalid_handshake}
        end

      {^port, {:exit_status, status}} ->
        {:error, {:shim_exited, status}}

      {^port, :guard_closed} ->
        {:error, {:shim_exited, :unknown}}

      {^port, _event} ->
        {:error, :invalid_handshake}
    after
      timeout_ms -> {:error, :handshake_timeout}
    end
  end

  defp decode_capabilities(payload, required_features) do
    case decode_json(payload) do
      {:ok, %{"protocol" => protocol, "features" => features}}
      when is_integer(protocol) and is_list(features) ->
        cond do
          not Enum.all?(features, &is_binary/1) -> {:error, :invalid_handshake}
          protocol == 1 and Enum.all?(required_features, &(&1 in features)) -> :ok
          true -> {:error, :incompatible_shim}
        end

      _ ->
        {:error, :invalid_handshake}
    end
  end

  @doc false
  def execution_options?(opts), do: Enum.any?(@execution_options, &Keyword.has_key?(opts, &1))

  @doc false
  def validate_execution_options!(opts, strict_env \\ false) do
    if Keyword.get(opts, :env_mode, :inherit) not in [:inherit, :replace],
      do: raise(ArgumentError, "env_mode must be :inherit or :replace")

    validate_env!(opts, strict_env or execution_options?(opts))
    validate_interrupt!(opts)
  end

  defp validate_env!(opts, strict) do
    env = Keyword.get(opts, :env)

    if strict and Keyword.has_key?(opts, :env) do
      unless is_list(env) and Enum.all?(env, &valid_strict_env?/1),
        do:
          raise(
            ArgumentError,
            "env must be a list of {name, value} entries; names must be nonempty strings without = or NUL; values must be strings without NUL, nil, or false"
          )
    else
      unless valid_legacy_env?(env),
        do:
          raise(
            ArgumentError,
            "env must be nil, a map, or a list of pairs with string, nil, or false values"
          )
    end
  end

  defp valid_legacy_env?(nil), do: true
  defp valid_legacy_env?(env) when is_map(env), do: valid_legacy_env?(Map.to_list(env))
  defp valid_legacy_env?(env) when is_list(env), do: Enum.all?(env, &valid_env_value?/1)
  defp valid_legacy_env?(_env), do: false

  defp valid_env_value?({_name, value}), do: is_binary(value) or value in [nil, false]
  defp valid_env_value?(_entry), do: false

  defp valid_strict_env?({name, value}) when is_binary(name) do
    name != "" and not String.contains?(name, ["=", <<0>>]) and
      (value in [nil, false] or (is_binary(value) and not String.contains?(value, <<0>>)))
  end

  defp valid_strict_env?(_entry), do: false

  defp validate_interrupt!(opts) do
    case {opts[:interrupt_signal], opts[:interrupt_grace_ms]} do
      {nil, nil} ->
        if Keyword.has_key?(opts, :interrupt_signal) or
             Keyword.has_key?(opts, :interrupt_grace_ms),
           do: raise(ArgumentError, "interrupt_signal :int requires interrupt_grace_ms")

      {:int, grace} when is_integer(grace) and grace > 0 and grace <= 4_294_967_295 ->
        :ok

      _ ->
        raise ArgumentError,
              "interrupt_signal must be :int with positive interrupt_grace_ms <= 4294967295"
    end
  end

  # From a KILL frame or its own timeout the shim spends at most the optional
  # INT grace, the TERM grace, a 500 ms SIGKILL probe and a 1 s output drain
  # before EXIT. With an active cgroup, finish_cgroup also waits up to the kill
  # grace plus 500 ms for the subtree to drain. The budget covers that drain
  # whenever containment was requested, so a stalled cgroup is reported from
  # the shim's unconfirmed EXIT rather than replaced by this side's backstop.
  @doc false
  def cleanup_timeout(opts) do
    grace = Keyword.get(opts, :kill_grace_ms, 5_000)

    grace + Keyword.get(opts, :interrupt_grace_ms, 0) + cgroup_drain_ms(opts, grace) +
      @cleanup_margin_ms
  end

  defp cgroup_drain_ms(opts, grace) do
    if Keyword.get(opts, :cgroup, false) in [true, :required],
      do: grace + @cgroup_drain_probe_ms,
      else: 0
  end

  @doc false
  def shutdown_timeout(opts) do
    budget = cleanup_timeout(opts) + @guard_close_timeout_ms + @supervisor_margin_ms
    if budget > 4_294_967_295, do: :infinity, else: budget
  end

  @doc false
  def validate_spawn_identity!(opts) do
    unless Keyword.get(opts, :spawn_identity, false) in [true, false],
      do: raise(ArgumentError, "spawn_identity must be a boolean")
  end

  @doc false
  def validate_structured_spawn_errors!(opts) do
    unless Keyword.get(opts, :structured_spawn_errors, false) in [true, false],
      do: raise(ArgumentError, "structured_spawn_errors must be a boolean")
  end

  @doc false
  def validate_spawn_observer!(opts) do
    case Keyword.get(opts, :spawn_observer) do
      nil -> :ok
      {pid, ref} when is_pid(pid) and node(pid) == node() and is_reference(ref) -> :ok
      _ -> raise ArgumentError, "spawn_observer must be {local_pid, reference} or nil"
    end
  end

  @doc false
  def reject_spawn_observer!(opts, mode) do
    if Keyword.has_key?(opts, :spawn_observer),
      do: raise(ArgumentError, "#{mode} does not support :spawn_observer")
  end

  @doc false
  def identity_requested?(opts), do: Keyword.get(opts, :spawn_identity, false)

  @doc false
  def features(opts, required \\ []) do
    selected =
      for {option, feature} <- [
            spawn_identity: "spawn_identity_v1",
            structured_spawn_errors: "structured_spawn_errors_v1"
          ],
          Keyword.get(opts, option, false),
          do: feature

    required = if execution_options?(opts), do: ["bounded_run_v1" | required], else: required
    required = if env_unsets?(opts[:env]), do: ["env_unset_v1" | required], else: required
    Enum.uniq(required ++ selected)
  end

  defp env_unsets?(nil), do: false
  defp env_unsets?(env), do: Enum.any?(env, fn {_name, value} -> value in [nil, false] end)

  @doc false
  def negotiated?(opts), do: features(opts) != []

  @doc false
  def open_with_features(opts, timeout_ms \\ 5_000) do
    case features(opts) do
      [] -> open(opts)
      features -> open_negotiated(opts, timeout_ms, features)
    end
  end

  defp open_negotiated(opts, timeout_ms, features) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    with {:ok, port} <- open(opts) do
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      with :ok <- negotiate(port, remaining, features),
           true <- System.monotonic_time(:millisecond) < deadline do
        {:ok, port}
      else
        false ->
          close(port)
          {:error, :handshake_timeout}

        {:error, _reason} = error ->
          close(port)
          error
      end
    end
  end

  @doc """
  Decodes a native process identity without creating atoms from wire data.

  Linux uses `/proc/<pid>/stat` field 22 (`:ticks`, in clock ticks since
  boot) paired with the kernel `:boot_id`. macOS uses `kp_proc.p_starttime`
  as Unix `:seconds` and `:microseconds`. Preserve every field when recording
  an identity; these values guard against PID reuse, and are not evidence
  that the process is still alive. The same payload shape is reserved for
  future tracked process-group identities.
  """
  @spec decode_identity(binary()) :: {:ok, identity()} | {:error, :invalid_identity}
  def decode_identity(payload) do
    with {:ok, %{"pid" => pid, "pgid" => pgid, "start_time" => start_time}} <-
           decode_json(payload),
         true <- is_integer(pid) and pid > 0 and is_integer(pgid) and pgid > 0,
         {:ok, start_time} <- decode_start_time(start_time) do
      {:ok, %{pid: pid, pgid: pgid, start_time: start_time}}
    else
      _ -> {:error, :invalid_identity}
    end
  end

  defp decode_start_time(%{"kind" => "linux", "boot_id" => boot_id, "ticks" => ticks})
       when is_binary(boot_id) and byte_size(boot_id) > 0 and is_integer(ticks) and ticks >= 0,
       do: {:ok, %{kind: :linux, boot_id: boot_id, ticks: ticks}}

  defp decode_start_time(%{
         "kind" => "macos",
         "seconds" => seconds,
         "microseconds" => microseconds
       })
       when is_integer(seconds) and seconds >= 0 and is_integer(microseconds) and
              microseconds >= 0 and microseconds < 1_000_000,
       do: {:ok, %{kind: :macos, seconds: seconds, microseconds: microseconds}}

  defp decode_start_time(_), do: {:error, :invalid_identity}

  @doc false
  def reject_run_options!(opts, mode, supported \\ []) do
    for key <- [:idle_timeout_ms, :max_output_bytes],
        key not in supported,
        Keyword.has_key?(opts, key) do
      raise ArgumentError, "#{mode} does not support :#{key}; use Forcola.run/2"
    end
  end

  @doc false
  def decode_json(payload) do
    {:ok, :json.decode(payload)}
  rescue
    _ -> {:error, :invalid_json}
  end

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

  Explicit `:env_mode` and interrupt options require `bounded_run_v1` in
  every mode. `:env_mode` encodes inheritance or replacement; the paired
  `:interrupt_signal`/`:interrupt_grace_ms` requests cooperative INT before
  TERM/KILL, including on owner EOF. Environment values of `nil` or `false`
  encode as JSON `null` and require `env_unset_v1` before SPAWN. Each public
  execution mode validates these options before opening a shim.

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
  the field is present and reads eagerly otherwise. Duplex pull and bounded
  Stream modes also supply `:stderr_window_bytes` and `:strict_output` so both
  pumps stay gated until output is consumed or explicitly discarded.
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
    |> maybe_put("env_mode", encode_atom(Keyword.get(opts, :env_mode)))
    |> maybe_put("interrupt_signal", encode_atom(Keyword.get(opts, :interrupt_signal)))
    |> maybe_put("interrupt_grace_ms", Keyword.get(opts, :interrupt_grace_ms))
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

  defp encode_atom(nil), do: nil
  defp encode_atom(value), do: Atom.to_string(value)

  defp encode_env(nil), do: nil

  defp encode_env(env) do
    Map.new(env, fn {name, value} -> {name, if(value in [nil, false], do: :null, else: value)} end)
  end

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

  @doc """
  Decodes an ERROR frame, retaining the legacy reason string by default.

  When structured errors were negotiated, native spawn details become a
  `Forcola.SpawnError` containing the original string in `:message`.
  Plain protocol errors remain strings. Malformed structured details fall
  back to the valid legacy reason. An unreadable frame or an invalid
  top-level reason returns `:invalid_error` without guessing a reason.
  """
  @spec decode_error(binary(), boolean()) :: String.t() | Forcola.SpawnError.t() | :invalid_error
  def decode_error(payload, structured? \\ false) do
    case decode_json(payload) do
      {:ok, %{"reason" => reason} = report} when is_binary(reason) ->
        if structured? and Map.has_key?(report, "spawn_error"),
          do: decode_spawn_error(report["spawn_error"], reason),
          else: reason

      _ ->
        :invalid_error
    end
  end

  defp decode_spawn_error(report, reason) do
    case Forcola.SpawnError.from_report(report, reason) do
      :invalid_error -> reason
      error -> error
    end
  end
end
