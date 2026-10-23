defmodule Forcola.Run do
  @moduledoc false
  # One-shot collection only. The native supervisor remains the sole process
  # owner; this module requests cancellation and records what it reports.
  require Logger
  alias Forcola.{Result, Shim}

  @window 8_192
  @observer_messages 1_024
  @margin 5_000
  @strict_options [
    :idle_timeout_ms,
    :max_output_bytes,
    :env_mode,
    :interrupt_signal,
    :interrupt_grace_ms
  ]

  def run(argv, opts) do
    validate!(opts)
    strict = strict?(opts)
    deadline = if strict, do: now() + Keyword.fetch!(opts, :timeout_ms)

    case Shim.open(opts) do
      {:ok, port} ->
        outcome = collect_open_port(port, argv, opts, strict, deadline)
        put_guard_close(outcome, Shim.close(port))

      {:error, :not_found} ->
        {:error, {:spawn, :shim_not_found}}

      {:error, reason} ->
        {:error, {:spawn, reason}}
    end
  end

  defp collect_open_port(port, argv, opts, strict, deadline) do
    execute_checked(port, argv, opts, strict, deadline)
  catch
    kind, reason ->
      Shim.close(port)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp execute_checked(port, argv, opts, false, nil), do: execute(port, argv, opts, false, nil)

  defp execute_checked(port, argv, opts, true, deadline) do
    with :ok <- Shim.negotiate(port, max(deadline - now(), 0)),
         true <- now() < deadline do
      execute(port, argv, opts, true, deadline)
    else
      false -> {:error, {:spawn, :handshake_timeout}}
      {:error, reason} -> {:error, {:spawn, reason}}
    end
  end

  defp execute(port, argv, opts, strict, deadline) do
    grace = Keyword.get(opts, :kill_grace_ms, 5_000)
    interrupt_grace = Keyword.get(opts, :interrupt_grace_ms, 0)
    now = now()
    timeout_at = deadline || now + Keyword.fetch!(opts, :timeout_ms)
    idle = opts[:idle_timeout_ms]
    spawn_opts = Keyword.put(opts, :kill_grace_ms, grace)

    # Merge in the collector so credit can be returned to the correct pipe.
    # On the wire both pumps stay independent, each with a finite window.
    spawn_opts =
      if strict do
        Keyword.merge(spawn_opts,
          window_bytes: @window,
          stderr_window_bytes: @window,
          strict_output: true,
          merge_stderr: false,
          timeout_ms: max(timeout_at - now(), 0)
        )
      else
        spawn_opts
      end

    Shim.send_frame(port, Shim.tag_spawn(), Shim.encode_spawn(argv, spawn_opts))
    Shim.send_frame(port, Shim.tag_eof())

    if strict do
      grant(port, :stdout, @window)
      grant(port, :stderr, @window)
    end

    acc = %{
      stdout: [],
      stderr: [],
      bytes: 0,
      strict: strict,
      partial_errors: strict,
      merge: Keyword.get(opts, :merge_stderr, false),
      observer: opts[:output_observer],
      max: opts[:max_output_bytes],
      idle: idle,
      idle_at: if(idle, do: now + idle),
      timeout_at: timeout_at,
      backstop: timeout_at + grace + interrupt_grace + @margin,
      cleanup_ms: grace + interrupt_grace + @margin,
      reason: nil,
      truncated: false
    }

    collect(port, acc)
  end

  defp collect(port, acc) do
    acc = check_deadlines(port, acc)
    deadline = next_deadline(acc)
    remaining = min(max(deadline - now(), 0), 4_294_967_295)

    # Deadlines are checked before receive, so queued output cannot starve a
    # whole-run or idle timeout. Cancellation never extends the backstop.
    if now() >= acc.backstop do
      failure(acc, :backstop, acc.reason || :timeout)
    else
      receive do
        {^port, {:data, <<tag, payload::binary>>}} ->
          handle_frame(port, tag, payload, acc)

        {^port, {:exit_status, status}} ->
          res = result(acc, {:signal, :unconfirmed}, evidence(acc, {:transport, status}))

          if acc.reason,
            do: {:error, {acc.reason, res}},
            else: {:error, {:spawn, {:shim_exited, res}}}
      after
        remaining -> collect(port, acc)
      end
    end
  end

  defp check_deadlines(port, %{strict: true, reason: nil} = acc) do
    cond do
      now() >= acc.timeout_at -> cancel(port, acc, :timeout)
      acc.idle_at != nil and now() >= acc.idle_at -> cancel(port, acc, :idle_timeout)
      true -> acc
    end
  end

  defp check_deadlines(_port, acc), do: acc

  defp next_deadline(%{strict: true, reason: nil} = acc) do
    Enum.min(Enum.reject([acc.timeout_at, acc.idle_at, acc.backstop], &is_nil/1))
  end

  defp next_deadline(acc), do: acc.backstop

  defp handle_frame(port, tag, payload, acc) do
    cond do
      tag == Shim.tag_stdout() -> output(port, :stdout, payload, acc)
      tag == Shim.tag_stderr() -> output(port, :stderr, payload, acc)
      tag == Shim.tag_exit() -> finish(payload, acc)
      tag == Shim.tag_error() -> spawn_error(payload, acc)
      true -> collect(port, acc)
    end
  end

  defp output(port, channel, payload, %{reason: nil} = acc) do
    size = byte_size(payload)
    kept = if acc.max, do: min(size, max(acc.max - acc.bytes, 0)), else: size
    bytes = binary_part(payload, 0, kept)
    destination = if acc.merge, do: :stdout, else: channel
    # Copy a bounded prefix: a sub-binary must not retain the discarded frame.
    bytes = :binary.copy(bytes)
    acc = Map.update!(acc, destination, &[&1, bytes])
    acc = %{acc | bytes: acc.bytes + kept, idle_at: if(acc.idle, do: now() + acc.idle)}
    overflow = notify(acc.observer, destination, bytes, acc.strict)

    acc =
      cond do
        kept < size -> cancel(port, %{acc | truncated: true}, :max_output_bytes)
        overflow -> cancel(port, %{acc | truncated: true}, :observer_overflow)
        true -> acc
      end

    if acc.strict and acc.reason == nil, do: grant(port, channel, size)
    collect(port, acc)
  end

  defp output(port, _channel, _payload, acc), do: collect(port, %{acc | truncated: true})

  defp cancel(port, acc, reason) do
    Shim.send_frame(port, Shim.tag_kill())
    %{acc | reason: reason, backstop: min(acc.backstop, now() + acc.cleanup_ms)}
  end

  defp grant(port, channel, bytes) do
    tag = if channel == :stdout, do: Shim.tag_credit(), else: Shim.tag_stderr_credit()
    Shim.send_frame(port, tag, Shim.encode_credit(bytes))
  end

  defp notify(nil, _channel, _bytes, _strict), do: false
  defp notify(_observer, _channel, "", _strict), do: false

  defp notify({pid, ref}, channel, bytes, strict) do
    if strict and
         match?(
           {:message_queue_len, n} when n >= @observer_messages,
           Process.info(pid, :message_queue_len)
         ) do
      true
    else
      send(pid, {ref, {channel, bytes}})
      false
    end
  end

  defp finish(payload, acc) do
    case Shim.decode_json(payload) do
      {:ok, report} when is_map(report) -> finish_report(report, acc)
      _ -> malformed(acc)
    end
  end

  defp finish_report(report, acc) do
    if valid_exit?(report) do
      ev = report_evidence(report, acc)
      raw = raw_status(report)
      status = project_status(raw, report["confirmed"], acc.strict)

      if ev.scope == :active_cgroup,
        do: Logger.debug("forcola: run completed under Linux cgroup v2 containment")

      res = result(acc, status, ev)
      if ev.reason, do: {:error, {ev.reason, res}}, else: {:ok, res}
    else
      malformed(acc, report)
    end
  end

  defp valid_exit?(report) do
    valid_flags =
      Enum.all?(
        ["confirmed", "contained", "timed_out", "output_truncated"],
        &(not Map.has_key?(report, &1) or is_boolean(report[&1]))
      )

    raw_status(report) != nil and valid_flags
  end

  defp raw_status(%{"status" => status} = report) when is_integer(status) and status >= 0 do
    if is_nil(report["signal"]), do: status
  end

  defp raw_status(%{"signal" => signal} = report) when is_integer(signal) and signal > 0 do
    if is_nil(report["status"]), do: {:signal, signal}
  end

  defp raw_status(_report), do: nil

  defp project_status(raw, true, _strict), do: raw
  defp project_status(raw, nil, false), do: raw
  defp project_status(_raw, _confirmed, _strict), do: {:signal, :unconfirmed}

  defp report_evidence(report, acc) do
    %{
      evidence(acc, :exit)
      | child_status: report["status"],
        child_signal: report["signal"],
        cleanup: confirmation(report["confirmed"]),
        scope: scope(report["contained"]),
        reason: acc.reason || timeout_reason(report["timed_out"]),
        output_truncated: truncation(acc.truncated, report["output_truncated"])
    }
  end

  defp confirmation(true), do: :confirmed
  defp confirmation(false), do: :unconfirmed
  defp confirmation(_), do: :unknown
  defp scope(true), do: :active_cgroup
  defp scope(false), do: :process_group
  defp scope(_), do: :unknown
  defp timeout_reason(true), do: :timeout
  defp timeout_reason(_), do: nil
  defp truncation(true, _), do: true
  defp truncation(_, true), do: true
  defp truncation(_, false), do: false
  defp truncation(_, _), do: :unknown

  defp malformed(acc, report \\ %{}) do
    ev = evidence(acc, :malformed_exit)
    # Retain a syntactically valid raw child observation even if other fields
    # are malformed. It cannot make the cleanup evidence positive.
    ev =
      case raw_status(report) do
        {:signal, signal} -> %{ev | child_signal: signal}
        status when is_integer(status) -> %{ev | child_status: status}
        _ -> ev
      end

    res = result(acc, {:signal, :unconfirmed}, ev)

    if acc.reason,
      do: {:error, {acc.reason, res}},
      else: {:error, {:spawn, {:invalid_exit, res}}}
  end

  defp spawn_error(payload, acc) do
    reason =
      case Shim.decode_json(payload) do
        {:ok, %{"reason" => reason}} when is_binary(reason) -> reason
        _ -> :invalid_error
      end

    if acc.partial_errors or acc.reason != nil do
      res = result(acc, {:signal, :unconfirmed}, evidence(acc, {:spawn, reason}))

      if acc.reason,
        do: {:error, {acc.reason, res}},
        else: {:error, {:spawn, {reason, res}}}
    else
      {:error, {:spawn, reason}}
    end
  end

  defp failure(acc, termination, reason) do
    ev = %{evidence(acc, termination) | reason: reason}
    {:error, {reason, result(acc, {:signal, :unconfirmed}, ev)}}
  end

  defp evidence(acc, termination) do
    %{
      child_status: nil,
      child_signal: nil,
      termination: termination,
      reason: acc.reason,
      output_truncated: if(acc.truncated, do: true, else: :unknown),
      cleanup: :unknown,
      scope: :unknown,
      guard_close: nil
    }
  end

  defp result(acc, status, evidence) do
    %Result{
      status: status,
      stdout: IO.iodata_to_binary(acc.stdout),
      stderr: IO.iodata_to_binary(acc.stderr),
      evidence: evidence
    }
  end

  defp put_guard_close({:ok, %Result{} = result}, close),
    do: {:ok, %{result | evidence: %{result.evidence | guard_close: close}}}

  defp put_guard_close({:error, {reason, %Result{} = result}}, close),
    do: {:error, {reason, %{result | evidence: %{result.evidence | guard_close: close}}}}

  defp put_guard_close({:error, {:spawn, {reason, %Result{} = result}}}, close),
    do:
      {:error, {:spawn, {reason, %{result | evidence: %{result.evidence | guard_close: close}}}}}

  defp put_guard_close(outcome, _close), do: outcome

  defp validate!(opts) do
    validate_observer!(opts[:output_observer])

    for key <- [:idle_timeout_ms, :max_output_bytes] do
      if Keyword.has_key?(opts, key), do: positive!(key, opts[key])
    end

    validate_env!(opts)
    validate_interrupt!(opts)
    timeout = Keyword.fetch!(opts, :timeout_ms)

    if strict?(opts) and
         not (is_integer(timeout) and timeout >= 0 and timeout <= 4_294_967_295),
       do:
         raise(
           ArgumentError,
           "timeout_ms must be an integer in 0..4294967295 with strict options"
         )
  end

  defp validate_observer!(nil), do: :ok

  defp validate_observer!({pid, ref})
       when is_pid(pid) and node(pid) == node() and is_reference(ref), do: :ok

  defp validate_observer!(_),
    do: raise(ArgumentError, "output_observer must be {local_pid, reference} or nil")

  defp validate_env!(opts) do
    if Keyword.get(opts, :env_mode, :inherit) not in [:inherit, :replace],
      do: raise(ArgumentError, "env_mode must be :inherit or :replace")

    if strict?(opts) and Keyword.has_key?(opts, :env), do: validate_env_entries!(opts[:env])
  end

  defp validate_env_entries!(env) do
    unless is_list(env) and Enum.all?(env, &valid_env?/1),
      do:
        raise(
          ArgumentError,
          "env must be a list of {name, value} strings without NUL; names must be nonempty and contain no ="
        )
  end

  defp validate_interrupt!(opts) do
    case {opts[:interrupt_signal], opts[:interrupt_grace_ms]} do
      {nil, nil} ->
        if Keyword.has_key?(opts, :interrupt_signal) or
             Keyword.has_key?(opts, :interrupt_grace_ms),
           do: raise(ArgumentError, "interrupt_signal :int requires interrupt_grace_ms")

      {:int, grace} ->
        positive!(:interrupt_grace_ms, grace)

      _ ->
        raise ArgumentError, "interrupt_signal must be :int with positive interrupt_grace_ms"
    end
  end

  defp positive!(key, value) do
    unless is_integer(value) and value > 0 and value <= 4_294_967_295,
      do: raise(ArgumentError, "#{key} must be a positive integer <= 4294967295")
  end

  defp valid_env?({name, value}) when is_binary(name) and is_binary(value),
    do:
      name != "" and not String.contains?(name, ["=", <<0>>]) and
        not String.contains?(value, <<0>>)

  defp valid_env?(_), do: false
  defp strict?(opts), do: Enum.any?(@strict_options, &Keyword.has_key?(opts, &1))
  defp now, do: System.monotonic_time(:millisecond)
end
