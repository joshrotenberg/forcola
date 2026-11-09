defmodule Forcola.Run do
  @moduledoc false
  # One-shot collection only. The native supervisor remains the sole process
  # owner; this module requests cancellation and records what it reports.
  require Logger
  alias Forcola.{Result, Shim, Tracking}

  @window 8_192
  @observer_messages 1_024
  @strict_options [
    :idle_timeout_ms,
    :max_output_bytes,
    :env_mode,
    :interrupt_signal,
    :interrupt_grace_ms
  ]

  def run(argv, opts) do
    opts = Tracking.validate_opts!(opts)
    validate!(opts)
    strict = strict?(opts)

    deadline =
      if strict or Shim.negotiated?(opts), do: now() + Keyword.fetch!(opts, :timeout_ms)

    case Shim.open(opts) do
      {:ok, port} ->
        outcome = collect_open_port(port, argv, opts, strict, deadline)
        put_guard_close(outcome, Shim.close(port))

      {:error, :not_found} ->
        startup_error(opts, :shim_not_found)

      {:error, reason} ->
        startup_error(opts, reason)
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

  defp execute_checked(port, argv, opts, strict, deadline) do
    features = Shim.features(opts, if(strict, do: ["bounded_run_v1"], else: []))

    with :ok <- Shim.negotiate(port, max(deadline - now(), 0), features),
         true <- now() < deadline do
      execute(port, argv, opts, strict, deadline)
    else
      false -> startup_error(opts, :handshake_timeout)
      {:error, reason} -> startup_error(opts, reason)
    end
  end

  defp startup_error(opts, reason) do
    Tracking.startup_failed(opts)
    {:error, {:spawn, reason}}
  end

  defp execute(port, argv, opts, strict, deadline) do
    grace = Keyword.get(opts, :kill_grace_ms, 5_000)
    cleanup_ms = Shim.cleanup_timeout(opts)
    now = now()
    timeout_at = deadline || now + Keyword.fetch!(opts, :timeout_ms)
    idle = opts[:idle_timeout_ms]
    spawn_opts = Keyword.put(opts, :kill_grace_ms, grace)

    spawn_opts =
      if deadline,
        do: Keyword.put(spawn_opts, :timeout_ms, max(deadline - now(), 0)),
        else: spawn_opts

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
      backstop: timeout_at + cleanup_ms,
      cleanup_ms: cleanup_ms,
      reason: nil,
      truncated: false,
      identity_enabled: Shim.identity_requested?(opts),
      structured_spawn_errors: Keyword.get(opts, :structured_spawn_errors, false),
      identity: nil,
      tracking: Tracking.new(opts)
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
          Tracking.finish(acc.tracking, :unknown, :transport_lost)
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
      tag == Shim.tag_spawned() -> spawned(port, payload, acc)
      tag == Shim.tag_tracked() -> tracked(port, payload, acc)
      tag == Shim.tag_exit() -> finish(payload, acc)
      tag == Shim.tag_error() -> spawn_error(payload, acc)
      true -> collect(port, acc)
    end
  end

  defp tracked(port, payload, acc) do
    {tracking, identity} = Tracking.record(acc.tracking, payload)
    acc = %{acc | tracking: tracking}
    Tracking.notify_record(tracking, identity)
    collect(port, acc)
  end

  defp spawned(port, payload, %{identity_enabled: true, identity: nil} = acc) do
    case Shim.decode_identity(payload) do
      {:ok, identity} -> collect(port, %{acc | identity: identity})
      {:error, _reason} -> collect(port, acc)
    end
  end

  defp spawned(port, _payload, acc), do: collect(port, acc)

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

      status =
        if acc.tracking.enabled,
          do: project_status(raw, ev.cleanup == :confirmed, true),
          else: project_status(raw, report["confirmed"], acc.strict)

      Tracking.finish(
        acc.tracking,
        ev.scope,
        ev.cleanup,
        ev.contained,
        ev.tracked_groups_truncated,
        ev
      )

      if report["contained"] == true,
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
    tracked = if acc.tracking.enabled, do: Tracking.report_map(acc.tracking, report)

    %{
      evidence(acc, :exit)
      | child_status: report["status"],
        child_signal: report["signal"],
        cleanup:
          if(tracked, do: Tracking.confirmation(tracked), else: confirmation(report["confirmed"])),
        scope: if(tracked, do: tracked.scope, else: scope(report["contained"])),
        contained: Tracking.contained(report["contained"]),
        tracked_groups_truncated: if(tracked, do: tracked.tracked_groups_truncated, else: false),
        cleanup_trigger: tracked && tracked.cleanup_trigger,
        uncertain_reason: tracked && tracked.uncertain_reason,
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

  defp diagnostics(%{tracking: %{enabled: true}} = acc, report) do
    acc.tracking
    |> Tracking.report_map(report)
    |> Map.take([:cleanup_trigger, :uncertain_reason])
  end

  defp diagnostics(_acc, _report), do: %{}

  defp malformed(acc, report \\ %{}) do
    ev = %{
      evidence(acc, :malformed_exit)
      | contained: Tracking.contained(report["contained"]),
        tracked_groups_truncated:
          acc.tracking.enabled and
            (acc.tracking.truncated or report["tracked_groups_truncated"] == true)
    }

    ev = Map.merge(ev, diagnostics(acc, report))

    Tracking.finish(
      acc.tracking,
      :unknown,
      :unconfirmed,
      ev.contained,
      ev.tracked_groups_truncated,
      ev
    )

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
    Tracking.finish(acc.tracking, :unknown, :not_started)
    reason = Shim.decode_error(payload, acc.structured_spawn_errors)

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
    Tracking.finish(acc.tracking, :unknown, :timeout)
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
      contained: :unknown,
      tracked_groups_truncated: acc.tracking.truncated,
      cleanup_trigger: nil,
      uncertain_reason: nil,
      guard_close: nil
    }
  end

  defp result(acc, status, evidence) do
    %Result{
      status: status,
      stdout: IO.iodata_to_binary(acc.stdout),
      stderr: IO.iodata_to_binary(acc.stderr),
      pid: if(acc.identity, do: acc.identity.pid),
      pgid: if(acc.identity, do: acc.identity.pgid),
      start_time: if(acc.identity, do: acc.identity.start_time),
      evidence: evidence,
      tracked_groups: Tracking.groups(acc.tracking),
      tracked_groups_truncated: evidence.tracked_groups_truncated
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
    Shim.reject_spawn_observer!(opts, "Forcola.run/2")
    Shim.validate_spawn_identity!(opts)
    Shim.validate_structured_spawn_errors!(opts)
    validate_observer!(opts[:output_observer])

    for key <- [:idle_timeout_ms, :max_output_bytes] do
      if Keyword.has_key?(opts, key), do: positive!(key, opts[key])
    end

    Shim.validate_execution_options!(opts, strict?(opts))
    timeout = Keyword.fetch!(opts, :timeout_ms)

    unless is_integer(timeout) and timeout >= 0 and timeout <= 4_294_967_295,
      do: raise(ArgumentError, "timeout_ms must be an integer in 0..4294967295")
  end

  defp validate_observer!(nil), do: :ok

  defp validate_observer!({pid, ref})
       when is_pid(pid) and node(pid) == node() and is_reference(ref) do
    :ok
  end

  defp validate_observer!(_),
    do: raise(ArgumentError, "output_observer must be {local_pid, reference} or nil")

  defp positive!(key, value) do
    unless is_integer(value) and value > 0 and value <= 4_294_967_295,
      do: raise(ArgumentError, "#{key} must be a positive integer <= 4294967295")
  end

  defp strict?(opts), do: Enum.any?(@strict_options, &Keyword.has_key?(opts, &1))
  defp now, do: System.monotonic_time(:millisecond)
end
