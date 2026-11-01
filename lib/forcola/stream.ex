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

  Environment unsets, `:env_mode`, and the interrupt pair have the same
  validation and capability negotiation as `Forcola.run/2`. `:max_output_bytes`
  remains a one-shot option; passing it raises `ArgumentError` when `lines/2`
  is called, before a shim is opened.

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
      exception downstream) kills the process group and blocks for the shim's
      bounded confirmation. If teardown cannot be confirmed, the halt still
      returns after the backstop and logs a warning.
    * If the consuming process dies, the port closes, the shim sees stdin
      EOF, and the group is killed.

  ## Idle timeout

  `:timeout_ms` bounds the whole run. `:idle_timeout_ms` (optional) bounds
  the gap between output frames instead, detecting a stalled producer
  without capping total runtime. The two are independent and composable:
  whichever bound is reached first fires, and the raised
  `Forcola.Stream.Error` marks which one (`:idle_timed_out` vs
  `:timed_out`).

  The idle deadline resets on any child liveness signal: every STDOUT or
  STDERR data frame resets it, not only newline-terminated lines. A child
  that writes bytes without a newline, or writes only to stderr, still
  counts as alive. On idle expiry the same early-halt kill-and-confirm
  path runs (KILL the group, wait for the shim to confirm death, close the
  port) and `Forcola.Stream.Error` is raised with `idle_timed_out: true`
  after any lines already produced have been emitted. Its `status` is the
  confirmed child status from the shim; `{:signal, :unconfirmed}` is reserved
  for a missing or invalid confirmation. Omitting the option (the default)
  leaves behavior identical to a run bounded only by `:timeout_ms`.

  ## Backpressure

  By default the shim pumps the child's stdout forward as fast as the pipes
  allow; unconsumed output buffers on the BEAM side, so a consumer slower than
  the producer can grow BEAM memory without bound. `:window_bytes` (or
  `backpressure: true`) opts into a demand-driven mode that bounds it.

  With backpressure the shim reads the child's stdout only while it holds read
  credit. The Stream grants one window of credit as its consumer pulls lines;
  when the window is exhausted the shim stops reading, the OS pipe fills, and
  the child's next write blocks. That block is the backpressure: the producer
  runs no faster than the consumer drains it. The buffering bound is roughly
  `window_bytes` of stdout in flight, `max_stderr_bytes + 1` bytes of stderr,
  a `max_line_bytes` partial line, and protocol frames. The OS pipes can each
  hold additional data outside BEAM memory. Stderr gets a finite read budget
  even with `merge_stderr: true`; the extra byte detects overflow. A line or
  stderr limit kills the process group and raises `Forcola.Stream.Error` with
  typed `:limit` and `output_truncated: true` evidence. The shim keeps both
  pumps gated after child exit until output is consumed or cancelled.

  Interaction with `:idle_timeout_ms`: a gap between frames can be caused by
  the consumer not yet granting credit rather than a stalled producer. A
  consumer-driven stall does not trip the idle timeout: the idle clock is reset
  each time the Stream grants credit, so time spent waiting on the consumer is
  excluded. A genuinely hung producer (credit available, no output) still trips
  it, and the whole-run `:timeout_ms` still bounds a consumer that never
  consumes.
  """

  alias Forcola.{Shim, Tracking}

  require Logger

  defmodule Error do
    @moduledoc """
    Raised when a streamed run does not end with a clean zero exit.

    Fields:

      * `:status` - exit code, `{:signal, n}` for death by signal, or
        `{:signal, :unconfirmed}` when the shim never confirmed death
      * `:timed_out` - whether the run hit the whole-run `:timeout_ms`
      * `:idle_timed_out` - whether the run hit `:idle_timeout_ms` (no
        output arrived within the idle interval); mutually exclusive with
        `:timed_out`
      * `:stderr` - stderr captured before termination (empty when
        `merge_stderr: true` routed it into the line stream)
      * `:reason` - spawn failure reason, `nil` for a run that started.
        Native failures are `Forcola.SpawnError` values when
        `structured_spawn_errors: true`, otherwise legacy strings.
      * `:limit` - `{:line, :stdout | :merged, n}` or `{:total, :stderr, n}`
        when bounded output exceeded its configured limit
      * `:output_truncated` - whether output was deliberately discarded or
        the shim reported an incomplete drain
      * `:tracked_groups` - observed cleanup-group identities, root first,
        including observations consumed during cancellation; defaults to `[]`
      * `:tracked_groups_truncated` - whether observations are missing,
        malformed, or dropped by a history/notification bound. Execution and
        native cleanup confirmation are independent of this history fact.
      * `:contained` - the native cgroup-placement flag, or `:unknown` if
        unavailable, independently of the tree cleanup scope
      * `:scope` / `:confirmation` - the reported cleanup mechanism and its
        confirmation. Tracking uses explicit `:tree_contained` or
        `:tree_sampled`; missing or unknown scope is `:unknown` and cannot
        confirm cleanup. See `Forcola.Result` for scope limitations.
    """

    defexception [
      :status,
      :reason,
      :limit,
      stderr: "",
      timed_out: false,
      idle_timed_out: false,
      output_truncated: false,
      tracked_groups: [],
      tracked_groups_truncated: false,
      contained: :unknown,
      scope: :unknown,
      confirmation: :unknown
    ]

    @type t :: %__MODULE__{
            status: non_neg_integer() | {:signal, atom() | non_neg_integer()} | nil,
            reason: Forcola.SpawnError.t() | String.t() | atom() | tuple() | nil,
            limit: {:line | :total, atom(), pos_integer()} | nil,
            stderr: binary(),
            timed_out: boolean(),
            idle_timed_out: boolean(),
            output_truncated: boolean(),
            tracked_groups: [Forcola.Shim.identity()],
            tracked_groups_truncated: boolean(),
            contained: boolean() | :unknown,
            scope: :process_group | :active_cgroup | :tree_contained | :tree_sampled | :unknown,
            confirmation:
              :confirmed | :unconfirmed | :unknown | :not_started | :timeout | :transport_lost
          }

    @impl true
    def message(%__MODULE__{reason: %Forcola.SpawnError{} = reason}) do
      "stream spawn failed: #{Exception.message(reason)}"
    end

    def message(%__MODULE__{reason: reason}) when not is_nil(reason) do
      "stream spawn failed: #{inspect(reason)}"
    end

    def message(%__MODULE__{limit: limit}) when not is_nil(limit) do
      "stream output limit exceeded: #{inspect(limit)}; process group killed"
    end

    def message(%__MODULE__{
          status: status,
          timed_out: timed_out,
          idle_timed_out: idle_timed_out,
          stderr: stderr
        }) do
      base =
        cond do
          idle_timed_out ->
            "stream idle timed out; process group killed (#{format_status(status)})"

          timed_out ->
            "stream timed out; process group killed (#{format_status(status)})"

          true ->
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

  # The shim owns the timeout and signal sequence. The Elixir backstop
  # includes both grace intervals and time for its confirmation.
  @default_kill_grace_ms 5_000

  # Default read window when backpressure is enabled with `backpressure: true`
  # rather than an explicit `window_bytes`.
  @default_window_bytes 64 * 1024
  @default_max_line_bytes 64 * 1024
  @default_max_stderr_bytes 1024 * 1024

  @doc """
  Run `argv` and return its stdout as a lazy stream of lines.

  Takes the common execution options from `Forcola.run/2`, including the trusted
  `:shim_path` override and `:user`/`:group`
  for running the child as a different user (POSIX-only, fail-closed,
  privileged shim required); `:timeout_ms` is required and bounds the
  whole run, not the gap between lines.
  `:max_output_bytes` raises `ArgumentError` before opening a shim.

  Environment values must be binary strings, `nil`, or `false`. Both `nil`
  and `false` remove a variable and require `env_unset_v1` negotiation.
  `:env_mode` accepts `:inherit` (default) or `:replace`, clearing inherited
  variables before applying `:env` in replacement mode. The optional interrupt
  pair accepts only `interrupt_signal: :int` with a positive integer
  `:interrupt_grace_ms` at most `4_294_967_295`. It sends SIGINT before the
  TERM/KILL sequence on timeout, early halt, output limits, or owner/BEAM EOF.
  Cleanup waits include the interrupt grace and kill grace.

  Explicit `:env_mode` or interrupt options require `bounded_run_v1` and
  strict environment entries: a list of binary names without `=` or NUL,
  with binary values without NUL or `nil`/`false` unsets. All option validation
  runs when `lines/2` is called. Negotiation runs when enumeration starts,
  counts toward `:timeout_ms`, and composes with the other capabilities.
  Incompatible shims fail before SPAWN without changing stream delivery.

  The child's stdin is closed immediately after spawn, as in `Forcola.run/2`:
  this mode has no way to write to it, so a child that reads until EOF sees
  EOF right away instead of blocking. Use `Forcola.Duplex` for interactive
  stdin.

  `:structured_spawn_errors` (default `false`) negotiates native
  `Forcola.SpawnError` reasons in `Forcola.Stream.Error.reason`, retaining
  the original string in the struct's `:message`. It requires
  `structured_spawn_errors_v1` and counts negotiation toward `:timeout_ms`,
  which must be an integer in `0..4_294_967_295`. Incompatible shims fail
  before SPAWN. This option works independently of identity observation
  and keeps the existing stream output and exception envelopes.

  `:track_descendants` (default `false`) selects `track_descendants_v1` for
  lifetime tracking and cleanup of descendant process groups. It requires no
  observer and preserves the line-only stream. Linux reports `:tree_contained`
  through subreaper adoption; macOS reports `:tree_sampled`. A descendant
  reparented before its first macOS sample is outside the sampled guarantee.
  Histories contain the root group first and at most 4,096 group identities.
  Missing, malformed or overflowing observations mark history truncated without
  changing native cleanup confirmation. Unknown scope or malformed EXIT cannot
  confirm cleanup. Tracking composes with the other capabilities and counts
  negotiation toward the whole-run timeout.

  Optional `:track_observer` accepts `{local_pid, reference}` and enables
  tracking even with `track_descendants: false`. It receives
  `{reference, {:tracked, identity}}` for each newly observed group and one
  `{reference, {:tracking_exit, evidence}}`
  when enumeration can report its completion, cancellation, or failure.
  The observer is the terminal evidence surface for successful or halted
  streams; exceptions also retain `:tracked_groups`, `:scope`, `:contained`,
  `:tracked_groups_truncated`, and `:confirmation`. See `Forcola.run/2` for
  confirmation values and `Forcola.Result` for identity fields. A consumer paused between pulls delays
  observer messages until frames are consumed; native tracking and cleanup
  continue. Tracking frames do not reset output idle deadlines or use output
  credit. Brutal consumer or VM death can prevent a final observer message.

  `:spawn_observer` accepts `{local_pid, reference}` and sends
  `{reference, {:spawned, identity}}` before the first output line is yielded.
  The identity is `%{pid: integer, pgid: integer, start_time: map}`; see
  `Forcola.Result` for the exact Linux and macOS start-time fields.
  The observer enables `spawn_identity_v1` negotiation even if
  `spawn_identity: false` is supplied. Setting `spawn_identity: true` without
  a non-nil observer raises `ArgumentError` before opening a shim, because
  a stream has no other identity result or query API. The whole-run timeout
  includes the capability exchange; incompatible shims fail before SPAWN.
  The default performs no identity negotiation or notification. Messages are
  unacknowledged, and a dead observer does not affect execution.

  `:idle_timeout_ms` (optional, milliseconds; default `nil` = disabled)
  bounds the gap between output frames: if no STDOUT or STDERR data
  arrives within the interval the producer is treated as stalled, the
  process group is killed, and `Forcola.Stream.Error` is raised with
  `idle_timed_out: true`. It is independent of and composable with
  `:timeout_ms`; whichever bound fires first wins. See the module docs for
  the exact reset semantic.

  `:window_bytes` (optional, positive integer) opts into demand-driven
  backpressure: the shim reads the child's stdout only while it holds read
  credit, and the Stream grants roughly one window as its consumer pulls
  lines, so a slow consumer blocks the producer instead of buffering its
  output on the BEAM. `backpressure: true` is shorthand for a default
  #{@default_window_bytes}-byte window. Omitting both keeps the default eager
  pump. See the module docs for the buffering bound and the interaction with
  `:idle_timeout_ms`.

  With backpressure enabled, `:max_line_bytes` defaults to
  #{@default_max_line_bytes} and `:max_stderr_bytes` to
  #{@default_max_stderr_bytes}; both accept a positive integer. The first
  bounds any unterminated line (including merged stderr), and the second
  bounds total stderr bytes. Exceeding either limit raises `Error` with
  `:limit` and `output_truncated: true` after group cleanup. These options
  require backpressure; without it, the legacy eager behavior is unchanged.

  Lines are emitted without their trailing newline. A partial line held
  across frame boundaries is emitted once its newline arrives; a final
  partial line with no newline is emitted before the stream terminates.

  See the module docs for how termination is surfaced.
  """
  @spec lines([String.t(), ...], keyword()) :: Enumerable.t()
  def lines([_binary | _] = argv, opts) do
    opts = Tracking.validate_opts!(opts)
    Shim.validate_spawn_identity!(opts)
    Shim.validate_structured_spawn_errors!(opts)
    Shim.validate_execution_options!(opts)
    Shim.validate_spawn_observer!(opts)
    validate_identity_observer!(opts)
    opts = if opts[:spawn_observer], do: Keyword.put(opts, :spawn_identity, true), else: opts
    Shim.reject_run_options!(opts, __MODULE__, [:idle_timeout_ms])
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    validate_negotiated_timeout!(opts, timeout_ms)
    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, @default_kill_grace_ms)
    idle_timeout_ms = Keyword.get(opts, :idle_timeout_ms)
    window_bytes = resolve_window_bytes(opts)
    {max_line_bytes, max_stderr_bytes} = resolve_bounds(opts, window_bytes)

    Stream.resource(
      fn ->
        start(
          argv,
          opts,
          timeout_ms,
          kill_grace_ms,
          idle_timeout_ms,
          window_bytes,
          max_line_bytes,
          max_stderr_bytes
        )
      end,
      &next/1,
      &cleanup/1
    )
  end

  defp validate_identity_observer!(opts) do
    if Shim.identity_requested?(opts) and is_nil(opts[:spawn_observer]),
      do: raise(ArgumentError, "Forcola.Stream :spawn_identity requires :spawn_observer")
  end

  defp validate_negotiated_timeout!(opts, timeout_ms) do
    if Shim.negotiated?(opts) and
         not (is_integer(timeout_ms) and timeout_ms >= 0 and timeout_ms <= 4_294_967_295),
       do:
         raise(
           ArgumentError,
           "timeout_ms must be an integer in 0..4294967295 with negotiated options"
         )
  end

  # Backpressure is opt-in. `:window_bytes` sets the read budget in bytes and
  # enables it; `backpressure: true` enables it with a default window. Absent,
  # the eager pump runs and behavior is byte-for-byte unchanged.
  defp resolve_window_bytes(opts) do
    case Keyword.get(opts, :window_bytes) do
      nil ->
        if Keyword.get(opts, :backpressure, false), do: @default_window_bytes, else: nil

      n when is_integer(n) and n > 0 ->
        n

      other ->
        raise ArgumentError,
              ":window_bytes must be a positive integer, got: #{inspect(other)}"
    end
  end

  defp resolve_bounds(opts, nil) do
    if Keyword.has_key?(opts, :max_line_bytes) or Keyword.has_key?(opts, :max_stderr_bytes) do
      raise ArgumentError, ":max_line_bytes and :max_stderr_bytes require backpressure"
    end

    {nil, nil}
  end

  defp resolve_bounds(opts, _window_bytes) do
    {positive_bound(opts, :max_line_bytes, @default_max_line_bytes),
     positive_bound(opts, :max_stderr_bytes, @default_max_stderr_bytes)}
  end

  defp positive_bound(opts, key, default) do
    case Keyword.get(opts, key, default) do
      n when is_integer(n) and n > 0 ->
        n

      other ->
        raise ArgumentError, "#{inspect(key)} must be a positive integer, got: #{inspect(other)}"
    end
  end

  defp start(
         argv,
         opts,
         timeout_ms,
         kill_grace_ms,
         idle_timeout_ms,
         window_bytes,
         max_line_bytes,
         max_stderr_bytes
       ) do
    started_at = System.monotonic_time(:millisecond)

    case Shim.open_with_features(opts, timeout_ms) do
      {:ok, port} ->
        timeout_ms = negotiated_timeout(port, opts, timeout_ms, started_at)

        spawn_opts =
          opts
          |> Keyword.put(:timeout_ms, timeout_ms)
          |> Keyword.put(:kill_grace_ms, kill_grace_ms)
          |> put_window_bytes(window_bytes)
          |> put_bounded_stderr(max_stderr_bytes)

        payload = Shim.encode_spawn(argv, spawn_opts)
        Shim.send_frame(port, Shim.tag_spawn(), payload)

        # lines/2 has no way to write to the child's stdin, so an open pipe
        # with no writer only serves to hang a child that reads until EOF.
        # Close it right away; Forcola.Duplex is the mode for interactive
        # stdin.
        Shim.send_frame(port, Shim.tag_eof(), "")

        if max_stderr_bytes do
          Shim.send_frame(
            port,
            Shim.tag_stderr_credit(),
            Shim.encode_credit(max_stderr_bytes + 1)
          )
        end

        now = System.monotonic_time(:millisecond)
        cleanup_timeout_ms = Shim.cleanup_timeout(opts)
        deadline = now + timeout_ms + cleanup_timeout_ms

        %{
          port: port,
          identity_enabled: Shim.identity_requested?(opts),
          structured_spawn_errors: Keyword.get(opts, :structured_spawn_errors, false),
          identity: nil,
          tracking: Tracking.new(opts),
          spawn_observer: opts[:spawn_observer],
          buffer: "",
          stderr: [],
          stderr_bytes: 0,
          max_line_bytes: max_line_bytes,
          max_stderr_bytes: max_stderr_bytes,
          merge_stderr: Keyword.get(opts, :merge_stderr, false),
          deadline: deadline,
          idle_timeout_ms: idle_timeout_ms,
          idle_deadline: idle_deadline(idle_timeout_ms, now),
          cleanup_timeout_ms: cleanup_timeout_ms,
          exit: nil,
          # Backpressure state. `window` nil = disabled (eager). `outstanding`
          # is the read budget granted to the shim but not yet accounted for by
          # received STDOUT bytes; the shim never holds more than one window.
          window: window_bytes,
          outstanding: 0
        }

      {:error, :not_found} ->
        Tracking.startup_failed(opts)
        raise Error, reason: :shim_not_found, confirmation: :not_started

      {:error, reason} ->
        Tracking.startup_failed(opts)
        raise Error, reason: reason, confirmation: :not_started
    end
  end

  defp negotiated_timeout(port, opts, timeout_ms, started_at) do
    if Shim.negotiated?(opts) do
      remaining = timeout_ms - (System.monotonic_time(:millisecond) - started_at)

      if remaining <= 0 do
        Tracking.startup_failed(opts)
        Shim.close(port)
        raise Error, reason: :handshake_timeout, confirmation: :not_started
      end

      remaining
    else
      timeout_ms
    end
  end

  defp put_window_bytes(opts, nil), do: opts
  defp put_window_bytes(opts, window_bytes), do: Keyword.put(opts, :window_bytes, window_bytes)

  defp put_bounded_stderr(opts, nil), do: opts

  defp put_bounded_stderr(opts, max_stderr_bytes) do
    opts
    |> Keyword.put(:stderr_window_bytes, max_stderr_bytes + 1)
    |> Keyword.put(:strict_output, true)
    # Preserve each pipe's identity for separate accounting. Stream merges
    # the received frames into the same line buffer below when requested.
    |> Keyword.put(:merge_stderr, false)
  end

  # nil idle_timeout_ms leaves the idle deadline nil (disabled). Otherwise
  # the deadline is now + interval and is reset on every liveness frame.
  defp idle_deadline(nil, _now), do: nil
  defp idle_deadline(idle_timeout_ms, now), do: now + idle_timeout_ms

  # Terminal conditions never raise directly out of the receive: they set
  # `:exit` in the state and raise on the next call, so cleanup/1 always
  # sees a state that says whether the shim already accounted for the
  # child (and can skip the KILL/confirm handshake when it has).
  defp next(%{exit: nil} = state) do
    # Under backpressure, top the shim's read budget back up to one window
    # before waiting for the next frame. This is the demand signal: it runs
    # only when the consumer pulls, so a paused consumer stops granting credit
    # and the producer blocks. It also resets the idle deadline (see
    # grant_credit) so a consumer-driven pause is excluded from the idle clock.
    state |> grant_credit() |> receive_next()
  end

  defp next(%{exit: :ok} = state), do: {:halt, state}
  defp next(%{exit: {:error, error}}), do: raise(error)

  defp receive_next(%{port: port} = state) do
    now = System.monotonic_time(:millisecond)
    run_remaining = state.deadline - now

    # With idle disabled the wait is just the whole-run backstop. With it
    # enabled the wait is the sooner of the two, and on expiry we attribute
    # to whichever deadline actually elapsed.
    wait =
      case state.idle_deadline do
        nil -> run_remaining
        idle_deadline -> min(run_remaining, idle_deadline - now)
      end

    receive do
      {^port, {:data, <<tag, payload::binary>>}} ->
        handle_frame(tag, payload, state)

      {^port, {:exit_status, _status}} ->
        # The shim exited without sending EXIT/ERROR (e.g. it crashed).
        state = finish_tracking(state, :unknown, :transport_lost)

        error =
          tracking_error(state, status: {:signal, :unconfirmed}, confirmation: :transport_lost)

        {[], %{state | exit: {:error, error}}}
    after
      min(max(wait, 0), 4_294_967_295) ->
        if wait > 4_294_967_295 do
          receive_next(state)
        else
          {error, state} = timeout_error(state)
          {[], %{state | exit: {:error, error}}}
        end
    end
  end

  # Backpressure disabled: nothing to grant, idle handling unchanged.
  defp grant_credit(%{window: nil} = state), do: state

  # Backpressure enabled: top the shim's read budget back up to one window,
  # then reset the idle deadline.
  #
  # The window bound: `outstanding` is bytes granted minus STDOUT bytes
  # received, so topping it to `window` means the shim is told to hold at most
  # one window of read-ahead. The buffering bound is roughly one window in
  # flight plus one frame in transit.
  #
  # The idle reset: a consumer pause happens BETWEEN next/1 calls (the reducer
  # sits without pulling), so the gap since the previous call is
  # consumer-driven, not a stalled producer. Resetting the idle deadline here
  # excludes that gap from the idle clock. A producer that is funded but truly
  # silent still trips the idle bound: this resets once, then the following
  # receive waits a full interval with credit already granted, and the idle
  # deadline elapses with no frame.
  defp grant_credit(%{window: window, outstanding: outstanding, port: port} = state) do
    if outstanding < window do
      send_credit(port, window - outstanding)
    end

    %{state | outstanding: window, idle_deadline: reset_idle_deadline(state)}
  end

  # Best-effort credit grant. The shim keeps its stdin open until the BEAM
  # closes the port (see the backpressure linger in supervisor.rs), so a grant
  # never races a closed pipe into an epipe. The badarg guard is defensive: if
  # the port is already closed there is nothing left to credit.
  defp send_credit(port, bytes) do
    Shim.send_frame(port, Shim.tag_credit(), Shim.encode_credit(bytes))
  catch
    :error, :badarg -> :ok
  end

  defp reset_idle_deadline(%{idle_timeout_ms: nil}), do: nil

  defp reset_idle_deadline(%{idle_timeout_ms: idle_timeout_ms}) do
    System.monotonic_time(:millisecond) + idle_timeout_ms
  end

  # Attribute an expired wait. When the idle deadline is the one that
  # elapsed it is an idle timeout: the shim is not timing out on its own
  # (the whole-run bound has not been reached), so we must actively KILL
  # the group and wait for the shim to confirm death before the port is
  # closed in cleanup/1. The whole-run backstop keeps its old behavior:
  # the shim already hit its own timeout plus grace, so closing the port
  # (stdin EOF) in cleanup/1 is the remaining kill lever.
  defp timeout_error(%{idle_deadline: idle_deadline, deadline: deadline} = state)
       when not is_nil(idle_deadline) and idle_deadline <= deadline do
    {result, state} = kill_and_confirm(state)
    {evidence, state} = cleanup_evidence(result, state)
    error = tracking_error(state, Map.to_list(evidence) ++ [idle_timed_out: true])
    {error, state}
  end

  defp timeout_error(state) do
    state = finish_tracking(state, :unknown, :timeout)

    error =
      tracking_error(state,
        status: {:signal, :unconfirmed},
        timed_out: true,
        confirmation: :timeout
      )

    {error, state}
  end

  # Preserve identities discovered during cancellation alongside its report.
  defp kill_and_confirm(%{port: port, cleanup_timeout_ms: cleanup_timeout_ms} = state) do
    Shim.send_frame(port, Shim.tag_kill())
    await_exit(state, cleanup_timeout_ms)
  catch
    :error, :badarg -> {:shim_exited, state}
  end

  defp finish_tracking(state, scope, confirmation, contained \\ :unknown, truncated \\ false),
    do: %{
      state
      | tracking: Tracking.finish(state.tracking, scope, confirmation, contained, truncated)
    }

  defp tracking_error(state, fields) do
    struct!(
      Error,
      [
        stderr: stderr(state),
        tracked_groups: Tracking.groups(state.tracking),
        tracked_groups_truncated: state.tracking.truncated
      ] ++ fields
    )
  end

  defp cleanup_evidence({:exit, report}, state) do
    confirmation = Tracking.confirmation(report)

    evidence = %{
      status: Tracking.status(report),
      scope: report.scope,
      confirmation: confirmation,
      contained: report.contained,
      tracked_groups_truncated: report.tracked_groups_truncated
    }

    {evidence,
     finish_tracking(
       state,
       report.scope,
       confirmation,
       report.contained,
       report.tracked_groups_truncated
     )}
  end

  defp cleanup_evidence(result, state) do
    confirmation =
      case result do
        :timeout -> :timeout
        :error -> :not_started
        :shim_exited -> :transport_lost
        _ -> :unconfirmed
      end

    evidence = %{
      status: {:signal, :unconfirmed},
      scope: :unknown,
      confirmation: confirmation,
      contained: :unknown
    }

    {evidence, finish_tracking(state, :unknown, confirmation)}
  end

  defp handle_frame(tag, payload, state) do
    cond do
      tag == Shim.tag_stdout() ->
        state
        |> reset_idle()
        |> account_stdout(byte_size(payload))
        |> consume_lines(payload)

      tag == Shim.tag_stderr() ->
        consume_stderr(reset_idle(state), payload)

      tag == Shim.tag_spawned() ->
        {[], record_identity(state, payload)}

      tag == Shim.tag_tracked() ->
        # Control frames do not finish a demand cycle or reset its idle clock.
        receive_next(record_tracked(state, payload))

      tag == Shim.tag_exit() ->
        handle_exit(payload, state)

      tag == Shim.tag_error() ->
        state = finish_tracking(state, :unknown, :not_started)

        error =
          tracking_error(state,
            reason: Shim.decode_error(payload, state.structured_spawn_errors),
            confirmation: :not_started
          )

        {[], %{state | exit: {:error, error}}}

      true ->
        {[], state}
    end
  end

  defp record_tracked(state, payload) do
    {tracking, identity} = Tracking.record(state.tracking, payload)
    state = %{state | tracking: tracking}
    Tracking.notify_record(tracking, identity)
    state
  end

  defp record_identity(%{identity_enabled: true, identity: nil} = state, payload) do
    case Shim.decode_identity(payload) do
      {:ok, identity} ->
        if state.spawn_observer do
          {pid, ref} = state.spawn_observer
          send(pid, {ref, {:spawned, identity}})
        end

        %{state | identity: identity}

      {:error, _reason} ->
        state
    end
  end

  defp record_identity(state, _payload), do: state

  defp consume_stderr(%{max_stderr_bytes: nil} = state, payload) do
    {[], %{state | stderr: [state.stderr, payload]}}
  end

  defp consume_stderr(state, payload) do
    remaining = state.max_stderr_bytes - state.stderr_bytes
    kept = binary_part(payload, 0, min(byte_size(payload), remaining))
    exceeded = byte_size(payload) > remaining
    state = %{state | stderr_bytes: state.stderr_bytes + byte_size(kept)}

    {lines, state} =
      if state.merge_stderr do
        consume_lines(state, kept)
      else
        {[], %{state | stderr: [state.stderr, kept]}}
      end

    if exceeded and state.exit == nil do
      {lines, stop_for_limit(state, {:total, :stderr, state.max_stderr_bytes})}
    else
      {lines, state}
    end
  end

  defp consume_lines(%{max_line_bytes: nil} = state, payload) do
    {lines, rest} = split_lines(state.buffer <> payload)
    {lines, %{state | buffer: rest}}
  end

  defp consume_lines(state, payload) do
    {lines, rest} = split_lines(state.buffer <> payload)
    max_line = state.max_line_bytes
    overlong = Enum.find_index(lines, &(byte_size(&1) > max_line))

    cond do
      not is_nil(overlong) ->
        limit = {:line, line_source(state), max_line}
        {Enum.take(lines, overlong), stop_for_limit(state, limit)}

      byte_size(rest) > max_line ->
        {lines, stop_for_limit(state, {:line, line_source(state), max_line})}

      true ->
        {lines, %{state | buffer: rest}}
    end
  end

  defp line_source(%{merge_stderr: true}), do: :merged
  defp line_source(_state), do: :stdout

  defp stop_for_limit(state, limit) do
    {result, state} = kill_and_confirm(state)
    {evidence, state} = cleanup_evidence(result, state)
    error = tracking_error(state, Map.to_list(evidence) ++ [limit: limit, output_truncated: true])
    %{state | buffer: "", exit: {:error, error}}
  end

  defp handle_exit(payload, state) do
    report = Tracking.report(state.tracking, payload)
    status = Tracking.status(report)
    confirmation = Tracking.confirmation(report)

    state =
      finish_tracking(
        state,
        report.scope,
        confirmation,
        report.contained,
        report.tracked_groups_truncated
      )

    final = if state.buffer == "", do: [], else: [state.buffer]

    outcome =
      if status == 0 and not report.timed_out and not report.output_truncated do
        :ok
      else
        {:error,
         tracking_error(state,
           status: status,
           timed_out: report.timed_out,
           output_truncated: report.output_truncated,
           scope: report.scope,
           contained: report.contained,
           tracked_groups_truncated: report.tracked_groups_truncated,
           confirmation: confirmation
         )}
      end

    {final, %{state | buffer: "", exit: outcome}}
  end

  # A liveness frame (any STDOUT or STDERR data) pushes the idle deadline
  # forward by the full interval. A no-op when idle timeout is disabled.
  defp reset_idle(%{idle_timeout_ms: nil} = state), do: state

  defp reset_idle(%{idle_timeout_ms: idle_timeout_ms} = state) do
    %{state | idle_deadline: System.monotonic_time(:millisecond) + idle_timeout_ms}
  end

  # Charge received STDOUT bytes against the outstanding read budget so the
  # next grant tops back up to exactly one window. A no-op when backpressure
  # is disabled. STDERR bytes are not charged: the shim never gates stderr.
  defp account_stdout(%{window: nil} = state, _bytes), do: state

  defp account_stdout(%{outstanding: outstanding} = state, bytes) do
    %{state | outstanding: max(outstanding - bytes, 0)}
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
  defp cleanup(%{port: port, exit: nil} = state) do
    {result, state} = kill_and_confirm(state)
    cleanup_evidence(result, state)
    warn_unconfirmed_cleanup(result)
    close_port(port)
  end

  defp cleanup(%{port: port}) do
    close_port(port)
  end

  defp await_exit(state, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_exit(state, deadline)
  end

  defp do_await_exit(%{port: port} = state, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:timeout, state}
    else
      receive do
        {^port, {:data, <<tag, payload::binary>>}} ->
          cond do
            tag == Shim.tag_exit() -> {decode_terminal_exit(state, payload), state}
            tag == Shim.tag_error() -> {:error, state}
            tag == Shim.tag_spawned() -> do_await_exit(record_identity(state, payload), deadline)
            tag == Shim.tag_tracked() -> do_await_exit(record_tracked(state, payload), deadline)
            true -> do_await_exit(state, deadline)
          end

        {^port, {:exit_status, _status}} ->
          {:shim_exited, state}
      after
        min(remaining, 4_294_967_295) -> do_await_exit(state, deadline)
      end
    end
  end

  defp decode_terminal_exit(state, payload) do
    {:exit, Tracking.report(state.tracking, payload)}
  rescue
    _invalid_payload -> :malformed_exit
  catch
    _kind, _reason -> :malformed_exit
  end

  defp warn_unconfirmed_cleanup({:exit, %{confirmed: false}}) do
    Logger.warning("forcola: stream cleanup could not confirm process-group death")
  end

  defp warn_unconfirmed_cleanup(:timeout) do
    Logger.warning("forcola: stream cleanup timed out before shim confirmation")
  end

  defp warn_unconfirmed_cleanup(:malformed_exit) do
    Logger.warning("forcola: stream cleanup received a malformed EXIT confirmation")
  end

  defp warn_unconfirmed_cleanup(:shim_exited) do
    Logger.warning("forcola: shim exited before stream cleanup confirmation")
  end

  defp warn_unconfirmed_cleanup(_confirmed), do: :ok

  defp close_port(port), do: Shim.close(port)
end
