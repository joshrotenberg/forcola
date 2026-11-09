defmodule Forcola.Tracking do
  @moduledoc """
  Checks and stops process groups recorded earlier, without a live session.

  A run started with `track_descendants: true` reports the process groups it
  tracked (`Forcola.Duplex.Terminal`'s `:tracked_groups`, the `:tracked`
  observer events), and `spawn_identity: true` reports the root's identity.
  Each is a `t:Forcola.Shim.identity/0`: `%{pid, pgid, start_time}`. A caller
  can persist these and later, from the same BEAM or a new one, ask whether
  the groups are gone (`check/2`) or stop them (`stop/2`). Both run a fresh
  `forcola_shim` invocation that holds only the identities it is given.

  ## What "gone" means

  Each identity names a process-group leader (`pid == pgid`). A group is
  `:present` while its recorded leader is alive with the recorded start time,
  or, once the leader has exited, while any non-zombie process remains in
  the recorded process group. Otherwise it is `:gone`:

    * the leader pid now belongs to a process with another start time. A pid
      is not reused while a process group with that id exists, so the
      recorded group no longer exists.
    * the identity is from an earlier boot (Linux boot id, or a macOS start
      time before the current boot).
    * no live process remains in the group.

  A group that cannot be observed is `{:unknown, reason}`: for example
  `:foreign_identity` for a start time recorded on another platform,
  `:not_a_group_leader` when `pid != pgid`, or a POSIX error such as
  `:eperm` when the operating system refuses a read.

  ## Remaining races

    * Reads and signals are not atomic, as in live tracking. A group can exit
      and its pid be reused between the identity read and `kill`.
    * If every member of a leaderless group exits and a new, unrelated group
      later receives the same pgid (its leader having exited too) before
      `check/2` runs, the new group is indistinguishable from the recorded
      one. This needs a long gap and heavy pid churn.
    * Only the recorded groups are covered. A descendant that moved to a
      group that was never recorded is outside the result, so the reported
      scope is `:process_group`.
    * On Linux, processes hidden by `/proc` `hidepid` are not seen as members.
  """

  alias Forcola.{Shim, SpawnError}

  @limit 4_096
  @max_ms 4_294_967_295
  @confirm_window_ms 500
  @margin_ms 5_000
  @cleanup_triggers Map.new(
                      ~w(child_exit kill timeout peer_disconnected transport_closed tracker_uncertain)a,
                      &{Atom.to_string(&1), &1}
                    )
  @uncertain_reasons Map.new(
                       ~w(
                         late_descendant scan_invalid root_identity_lost process_limit
                         scan_attempts_exceeded reap_failed group_limit
                         final_scan_incomplete identity_read_failed invalid_pid signal_failed
                         cleanup_shortened cleanup_deadline tracked_channel_closed
                         tracked_encode_failed child_wait_failed cgroup_unconfirmed
                         root_not_reaped cleanup_incomplete
                       )a,
                       &{Atom.to_string(&1), &1}
                     )
  @known_reasons %{
    "foreign_identity" => :foreign_identity,
    "not_a_group_leader" => :not_a_group_leader,
    "invalid_identity" => :invalid_identity
  }

  @typedoc "The observed state of one recorded group."
  @type group_state :: :gone | :present | {:unknown, atom() | String.t()}

  @typedoc "Reasons `check/2` and `stop/2` can fail as a whole."
  @type error ::
          :not_found
          | {:invalid_shim_path, atom()}
          | {:shim_start_failed, term()}
          | {:shim_exited, term()}
          | {:invalid_request, String.t()}
          | :invalid_reply
          | :timeout

  @doc """
  Reports whether each recorded group is gone, without sending any signal.

  Returns `{:ok, states}` with one `t:group_state/0` per identity, in order.

  ## Options

    * `:timeout_ms` - how long to wait for the shim's answer, a non-negative
      integer. Defaults to 5000.
    * `:shim_path` - trusted absolute shim path, as in `Forcola.run/2`.

  Raises `ArgumentError` if `identities` is not a list of at most 4096
  identities in the `t:Forcola.Shim.identity/0` shape.
  """
  @spec check([Shim.identity()], keyword()) :: {:ok, [group_state()]} | {:error, error()}
  def check(identities, opts \\ []) do
    groups = encode_identities!(identities)
    timeout_ms = Keyword.get(opts, :timeout_ms, @margin_ms)

    unless is_integer(timeout_ms) and timeout_ms >= 0,
      do: raise(ArgumentError, ":timeout_ms must be a non-negative integer")

    with {:ok, %{states: states}} <-
           request(%{"op" => "check", "groups" => groups}, length(groups), opts, timeout_ms) do
      {:ok, states}
    end
  end

  @doc """
  Stops every recorded group that is still present, then reports each one.

  Every present group receives the optional interrupt signal and its grace,
  then SIGTERM and `:kill_grace_ms`, then SIGKILL followed by a 500 ms
  confirmation window. A stage ends early once no group is present. Each
  signal is preceded by a fresh identity read: a verified leader receives a
  group signal, members of a leaderless group receive individual signals
  after their own reads, and a group whose leader pid now belongs to another
  process is never signalled. A group seen gone stays gone for the rest of
  the call.

  Returns `{:ok, %{groups: states, confirmation: confirmation, scope:
  :process_group}}`. `confirmation` is `:confirmed` only when every group
  ends `:gone`; any `:present` or `{:unknown, _}` group leaves it
  `:unconfirmed`.

  ## Options

    * `:interrupt_signal` and `:interrupt_grace_ms` - as in `Forcola.run/2`:
      `:int` with a positive grace, or both absent.
    * `:kill_grace_ms` - wait after SIGTERM before SIGKILL, an integer in
      `0..4_294_967_295`. Defaults to 5000.
    * `:shim_path` - trusted absolute shim path, as in `Forcola.run/2`.

  The call waits for the whole sequence plus a five-second margin before
  returning `{:error, :timeout}`. Invalid identities or options raise
  `ArgumentError` before the shim runs.
  """
  @spec stop([Shim.identity()], keyword()) ::
          {:ok,
           %{
             groups: [group_state()],
             confirmation: :confirmed | :unconfirmed,
             scope: :process_group
           }}
          | {:error, error()}
  def stop(identities, opts \\ []) do
    groups = encode_identities!(identities)
    Shim.validate_interrupt!(opts)
    kill_grace_ms = Keyword.get(opts, :kill_grace_ms, 5_000)

    unless is_integer(kill_grace_ms) and kill_grace_ms in 0..@max_ms,
      do: raise(ArgumentError, ":kill_grace_ms must be an integer in 0..4294967295")

    payload =
      case opts[:interrupt_signal] do
        :int ->
          %{"interrupt_signal" => "int", "interrupt_grace_ms" => opts[:interrupt_grace_ms]}

        nil ->
          %{}
      end
      |> Map.merge(%{"op" => "stop", "groups" => groups, "kill_grace_ms" => kill_grace_ms})

    timeout_ms =
      Keyword.get(opts, :interrupt_grace_ms, 0) + kill_grace_ms + @confirm_window_ms +
        @margin_ms

    with {:ok, %{states: states, confirmed: confirmed}} <-
           request(payload, length(groups), opts, timeout_ms) do
      {:ok,
       %{
         groups: states,
         confirmation: if(confirmed, do: :confirmed, else: :unconfirmed),
         scope: :process_group
       }}
    end
  end

  defp encode_identities!(identities) when is_list(identities) do
    if length(identities) > @limit,
      do: raise(ArgumentError, "at most #{@limit} identities are accepted")

    Enum.map(identities, &encode_identity!/1)
  end

  defp encode_identities!(_identities),
    do: raise(ArgumentError, "identities must be a list of process identities")

  defp encode_identity!(%{pid: pid, pgid: pgid, start_time: start_time})
       when is_integer(pid) and pid > 0 and is_integer(pgid) and pgid > 0 do
    %{"pid" => pid, "pgid" => pgid, "start_time" => encode_start_time!(start_time)}
  end

  defp encode_identity!(identity),
    do: raise(ArgumentError, "invalid process identity: #{inspect(identity)}")

  defp encode_start_time!(%{kind: :linux, boot_id: boot_id, ticks: ticks})
       when is_binary(boot_id) and byte_size(boot_id) > 0 and is_integer(ticks) and ticks >= 0,
       do: %{"kind" => "linux", "boot_id" => boot_id, "ticks" => ticks}

  defp encode_start_time!(%{kind: :macos, seconds: seconds, microseconds: microseconds})
       when is_integer(seconds) and seconds >= 0 and is_integer(microseconds) and
              microseconds >= 0 and microseconds < 1_000_000,
       do: %{"kind" => "macos", "seconds" => seconds, "microseconds" => microseconds}

  defp encode_start_time!(start_time),
    do: raise(ArgumentError, "invalid process start time: #{inspect(start_time)}")

  # The exchange runs in its own process so the port, its exit signal and any
  # late messages never reach the caller. The result travels as the exit reason.
  defp request(payload, count, opts, timeout_ms) do
    with {:ok, bin} <- Shim.path(opts) do
      {worker, monitor} =
        spawn_monitor(fn -> exit({:forcola_groups, exchange(bin, payload, count)}) end)

      receive do
        {:DOWN, ^monitor, :process, ^worker, {:forcola_groups, result}} -> result
        {:DOWN, ^monitor, :process, ^worker, reason} -> {:error, {:shim_start_failed, reason}}
      after
        timeout_ms ->
          Process.exit(worker, :kill)

          receive do
            {:DOWN, ^monitor, :process, ^worker, _reason} -> :ok
          end

          {:error, :timeout}
      end
    end
  end

  defp exchange(bin, payload, count) do
    port =
      Port.open({:spawn_executable, bin}, [
        :binary,
        :exit_status,
        :use_stdio,
        :hide,
        {:line, 65_536},
        args: ["groups"]
      ])

    Port.command(port, [:json.encode(payload), "\n"])
    await_line(port, "", count)
  catch
    :error, reason -> {:error, {:shim_start_failed, reason}}
  end

  defp await_line(port, partial, count) do
    receive do
      {^port, {:data, {:noeol, chunk}}} ->
        await_line(port, partial <> chunk, count)

      {^port, {:data, {:eol, chunk}}} ->
        line = partial <> chunk

        receive do
          {^port, {:exit_status, status}} -> decode_reply(status, line, count)
        end

      {^port, {:exit_status, status}} ->
        {:error, {:shim_exited, status}}
    end
  end

  defp decode_reply(0, line, count) do
    with {:ok, %{"groups" => groups, "confirmed" => confirmed}}
         when is_list(groups) and length(groups) == count and is_boolean(confirmed) <-
           Shim.decode_json(line),
         states when is_list(states) <- decode_states(groups) do
      {:ok, %{states: states, confirmed: confirmed}}
    else
      _ -> {:error, :invalid_reply}
    end
  end

  defp decode_reply(2, line, _count) do
    case Shim.decode_json(line) do
      {:ok, %{"error" => reason}} when is_binary(reason) -> {:error, {:invalid_request, reason}}
      _ -> {:error, :invalid_reply}
    end
  end

  defp decode_reply(status, _line, _count), do: {:error, {:shim_exited, status}}

  defp decode_states(groups) do
    Enum.reduce_while(groups, [], fn group, acc ->
      case decode_state(group) do
        {:ok, state} -> {:cont, [state | acc]}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      states -> Enum.reverse(states)
    end
  end

  defp decode_state(%{"state" => "gone"}), do: {:ok, :gone}
  defp decode_state(%{"state" => "present"}), do: {:ok, :present}

  defp decode_state(%{"state" => "unknown", "reason" => reason})
       when is_binary(reason) and byte_size(reason) > 0,
       do:
         {:ok,
          {:unknown, Map.get_lazy(@known_reasons, reason, fn -> SpawnError.reason(reason) end)}}

  defp decode_state(_group), do: :error

  defstruct enabled: false,
            observer: nil,
            groups: [],
            seen: MapSet.new(),
            truncated: false,
            finished: false

  @doc false
  def validate_opts!(opts) do
    unless Keyword.get(opts, :track_descendants, false) in [true, false],
      do: raise(ArgumentError, "track_descendants must be a boolean")

    case opts[:track_observer] do
      nil ->
        opts

      {pid, ref} when is_pid(pid) and node(pid) == node() and is_reference(ref) ->
        Keyword.put(opts, :track_descendants, true)

      _ ->
        raise ArgumentError, "track_observer must be {local_pid, reference} or nil"
    end
  end

  @doc false
  def new(opts),
    do: %__MODULE__{
      enabled: Keyword.get(opts, :track_descendants, false),
      observer: opts[:track_observer]
    }

  @doc false
  def groups(tracking), do: Enum.reverse(tracking.groups)

  @doc false
  def record(%{enabled: false} = tracking, _payload), do: {tracking, nil}

  def record(tracking, payload) do
    case Shim.decode_identity(payload) do
      {:ok, %{pid: pid, pgid: pid} = identity} -> record_identity(tracking, identity)
      _ -> {%{tracking | truncated: true}, nil}
    end
  end

  defp record_identity(tracking, identity) do
    cond do
      MapSet.member?(tracking.seen, identity) ->
        {tracking, nil}

      MapSet.size(tracking.seen) >= @limit ->
        {%{tracking | truncated: true}, nil}

      true ->
        {%{
           tracking
           | groups: [identity | tracking.groups],
             seen: MapSet.put(tracking.seen, identity)
         }, identity}
    end
  end

  @doc false
  def notify_record(_tracking, nil), do: :ok
  def notify_record(tracking, identity), do: notify(tracking, {:tracked, identity})

  @doc false
  def report(%{enabled: false}, payload) do
    report = Shim.decode_exit_report(payload)

    scope =
      cond do
        not report.valid -> :unknown
        report.contained == true -> :active_cgroup
        true -> :process_group
      end

    Map.put(report, :scope, scope)
  end

  def report(tracking, payload) do
    case Shim.decode_json(payload) do
      {:ok, report} when is_map(report) -> report_map(tracking, report)
      _ -> report_map(tracking, %{})
    end
  end

  @doc false
  def report_map(tracking, report) do
    status = raw_status(report)
    scope = scope(report["scope"])

    valid_flags =
      Enum.all?(
        ["confirmed", "timed_out", "contained", "output_truncated", "tracked_groups_truncated"],
        fn key ->
          not Map.has_key?(report, key) or is_boolean(report[key])
        end
      )

    # A tracked report is well formed only with a child status, boolean flags
    # and a known tracking scope. Nothing else may confirm cleanup.
    valid = status != nil and valid_flags and scope != :unknown
    confirmed = valid and report["confirmed"] == true

    %{
      valid: valid,
      status: status,
      confirmed: confirmed,
      scope: scope,
      timed_out: report["timed_out"] == true,
      contained: contained(report["contained"]),
      tracked_groups_truncated:
        tracking.truncated or tracking.groups == [] or report["tracked_groups_truncated"] == true,
      output_truncated: report["output_truncated"] == true,
      cleanup_trigger: known(@cleanup_triggers, report["cleanup_trigger"]),
      uncertain_reason:
        if(confirmed, do: nil, else: known(@uncertain_reasons, report["uncertain_reason"])),
      scan_errno: errno(report["scan_errno"])
    }
  end

  # Diagnostics only. An absent or unrecognized value is nil and is never used
  # to decide confirmation.
  defp known(values, value) when is_binary(value), do: Map.get(values, value)
  defp known(_values, _value), do: nil

  defp errno(value) when is_integer(value) and value > 0, do: value
  defp errno(_value), do: nil

  @doc false
  def contained(value) when is_boolean(value), do: value
  def contained(_value), do: :unknown

  defp scope("tree_contained"), do: :tree_contained
  defp scope("tree_sampled"), do: :tree_sampled
  defp scope(_scope), do: :unknown

  defp raw_status(%{"status" => status} = report) when is_integer(status) and status >= 0 do
    if report["signal"] in [nil, :null], do: status
  end

  defp raw_status(%{"signal" => signal} = report) when is_integer(signal) and signal > 0 do
    if report["status"] in [nil, :null], do: {:signal, signal}
  end

  defp raw_status(_report), do: nil

  @doc false
  def status(%{confirmed: true, status: status}), do: status
  def status(_report), do: {:signal, :unconfirmed}

  @doc false
  def confirmation(%{confirmed: true}), do: :confirmed
  def confirmation(_report), do: :unconfirmed

  @doc false
  def finish(
        tracking,
        scope,
        confirmation,
        contained \\ :unknown,
        truncated \\ false,
        diagnostics \\ %{}
      )

  def finish(%{enabled: false} = tracking, _scope, _confirmation, _contained, _truncated, _diag),
    do: tracking

  def finish(%{finished: true} = tracking, _scope, _confirmation, _contained, _truncated, _diag),
    do: tracking

  def finish(tracking, scope, confirmation, contained, truncated, diagnostics) do
    notify(
      tracking,
      {:tracking_exit,
       %{
         scope: scope,
         confirmation: confirmation,
         contained: contained,
         tracked_groups_truncated: tracking.truncated or truncated,
         cleanup_trigger: Map.get(diagnostics, :cleanup_trigger),
         uncertain_reason: Map.get(diagnostics, :uncertain_reason),
         scan_errno: Map.get(diagnostics, :scan_errno)
       }}
    )

    %{tracking | finished: true, truncated: tracking.truncated or truncated}
  end

  @doc false
  def startup_failed(opts), do: opts |> new() |> finish(:unknown, :not_started)

  defp notify(%{observer: nil}, _event), do: :ok
  defp notify(%{observer: {pid, ref}}, event), do: send(pid, {ref, event})
end
