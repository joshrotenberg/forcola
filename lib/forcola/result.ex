defmodule Forcola.Result do
  @moduledoc """
  What an OS process did: exit status, captured output, and termination evidence.

  `:status` is the exit code for a normal exit, or `{:signal, number}`
  when the process died from a signal, where `number` is the signal
  number (for example `{:signal, 15}` for the SIGTERM Forcola itself
  sends on timeout). The only atom case is `{:signal, :unconfirmed}`,
  which means the shim could not confirm death before its own deadline;
  treat it as leaked and investigate.

  With `spawn_identity: true`, `:pid`, `:pgid`, and `:start_time` describe
  the successfully spawned child. They are `nil` when identity was not
  requested or no valid identity frame arrived. They remain available in
  partial results after timeout or transport loss. `:pgid` equals `:pid`
  because the shim creates a new session for the child.

  `:start_time` is `%{kind: :linux, boot_id: string, ticks: integer}` on
  Linux, with ticks from `/proc/<pid>/stat` field 22, or
  `%{kind: :macos, seconds: integer, microseconds: integer}` on macOS,
  from `kp_proc.p_starttime`. Preserve the complete map when recording it
  to distinguish a reused PID. An identity is an observation at spawn,
  not proof that the child is still alive.

  With `track_descendants: true`, `:tracked_groups` is the first-observed
  history of cleanup-group leaders, including the root group. Each entry is
  `%{pid: integer, pgid: integer, start_time: map}` with `pid == pgid` and the
  same start-time shape above. Repeated identities are deduplicated; a reused
  PID with a different start time is a distinct entry. The list defaults to
  `[]`, is bounded at 4,096 entries, and survives in partial results. It is
  not a list of processes known to be alive. `:tracked_groups_truncated` is
  true when history is missing, malformed, or exceeds its retention/notification
  bound. Execution continues and cleanup confirmation comes independently from
  the native live-set report. Root identity in this history
  does not populate `:pid`, `:pgid`, or `:start_time` without `:spawn_identity`.

  `:evidence` defaults to `nil` for source-compatible struct construction.
  `Forcola.run/2` populates a map with these independent observations:

    * `:child_status` / `:child_signal` - raw direct-child exit code or signal,
      or `nil` when unavailable. These survive unconfirmed cleanup.
    * `:termination` - `:exit`, `:malformed_exit`, `:backstop`,
      `{:transport, port_status}`, or `{:spawn, reason}`.
    * `:reason` - `nil`, `:timeout`, `:idle_timeout`, `:max_output_bytes`, or
      `:observer_overflow`. Early JSON on stdout does not override this reason.
    * `:output_truncated` - `true`, `false`, or `:unknown` when not reported.
    * `:contained` - `true` or `false` when cgroup placement was reported, or
      `:unknown` when unavailable. Independent of the tree cleanup scope.
    * `:tracked_groups_truncated` - whether the 4,096-entry history limit or
      native observation queue dropped an entry, or observations were missing
      or malformed. Independent of native cleanup confirmation.
    * `:cleanup` - `:confirmed`, `:unconfirmed`, or `:unknown`. Only an explicit,
      valid native EXIT confirmation yields `:confirmed`.
    * `:scope` - `:process_group`, `:active_cgroup`, `:tree_contained`,
      `:tree_sampled`, or `:unknown`. Process-group confirmation says nothing
      about descendants that escaped that group. Tracking uses an explicit
      native tree scope: Linux subreaper adoption is `:tree_contained`, while
      macOS sampling is `:tree_sampled`. A descendant reparented before its
      first macOS sample is outside the sampled guarantee. Tracking never
      infers a tree scope from the older `contained` flag. A valid native EXIT
      with a recognized scope supplies cleanup confirmation independently of
      observation history. Unknown scope or malformed EXIT cannot confirm it.
    * `:guard_close` - `:ok` or `{:error, :guard_close_timeout}`. Closing the
      transport is separate from native cleanup confirmation.

  Without new options, a legacy shim's EXIT missing `confirmed` retains its
  original status projection; its evidence still records unknown cleanup.
  Missing or malformed EXIT never supplies positive cleanup evidence.
  """

  @type evidence :: %{
          child_status: non_neg_integer() | nil,
          child_signal: pos_integer() | nil,
          termination:
            :exit | :malformed_exit | :backstop | {:transport, term()} | {:spawn, term()},
          reason: nil | :timeout | :idle_timeout | :max_output_bytes | :observer_overflow,
          output_truncated: boolean() | :unknown,
          contained: boolean() | :unknown,
          tracked_groups_truncated: boolean(),
          cleanup: :confirmed | :unconfirmed | :unknown,
          scope: :process_group | :active_cgroup | :tree_contained | :tree_sampled | :unknown,
          guard_close: :ok | {:error, :guard_close_timeout}
        }

  @enforce_keys [:status]
  defstruct [
    :status,
    :pid,
    :pgid,
    :start_time,
    stdout: "",
    stderr: "",
    evidence: nil,
    tracked_groups: [],
    tracked_groups_truncated: false
  ]

  @type t :: %__MODULE__{
          status: non_neg_integer() | {:signal, atom() | non_neg_integer()},
          pid: pos_integer() | nil,
          pgid: pos_integer() | nil,
          start_time: Forcola.Shim.start_time() | nil,
          tracked_groups: [Forcola.Shim.identity()],
          tracked_groups_truncated: boolean(),
          stdout: binary(),
          stderr: binary(),
          evidence: evidence() | nil
        }
end
