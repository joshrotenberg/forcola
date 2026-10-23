defmodule Forcola.Result do
  @moduledoc """
  What an OS process did: exit status, captured output, and termination evidence.

  `:status` is the exit code for a normal exit, or `{:signal, number}`
  when the process died from a signal, where `number` is the signal
  number (for example `{:signal, 15}` for the SIGTERM Forcola itself
  sends on timeout). The only atom case is `{:signal, :unconfirmed}`,
  which means the shim could not confirm death before its own deadline;
  treat it as leaked and investigate.

  `:evidence` defaults to `nil` for source-compatible struct construction.
  `Forcola.run/2` populates a map with these independent observations:

    * `:child_status` / `:child_signal` - raw direct-child exit code or signal,
      or `nil` when unavailable. These survive unconfirmed cleanup.
    * `:termination` - `:exit`, `:malformed_exit`, `:backstop`,
      `{:transport, port_status}`, or `{:spawn, reason}`.
    * `:reason` - `nil`, `:timeout`, `:idle_timeout`, `:max_output_bytes`, or
      `:observer_overflow`. Early JSON on stdout does not override this reason.
    * `:output_truncated` - `true`, `false`, or `:unknown` when not reported.
    * `:cleanup` - `:confirmed`, `:unconfirmed`, or `:unknown`. Only an explicit,
      valid native EXIT confirmation yields `:confirmed`.
    * `:scope` - `:process_group`, `:active_cgroup`, or `:unknown`.
      Process-group confirmation says nothing about descendants that escaped
      that group. It never confirms an entire process tree on macOS.
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
          cleanup: :confirmed | :unconfirmed | :unknown,
          scope: :process_group | :active_cgroup | :unknown,
          guard_close: :ok | {:error, :guard_close_timeout}
        }

  @enforce_keys [:status]
  defstruct [:status, stdout: "", stderr: "", evidence: nil]

  @type t :: %__MODULE__{
          status: non_neg_integer() | {:signal, atom() | non_neg_integer()},
          stdout: binary(),
          stderr: binary(),
          evidence: evidence() | nil
        }
end
