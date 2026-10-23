defmodule Forcola do
  @moduledoc """
  Leak-free external process execution.

  Every command runs under a small Rust shim (a port program, not a NIF)
  that places the child in its own process group via `setsid` and kills
  the whole group, SIGTERM then SIGKILL, when the run times out or the
  BEAM dies. The shim detects BEAM death as stdin EOF, so cleanup happens
  even on `kill -9` of the VM.

  ## Why not `System.cmd` in a `Task`?

  `Task.shutdown` closes the Erlang port, and closing a port closes pipes;
  it sends no signal. The external process runs on until it next touches a
  closed pipe, and its children are never signaled at all. See the [process
  groups guide](process_groups.html) for the full mechanism.

  ## Modes

    * `Forcola.run/2` - bounded one-shot run, mandatory timeout
    * `Forcola.Stream` - line-by-line output as an `Enumerable`
    * `Forcola.Daemon` - long-running server under a supervision tree
    * `Forcola.Duplex` - bidirectional stdin/stdout session

  Maintainers of CLI wrapper libraries who want to offer Forcola-backed
  execution without a mandatory dependency: see the [adoption
  guide](adopting_forcola.html).
  """

  alias Forcola.Result

  @typedoc """
  Errors a bounded run can return.

  The spawn reason describes shim discovery/startup, a failure reported
  by the shim, or `{:shim_exited, %Forcola.Result{}}`; see `run/2`.
  """
  @type run_error ::
          {:timeout | :idle_timeout | :max_output_bytes | :observer_overflow, Result.t()}
          | {:spawn, term()}

  @doc """
  Run `argv` (`[binary | args]`) to completion under the shim.

  The child's stdin is closed immediately after spawn: this mode has no
  way to write to it, so a child that reads until EOF (`cat`, `codex exec`,
  anything that checks for piped input) sees EOF right away instead of
  blocking until `:timeout_ms`. Use `Forcola.Duplex` for interactive stdin.

  ## Options

    * `:timeout_ms` - required. On expiry the child's process group is
      killed (SIGTERM, then SIGKILL after the kill grace) and
      `{:error, {:timeout, partial_result}}` is returned with output
      captured so far. The group is normally confirmed dead before the call
      returns. If the shim's bounded kill probes cannot confirm that the group
      (or an active cgroup) drained, or if the shim itself never reports back,
      the result status is `{:signal, :unconfirmed}` (see `Forcola.Result`). A
      child that exits exactly at the timeout
      boundary can be reported as a timeout whose result carries the
      normal exit status, including `status: 0`.
    * `:kill_grace_ms` - SIGTERM-to-SIGKILL grace in milliseconds,
      default `5_000`. Also accepted by `Forcola.Stream.lines/2`.
    * `:cd` - working directory.
    * `:idle_timeout_ms` - optional positive integer (at most `4_294_967_295`).
      Cancels after this long without output, independently of `:timeout_ms`.
      Returns `{:error, {:idle_timeout, partial_result}}`. Output never resets
      the whole-run deadline.
    * `:max_output_bytes` - optional positive integer (at most `4_294_967_295`),
      counting stdout and stderr together. Exactly the limit is allowed; an
      extra byte cancels with `{:error, {:max_output_bytes, partial_result}}`.
      The captured and observed prefix never exceeds the limit, even for
      unterminated output. Both pumps use finite 8 KiB credit windows, retained
      through exit drain, bounding queued output as well as accumulation.
    * `:interrupt_signal` / `:interrupt_grace_ms` - optional pair. The only
      signal is `:int` (SIGINT); grace must be a positive integer at most
      `4_294_967_295`. The native supervisor sends INT to the process group,
      waits this grace, then applies the existing TERM/KILL escalation if
      needed. This also applies on KILL requests and owner/BEAM EOF, including
      a SIGKILLed VM. It lets a cooperative parent stop children in separate
      groups; it cannot contain an uncooperative escaped descendant.
    * `:env` - environment entries. Legacy calls accept `nil`, maps, and
      lists of pairs, including atom names as supported by JSON encoding;
      native spawn errors retain their ordinary spawn-reason tuple. With
      any new strict option, explicit `:env` must be a list of `{name, value}`
      binary strings. Names must be nonempty and contain no `=` or NUL;
      values must contain no NUL.
    * `:env_mode` - `:inherit` (default) or `:replace`. Replacement clears the
      inherited environment before applying `:env`, including when it is empty.
      Supply an absolute executable path when clearing PATH.
    * `:merge_stderr` - route stderr into stdout; default `false`.
    * `:output_observer` - optional `{local_pid, reference}` receiving
      `{reference, {:stdout, bytes}}` and `{reference, {:stderr, bytes}}`
      as raw output frames arrive. Notifications are sent by the collector
      before `run/2` returns; the final result and termination behavior are
      unchanged on legacy runs. A dead recipient is harmless. Invalid observers
      raise `ArgumentError` before the shim is opened.

      Frame boundaries are arbitrary: bytes may contain partial lines,
      multiple lines, or incomplete UTF-8 sequences. Notifications are
      unacknowledged and do not provide backpressure or durable delivery;
      the recipient is responsible for draining its mailbox. With any new
      strict option, the collector checks the recipient's total queue before
      each delivery and cancels at 1,024 queued messages with
      `{:error, {:observer_overflow, partial_result}}`. Other senders can add
      messages independently. Observation is not acknowledged or durable. With
      `merge_stderr: true`, both channels are reported as `:stdout`. Keep merging disabled when
      the original output channel matters.
    * `:shim_path` - trusted absolute path to a matching native shim,
      overriding normal discovery. Also accepted by Stream, Duplex, and
      Daemon. The caller owns installation, permissions, protection from
      replacement, and cleanup; see `Forcola.Shim.path/1`.
    * `:user` - run the child as this user, a string username or an
      integer uid. The user's primary gid and supplementary groups are
      taken from the passwd/group database unless `:group` overrides the
      gid. See ["Running as a different user"](#run/2-running-as-a-different-user).
    * `:group` - run the child with this group as its primary gid, a
      string group name or an integer gid. Given without `:user` it sets
      the gid (and clears supplementary groups to just that gid) without
      changing the uid; given with `:user` it overrides the user's
      primary gid.
    * `:cgroup` - opt in to Linux cgroup v2 containment, default `false`.
      When `true` on a Linux host with a delegated cgroup v2 subtree, the
      child runs in a dedicated cgroup so descendants that escape the
      process group by deliberately daemonizing are still reaped. Linux
      only, requires cgroup delegation, and falls back with a warning
      otherwise. Use `:required` to refuse to execute the child unless
      placement and `cgroup.kill` are available. See
      ["cgroup containment"](#run/2-cgroup-containment).

  New options (`:idle_timeout_ms`, `:max_output_bytes`, `:env_mode`, or the
  interrupt pair) require a versioned capability exchange before SPAWN.
  `cgroup: :required` alone keeps legacy collection and error shapes without
  negotiation; its existing string encoding enforces required containment.
  An older shim rejecting HELLO or a valid incompatible capability reply returns
  `{:error, {:spawn, :incompatible_shim}}` without executing the child.
  Invalid new options raise `ArgumentError` before opening the shim.
  Stream, Duplex, and Daemon reject `:env_mode` and the interrupt options
  before opening a shim. Stream and Daemon also reject `:max_output_bytes`;
  Duplex retains its existing support with `delivery: :pull`. Duplex and
  Daemon reject `:idle_timeout_ms`; Stream retains its existing idle-timeout
  support. These existing mode options keep their own validation and semantics.

  With new strict options, `:timeout_ms` must be an integer in
  `0..4_294_967_295`. Its deadline starts before opening the shim and includes
  capability negotiation; only the remaining budget is sent in SPAWN. Idle
  timing starts after negotiation. A stalled handshake returns
  `{:error, {:spawn, :handshake_timeout}}`; malformed or unexpected replies
  return `{:error, {:spawn, :invalid_handshake}}`. Transport exit before SPAWN
  returns `{:error, {:spawn, {:shim_exited, status}}}` (an OS exit status,
  `{:shim_error, reason}`, or `:unknown` when no status is available).
  These pre-SPAWN errors have no partial child result. Port opening is
  synchronous; its OS latency and final guard close can outlast the deadline.
  After SPAWN, timeout cancellation allows the interrupt grace, kill grace,
  and a 5-second collection backstop; guard close has its own 5-second bound.

  Strict output draining keeps the run open after the root child exits until
  both pipes close or a whole-run/idle deadline cancels draining. An escaped
  descendant retaining a pipe can therefore produce `:timeout` or
  `:idle_timeout` alongside `evidence.child_status: 0`. Wrappers must read the
  child observation and the run reason independently; group cleanup does not
  establish that escaped descendants are gone.

  Every returned result includes `Forcola.Result` evidence. Spawn failures
  after SPAWN with new options return `{:error, {:spawn, {reason, partial_result}}}`;
  malformed EXIT returns `{:error, {:spawn, {:invalid_exit, partial_result}}}`.
  Transport loss keeps the existing `{:shim_exited, partial_result}` shape.
  A bounded failure keeps its original timeout/limit reason if transport is
  subsequently lost. A missing native report never confirms cleanup.

  ## cgroup containment

  The process-group kill reaches every descendant that stays in the
  child's process group, but a target that deliberately daemonizes
  (double-fork plus `setsid`, or a `--daemon` flag) leaves the group and
  survives. `cgroup: true` adds a Linux-only backstop: the child is
  placed in a dedicated cgroup v2 cgroup before exec, so every descendant
  it forks inherits the cgroup regardless of process-group games, and on
  kill the shim writes `cgroup.kill` to SIGKILL the whole subtree at once.
  It is layered on top of the process-group kill, never in place of it.

  It requires a delegated cgroup v2 subtree, which means the BEAM must run
  inside a delegatable unit: under systemd, `Delegate=yes` on the service,
  or wrapping the run in `systemd-run --user --scope`. See the [process
  groups guide](process_groups.html#deliberate-daemonizers).

  With `cgroup: true`, unavailability never turns into an error. On macOS, on non-cgroup-v2 systems, or
  when the subtree is not writable/delegated, `cgroup: true` degrades to
  the ordinary process-group kill and logs a warning from the shim; the
  group-kill guarantee (ordinary grandchildren still die) is unchanged. A
  `Logger.debug` line is emitted when containment was actually active.

  Use `cgroup: :required` when fallback would be unsafe. The shim verifies a
  delegated cgroup and writable `cgroup.kill` before spawning, and the child
  joins that cgroup before exec. If either step fails, the command does not
  execute and the normal spawn-error path reports the failure. This mode is
  Linux-only. It contains descendants of the child, not work started through
  a daemon or scheduler outside its process tree.

  ## Running as a different user

  `:user`/`:group` make the shim drop privileges (`setgroups`, then
  `setgid`, then `setuid`, in that order) in the child before exec. This
  is POSIX-only, a one-way drop, and requires the shim process itself to
  run with enough privilege to drop: root, or `CAP_SETUID`/`CAP_SETGID`
  on Linux. Requesting only the user the shim already runs as is a no-op
  and always succeeds. An explicit `:group` always replaces supplementary
  groups, even when it is the current primary gid, so that path still needs
  permission to call `setgroups`.

  It fails closed. If the user or group cannot be resolved, or the shim
  lacks the privilege to drop, the child is never executed and the call
  returns the mode's normal spawn error (`{:error, {:spawn, reason}}` for
  `run/2`). The command never runs as the shim's own (possibly
  privileged) user when a different user was requested. Names are
  resolved in the parent before fork; only the numeric syscalls run in
  the child.

  Any exit status is `{:ok, %Forcola.Result{}}`; callers branch on
  `:status`. A non-zero exit is a result, not an error.

  ## Spawn errors

    * `{:error, {:spawn, :shim_not_found}}` - no shim binary exists for
      this target (neither downloaded nor built).
    * `{:error, {:spawn, {:invalid_shim_path, reason}}}` - an explicit
      `:shim_path` is not an absolute binary path to a regular executable
      file, or the file cannot be accessed. Filesystem failures use POSIX
      reasons such as `:enoent`.
    * `{:error, {:spawn, {:shim_start_failed, reason}}}` - the operating
      system could not start the shim, for example `:eacces` or `:enoexec`.
    * `{:error, {:spawn, reason}}` where `reason` is a string - the shim
      reported the spawn failure, for example a missing or
      non-executable command.
    * `{:error, {:spawn, {:shim_exited, %Forcola.Result{}}}}` - the shim
      exited without reporting an exit or error, for example because it
      was SIGKILLed. The result's status is `{:signal, :unconfirmed}`. A
      SIGKILLed shim gets no chance to kill the group, so the child may
      survive, reparented to pid 1.
  """
  @spec run([String.t(), ...], keyword()) :: {:ok, Result.t()} | {:error, run_error()}
  def run([_binary | _] = argv, opts), do: Forcola.Run.run(argv, opts)
end
