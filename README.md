# Forcola

[![CI](https://github.com/joshrotenberg/forcola/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/joshrotenberg/forcola/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/forcola.svg)](https://hex.pm/packages/forcola)
[![Docs](https://img.shields.io/badge/docs-hexdocs.pm-blue.svg)](https://hexdocs.pm/forcola)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/joshrotenberg/forcola/blob/main/LICENSE)

Leak-free external process execution for the BEAM.

Forcola runs OS processes through a small Rust shim that puts each child in
its own process group and kills the whole group, SIGTERM then SIGKILL, when
the run times out or the BEAM dies. Children and grandchildren that remain in
the group die with the command.

Named for the forcola, the carved oarlock of a Venetian gondola.

## Installation

Add `forcola` to your dependencies:

```elixir
def deps do
  [
    {:forcola, "~> 0.4"}
  ]
end
```

Requires Elixir 1.18+ and OTP 27+. No Rust toolchain is needed on the five
precompiled targets (macOS arm64 and x86-64, Linux x86-64 and arm64 glibc,
x86-64 musl): the shim binary is downloaded from the matching GitHub Release
and verified against a SHA256 checksum at compile time. On other targets, or
to opt out of the download, set `FORCOLA_BUILD=1` to build from source with
cargo. See the [getting started guide](https://hexdocs.pm/forcola/getting_started.html).

For Mix escripts, include Forcola's `priv` files, extract the bundled shim to
a trusted executable path, and pass `shim_path: path` to any execution mode.
The [escript installation example](https://hexdocs.pm/forcola/getting_started.html#mix-escripts)
covers extraction, permissions, and cleanup.

## Observe a bounded run's output

Pass `output_observer: {observer_pid, run_ref}` to `Forcola.run/2` to receive
raw output while the command is still running:

```elixir
Forcola.run(["my-cli", "--json"],
  timeout_ms: 60_000,
  output_observer: {observer_pid, run_ref}
)
# observer_pid receives {run_ref, {:stdout, bytes}} or
# {run_ref, {:stderr, bytes}} for each output frame.
```

The target must be a local PID and the tag a reference. On legacy runs, the collector sends
notifications before returning the same final `Forcola.Result` it
would return without an observer. Exit, timeout, and cleanup behavior stay
the same; a dead observer does not affect the run.

Frames are bytes, not lines or decoded messages. A frame can split a line or
UTF-8 character. Notifications are opt-in and unacknowledged; the observer
must drain its mailbox. They provide neither backpressure nor durable
delivery. With `merge_stderr: true`, merged stderr is observed as stdout;
leave merging disabled when the original channel matters.

## Bound output, idle time, and environment

```elixir
Forcola.run(["/absolute/path/to/cli", "--json"],
  timeout_ms: 60_000,
  idle_timeout_ms: 10_000,
  max_output_bytes: 1_048_576,
  env_mode: :replace,
  env: [{"PATH", "/usr/bin:/bin"}],
  interrupt_signal: :int,
  interrupt_grace_ms: 500
)
```

The new integer options must be positive and at most `4_294_967_295`.
Output bytes count stdout and stderr together, including partial lines. Exactly
the limit is allowed; an additional byte cancels. The captured and observed
prefix never exceeds it. Finite credits keep at most 8 KiB per output pipe in
flight. The independent whole-run deadline never resets on output; the idle
deadline does. Without `max_output_bytes`, captured output has no cumulative
memory limit. `env_mode: :inherit` remains the default; `:replace` clears the
environment before applying the validated `{name, value}` string pairs.
Without a new strict option, legacy `env: nil`, maps, and atom names keep
their existing encoding and spawn-error behavior.

The optional signal/grace pair sends process-group SIGINT before the existing
TERM/KILL sequence on timeout, explicit cancellation, and owner or BEAM death.
A cooperative parent can use that opportunity to stop children in other groups.
An uncooperative escaped child remains outside group cleanup on macOS.

These options require a matching shim capability exchange before child
execution. Older shims return `{:error, {:spawn, :incompatible_shim}}`.
Handshake stalls return `{:error, {:spawn, :handshake_timeout}}`, malformed
replies return `{:error, {:spawn, :invalid_handshake}}`, and transport exit
before SPAWN returns `{:error, {:spawn, {:shim_exited, status}}}`. With new
options, `timeout_ms` is an integer in `0..4_294_967_295` and includes startup
negotiation; SPAWN receives only its remaining budget. Cleanup and guard close
can extend beyond that execution deadline (see `Forcola.run/2`).
`cgroup: :required` alone keeps legacy collection and errors without HELLO;
its existing required-containment wire encoding remains enforced.
Stream, Duplex, and Daemon reject the new one-shot environment and interrupt
options before opening a shim. Stream and Daemon also reject `max_output_bytes`;
Duplex keeps its existing support with `delivery: :pull`. Duplex and Daemon
reject `idle_timeout_ms`; Stream keeps its existing idle option. These existing
mode options keep their own validation and semantics.
Bounded failures return `{:error, {reason, partial_result}}`, with reason
`:timeout`, `:idle_timeout`, `:max_output_bytes`, or `:observer_overflow`.
Strict observers cancel at 1,024 queued messages rather than growing the
recipient's mailbox indefinitely; notifications remain unacknowledged.

`result.evidence` keeps raw `child_status` and `child_signal`, `termination`,
`reason`, `output_truncated`, `cleanup`, `scope`, and `guard_close` independent
of the legacy status. Only explicit valid EXIT evidence confirms cleanup.
Strict drain waits for pipe closure after root exit. An escaped descendant
holding a pipe can cause `reason: :timeout` or `:idle_timeout` even with
`child_status: 0`; wrappers must consume those facts independently.
`:process_group` confirmation never proves whole-tree containment. Early JSON
on stdout is partial output until the terminal result arrives. See
[`Forcola.Result`](https://hexdocs.pm/forcola/Forcola.Result.html) for field values
and [`Forcola.run/2`](https://hexdocs.pm/forcola/Forcola.html#run/2) for error shapes.

## The problem

The common Elixir timeout pattern leaks processes:

```elixir
task = Task.async(fn -> System.cmd(binary, args) end)

case Task.yield(task, timeout) || Task.shutdown(task) do
  {:ok, result} -> result
  nil -> {:error, :timeout}
end
```

`Task.shutdown` kills the BEAM task, which closes the Erlang port. Closing a
port closes pipes; it sends no signal. The external process keeps running
until it next writes to a closed pipe, and any children it spawned are never
signaled at all. The caller gets `{:error, :timeout}` while the command keeps
running.

## The design

A port program, not a NIF: the shim is a separate OS process, so a bug in it
cannot crash the BEAM, and BEAM death reaches it as stdin EOF.

```text
BEAM <--stdin/stdout pipes--> forcola_shim <--forks--> child (own process group)
                                   |                      |- grandchild
                                   |                      |- grandchild
                                   |
                     on timeout or stdin EOF:
                     kill(-pgid, SIGTERM), then SIGKILL
```

- The shim calls `setsid` before exec, so the child leads a new process
  group. Kill means the whole group: the CLI and everything it forked.
- Timeout is mandatory on bounded runs. On expiry the caller receives
  `{:error, {:timeout, partial_result}}` with output captured so far, and
  the group is confirmed dead before the call returns (or the result is
  explicitly marked `{:signal, :unconfirmed}`).
- If the BEAM dies, even by `kill -9`, the shim sees stdin EOF and kills the
  group before exiting.
- Shim binaries ship precompiled per target via GitHub Releases with SHA256
  verification. Consumers need no Rust, C, or C++ toolchain.

## Execution modes

| Mode | API | Use |
|---|---|---|
| Bounded run | `Forcola.run/2` | One-shot command with mandatory timeout |
| Line stream | `Forcola.Stream.lines/2` | Line output consumed as an `Enumerable` |
| Daemon | `Forcola.Daemon` | Long-running server under a supervision tree |
| Duplex | `Forcola.Duplex` | Bidirectional stdin/stdout session |

The [getting started guide](https://hexdocs.pm/forcola/getting_started.html) has a runnable example,
options, and return/message shapes for each mode.

## Process groups and cleanup

The group kill covers the child and everything it keeps in its process group:
ordinary grandchildren die with the command. Deliberate daemonizers (double-fork
plus `setsid`) leave the group; on Linux the opt-in `cgroup: true` layer
contains them when active; `cgroup: :required` refuses to execute without it.
Daemon control channels like docker and work handed
to system schedulers stay out of reach of any process-based mechanism. The
[process groups guide](https://hexdocs.pm/forcola/process_groups.html) covers
the kill sequence, cgroup containment, the confirmation guarantee and its
exceptions, and the full "What group kill cannot reach" audit.

## Adopting in a wrapper library

Forcola slots into existing CLI wrapper libraries without becoming a mandatory
dependency: the wrapper defines a small runner behaviour, keeps its
`System.cmd/3` path as the default, and accepts a Forcola-backed one via
config, with Forcola as an optional dep. The [adoption
guide](https://hexdocs.pm/forcola/adopting_forcola.html) covers the pattern, a worked example against
a real wrapper, the mode mapping, and migration notes for erlexec-based
wrappers.

## Prior art

- [erlexec](https://github.com/saleyn/erlexec) has opt-in process-group kill
  and best-effort Linux cgroup attachment, but compiles C++ on the consumer's
  machine.
- [MuonTrap](https://github.com/fhunleth/muontrap) has the port-program
  architecture, default stdout/stderr flow control, and cgroup v2 controls;
  without a cgroup, it signals only the direct child.
- [Rambo](https://github.com/jayjun/rambo) proved a Rust shim works in a hex
  package; its x86-64-only binary distribution is the cautionary tale the
  release workflow here is designed around.

The [alternatives guide](https://hexdocs.pm/forcola/alternatives.html) compares these in detail.

## Development

The local quality gate mirrors CI:

```sh
mix deps.get
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix credo --strict
mix dialyzer
cargo test --manifest-path native/forcola_shim/Cargo.toml
cargo clippy --manifest-path native/forcola_shim/Cargo.toml --all-targets -- -D warnings
```

CI runs the Elixir integration suite on Ubuntu and macOS, and runs the
privilege-drop tests under passwordless `sudo` on its Linux runner. Real cgroup
containment needs a delegated writable cgroup v2 subtree, which hosted runners
do not always provide; the platform job reports availability in its job summary.
Set `FORCOLA_REQUIRE_ROOT_TESTS=1` or `FORCOLA_REQUIRE_CGROUP=1` to turn either
platform prerequisite into a hard failure on a suitable dedicated runner.

## License

MIT
