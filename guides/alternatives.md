# Alternatives and tradeoffs

Forcola is one of several ways to run external processes from the BEAM. This
comparison reflects published packages and upstream source as of October 2026.
The BEAM-death and grandchild behavior marked "tested" comes from the project's
earlier macOS experiments; this update did not rerun those experiments against
every new upstream release.

| Option | Cleanup and containment | Output flow | Installation and status |
|---|---|---|---|
| `System.cmd` / Port / `:os.cmd` | Port close sends no signal; no process-tree cleanup (tested) | Caller-managed | OTP/Elixir standard library |
| [erlexec 2.5.0](https://github.com/saleyn/erlexec/releases/tag/2.5.0) | BEAM-death cleanup; opt-in process-group kill; Linux cgroup attachment is best-effort | Multiple redirection and interactive APIs | Shared C++ port program; compiles from source with rebar3 |
| [MuonTrap 2.0.0](https://github.com/fhunleth/muontrap/releases/tag/v2.0.0) | BEAM-death cleanup; Linux cgroup v2 can kill the subtree; without it, direct-child kill (macOS tested) | Stdout and stderr flow control, 10 KiB window by default | C wrapper per command; compiles with elixir_make; active |
| [Porcelain 2.0.3](https://hex.pm/packages/porcelain) + goon | Closing stdin and waiting does not kill the child; no tree kill | Stream and file APIs | Separate goon install; last Hex release in 2016 |
| [Rambo 0.3.4](https://hex.pm/packages/rambo) | Shim kills the direct child on stdin EOF; no tree kill | One-shot | Bundled x86-64 binaries; last Hex release in 2021; Apple Silicon install failed in testing |
| [exile 0.15.0](https://github.com/akash-akya/exile/releases/tag/v0.15.0) | Normal cleanup; BEAM `kill -9` orphaned the child in earlier macOS testing | Backpressure-first streaming | NIF-based IO; compiles native code; active |
| [Forcola 0.4.0](https://github.com/joshrotenberg/forcola/releases/tag/v0.4.0) | BEAM-death process-group kill with explicit unconfirmed results; opt-in Linux cgroup v2 contains daemonizers when active | Opt-in stdout backpressure for Stream; opt-in bounded stdout and stderr pull for Duplex | Per-command Rust shim; five precompiled targets, cargo elsewhere |

## erlexec

The most mature and broadest process-control API here: a shared C++ port
program, PTYs with RFC 4254 options, user switching, Linux capabilities,
custom signals, and opt-in group kill. Version 2.4 adds
[Linux cgroup attachment](https://github.com/saleyn/erlexec/blob/2.5.0/README.md#linux-cgroup-support);
2.5 fixes a `kill_group` failure that could kill the port program when group
assignment failed. The cgroup option can set CPU, memory, IO, and PID limits,
but permission or setup failure is best-effort: the child may still run without
cgroup placement. A caller requiring containment must account for that.

Forcola has a narrower API and uses one shim per command rather than erlexec's
shared port. Its group kill is the default, its precompiled shim avoids a C++
build in supported installations, and `Forcola.Duplex.Terminal` separates the
observed child status, cleanup confirmation, and active scope. Forcola's own
`cgroup: true` also falls back when placement is unavailable. Callers that
require containment can use `cgroup: :required`, which refuses to execute
unless the child can join a delegated cgroup with writable `cgroup.kill`.

## MuonTrap

MuonTrap 2.0 replaces cgroup v1 with v2, adds an Elixir-friendly `:cgroup`
map for resource limits, exposes cgroup statistics, and uses `cgroup.kill`
when available. It remains especially well suited to Nerves and embedded
Linux. A writable cgroup parent still needs to be arranged; desktop Linux
setup may take work. Without an active cgroup, its wrapper kills the direct
child, so ordinary grandchildren can survive on macOS. Forcola's process-group
kill reaches ordinary grandchildren on both macOS and Linux, with optional
cgroup containment for deliberate daemonizers on delegated Linux hosts.

[MuonTrap's stdio flow control](https://github.com/fhunleth/muontrap#stdio-flow-control)
predates 2.0. It bounds unacknowledged stdout and stderr by default (10 KiB).
Backpressure in `Forcola.Stream.lines/2` is opt-in and gates stdout only;
stderr still arrives eagerly. Forcola 0.4.0 adds `Forcola.Duplex.open/2` with
`delivery: :pull`, where both output pumps wait for `recv/2` demand and line,
total-output, and pending-byte limits bound delivery. Its original message
mode remains the default. Forcola does not currently expose MuonTrap-style
cgroup resource limits or usage statistics.

## exile

Exile's NIF-based streaming makes backpressure the default. Version 0.15.0
improves cancellation, input-producer error reporting, and native startup.
Choose it when demand-driven IO is the primary requirement and its lifecycle
tradeoff is acceptable. In the earlier macOS test, a `kill -9` of the BEAM
orphaned exile's child; the 0.15.0 release notes do not claim to change this,
but that test should be rerun before treating the result as current-version
verification. Forcola uses a separate shim that sees stdin EOF on BEAM death.

## Porcelain and Rambo

Neither has a recent Hex release. Porcelain's goon driver needs a separately
installed executable; the released goon closes child stdin and waits rather
than killing the process tree. Rambo established the Rust-shim pattern, but
its bundled binaries omit Apple Silicon and its shim kills only the direct
child. Neither is a strong default for a new process-containment integration.

## Choosing an option

- Choose erlexec for its extensive process controls, PTY options, Linux
  capabilities, or shared-port architecture.
- Choose MuonTrap for Nerves-native cgroup v2 controls, resource limits and
  statistics, or default stdout/stderr flow control.
- Choose exile when backpressure-first streaming is the main requirement and
  BEAM-death cleanup is not the deciding constraint.
- Choose Forcola for process-group cleanup across supported POSIX platforms,
  precompiled installation, and the focused run, stream, daemon, and Duplex
  APIs. Use `delivery: :pull` for bounded Duplex output. `Forcola.run/2`
  collects output, and `Forcola.Stream.lines/2` still handles stderr eagerly, so neither has
  a general total-output memory bound.
- Windows support remains open in
  [#34](https://github.com/joshrotenberg/forcola/issues/34).
