# Forcola

[![CI](https://github.com/joshrotenberg/forcola/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/joshrotenberg/forcola/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/forcola.svg)](https://hex.pm/packages/forcola)
[![Docs](https://img.shields.io/badge/docs-hexdocs.pm-blue.svg)](https://hexdocs.pm/forcola)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/joshrotenberg/forcola/blob/main/LICENSE)

Leak-free external process execution for the BEAM.

Forcola runs OS processes through a small Rust shim that puts each child in
its own process group and kills the whole group, SIGTERM then SIGKILL, when
the run times out or the BEAM dies. Children and grandchildren die with the
command.

Named for the forcola, the carved oarlock of a Venetian gondola.

## Installation

Add `forcola` to your dependencies:

```elixir
def deps do
  [
    {:forcola, "~> 0.3"}
  ]
end
```

Requires Elixir 1.18+ and OTP 27+. No Rust toolchain is needed on the five
precompiled targets (macOS arm64 and x86-64, Linux x86-64 and arm64 glibc,
x86-64 musl): the shim binary is downloaded from the matching GitHub Release
and verified against a SHA256 checksum at compile time. On other targets, or
to opt out of the download, set `FORCOLA_BUILD=1` to build from source with
cargo. See the [getting started guide](guides/getting_started.md).

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
  the group is confirmed dead before the call returns.
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

The [getting started guide](guides/getting_started.md) has a runnable example,
options, and return/message shapes for each mode.

## Process groups and cleanup

The group kill covers the child and everything it keeps in its process group:
ordinary grandchildren die with the command. Deliberate daemonizers (double-fork
plus `setsid`) leave the group; on Linux the opt-in `cgroup: true` layer
contains them. Daemon control channels like docker and work handed to system
schedulers stay out of reach of any process-based mechanism. The [process groups
guide](guides/process_groups.md) covers the kill sequence, the cgroup containment
layer, the death-confirmed-before-return guarantee and its exception, and the
full "What group kill cannot reach" audit.

## Adopting in a wrapper library

Forcola slots into existing CLI wrapper libraries without becoming a mandatory
dependency: the wrapper defines a small runner behaviour, keeps its
`System.cmd/3` path as the default, and accepts a Forcola-backed one via
config, with Forcola as an optional dep. The [adoption
guide](guides/adopting_forcola.md) covers the pattern, a worked example against
a real wrapper, the mode mapping, and migration notes for erlexec-based
wrappers.

## Prior art

- [erlexec](https://github.com/saleyn/erlexec) has process-group kill (opt-in
  per command via `kill_group`) but compiles C++ on the consumer's machine.
- [MuonTrap](https://github.com/fhunleth/muontrap) has the port-program
  architecture, but full process-tree kill requires Linux cgroups; on macOS
  only the direct child is signaled.
- [Rambo](https://github.com/jayjun/rambo) proved a Rust shim works in a hex
  package; its x86-64-only binary distribution is the cautionary tale the
  release workflow here is designed around.

The [alternatives guide](guides/alternatives.md) compares these in detail.

## License

MIT
