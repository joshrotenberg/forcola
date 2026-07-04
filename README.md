# Forcola

[![CI](https://github.com/joshrotenberg/forcola/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/joshrotenberg/forcola/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/forcola.svg)](https://hex.pm/packages/forcola)
[![Docs](https://img.shields.io/badge/docs-hexdocs.pm-blue.svg)](https://hexdocs.pm/forcola)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/joshrotenberg/forcola/blob/main/LICENSE)

Leak-free external process execution for the BEAM.

Forcola runs OS processes through a small Rust shim that puts each child in
its own process group and kills the whole group, SIGTERM then SIGKILL, when
the run times out or the BEAM dies. No leaked CLIs, no orphaned grandchildren,
no zombie holding a lock after the caller was told the command failed.

Named for the forcola, the carved oarlock of a Venetian gondola: the one
small piece everything passes through, keeping the oar attached to the boat.

## Status

All four execution modes are implemented against the Rust shim:
`Forcola.run/2`, `Forcola.Stream.lines/2`, `Forcola.Daemon`, and
`Forcola.Duplex`.

## Installation

Add `forcola` to your dependencies:

```elixir
def deps do
  [
    {:forcola, "~> 0.1"}
  ]
end
```

Requires Elixir 1.18+ and OTP 27+. No Rust toolchain is needed on the five
precompiled targets (macOS arm64 and x86-64, Linux x86-64 and arm64 glibc,
x86-64 musl): the shim binary is downloaded from the matching GitHub Release
and verified against a SHA256 checksum at compile time. On other targets, or
to opt out of the download, set `FORCOLA_BUILD=1` to build the shim from
source with cargo.

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
until it next writes to stdout, which for a process mid-work can be minutes,
and any children it spawned are never signaled at all. The caller gets
`{:error, :timeout}` while the command keeps mutating the working directory,
holding locks, and spending money.

## The design

A port program, not a NIF: the shim is a separate OS process, so a bug in it
cannot crash the BEAM, and BEAM death reaches it for free as stdin EOF.

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
- Shim binaries ship precompiled per target (including Apple Silicon) via
  GitHub Releases with SHA256 verification. Consumers need no Rust, C, or
  C++ toolchain.

## What group kill cannot reach

A process-group escape audit (#9) tested the target CLI set on macOS: agent
CLIs with stdio MCP servers, git with hooks and the fsmonitor daemon, make,
cargo, npm, aws, gcloud, ffmpeg, redis-server in foreground mode, and shell
constructs like `nohup` and `disown`. All of them keep their entire tree in
the child's process group and die to the group kill. The escapes fall into
three classes, and no client-side mechanism closes them:

- A child that deliberately daemonizes, by double-forking plus `setsid` or
  via a flag like `redis-server --daemonize yes`, leaves the process group
  and survives the kill. Run servers in foreground mode under
  `Forcola.Daemon`; foreground operation is the same contract every process
  supervisor (systemd, runit, foreman) imposes.
- Client/daemon CLIs such as docker: the CLI is only a control channel.
  Killing the client never stops the container or build running under the
  daemon, and no client-side mechanism, process group or cgroup, can. Use
  the tool's own teardown semantics (`docker run --rm`, `docker kill`) on
  top of Forcola.
- Work handed to system schedulers (`git maintenance` background jobs,
  launchd or systemd timers) was never a child of the CLI at all and is out
  of scope.

On Linux, a future opt-in cgroup v2 layer (#15) could contain deliberate
daemonizers. On macOS nothing can, which is equally true of erlexec and
MuonTrap.

## Execution modes

Four shapes, matching what CLI wrapper libraries actually need:

| Mode | API | Use |
|---|---|---|
| Bounded run | `Forcola.run/2` | One-shot command with mandatory timeout |
| Line stream | `Forcola.Stream.lines/2` | NDJSON/line output consumed as an `Enumerable` |
| Daemon | `Forcola.Daemon` | Long-running server under a supervision tree |
| Duplex | `Forcola.Duplex` | Bidirectional stdin/stdout session |

## Adopting in a wrapper library

Forcola is designed to slot into existing CLI wrapper libraries without
becoming a mandatory dependency: the wrapper defines a small runner
behaviour, keeps its `System.cmd/3` path as the default implementation, and
accepts a Forcola-backed one via config, with Forcola as an optional dep.
The [adoption guide](guides/adopting_forcola.md) covers the pattern, a
worked example against a real wrapper, the mode mapping for common wrapper
shapes, and migration notes for erlexec-based wrappers.

## Prior art

- [erlexec](https://github.com/saleyn/erlexec) has process-group kill
  (opt-in per command via `kill_group`) but compiles C++ on the consumer's
  machine.
- [MuonTrap](https://github.com/fhunleth/muontrap) has the right port-program
  architecture, but full process-tree kill requires Linux cgroups; on macOS
  only the direct child is signaled.
- [Rambo](https://github.com/jayjun/rambo) proved a Rust shim works in a hex
  package; its x86-64-only binary distribution is the cautionary tale the
  release workflow here is designed around.

## License

MIT
