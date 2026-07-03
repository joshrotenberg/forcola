# Forcola

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
`Forcola.Duplex`. Not yet published to Hex.

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

## Execution modes

Four shapes, matching what CLI wrapper libraries actually need:

| Mode | API | Use |
|---|---|---|
| Bounded run | `Forcola.run/2` | One-shot command with mandatory timeout |
| Line stream | `Forcola.Stream.lines/2` | NDJSON/line output consumed as an `Enumerable` |
| Daemon | `Forcola.Daemon` | Long-running server under a supervision tree |
| Duplex | `Forcola.Duplex` | Bidirectional stdin/stdout session |

## Prior art

- [ordito's `Ordito.OsProc`](https://github.com/joshrotenberg/ordito) proved
  this contract against erlexec; Forcola ports its API and test suite
  (including the SIGTERM-ignoring child).
- [erlexec](https://github.com/saleyn/erlexec) has first-class process-group
  kill but compiles C++ on the consumer's machine.
- [MuonTrap](https://github.com/fhunleth/muontrap) has the right port-program
  architecture, but full process-tree kill requires Linux cgroups; on macOS
  only the direct child is signaled.
- [Rambo](https://github.com/jayjun/rambo) proved a Rust shim works in a hex
  package; its x86-64-only binary distribution is the cautionary tale the
  release workflow here is designed around.

## License

MIT
