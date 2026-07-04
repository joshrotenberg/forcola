# Alternatives and tradeoffs

Forcola is not the only way to run external processes from the BEAM. The table
below reflects each option's published source and tracker as of mid-2026; rows
marked "tested" were verified empirically on macOS with Elixir 1.20 / OTP 29.

| Option | Architecture | BEAM-death cleanup | Grandchild kill | Install footprint | Maintenance (mid-2026) |
|---|---|---|---|---|---|
| System.cmd / Port / :os.cmd | BEAM port | None; no signal on port close (tested) | No | None | OTP/Elixir stdlib |
| [erlexec](https://github.com/saleyn/erlexec) | One C++ port program for all commands | Yes, incl. kill -9: SIGTERM then SIGKILL in 6 s | Opt-in (`{group, GID}` + `kill_group`) | C++ toolchain + rebar3 at dep compile, source-only package | Active (2.3.4, June 2026) |
| [MuonTrap](https://github.com/fhunleth/muontrap) | C wrapper per command | Yes, incl. kill -9 (tested) | Linux cgroups: full tree; macOS: direct child only (tested) | C compiler (elixir_make) | Active (1.8.0 May 2026, 2.0 rc June 2026) |
| [Porcelain](https://github.com/alco/porcelain) + goon | Go middleman, manual download | Closes child stdin and waits; never kills | No | goon fetched by hand; last goon release 2014 | Unmaintained (last release 2016, last commit 2020) |
| [Rambo](https://github.com/jayjun/rambo) | Rust shim per call | SIGKILLs direct child on stdin EOF | No | Bundled x86-64 binaries only; broken out of the box on Apple Silicon (tested) | Dormant (last release March 2021) |
| [exile](https://github.com/akash-akya/exile) | NIF IO + spawner that execs into the command | Normal exits yes; kill -9 of BEAM orphans the child (tested) | No | C compiler (elixir_make) | Maintained, single author (0.14.0, Feb 2026) |
| forcola | Rust shim per command | stdin EOF kills the process group, covers kill -9; death confirmed before EXIT | Yes (setsid + kill(-pgid), TERM then KILL); opt-in Linux cgroup v2 also contains daemonizers | None on 5 precompiled targets; cargo elsewhere | New (v0.1.0) |

## erlexec

The most capable and most mature option: a single C++ port program with pty
support, user switching, and opt-in process-group kill, actively maintained
since 2003. Its costs are a C++ toolchain at dependency compile time and a
larger API surface. forcola now supports a pty in `Forcola.Duplex`
(`pty: true`), so a pty alone is no longer a reason to reach for erlexec;
forcola's pty is Duplex-only and does no RFC 4254 option negotiation. forcola
also does a basic run-as-user drop now
([#31](https://github.com/joshrotenberg/forcola/issues/31)): `:user`/`:group`
options do a straight `setgroups`/`setgid`/`setuid` from a privileged shim.
erlexec goes further, with a sudo/SUID helper for privilege escalation and
Linux capability management; forcola does neither. Choose erlexec when you need
those.

## MuonTrap

Solves the same core problem as forcola with a per-command C wrapper, and on
Linux adds cgroup containment that kills entire process trees, including
deliberate daemonizers. Without cgroups it kills the direct child only, so
grandchildren escape; forcola's group kill covers ordinary grandchildren
everywhere. forcola now has its own opt-in Linux cgroup v2 layer (`cgroup:
true`, [#15](https://github.com/joshrotenberg/forcola/issues/15)) that contains
deliberate daemonizers under a delegated subtree; MuonTrap's cgroup support is
Nerves-native and more mature here, where forcola's is new and requires cgroup
delegation. On macOS neither can contain a deliberate daemonizer. On Nerves or
embedded Linux, MuonTrap is the native choice.

## exile

Takes a different shape: NIF-based demand-driven IO with real backpressure,
ideal when a slow consumer must stream huge output without buffering. The
tradeoff is cleanup: with no middleman process, a kill -9 of the BEAM orphans
the child (in testing on macOS, exile's child survived where forcola's and
MuonTrap's shims cleaned up). Forcola tracks a backpressure streaming mode in
[#32](https://github.com/joshrotenberg/forcola/issues/32).

## Porcelain and Rambo

Both are effectively frozen. Porcelain has had no release since 2016 and its
goon driver's last release is from 2014; the released goon never kills the
child, it only closes stdin and waits. Rambo is a one-shot Rust shim design but
has had no release since March 2021, ships x86-64-only binaries, and in testing
on an Apple Silicon Mac it failed out of the box. Rambo proved a Rust shim
works in a hex package; its binary distribution is the cautionary tale
forcola's release workflow is designed around.

## Choosing something else

- You need privilege escalation via a sudo/SUID helper, or Linux capability
  management: erlexec. forcola does a basic uid/gid drop with `:user`/`:group`
  ([#31](https://github.com/joshrotenberg/forcola/issues/31)): a straight
  `setgroups`/`setgid`/`setuid` from a privileged shim, not sudo/SUID or
  capabilities.
- You need Linux cgroup containment of daemonizers: forcola now has an opt-in
  cgroup v2 layer (`cgroup: true`,
  [#15](https://github.com/joshrotenberg/forcola/issues/15)) that contains
  daemonizers under a delegated cgroup v2 subtree (systemd `Delegate=yes` or
  `systemd-run --scope`). MuonTrap's Nerves-native cgroup support is more mature
  and does not require you to arrange delegation; on Nerves or embedded Linux it
  is the native choice.
- You need backpressure-first streaming and accept the kill -9 orphan risk:
  exile. forcola tracks a backpressure mode in
  [#32](https://github.com/joshrotenberg/forcola/issues/32).
- You need Windows: Rambo's bundled binary or plain System.cmd. forcola tracks
  Windows support in [#34](https://github.com/joshrotenberg/forcola/issues/34).
- You cannot ship native binaries at all: System.cmd/ports, with the
  orphan-on-death leak documented and accepted.
- forcola is new (v0.1.0). If that is a blocker, erlexec and MuonTrap are the
  mature, actively maintained alternatives that cover the closest ground.
