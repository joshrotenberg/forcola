defmodule Forcola.SpawnIdentityTest do
  use ExUnit.Case, async: false

  alias Forcola.{Daemon, Duplex, Result}

  @moduletag :tmp_dir
  @moduletag :capture_log
  @fixture Path.expand("../fixtures/identity_shim.py", __DIR__)

  test "identity decoding retains native precision and ignores future extra fields" do
    for start_time <- [
          %{kind: "linux", boot_id: "boot-id", ticks: 123_456_789},
          %{kind: "macos", seconds: 1_800_000_000, microseconds: 987_654}
        ] do
      payload =
        %{pid: 123, pgid: 123, start_time: start_time, future_field: "ignored"}
        |> :json.encode()
        |> IO.iodata_to_binary()

      assert {:ok, identity} = Forcola.Shim.decode_identity(payload)
      assert identity.pid == 123
      assert identity.pgid == 123
      assert identity.start_time == %{start_time | kind: String.to_existing_atom(start_time.kind)}
    end
  end

  test "malformed identities never become recovery records" do
    valid = %{pid: 123, pgid: 123, start_time: %{kind: "macos", seconds: 100, microseconds: 0}}

    for invalid <- [
          %{},
          %{valid | pid: 0},
          %{valid | pgid: -1},
          %{valid | start_time: %{kind: "unexpected", seconds: 100, microseconds: 0}},
          %{valid | start_time: %{kind: "macos", seconds: 100, microseconds: 1_000_000}},
          %{valid | start_time: %{kind: "linux", ticks: 123}},
          %{valid | start_time: %{kind: "linux", boot_id: "", ticks: 123}},
          %{valid | start_time: %{kind: "linux", boot_id: "boot-id", ticks: -1}}
        ] do
      payload = invalid |> :json.encode() |> IO.iodata_to_binary()
      assert {:error, :invalid_identity} = Forcola.Shim.decode_identity(payload)
    end

    assert {:error, :invalid_identity} = Forcola.Shim.decode_identity("{broken")
  end

  test "run exposes identity for fast exits without changing output or status" do
    for opts <- [[], [max_output_bytes: 1024], [pty: true]] do
      assert {:ok, result} =
               Forcola.run(
                 ["/bin/sh", "-c", "printf identity"],
                 opts ++ [timeout_ms: 5_000, spawn_identity: true]
               )

      assert result.status == 0
      assert result.stdout == "identity"
      assert_identity(result)
    end
  end

  test "identity remains on timeout partial results" do
    assert {:error, {:timeout, result}} =
             Forcola.run(["/bin/sleep", "60"],
               timeout_ms: 200,
               kill_grace_ms: 100,
               spawn_identity: true
             )

    assert_identity(result)
  end

  test "identity-disabled results keep nil fields and legacy collection" do
    assert {:ok, %Result{pid: nil, pgid: nil, start_time: nil, stdout: "legacy\n"}} =
             Forcola.run(["/bin/echo", "legacy"], timeout_ms: 5_000)

    assert {:ok, %Result{pid: nil, pgid: nil, start_time: nil}} =
             Forcola.run(["true"], timeout_ms: 5_000, spawn_identity: false)
  end

  test "Duplex reports identity before output and before the first stdin write" do
    for opts <- [[], [pty: true]] do
      {:ok, session} =
        Duplex.open(
          ["/bin/sh", "-c", "printf 'ready\n'; read line; printf 'reply:%s\n' \"$line\""],
          opts ++ [spawn_identity: true, kill_grace_ms: 100]
        )

      try do
        identity = first_spawned(session)
        assert_identity(identity)
        assert Duplex.spawned(session) == {:ok, identity}
        ready = if opts[:pty], do: "ready\r", else: "ready"
        reply = if opts[:pty], do: "reply:recorded\r", else: "reply:recorded"
        assert_receive {:forcola_line, ^session, ^ready}, 5_000
        assert :ok = Duplex.send_line(session, "recorded")
        assert_receive {:forcola_line, ^session, ^reply}, 5_000
        assert_receive {:forcola_exit, ^session, 0}, 5_000
        assert Duplex.spawned(session) == {:ok, identity}
      after
        Duplex.close(session)
      end
    end
  end

  test "Duplex pull delivery reports identity without output demand" do
    {:ok, session} =
      Duplex.open(["/bin/cat"], spawn_identity: true, delivery: :pull, kill_grace_ms: 100)

    try do
      identity = first_spawned(session)
      assert_identity(identity)
      assert Duplex.spawned(session) == {:ok, identity}
      assert :ok = Duplex.send_line(session, "recorded")
      assert {:ok, {:stdout, "recorded"}} = Duplex.recv(session, 5_000)
    after
      Duplex.close(session)
    end
  end

  test "Daemon reports identity before output, including while waiting for readiness", %{
    tmp_dir: dir
  } do
    for ready? <- [false, true] do
      marker = Path.join(dir, "ready-#{ready?}")
      opts = if ready?, do: [ready: fn -> File.exists?(marker) end, ready_poll_ms: 10], else: []

      {:ok, daemon} =
        Daemon.start_link(
          opts ++
            [
              argv: ["/bin/sh", "-c", "printf ready; touch \"$READY\"; exec sleep 60"],
              env: [{"READY", marker}],
              spawn_identity: true,
              output: {:send, self()},
              kill_grace_ms: 100
            ]
        )

      try do
        identity = first_spawned(daemon)
        assert_identity(identity)
        assert Daemon.spawned(daemon) == {:ok, identity}
        assert_receive {Daemon, ^daemon, {:stdout, "ready"}}, 5_000
      after
        GenServer.stop(daemon)
      end

      assert Daemon.spawned(daemon) == {:error, :closed}
    end
  end

  test "Stream observer receives identity before the first line in eager and bounded modes" do
    for opts <- [[], [backpressure: true]] do
      ref = make_ref()

      lines =
        ["/bin/sh", "-c", "printf 'first\nsecond\n'"]
        |> Forcola.Stream.lines(opts ++ [timeout_ms: 5_000, spawn_observer: {self(), ref}])
        |> Stream.with_index()
        |> Enum.map(fn {line, index} ->
          if index == 0 do
            assert_receive {^ref, {:spawned, identity}}, 0
            assert_identity(identity)
          end

          line
        end)

      assert lines == ["first", "second"]
      refute_receive {^ref, {:spawned, _}}, 0
    end
  end

  test "spawn failure emits no identity in every mode" do
    missing = "/forcola-missing-identity-test"

    assert {:error, {:spawn, reason}} =
             Forcola.run([missing], timeout_ms: 5_000, spawn_identity: true)

    assert is_binary(reason)

    {:ok, session} = Duplex.open([missing], spawn_identity: true)
    assert_receive {:forcola_exit, ^session, {:spawn_error, _}}, 5_000
    refute_receive {:forcola_spawned, ^session, _}, 0
    Duplex.close(session)

    ref = make_ref()

    assert_raise Forcola.Stream.Error, fn ->
      [missing]
      |> Forcola.Stream.lines(timeout_ms: 5_000, spawn_observer: {self(), ref})
      |> Enum.to_list()
    end

    refute_receive {^ref, {:spawned, _}}, 0

    previous = Process.flag(:trap_exit, true)

    try do
      {:ok, daemon} = Daemon.start_link(argv: [missing], spawn_identity: true)
      assert_receive {:EXIT, ^daemon, _reason}, 5_000
      refute_receive {:forcola_spawned, ^daemon, _}, 0
    after
      Process.flag(:trap_exit, previous)
    end
  end

  test "queries distinguish disabled identity while sessions are running" do
    {:ok, session} = Duplex.open(["/bin/cat"], kill_grace_ms: 100)

    try do
      assert Duplex.spawned(session) == {:error, :not_requested}
    after
      Duplex.close(session)
    end

    {:ok, daemon} = Daemon.start_link(argv: ["/bin/sleep", "60"], kill_grace_ms: 100)

    try do
      assert Daemon.spawned(daemon) == {:error, :not_requested}
    after
      GenServer.stop(daemon)
    end

    refute_receive {:forcola_spawned, _, _}, 0
  end

  test "immediate Duplex and Daemon shutdown still publishes queued identity", %{tmp_dir: dir} do
    shim = fake_shim(dir, "late_spawned")
    opts = [spawn_identity: true, shim_path: shim, kill_grace_ms: 100]
    {:ok, session} = Duplex.open(["unused"], opts)
    assert Duplex.spawned(session) == {:error, :pending}
    assert :ok = Duplex.close(session)
    assert_receive {:forcola_spawned, ^session, %{pid: 123, pgid: 123}}, 5_000

    {:ok, daemon} = Daemon.start_link([argv: ["unused"]] ++ opts)
    assert Daemon.spawned(daemon) == {:error, :pending}
    assert :ok = GenServer.stop(daemon)
    assert_receive {:forcola_spawned, ^daemon, %{pid: 123, pgid: 123}}, 5_000
  end

  test "old shims and missing identity capabilities fail before SPAWN in every mode", %{
    tmp_dir: dir
  } do
    for mode <- ["legacy", "bounded"] do
      shim = fake_shim(dir, mode)
      opts = [spawn_identity: true, shim_path: shim]

      assert {:error, {:spawn, :incompatible_shim}} =
               Forcola.run(["unused"], [timeout_ms: 5_000] ++ opts)

      assert {:error, :incompatible_shim} = Duplex.open(["unused"], opts)

      assert_raise Forcola.Stream.Error, fn ->
        ["unused"]
        |> Forcola.Stream.lines([timeout_ms: 5_000] ++ opts)
        |> Enum.to_list()
      end

      # Init failures can send a linked EXIT after start_link returns its error.
      previous = Process.flag(:trap_exit, true)

      try do
        assert {:error, {:spawn, :incompatible_shim}} =
                 Daemon.start_link([argv: ["unused"]] ++ opts)

        receive do
          {:EXIT, _pid, {:spawn, :incompatible_shim}} -> :ok
        after
          100 -> :ok
        end
      after
        Process.flag(:trap_exit, previous)
      end

      refute File.exists?(Path.join(dir, "spawn_received"))
    end
  end

  test "unselected legacy shims still work in every mode", %{tmp_dir: dir} do
    shim = fake_shim(dir, "legacy")

    assert {:ok, %Result{stdout: "legacy\n", pid: nil}} =
             Forcola.run(["unused"], timeout_ms: 5_000, shim_path: shim)

    {:ok, session} = Duplex.open(["unused"], shim_path: shim)
    assert_receive {:forcola_line, ^session, "legacy"}, 5_000
    assert_receive {:forcola_exit, ^session, 0}, 5_000
    Duplex.close(session)

    assert ["legacy"] ==
             ["unused"]
             |> Forcola.Stream.lines(timeout_ms: 5_000, shim_path: shim)
             |> Enum.to_list()

    {:ok, daemon} = Daemon.start_link(argv: ["unused"], shim_path: shim, output: {:send, self()})
    monitor = Process.monitor(daemon)
    assert_receive {Daemon, ^daemon, {:stdout, "legacy\n"}}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^daemon, _}, 5_000
    refute_receive {:forcola_spawned, _, _}, 0
  end

  test "invalid identity options fail before opening a shim" do
    for value <- [nil, :yes, 1, "true"] do
      opts = [spawn_identity: value, shim_path: "/missing-shim"]

      assert_raise ArgumentError, ~r/spawn_identity/, fn ->
        Forcola.run(["unused"], [timeout_ms: 5_000] ++ opts)
      end

      assert_raise ArgumentError, ~r/spawn_identity/, fn -> Duplex.open(["unused"], opts) end

      assert_raise ArgumentError, ~r/spawn_identity/, fn ->
        Daemon.start_link([argv: ["unused"]] ++ opts)
      end

      assert_raise ArgumentError, ~r/spawn_identity/, fn ->
        Forcola.Stream.lines(["unused"], [timeout_ms: 5_000] ++ opts)
      end
    end

    assert_raise ArgumentError, ~r/spawn_observer/, fn ->
      Forcola.Stream.lines(["unused"], timeout_ms: 5_000, spawn_observer: {self(), :invalid})
    end
  end

  defp first_spawned(session) do
    receive do
      message ->
        assert {:forcola_spawned, ^session, identity} = message
        identity
    after
      5_000 -> flunk("missing spawn identity")
    end
  end

  defp assert_identity(%{pid: pid, pgid: pgid, start_time: start_time}) do
    assert is_integer(pid) and pid > 0
    assert pgid == pid

    case :os.type() do
      {:unix, :linux} ->
        assert %{kind: :linux, boot_id: boot_id, ticks: ticks} = start_time
        assert is_integer(ticks) and ticks >= 0
        assert boot_id == File.read!("/proc/sys/kernel/random/boot_id") |> String.trim()

      {:unix, :darwin} ->
        assert %{kind: :macos, seconds: seconds, microseconds: micros} = start_time
        assert seconds > 0
        assert micros in 0..999_999
    end
  end

  defp fake_shim(dir, mode) do
    path = Path.join(dir, "shim_#{mode}")
    source = File.read!(@fixture)
    File.write!(path, String.replace(source, ~s(MODE = "legacy"), ~s(MODE = "#{mode}")))
    File.chmod!(path, 0o700)
    path
  end
end
