defmodule Forcola.DaemonTest do
  # async: false -- pid liveness assertions across tests should not race
  # each other's spawned processes.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  @moduletag :tmp_dir
  # Daemons stopped by child exit log crash reports; keep them out of
  # the test output unless a test fails.
  @moduletag :capture_log

  describe "option validation" do
    test ":timeout_ms is rejected" do
      assert_raise ArgumentError, ~r/no :timeout_ms/, fn ->
        Forcola.Daemon.start_link(argv: ["/bin/sh", "-c", "true"], timeout_ms: 1_000)
      end
    end

    test ":argv is required and must be a non-empty list of binaries" do
      assert_raise ArgumentError, ~r/requires :argv/, fn ->
        Forcola.Daemon.start_link([])
      end

      assert_raise ArgumentError, ~r/non-empty list of binaries/, fn ->
        Forcola.Daemon.start_link(argv: [])
      end

      assert_raise ArgumentError, ~r/non-empty list of binaries/, fn ->
        Forcola.Daemon.start_link(argv: ["/bin/echo", :hi])
      end
    end

    test ":output must be :logger, a 2-arity function, or {:send, pid}" do
      assert_raise ArgumentError, ~r/:output must be/, fn ->
        Forcola.Daemon.start_link(argv: ["/bin/sh", "-c", "true"], output: :stdout)
      end
    end

    test ":ready must be a zero-arity function" do
      assert_raise ArgumentError, ~r/:ready must be/, fn ->
        Forcola.Daemon.start_link(argv: ["/bin/sh", "-c", "true"], ready: :never)
      end
    end
  end

  describe "kill discipline" do
    test "stopping the supervisor kills the whole group, including a grandchild",
         %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)

      child =
        {Forcola.Daemon,
         argv: ["/bin/sh", "-c", script],
         env: [{"PID_FILE", pid_file}],
         kill_grace_ms: 1_000,
         ready: fn -> File.exists?(pid_file) end}

      {:ok, sup} = Supervisor.start_link([child], strategy: :one_for_one)
      :ok = Supervisor.stop(sup)

      # Supervisor.stop blocks on the daemon's terminate, and terminate
      # blocks until the shim confirms group death, so no grace period
      # is needed before probing.
      [parent, grandchild] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the supervisor stop"
      refute alive?(grandchild), "grandchild survived the supervisor stop (group kill failed)"
    end

    test "owner crash kills the group", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)
      test_pid = self()

      owner =
        spawn(fn ->
          {:ok, daemon} =
            Forcola.Daemon.start_link(
              argv: ["/bin/sh", "-c", script],
              env: [{"PID_FILE", pid_file}],
              kill_grace_ms: 1_000,
              ready: fn -> File.exists?(pid_file) end
            )

          send(test_pid, {:started, daemon})
          Process.sleep(:infinity)
        end)

      assert_receive {:started, daemon}, 10_000
      Process.exit(owner, :kill)

      # The daemon traps exits; the parent EXIT drives terminate, which
      # kills the group. That path is asynchronous from here, so poll.
      [parent, grandchild] = pid_file |> File.read!() |> String.split()

      assert eventually(fn -> not alive?(parent) and not alive?(grandchild) end),
             "group survived owner crash"

      assert eventually(fn -> not Process.alive?(daemon) end)
    end

    test "brutal kill of the daemon (no terminate) still kills the group",
         %{tmp_dir: tmp_dir} do
      Process.flag(:trap_exit, true)
      pid_file = Path.join(tmp_dir, "pids")
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)

      {:ok, daemon} =
        Forcola.Daemon.start_link(
          argv: ["/bin/sh", "-c", script],
          env: [{"PID_FILE", pid_file}],
          ready: fn -> File.exists?(pid_file) end
        )

      Process.exit(daemon, :kill)

      # terminate/2 never runs on a brutal kill; the port closes, the
      # shim sees stdin EOF, and the shim kills the group. Asynchronous
      # from here, so poll.
      [parent, grandchild] = pid_file |> File.read!() |> String.split()

      assert eventually(fn -> not alive?(parent) and not alive?(grandchild) end),
             "group survived brutal kill of the daemon"
    end
  end

  describe "restart strategies" do
    test "a permanent daemon is restarted when the child exits non-zero",
         %{tmp_dir: tmp_dir} do
      count_file = Path.join(tmp_dir, "count")
      script = ~S(echo x >> "$COUNT_FILE"; sleep 0.2; exit 1)

      child =
        Supervisor.child_spec(
          {Forcola.Daemon,
           argv: ["/bin/sh", "-c", script], env: [{"COUNT_FILE", count_file}], kill_grace_ms: 500},
          restart: :permanent
        )

      {:ok, sup} =
        Supervisor.start_link([child], strategy: :one_for_one, max_restarts: 100)

      assert eventually(fn -> runs(count_file) >= 2 end),
             "daemon was not restarted after a non-zero child exit"

      :ok = Supervisor.stop(sup)
    end

    test "a transient daemon is not restarted when the child exits zero",
         %{tmp_dir: tmp_dir} do
      count_file = Path.join(tmp_dir, "count")
      script = ~S(echo x >> "$COUNT_FILE"; exit 0)

      child =
        Supervisor.child_spec(
          {Forcola.Daemon,
           argv: ["/bin/sh", "-c", script], env: [{"COUNT_FILE", count_file}], kill_grace_ms: 500},
          restart: :transient
        )

      {:ok, sup} = Supervisor.start_link([child], strategy: :one_for_one)

      assert eventually(fn -> runs(count_file) == 1 end)
      # Give a wrongly-configured restart time to happen before asserting
      # it did not.
      Process.sleep(500)
      assert runs(count_file) == 1, "daemon was restarted after a clean zero exit"

      :ok = Supervisor.stop(sup)
    end
  end

  describe "readiness" do
    test "start_link blocks until the ready check passes", %{tmp_dir: tmp_dir} do
      ready_file = Path.join(tmp_dir, "ready")
      script = ~S(sleep 0.3; : > "$READY_FILE"; sleep 60)

      {:ok, daemon} =
        Forcola.Daemon.start_link(
          argv: ["/bin/sh", "-c", script],
          env: [{"READY_FILE", ready_file}],
          kill_grace_ms: 500,
          name: :forcola_daemon_ready_test,
          ready: fn -> File.exists?(ready_file) end
        )

      # start_link only returned because the check passed.
      assert File.exists?(ready_file)
      assert Process.whereis(:forcola_daemon_ready_test) == daemon

      :ok = GenServer.stop(daemon)
    end

    test "ready timeout fails startup and kills the group", %{tmp_dir: tmp_dir} do
      Process.flag(:trap_exit, true)
      pid_file = Path.join(tmp_dir, "pids")
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)

      assert {:error, :ready_timeout} =
               Forcola.Daemon.start_link(
                 argv: ["/bin/sh", "-c", script],
                 env: [{"PID_FILE", pid_file}],
                 kill_grace_ms: 500,
                 ready: fn -> false end,
                 ready_timeout_ms: 500
               )

      # init kills the group and waits for confirmation before failing,
      # so no grace period is needed before probing.
      [parent, grandchild] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the ready-timeout kill"
      refute alive?(grandchild), "grandchild survived the ready-timeout kill"
    end

    test "a child that exits before becoming ready fails startup" do
      Process.flag(:trap_exit, true)

      assert {:error, {:exited_before_ready, {:exit_status, 7}}} =
               Forcola.Daemon.start_link(
                 argv: ["/bin/sh", "-c", "exit 7"],
                 ready: fn -> false end,
                 ready_timeout_ms: 5_000
               )
    end
  end

  describe "output handling" do
    test "output is routed to Logger by default, line by line, with the prefix",
         %{tmp_dir: tmp_dir} do
      done_file = Path.join(tmp_dir, "done")
      script = ~S(echo first-line; echo second-line; : > "$DONE_FILE"; sleep 60)

      log =
        capture_log(fn ->
          {:ok, daemon} =
            Forcola.Daemon.start_link(
              argv: ["/bin/sh", "-c", script],
              env: [{"DONE_FILE", done_file}],
              kill_grace_ms: 500,
              log_prefix: "probe: ",
              ready: fn -> File.exists?(done_file) end
            )

          # The ready check passing does not guarantee the output frames
          # were delivered yet; give them a moment before stopping.
          Process.sleep(300)
          :ok = GenServer.stop(daemon)
        end)

      assert log =~ "probe: first-line"
      assert log =~ "probe: second-line"
    end

    test "output: fun receives chunks tagged with their stream" do
      test_pid = self()

      {:ok, daemon} =
        Forcola.Daemon.start_link(
          argv: ["/bin/sh", "-c", "echo on-stdout; echo on-stderr >&2; sleep 60"],
          kill_grace_ms: 500,
          output: fn stream, chunk -> send(test_pid, {:output, stream, chunk}) end
        )

      assert_receive {:output, :stdout, stdout}, 10_000
      assert stdout =~ "on-stdout"
      assert_receive {:output, :stderr, stderr}, 10_000
      assert stderr =~ "on-stderr"

      :ok = GenServer.stop(daemon)
    end

    test "output: {:send, pid} delivers subscriber messages" do
      {:ok, daemon} =
        Forcola.Daemon.start_link(
          argv: ["/bin/sh", "-c", "echo for-subscriber; sleep 60"],
          kill_grace_ms: 500,
          output: {:send, self()}
        )

      assert_receive {Forcola.Daemon, ^daemon, {:stdout, chunk}}, 10_000
      assert chunk =~ "for-subscriber"

      :ok = GenServer.stop(daemon)
    end
  end

  describe "child exit" do
    test "the daemon stops :normal on a zero exit" do
      Process.flag(:trap_exit, true)

      {:ok, daemon} =
        Forcola.Daemon.start_link(argv: ["/bin/sh", "-c", "exit 0"], kill_grace_ms: 500)

      assert_receive {:EXIT, ^daemon, :normal}, 10_000
    end

    test "the daemon stops {:exit_status, n} on a non-zero exit" do
      Process.flag(:trap_exit, true)

      {:ok, daemon} =
        Forcola.Daemon.start_link(argv: ["/bin/sh", "-c", "exit 5"], kill_grace_ms: 500)

      assert_receive {:EXIT, ^daemon, {:exit_status, 5}}, 10_000
    end

    test "the daemon stops {:spawn, reason} when the binary is missing" do
      Process.flag(:trap_exit, true)

      {:ok, daemon} =
        Forcola.Daemon.start_link(argv: ["/nonexistent/binary/path"], kill_grace_ms: 500)

      assert_receive {:EXIT, ^daemon, {:spawn, reason}}, 10_000
      assert reason
    end
  end

  # kill -0: probes existence without signalling.
  defp alive?(pid) do
    {_out, status} = System.cmd("kill", ["-0", pid], stderr_to_stdout: true)
    status == 0
  end

  defp runs(count_file) do
    case File.read(count_file) do
      {:ok, contents} -> contents |> String.split("\n", trim: true) |> length()
      {:error, _} -> 0
    end
  end

  defp eventually(fun, deadline_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) > deadline ->
        false

      true ->
        Process.sleep(50)
        poll(fun, deadline)
    end
  end
end
