defmodule Forcola.DuplexTest do
  # async: false -- pid liveness assertions across tests should not race
  # each other's spawned processes.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  describe "option validation" do
    test ":timeout_ms is rejected" do
      assert_raise ArgumentError, ~r/no :timeout_ms/, fn ->
        Forcola.Duplex.open(["/bin/cat"], timeout_ms: 1_000)
      end
    end

    test "argv must be a list of binaries" do
      assert_raise ArgumentError, ~r/list of binaries/, fn ->
        Forcola.Duplex.open(["/bin/echo", :hi], [])
      end
    end
  end

  describe "round-trip" do
    test "lines echo through cat" do
      {:ok, session} = Forcola.Duplex.open(["/bin/cat"], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_line(session, "hello")
      assert_receive {:forcola_line, ^session, "hello"}, 10_000

      :ok = Forcola.Duplex.send_line(session, "again")
      assert_receive {:forcola_line, ^session, "again"}, 10_000

      :ok = Forcola.Duplex.close(session)
    end

    test "a line split across frame boundaries is reassembled" do
      # The child answers each input line in two writes with a pause
      # between them, forcing the halves into separate STDOUT frames.
      script = ~S(while read l; do printf 'echo-'; sleep 0.3; printf '%s\n' "$l"; done)

      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", script], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_line(session, "partial")
      assert_receive {:forcola_line, ^session, "echo-partial"}, 10_000

      :ok = Forcola.Duplex.close(session)
    end

    test "stderr lines arrive as :forcola_stderr by default" do
      script = ~S(while read l; do echo "out-$l"; echo "err-$l" >&2; done)

      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", script], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_line(session, "x")
      assert_receive {:forcola_line, ^session, "out-x"}, 10_000
      assert_receive {:forcola_stderr, ^session, "err-x"}, 10_000

      :ok = Forcola.Duplex.close(session)
    end

    test "merge_stderr routes stderr into :forcola_line" do
      script = ~S(while read l; do echo "err-$l" >&2; done)

      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", script],
          merge_stderr: true,
          kill_grace_ms: 1_000
        )

      :ok = Forcola.Duplex.send_line(session, "x")
      assert_receive {:forcola_line, ^session, "err-x"}, 10_000

      :ok = Forcola.Duplex.close(session)
    end
  end

  describe "stdin EOF" do
    test "send_eof closes child stdin without killing; the exit is reported" do
      {:ok, session} = Forcola.Duplex.open(["/bin/cat"], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_line(session, "before-eof")
      assert_receive {:forcola_line, ^session, "before-eof"}, 10_000

      :ok = Forcola.Duplex.send_eof(session)

      # cat exits 0 on stdin EOF; nothing was killed.
      assert_receive {:forcola_exit, ^session, 0}, 10_000

      # The session is over: writes fail, close is a no-op.
      assert {:error, :closed} = Forcola.Duplex.send_line(session, "late")
      assert :ok = Forcola.Duplex.close(session)
    end

    test "send_line after send_eof returns {:error, :closed}" do
      # A child that never exits on EOF, so the session is still up when
      # the late write is attempted.
      script = ~S(while :; do sleep 1; done)

      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", script], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_eof(session)
      assert {:error, :closed} = Forcola.Duplex.send_line(session, "late")

      :ok = Forcola.Duplex.close(session)
    end
  end

  describe "kill discipline" do
    test "close kills the whole group, including a grandchild", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")

      # A parent that forks a grandchild, reports both pids, then echoes
      # stdin. The pid file path rides in the env because tmp_dir
      # contains shell metachars.
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; while read l; do echo "$l"; done)

      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", script],
          env: [{"PID_FILE", pid_file}],
          kill_grace_ms: 1_000
        )

      # The echo confirms the child is past the pid-file write.
      :ok = Forcola.Duplex.send_line(session, "ping")
      assert_receive {:forcola_line, ^session, "ping"}, 10_000

      :ok = Forcola.Duplex.close(session)

      # close blocks until the shim confirms group death, so no grace
      # period is needed before probing.
      [parent, grandchild] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived close"
      refute alive?(grandchild), "grandchild survived close (group kill failed)"
    end

    test "owner death kills the group", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; echo ready; while read l; do :; done)
      test_pid = self()

      owner =
        spawn(fn ->
          {:ok, session} =
            Forcola.Duplex.open(["/bin/sh", "-c", script],
              env: [{"PID_FILE", pid_file}],
              kill_grace_ms: 1_000
            )

          receive do
            {:forcola_line, ^session, "ready"} -> send(test_pid, :ready)
          end

          Process.sleep(:infinity)
        end)

      assert_receive :ready, 10_000
      Process.exit(owner, :kill)

      # The session monitors its owner; the DOWN drives terminate, which
      # kills the group. That path is asynchronous from here, so poll.
      [parent, grandchild] = pid_file |> File.read!() |> String.split()

      assert eventually(fn -> not alive?(parent) and not alive?(grandchild) end),
             "group survived owner death"
    end
  end

  describe "child exit" do
    test "a child that exits on its own reports its status" do
      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", "read l; exit 7"], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_line(session, "go")
      assert_receive {:forcola_exit, ^session, 7}, 10_000
    end

    test "a final partial line is delivered before the exit message" do
      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", "read l; printf no-newline"], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_line(session, "go")
      assert_receive {:forcola_line, ^session, "no-newline"}, 10_000
      assert_receive {:forcola_exit, ^session, 0}, 10_000
    end

    test "a missing binary reports a spawn error" do
      {:ok, session} = Forcola.Duplex.open(["/nonexistent/binary/path"], [])

      assert_receive {:forcola_exit, ^session, {:spawn_error, reason}}, 10_000
      assert reason
    end
  end

  # kill -0: probes existence without signalling.
  defp alive?(pid) do
    {_out, status} = System.cmd("kill", ["-0", pid], stderr_to_stdout: true)
    status == 0
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
