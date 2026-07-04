defmodule Forcola.StreamTest do
  # async: false -- pid liveness assertions across tests should not race
  # each other's spawned processes.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  describe "lines/2" do
    test "streams stdout as lines without trailing newlines" do
      lines =
        ["/bin/sh", "-c", "printf 'a\\nb\\nc\\n'"]
        |> Forcola.Stream.lines(timeout_ms: 5_000)
        |> Enum.to_list()

      assert lines == ["a", "b", "c"]
    end

    test "reassembles a line split across frame boundaries" do
      # The sleep forces the two halves of "partial" into separate
      # STDOUT frames; the stream must join them across the boundary.
      script = ~S(printf 'par'; sleep 0.3; printf 'tial\nsecond\n')

      lines =
        ["/bin/sh", "-c", script]
        |> Forcola.Stream.lines(timeout_ms: 5_000)
        |> Enum.to_list()

      assert lines == ["partial", "second"]
    end

    test "emits a final partial line that has no trailing newline" do
      lines =
        ["/bin/sh", "-c", "printf 'complete\\nno-newline'"]
        |> Forcola.Stream.lines(timeout_ms: 5_000)
        |> Enum.to_list()

      assert lines == ["complete", "no-newline"]
    end

    test "a non-zero exit raises after delivering the lines that preceded it" do
      {:ok, agent} = Agent.start_link(fn -> [] end)

      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/bin/sh", "-c", "echo out; echo err >&2; exit 3"]
          |> Forcola.Stream.lines(timeout_ms: 5_000)
          |> Stream.each(fn line -> Agent.update(agent, &[line | &1]) end)
          |> Stream.run()
        end

      assert error.status == 3
      assert error.stderr =~ "err"
      refute error.timed_out
      assert Agent.get(agent, &Enum.reverse/1) == ["out"]
    end

    test "merge_stderr routes stderr into the line stream" do
      lines =
        ["/bin/sh", "-c", "echo out; echo err >&2; sleep 0.1"]
        |> Forcola.Stream.lines(timeout_ms: 5_000, merge_stderr: true)
        |> Enum.to_list()

      assert "out" in lines
      assert "err" in lines
    end

    test "a missing binary raises with the spawn reason" do
      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/nonexistent/binary/path"]
          |> Forcola.Stream.lines(timeout_ms: 5_000)
          |> Enum.to_list()
        end

      assert error.reason
    end
  end

  describe "the kill discipline" do
    test "early halt kills the whole process group, including a grandchild", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")

      # A parent that forks a grandchild and then emits lines forever.
      # The pid file path rides in the env because tmp_dir contains
      # shell metachars.
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; while :; do echo line; sleep 0.05; done)

      assert ["line"] ==
               ["/bin/sh", "-c", script]
               |> Forcola.Stream.lines(
                 timeout_ms: 60_000,
                 kill_grace_ms: 1_000,
                 env: [{"PID_FILE", pid_file}]
               )
               |> Enum.take(1)

      # The halt blocks until the shim confirms group death, so no grace
      # period is needed before probing.
      [parent, child] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the early-halt kill"
      refute alive?(child), "grandchild survived the early-halt kill (group kill failed)"
    end

    test "timeout kills the group and raises with timed_out set", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; echo started; wait)

      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/bin/sh", "-c", script]
          |> Forcola.Stream.lines(
            timeout_ms: 300,
            kill_grace_ms: 1_000,
            env: [{"PID_FILE", pid_file}]
          )
          |> Enum.to_list()
        end

      assert error.timed_out

      [parent, child] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the timeout kill"
      refute alive?(child), "grandchild survived the timeout kill (group kill failed)"
    end

    test "owner death kills the group", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; echo ready; wait)
      test_pid = self()

      owner =
        spawn(fn ->
          ["/bin/sh", "-c", script]
          |> Forcola.Stream.lines(timeout_ms: 60_000, env: [{"PID_FILE", pid_file}])
          |> Stream.each(fn line -> send(test_pid, {:line, line}) end)
          |> Stream.run()
        end)

      assert_receive {:line, "ready"}, 5_000
      Process.exit(owner, :kill)

      # Owner death closes the port; the shim sees stdin EOF and kills
      # the group. That path is asynchronous from here, so poll.
      [parent, child] = pid_file |> File.read!() |> String.split()

      assert eventually(fn -> not alive?(parent) and not alive?(child) end),
             "group survived owner death"
    end
  end

  describe "the idle timeout" do
    test "fires when the producer stalls and kills the whole group", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")

      # Emit one line, record the forked grandchild, then stall well past
      # the idle interval with the whole-run bound left generous so only
      # the idle bound can fire.
      script =
        ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; echo tick; sleep 5)

      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/bin/sh", "-c", script]
          |> Forcola.Stream.lines(
            timeout_ms: 60_000,
            idle_timeout_ms: 300,
            kill_grace_ms: 1_000,
            env: [{"PID_FILE", pid_file}]
          )
          |> Enum.to_list()
        end

      assert error.idle_timed_out
      refute error.timed_out

      [parent, child] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the idle-timeout kill"
      refute alive?(child), "grandchild survived the idle-timeout kill (group kill failed)"
    end

    test "does not fire while the producer keeps emitting within the interval" do
      # A line every ~50ms for several iterations, each well under the
      # 500ms idle interval, then a clean exit. The idle bound must never
      # trip because output keeps resetting it.
      script = ~S(for i in 1 2 3 4 5 6; do echo "line $i"; sleep 0.05; done)

      lines =
        ["/bin/sh", "-c", script]
        |> Forcola.Stream.lines(timeout_ms: 60_000, idle_timeout_ms: 500)
        |> Enum.to_list()

      assert lines == ["line 1", "line 2", "line 3", "line 4", "line 5", "line 6"]
    end

    test "composes with the whole-run bound: whole-run wins when it is sooner", %{
      tmp_dir: tmp_dir
    } do
      pid_file = Path.join(tmp_dir, "pids")
      # No output at all, so the idle bound would trip; but the whole-run
      # bound is much sooner, so the error must be attributed to it.
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)

      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/bin/sh", "-c", script]
          |> Forcola.Stream.lines(
            timeout_ms: 300,
            idle_timeout_ms: 30_000,
            kill_grace_ms: 1_000,
            env: [{"PID_FILE", pid_file}]
          )
          |> Enum.to_list()
        end

      assert error.timed_out
      refute error.idle_timed_out

      [parent, child] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the whole-run timeout kill"
      refute alive?(child), "grandchild survived the whole-run timeout kill"
    end

    test "composes with the whole-run bound: idle wins when it is sooner", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")
      # One line, then a stall. The idle bound is much sooner than the
      # whole-run bound, so the error must be attributed to the idle bound.
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; echo tick; sleep 5)

      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/bin/sh", "-c", script]
          |> Forcola.Stream.lines(
            timeout_ms: 30_000,
            idle_timeout_ms: 300,
            kill_grace_ms: 1_000,
            env: [{"PID_FILE", pid_file}]
          )
          |> Enum.to_list()
        end

      assert error.idle_timed_out
      refute error.timed_out

      [parent, child] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the idle-timeout kill"
      refute alive?(child), "grandchild survived the idle-timeout kill"
    end

    test "default (no :idle_timeout_ms) leaves a plain run unchanged" do
      lines =
        ["/bin/sh", "-c", "printf 'a\\nb\\nc\\n'"]
        |> Forcola.Stream.lines(timeout_ms: 5_000)
        |> Enum.to_list()

      assert lines == ["a", "b", "c"]
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
