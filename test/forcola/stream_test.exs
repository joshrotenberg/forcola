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

  describe "backpressure" do
    test "delivers every line byte-exact under a small window (multi-MB producer)" do
      # 2 MB of output through a 4 KB window: heavy backpressure, fast
      # consumer. `yes` reprints the line; `head` bounds it and exits 0.
      line = String.duplicate("x", 100)
      script = "yes #{line} | head -n 20000"

      lines =
        ["/bin/sh", "-c", script]
        |> Forcola.Stream.lines(timeout_ms: 60_000, window_bytes: 4096)
        |> Enum.to_list()

      assert length(lines) == 20_000
      assert Enum.all?(lines, &(&1 == line)), "a line was corrupted or truncated"
    end

    test "blocks the producer near the window and resumes when the consumer resumes",
         %{tmp_dir: tmp_dir} do
      progress = Path.join(tmp_dir, "progress")
      test = self()

      consumer =
        spawn(fn ->
          result =
            ["/bin/sh", "-c", producer_script()]
            |> Forcola.Stream.lines(
              timeout_ms: 60_000,
              window_bytes: 4096,
              kill_grace_ms: 1_000,
              env: [{"PROGRESS", progress}]
            )
            |> gated_consume(test)

          send(test, {:done, result})
        end)

      # Consumer warms up (pulls a few lines) then pauses.
      assert_receive :warmed, 15_000

      # With the consumer paused, the producer blocks once it has filled the
      # window plus the OS pipe buffer, so the byte counter stops advancing
      # well below any unbounded growth threshold.
      stalled = await_stall(progress)

      assert stalled < 256_000,
             "producer did not block under backpressure; wrote #{stalled} bytes while paused"

      # Resume: the producer advances well past where it stalled.
      send(consumer, :resume)

      assert await_above(progress, stalled + 200_000, 20_000),
             "producer did not resume after the consumer resumed"

      assert_receive {:done, _}, 20_000
    end

    test "without backpressure the same paused consumer does not block the producer",
         %{tmp_dir: tmp_dir} do
      progress = Path.join(tmp_dir, "progress")
      test = self()

      consumer =
        spawn(fn ->
          result =
            ["/bin/sh", "-c", producer_script()]
            |> Forcola.Stream.lines(
              timeout_ms: 60_000,
              kill_grace_ms: 1_000,
              env: [{"PROGRESS", progress}]
            )
            |> gated_consume(test)

          send(test, {:done, result})
        end)

      assert_receive :warmed, 15_000

      # Eager pump: the shim keeps reading and buffering on the BEAM side, so
      # the producer never blocks and the byte counter blows past the bound.
      assert await_above(progress, 256_000, 20_000),
             "producer stalled without backpressure"

      send(consumer, :resume)
      assert_receive {:done, _}, 20_000
    end

    test "a slow-but-alive consumer under backpressure does not trip a short idle timeout" do
      # The producer emits far more than one window, so it is genuinely
      # blocked by backpressure while the consumer pauses. The pause (600ms)
      # exceeds the idle interval (200ms), but it is consumer-driven and must
      # not raise. All lines must still be delivered.
      test = self()

      consumer =
        spawn(fn ->
          result =
            ["/bin/sh", "-c", "seq 1 100000"]
            |> Forcola.Stream.lines(
              timeout_ms: 60_000,
              idle_timeout_ms: 200,
              window_bytes: 4096
            )
            |> Enum.reduce_while({0, []}, fn line, {n, acc} ->
              n = n + 1
              acc = [line | acc]

              if n == 5 do
                send(test, :warmed)

                receive do
                  :resume -> {:cont, {n, acc}}
                end
              else
                {:cont, {n, acc}}
              end
            end)

          {_n, collected} = result
          send(test, {:result, collected})
        end)

      assert_receive :warmed, 15_000
      # Hold the consumer paused past the idle interval, then resume.
      Process.sleep(600)
      send(consumer, :resume)

      assert_receive {:result, collected}, 30_000
      lines = Enum.reverse(collected)
      assert length(lines) == 100_000
      assert List.first(lines) == "1"
      assert List.last(lines) == "100000"
    end

    test "a genuinely hung producer under backpressure still raises the idle timeout" do
      # One line, then the producer hangs with credit available. The idle
      # bound must fire even though backpressure is on.
      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/bin/sh", "-c", "echo tick; sleep 60"]
          |> Forcola.Stream.lines(
            timeout_ms: 60_000,
            idle_timeout_ms: 300,
            window_bytes: 4096,
            kill_grace_ms: 1_000
          )
          |> Enum.to_list()
        end

      assert error.idle_timed_out
      refute error.timed_out
    end
  end

  # An infinite producer that writes 1000-byte lines to stdout and records the
  # cumulative bytes it has successfully written to a side file. When stdout
  # backpressures, the write blocks and the counter stops advancing.
  defp producer_script do
    ~S"""
    CHUNK=$(printf '%01000d' 0)
    i=0
    while :; do
      printf '%s\n' "$CHUNK"
      i=$((i + 1001))
      printf '%s\n' "$i" >> "$PROGRESS"
    done
    """
  end

  # Pulls a few lines, signals :warmed, blocks until :resume, then consumes a
  # bounded burst before halting (which triggers the early-halt kill).
  defp gated_consume(stream, test) do
    Enum.reduce_while(stream, 0, fn _line, n ->
      n = n + 1

      cond do
        n == 5 ->
          send(test, :warmed)

          receive do
            :resume -> {:cont, n}
          end

        n >= 5_005 ->
          {:halt, n}

        true ->
          {:cont, n}
      end
    end)
  end

  # The last complete integer written to the progress file, tolerating a
  # partially written trailing line and a not-yet-created file.
  defp progress(path) do
    case File.read(path) do
      {:ok, data} -> latest_count(data)
      _ -> 0
    end
  end

  defp latest_count(data) do
    data
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find_value(0, &parse_count/1)
  end

  defp parse_count(str) do
    case Integer.parse(str) do
      {n, ""} -> n
      _ -> false
    end
  end

  # Waits until the progress counter has not advanced for ~400ms (the producer
  # has blocked), bounded overall, and returns the stalled value.
  defp await_stall(path) do
    deadline = System.monotonic_time(:millisecond) + 10_000
    do_await_stall(path, progress(path), 0, deadline)
  end

  defp do_await_stall(path, last, stable, deadline) do
    Process.sleep(80)
    now = progress(path)

    cond do
      now > last -> do_await_stall(path, now, 0, deadline)
      stable >= 5 -> now
      System.monotonic_time(:millisecond) > deadline -> now
      true -> do_await_stall(path, now, stable + 1, deadline)
    end
  end

  # Polls until the progress counter exceeds `target`, up to `deadline_ms`.
  defp await_above(path, target, deadline_ms) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    poll(fn -> progress(path) > target end, deadline)
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
