defmodule Forcola.InterruptOptionsTest do
  use ExUnit.Case, async: false

  alias Forcola.{Daemon, Duplex}

  @moduletag :tmp_dir
  @moduletag :capture_log
  @fixture Path.expand("../fixtures/interrupt_child.py", __DIR__)
  @grace 150
  @kill_grace 100

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  test "Duplex close and shutdown send INT then TERM then KILL on pipes and PTYs", %{tmp_dir: dir} do
    for delivery <- [:messages, :pull], pty <- [false, true], operation <- [:close, :shutdown] do
      child = child(dir)
      {:ok, session} = Duplex.open(child.argv, options(delivery: delivery, pty: pty))

      try do
        await_ready(child)

        case operation do
          :close ->
            assert :ok = Duplex.close(session)

          :shutdown ->
            assert {:ok, %{status: {:signal, 9}} = terminal} = Duplex.shutdown(session)

            # A native report may explicitly leave group cleanup unconfirmed.
            # Check the actual signal sequence and child death below as well.
            assert terminal.confirmation in [:confirmed, :unconfirmed]
        end

        assert_escalated(child)
      after
        Duplex.close(session)
      end
    end
  end

  test "Daemon stop and supervisor shutdown use the full sequence", %{tmp_dir: dir} do
    for supervised <- [false, true], pty <- [false, true] do
      child = child(dir)

      opts =
        options(argv: child.argv, pty: pty, ready: fn -> ready?(child) end, ready_poll_ms: 10)

      if supervised do
        {:ok, supervisor} = Supervisor.start_link([{Daemon, opts}], strategy: :one_for_one)

        try do
          Supervisor.stop(supervisor)
          assert_escalated(child)
        after
          if Process.alive?(supervisor), do: Supervisor.stop(supervisor)
        end
      else
        {:ok, daemon} = Daemon.start_link(opts)

        try do
          GenServer.stop(daemon)
          assert_escalated(child)
        after
          if Process.alive?(daemon), do: GenServer.stop(daemon)
        end
      end
    end
  end

  test "Stream halt and consumer exceptions cancel through the interrupt sequence", %{
    tmp_dir: dir
  } do
    for backpressure <- [false, true], pty <- [false, true], stop <- [:halt, :exception] do
      child = child(dir)

      stream =
        Forcola.Stream.lines(
          child.argv,
          options(timeout_ms: 5_000, backpressure: backpressure, pty: pty)
        )

      case stop do
        :halt ->
          assert length(Enum.take(stream, 1)) == 1

        :exception ->
          assert_raise RuntimeError, "consumer stopped", fn ->
            Enum.each(stream, fn _ -> raise "consumer stopped" end)
          end
      end

      assert_escalated(child)
    end
  end

  test "whole-run and idle Stream timeouts and Daemon readiness timeout interrupt first", %{
    tmp_dir: dir
  } do
    for kind <- [:whole_run, :idle, :ready] do
      child = child(dir)

      case kind do
        :whole_run ->
          error =
            assert_raise Forcola.Stream.Error, fn ->
              child.argv |> Forcola.Stream.lines(options(timeout_ms: 500)) |> Enum.to_list()
            end

          assert error.timed_out
          assert error.status == {:signal, 9}

        :idle ->
          error =
            assert_raise Forcola.Stream.Error, fn ->
              child.argv
              |> Forcola.Stream.lines(options(timeout_ms: 5_000, idle_timeout_ms: 300))
              |> Enum.to_list()
            end

          assert error.idle_timed_out
          assert error.status == {:signal, 9}

        :ready ->
          assert {:error, :ready_timeout} =
                   Daemon.start_link(
                     options(
                       argv: child.argv,
                       ready: fn -> false end,
                       ready_timeout_ms: 500,
                       ready_poll_ms: 10
                     )
                   )
      end

      assert_escalated(child)
    end
  end

  test "Stream and Duplex output limits use the same signal sequence", %{tmp_dir: dir} do
    for mode <- [:stream, :duplex] do
      child = child(dir, "flood")

      case mode do
        :stream ->
          error =
            assert_raise Forcola.Stream.Error, fn ->
              child.argv
              |> Forcola.Stream.lines(
                options(timeout_ms: 5_000, backpressure: true, max_line_bytes: 100)
              )
              |> Enum.to_list()
            end

          assert error.output_truncated

        :duplex ->
          {:ok, session} = Duplex.open(child.argv, options(delivery: :pull, max_line_bytes: 100))

          try do
            assert {:ok, {:stdout, "ready"}} = Duplex.recv(session, 5_000)

            assert {:error, {:output_limit, %{status: {:signal, 9}, confirmation: :confirmed}}} =
                     Duplex.recv(session, 5_000)
          after
            Duplex.close(session)
          end
      end

      assert_escalated(child)
    end
  end

  test "owner death and brutal server death preserve interrupt-first cleanup", %{tmp_dir: dir} do
    for mode <- [:run, :stream, :duplex, :daemon], death <- [:owner, :server] do
      child = child(dir)
      parent = self()

      owner =
        spawn(fn ->
          case mode do
            :run ->
              Forcola.run(child.argv, options(timeout_ms: 60_000))

            :stream ->
              child.argv |> Forcola.Stream.lines(options(timeout_ms: 60_000)) |> Enum.to_list()

            :duplex ->
              {:ok, session} = Duplex.open(child.argv, options())
              send(parent, {:server, session.pid})
              Process.sleep(:infinity)

            :daemon ->
              {:ok, daemon} = Daemon.start_link(options(argv: child.argv))
              send(parent, {:server, daemon})
              Process.sleep(:infinity)
          end
        end)

      try do
        await_ready(child)
        server = if mode in [:duplex, :daemon], do: receive_server(), else: owner
        Process.exit(if(death == :owner, do: owner, else: server), :kill)
        assert_escalated(child)
      after
        Process.exit(owner, :kill)
      end
    end
  end

  test "killing a separate BEAM reaches the native EOF sequence in every mode", %{tmp_dir: dir} do
    for mode <- [:run, :stream, :duplex, :daemon] do
      child = child(dir)
      beam_file = Path.join(child.dir, "beam")
      opts = options(timeout_ms: 60_000)
      argv = inspect(child.argv)

      invocation =
        case mode do
          :run ->
            "Forcola.run(#{argv}, #{inspect(opts)})"

          :stream ->
            "Forcola.Stream.lines(#{argv}, #{inspect(opts)}) |> Enum.to_list()"

          :duplex ->
            "{:ok, _} = Forcola.Duplex.open(#{argv}, #{inspect(options())}); Process.sleep(:infinity)"

          :daemon ->
            "{:ok, _} = Forcola.Daemon.start_link(#{inspect(options(argv: child.argv))}); Process.sleep(:infinity)"
        end

      code = "File.write!(#{inspect(beam_file)}, System.pid()); " <> invocation
      executable = System.find_executable("elixir")
      ebin = :code.lib_dir(:forcola) |> List.to_string() |> Path.join("ebin")

      port =
        Port.open({:spawn_executable, executable}, [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: ["--erl", "+S 2:2", "-pa", ebin, "-e", code]
        ])

      try do
        await_ready(child)
        beam = beam_file |> File.read!() |> String.trim()
        assert {_, 0} = System.cmd("kill", ["-KILL", beam], stderr_to_stdout: true)
        assert_receive {^port, {:exit_status, _}}, 5_000
        assert_escalated(child)
      after
        if File.exists?(beam_file) do
          System.cmd("kill", ["-KILL", File.read!(beam_file)], stderr_to_stdout: true)
        end

        if Port.info(port), do: Port.close(port)
      end
    end
  end

  test "cooperative INT exits without TERM, while recv timeout and stdin EOF do not cancel", %{
    tmp_dir: dir
  } do
    child = child(dir, "cooperate")
    {:ok, session} = Duplex.open(child.argv, options(delivery: :pull))

    try do
      assert {:ok, {:stdout, "ready"}} = Duplex.recv(session, 5_000)
      assert {:error, :timeout} = Duplex.recv(session, 10)
      assert :ok = Duplex.send_eof(session)
      Process.sleep(25)
      refute File.exists?(Path.join(child.dir, "signals"))
      assert {:ok, %{status: 42, confirmation: :confirmed}} = Duplex.shutdown(session)
      assert ["INT " <> _] = signal_lines(child)
      refute alive?(await_ready(child))
    after
      Duplex.close(session)
    end
  end

  test "maximum interrupt grace remains usable when the child exits cooperatively", %{
    tmp_dir: dir
  } do
    for mode <- [:duplex, :daemon, :stream] do
      child = child(dir, "cooperate")
      opts = options(interrupt_grace_ms: 4_294_967_295)

      case mode do
        :duplex ->
          {:ok, session} = Duplex.open(child.argv, opts)

          try do
            await_ready(child)
            assert {:ok, %{status: 42, confirmation: :confirmed}} = Duplex.shutdown(session)
          after
            Duplex.close(session)
          end

        :daemon ->
          {:ok, supervisor} =
            Supervisor.start_link(
              [
                {Daemon,
                 opts ++ [argv: child.argv, ready: fn -> ready?(child) end, ready_poll_ms: 10]}
              ],
              strategy: :one_for_one
            )

          Supervisor.stop(supervisor)

        :stream ->
          assert length(
                   child.argv
                   |> Forcola.Stream.lines(opts ++ [timeout_ms: 5_000])
                   |> Enum.take(1)
                 ) == 1
      end

      assert ["INT " <> _] = signal_lines(child)
      refute alive?(await_ready(child))
    end
  end

  @tag timeout: 30_000
  test "cleanup waits longer than the old five-second margin when the interrupt grace requires it",
       %{tmp_dir: dir} do
    # Run independent children concurrently so a real long grace costs one wait.
    tasks =
      for mode <- [:duplex, :daemon, :stream, :stream_idle] do
        child = child(dir)

        task =
          Task.async(fn ->
            opts = options(interrupt_grace_ms: 5_300)

            case mode do
              :duplex ->
                {:ok, session} = Duplex.open(child.argv, opts)

                try do
                  await_ready(child)

                  assert {:ok, %{status: {:signal, 9}, confirmation: :confirmed}} =
                           Duplex.shutdown(session)
                after
                  Duplex.close(session)
                end

              :daemon ->
                opts =
                  opts ++ [argv: child.argv, ready: fn -> ready?(child) end, ready_poll_ms: 10]

                {:ok, supervisor} =
                  Supervisor.start_link([{Daemon, opts}], strategy: :one_for_one)

                Supervisor.stop(supervisor, :normal, :infinity)

              :stream ->
                assert length(
                         child.argv
                         |> Forcola.Stream.lines(opts ++ [timeout_ms: 10_000])
                         |> Enum.take(1)
                       ) == 1

              :stream_idle ->
                error =
                  assert_raise Forcola.Stream.Error, fn ->
                    child.argv
                    |> Forcola.Stream.lines(opts ++ [timeout_ms: 10_000, idle_timeout_ms: 100])
                    |> Enum.to_list()
                  end

                assert error.status == {:signal, 9}
            end

            assert_escalated(child, 5_300)
          end)

        task
      end

    Enum.each(tasks, &Task.await(&1, 20_000))
  end

  defp options(extra \\ []),
    do:
      Keyword.merge(
        [interrupt_signal: :int, interrupt_grace_ms: @grace, kill_grace_ms: @kill_grace],
        extra
      )

  defp child(parent, mode \\ "stubborn") do
    dir = Path.join(parent, Integer.to_string(System.unique_integer([:positive])))
    File.mkdir_p!(dir)
    child = %{dir: dir, argv: [System.find_executable("python3"), @fixture, mode, dir]}

    on_exit(fn -> cleanup_child(dir) end)

    child
  end

  defp cleanup_child(dir) do
    case File.read(Path.join(dir, "pid")) do
      {:ok, pid} ->
        pid = String.trim(pid)
        if alive?(pid), do: System.cmd("kill", ["-KILL", pid], stderr_to_stdout: true)

      _ ->
        :ok
    end
  end

  defp receive_server do
    receive do
      {:server, server} -> server
    after
      5_000 -> flunk("server never started")
    end
  end

  defp ready?(child) do
    case File.read(Path.join(child.dir, "pid")) do
      {:ok, pid} -> String.ends_with?(pid, "\n")
      _ -> false
    end
  end

  defp await_ready(child) do
    assert eventually(fn -> ready?(child) end), "child never installed its signal handlers"
    child.dir |> Path.join("pid") |> File.read!() |> String.trim()
  end

  defp assert_escalated(child, grace \\ @grace) do
    pid = await_ready(child)
    assert eventually(fn -> not alive?(pid) end), "child survived the kill sequence"
    assert ["INT " <> int_at, "TERM " <> term_at] = signal_lines(child)
    int_at = String.to_integer(int_at)
    term_at = String.to_integer(term_at)
    # Millisecond timestamps permit one millisecond of rounding at each end.
    assert term_at - int_at >= grace - 2
  end

  defp signal_lines(child),
    do: child.dir |> Path.join("signals") |> File.read!() |> String.split("\n", trim: true)

  defp alive?(pid) do
    {output, status} = System.cmd("ps", ["-o", "stat=", "-p", pid], stderr_to_stdout: true)

    status == 0 and String.trim(output) != "" and
      not String.starts_with?(String.trim(output), "Z")
  end

  defp eventually(fun) do
    deadline = System.monotonic_time(:millisecond) + 10_000
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(10)
        poll(fun, deadline)
    end
  end
end
