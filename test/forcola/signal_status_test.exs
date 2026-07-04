defmodule Forcola.SignalStatusTest do
  # async: false -- consistent with the other suites; these tests spawn
  # and signal real OS processes.
  use ExUnit.Case, async: false

  # A child killed by an external signal (here: itself, via kill) is a
  # normal completion with a signal status, not an error. These tests
  # pin the `{:signal, n}` decode in every mode; the daemon's
  # `{:exit_signal, n}` stop reason rides the same shim EXIT frame.

  describe "external-signal exit status" do
    test "run/2 returns {:ok, result} with status {:signal, 9}" do
      assert {:ok, %Forcola.Result{status: {:signal, 9}, stdout: "", stderr: ""}} =
               Forcola.run(["/bin/sh", "-c", "kill -KILL $$"], timeout_ms: 5_000)
    end

    test "Stream.lines raises with status {:signal, 9} after delivering prior lines" do
      {:ok, agent} = Agent.start_link(fn -> [] end)

      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["/bin/sh", "-c", "echo first; kill -KILL $$"]
          |> Forcola.Stream.lines(timeout_ms: 5_000)
          |> Stream.each(fn line -> Agent.update(agent, &[line | &1]) end)
          |> Stream.run()
        end

      assert error.status == {:signal, 9}
      refute error.timed_out
      assert Agent.get(agent, &Enum.reverse/1) == ["first"]
    end

    test "Duplex delivers {:forcola_exit, session, {:signal, 9}}" do
      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", "read _; kill -KILL $$"], kill_grace_ms: 1_000)

      :ok = Forcola.Duplex.send_line(session, "go")
      assert_receive {:forcola_exit, ^session, {:signal, 9}}, 10_000
    end

    @tag :capture_log
    test "Daemon stops {:exit_signal, 9} when the child dies by signal" do
      Process.flag(:trap_exit, true)

      {:ok, daemon} =
        Forcola.Daemon.start_link(argv: ["/bin/sh", "-c", "kill -KILL $$"], kill_grace_ms: 500)

      assert_receive {:EXIT, ^daemon, {:exit_signal, 9}}, 10_000
    end
  end
end
