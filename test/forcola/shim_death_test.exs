defmodule Forcola.ShimDeathTest do
  # async: false -- pid liveness assertions across tests should not race
  # each other's spawned processes.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  # SIGKILL of the shim itself is the one death the shim cannot clean up
  # after: it gets no chance to signal the child's process group. These
  # tests pin both the reported shape and the documented limitation that
  # the child leaks in this case.

  describe "the shim SIGKILLed mid-run" do
    test "run/2 reports {:shim_exited, partial} and the child leaks", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pid")
      script = ~S(echo $$ > "$PID_FILE"; sleep 60)

      task =
        Task.async(fn ->
          Forcola.run(["/bin/sh", "-c", script],
            timeout_ms: 60_000,
            env: [{"PID_FILE", pid_file}]
          )
        end)

      child = await_pid_file(pid_file)
      shim = os_ppid(child)
      {_, 0} = System.cmd("kill", ["-KILL", shim])

      # The port sees the shim's death as :exit_status; the child's own
      # group state cannot be confirmed, hence {:signal, :unconfirmed}.
      assert {:error, {:spawn, {:shim_exited, %Forcola.Result{status: {:signal, :unconfirmed}}}}} =
               Task.await(task, 10_000)

      # Documented limitation, pinned deliberately: SIGKILL gives the
      # shim no chance to kill the group, so the child survives,
      # reparented to pid 1. The :unconfirmed status reports exactly
      # this. If this assertion ever fails because the child died, the
      # shim grew a cleanup path this test should be updated to match.
      assert alive?(child), "child was expected to leak after SIGKILL of the shim"
      assert os_ppid(child) == "1", "leaked child was expected to be reparented to pid 1"

      # The leak is ours to clean up: kill the child by pid.
      System.cmd("kill", ["-KILL", child])
      assert eventually(fn -> not alive?(child) end), "could not clean up the leaked child"
    end

    test "Duplex delivers {:forcola_exit, session, :shim_exited}" do
      # The child reports its own pid on stdout, then blocks on stdin.
      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", ~S(echo $$; read _)], kill_grace_ms: 1_000)

      child =
        receive do
          {:forcola_line, ^session, line} -> String.trim(line)
        after
          10_000 -> flunk("child never reported its pid")
        end

      shim = os_ppid(child)
      {_, 0} = System.cmd("kill", ["-KILL", shim])

      assert_receive {:forcola_exit, ^session, :shim_exited}, 10_000

      # Unlike the run/2 case, this child usually dies on its own: its
      # stdin pipe was fed by the dead shim, so `read` hits EOF and the
      # shell exits. That is incidental (a stdin-independent child would
      # leak the same way), so clean up by pid if it is still there
      # rather than asserting either way.
      if alive?(child), do: System.cmd("kill", ["-KILL", child])
      assert eventually(fn -> not alive?(child) end)

      # close/1 after shim death is a no-op and still returns :ok.
      assert :ok = Forcola.Duplex.close(session)
    end
  end

  # Polls until the child has written its full pid line.
  defp await_pid_file(pid_file) do
    assert eventually(fn ->
             case File.read(pid_file) do
               {:ok, contents} -> String.ends_with?(contents, "\n")
               _ -> false
             end
           end),
           "child never wrote its pid file"

    pid_file |> File.read!() |> String.trim()
  end

  # The shim is the direct parent of the child it spawns, so the child's
  # PPID identifies this run's shim without a pgrep that could collide
  # with other forcola_shim processes on the machine.
  defp os_ppid(pid) do
    {out, 0} = System.cmd("ps", ["-o", "ppid=", "-p", pid])
    String.trim(out)
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
