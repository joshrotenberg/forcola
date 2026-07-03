defmodule Forcola.RunTest do
  # async: false -- pid liveness assertions across tests should not race
  # each other's spawned processes.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  describe "run/2" do
    test "captures stdout and exit status" do
      assert {:ok, %Forcola.Result{status: 0, stdout: "hello\n", stderr: ""}} =
               Forcola.run(["/bin/sh", "-c", "echo hello"], timeout_ms: 5_000)
    end

    test "captures a non-zero exit without wrapping it in an error" do
      assert {:ok, %Forcola.Result{status: 3, stdout: "out\n", stderr: "err\n"}} =
               Forcola.run(["/bin/sh", "-c", "echo out; echo err >&2; exit 3"],
                 timeout_ms: 5_000
               )
    end

    test "merge_stderr routes stderr into stdout" do
      assert {:ok, %Forcola.Result{status: 1, stdout: stdout, stderr: ""}} =
               Forcola.run(["/bin/sh", "-c", "echo out; echo err >&2; exit 1"],
                 timeout_ms: 5_000,
                 merge_stderr: true
               )

      assert stdout =~ "out"
      assert stdout =~ "err"
    end

    test "cd and env are applied", %{tmp_dir: tmp_dir} do
      assert {:ok, %Forcola.Result{status: 0, stdout: stdout}} =
               Forcola.run(["/bin/sh", "-c", "pwd; echo $FORCOLA_PROBE"],
                 timeout_ms: 5_000,
                 cd: tmp_dir,
                 env: [{"FORCOLA_PROBE", "probe-value"}]
               )

      assert stdout =~ Path.basename(tmp_dir)
      assert stdout =~ "probe-value"
    end

    test "a missing binary is a clean error, not a crash" do
      assert {:error, {:spawn, reason}} =
               Forcola.run(["/nonexistent/binary/path"], timeout_ms: 5_000)

      assert reason
    end
  end

  describe "the port-kill discipline" do
    test "timeout kills the whole OS process group, not just the parent", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")

      # A parent that spawns a child and parks: the exact shape of a
      # subprocess that forked a tool subprocess and hung. The pid file
      # path rides in the env because tmp_dir contains shell metachars.
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)

      assert {:error, {:timeout, %Forcola.Result{}}} =
               Forcola.run(["/bin/sh", "-c", script],
                 timeout_ms: 300,
                 env: [{"PID_FILE", pid_file}]
               )

      [parent, child] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the timeout kill"
      refute alive?(child), "child survived the timeout kill (group kill failed)"
    end

    test "a SIGTERM-ignoring process is escalated to SIGKILL", %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pid")
      script = ~S(trap '' TERM; echo $$ > "$PID_FILE"; while true; do sleep 0.1; done)

      assert {:error, {:timeout, %Forcola.Result{}}} =
               Forcola.run(["/bin/sh", "-c", script],
                 timeout_ms: 300,
                 kill_grace_ms: 300,
                 env: [{"PID_FILE", pid_file}]
               )

      pid = pid_file |> File.read!() |> String.trim()
      refute alive?(pid), "TERM-ignoring process survived: SIGKILL escalation failed"
    end

    test "output produced before the timeout rides in the partial result" do
      assert {:error, {:timeout, %Forcola.Result{stdout: stdout}}} =
               Forcola.run(["/bin/sh", "-c", "echo partial; sleep 60"], timeout_ms: 300)

      assert stdout =~ "partial"
    end
  end

  # kill -0: probes existence without signalling. Forcola.run/2 only
  # returns after the shim confirms the group is dead, so no grace
  # period is needed here.
  defp alive?(pid) do
    {_out, status} = System.cmd("kill", ["-0", pid], stderr_to_stdout: true)
    status == 0
  end
end
