defmodule Forcola.CgroupTest do
  # async: false -- these spawn real processes and probe pid liveness.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  alias Forcola.Shim

  describe "encode_spawn/2 cgroup wiring" do
    test "the default payload is unchanged (no cgroup key)" do
      decoded = decode(Shim.encode_spawn(["echo", "hi"], []))
      refute Map.has_key?(decoded, "cgroup")
    end

    test "cgroup: false leaves the key out, keeping the default path byte-for-byte" do
      with_opt = Shim.encode_spawn(["echo", "hi"], cgroup: false)
      without_opt = Shim.encode_spawn(["echo", "hi"], [])
      assert with_opt == without_opt
      refute Map.has_key?(decode(with_opt), "cgroup")
    end

    test "cgroup: true adds the opt-in flag" do
      decoded = decode(Shim.encode_spawn(["echo", "hi"], cgroup: true))
      assert decoded["cgroup"] == true
    end
  end

  describe "decode_contained/1" do
    test "reads a true flag" do
      assert Shim.decode_contained(:json.encode(%{"contained" => true}) |> IO.iodata_to_binary())
    end

    test "reads a false flag" do
      refute Shim.decode_contained(:json.encode(%{"contained" => false}) |> IO.iodata_to_binary())
    end

    test "defaults to false when the field is absent (older shim)" do
      refute Shim.decode_contained(:json.encode(%{"status" => 0}) |> IO.iodata_to_binary())
    end
  end

  describe "cgroup: true fallback behavior (holds on every platform)" do
    test "an ordinary run under cgroup: true still succeeds" do
      assert {:ok, %Forcola.Result{status: 0, stdout: "hi\n"}} =
               Forcola.run(["/bin/sh", "-c", "echo hi"], timeout_ms: 5_000, cgroup: true)
    end

    test "cgroup: true never turns into a spawn error" do
      # Whatever the host (macOS, no cgroup v2, or no delegation), cgroup: true
      # degrades to the process-group kill rather than erroring.
      assert {:ok, %Forcola.Result{}} =
               Forcola.run(["/bin/sh", "-c", "exit 0"], timeout_ms: 5_000, cgroup: true)
    end

    test "the existing group-kill guarantee holds: ordinary grandchildren still die",
         %{tmp_dir: tmp_dir} do
      pid_file = Path.join(tmp_dir, "pids")

      # A grandchild that stays in the process group. The process-group kill
      # must reap it even when cgroup falls back, so this passes with or
      # without a delegated cgroup.
      script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)

      assert {:error, {:timeout, %Forcola.Result{}}} =
               Forcola.run(["/bin/sh", "-c", script],
                 timeout_ms: 300,
                 cgroup: true,
                 env: [{"PID_FILE", pid_file}]
               )

      [parent, child] = pid_file |> File.read!() |> String.split()
      refute alive?(parent), "parent survived the timeout kill under cgroup: true"

      refute alive?(child),
             "in-group grandchild survived under cgroup: true (group kill regressed)"
    end
  end

  describe "real cgroup v2 containment (delegated host only)" do
    @describetag :cgroup_delegated

    test "a deliberate daemonizer that escaped the process group is still reaped",
         %{tmp_dir: tmp_dir} do
      if cgroup_contained_available?() do
        pid_file = Path.join(tmp_dir, "escapee_pid")

        # `setsid` puts the sleep in its own session/pgroup, so it escapes the
        # child's process group entirely: kill(-pgid, ...) can never reach it.
        # Only the cgroup.kill backstop can. Guard hosts without setsid.
        script =
          ~S(command -v setsid >/dev/null || { echo NO_SETSID; exit 3; }; ) <>
            ~S(setsid sleep 120 & echo "$!" > "$PID_FILE"; sleep 120)

        assert {:error, {:timeout, %Forcola.Result{}}} =
                 Forcola.run(["/bin/sh", "-c", script],
                   timeout_ms: 400,
                   kill_grace_ms: 200,
                   cgroup: true,
                   env: [{"PID_FILE", pid_file}]
                 )

        escapee = pid_file |> File.read!() |> String.trim()

        refute alive?(escapee),
               "daemonized escapee survived: cgroup.kill did not reap it (pid #{escapee})"
      else
        # No delegated cgroup v2 subtree on this host. Skip LOUDLY: print a
        # clear reason rather than passing silently, and do not fail red.
        IO.puts(
          "\nSKIP real cgroup containment: no delegated cgroup v2 subtree on this host " <>
            "(cgroup: true fell back to process-group kill). Run under " <>
            "`systemd-run --user --scope mix test` or a delegated cgroup to exercise it."
        )

        assert true
      end
    end
  end

  # Drives the shim port directly, the way Forcola.Shim does, to read the EXIT
  # frame's `contained` flag. Returns true only when a delegated cgroup v2
  # subtree is present, so the caller can skip loudly otherwise.
  defp cgroup_contained_available? do
    case Shim.open() do
      {:ok, port} ->
        try do
          payload = Shim.encode_spawn(["true"], cgroup: true, kill_grace_ms: 200)
          Shim.send_frame(port, Shim.tag_spawn(), payload)
          await_contained(port, System.monotonic_time(:millisecond) + 5_000)
        after
          if Port.info(port) != nil, do: Port.close(port)
        end

      {:error, _} ->
        false
    end
  end

  defp await_contained(port, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    receive do
      {^port, {:data, <<tag, payload::binary>>}} ->
        cond do
          tag == Shim.tag_exit() -> Shim.decode_contained(payload)
          tag == Shim.tag_error() -> false
          true -> await_contained(port, deadline)
        end

      {^port, {:exit_status, _status}} ->
        false
    after
      max(remaining, 0) -> false
    end
  end

  # kill -0: probes existence without signalling. Forcola.run/2 only returns
  # after the shim confirms the group is dead, so no grace period is needed.
  defp alive?(pid) do
    {_out, status} = System.cmd("kill", ["-0", pid], stderr_to_stdout: true)
    status == 0
  end

  defp decode(json), do: :json.decode(json)
end
