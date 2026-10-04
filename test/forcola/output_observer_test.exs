defmodule Forcola.OutputObserverTest do
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  test "observes output before a blocked child finishes", %{tmp_dir: dir} do
    release = Path.join(dir, "release")
    observer = {self(), make_ref()}
    {_pid, reference} = observer
    script = ~S(printf 'ready\n'; while [ ! -f "$RELEASE" ]; do sleep 0.01; done; printf 'done')

    task =
      Task.async(fn ->
        Forcola.run(["/bin/sh", "-c", script],
          timeout_ms: 5_000,
          env: [{"RELEASE", release}],
          output_observer: observer
        )
      end)

    try do
      assert await_stdout(reference, byte_size("ready\n")) == "ready\n"
      assert Task.yield(task, 0) == nil
      File.write!(release, "continue")

      assert {:ok, %Forcola.Result{status: 0, stdout: "ready\ndone", stderr: ""}} =
               Task.await(task, 5_000)

      assert drain(reference).stdout == "done"
    after
      File.write!(release, "continue")
      Task.shutdown(task)
    end
  end

  test "raw frames preserve binary output and an unterminated line exactly" do
    reference = make_ref()
    command = ["/bin/sh", "-c", ~S(printf 'first\n\377\000tail'; printf 'warning\nend' >&2)]
    assert {:ok, expected} = Forcola.run(command, timeout_ms: 5_000)

    assert {:ok, ^expected} =
             Forcola.run(command, timeout_ms: 5_000, output_observer: {self(), reference})

    assert expected.stdout == <<"first\n", 255, 0, "tail">>
    assert expected.stderr == "warning\nend"
    assert drain(reference) == %{stdout: expected.stdout, stderr: expected.stderr}
  end

  test "merged stderr retains the final bytes and is observed only as stdout" do
    reference = make_ref()
    command = ["/bin/sh", "-c", "printf out; printf err >&2"]
    assert {:ok, expected} = Forcola.run(command, timeout_ms: 5_000, merge_stderr: true)

    assert {:ok, ^expected} =
             Forcola.run(command,
               timeout_ms: 5_000,
               merge_stderr: true,
               output_observer: {self(), reference}
             )

    assert expected.stdout == "outerr"
    assert expected.stderr == ""
    assert drain(reference) == %{stdout: "outerr", stderr: ""}
  end

  test "invalid observers are rejected before opening the shim or executing a child", %{
    tmp_dir: dir
  } do
    marker = Path.join(dir, "spawned")

    invalid = [
      false,
      {},
      {self(), :not_a_reference},
      {:not_a_pid, make_ref()},
      {remote_pid(), make_ref()}
    ]

    for observer <- invalid do
      assert_raise ArgumentError, ~r/output_observer/, fn ->
        Forcola.run(["/bin/sh", "-c", ~S(printf spawned > "$MARKER")],
          timeout_ms: 5_000,
          env: [{"MARKER", marker}],
          output_observer: observer
        )
      end

      assert_raise ArgumentError, ~r/output_observer/, fn ->
        Forcola.run(["true"],
          timeout_ms: 5_000,
          shim_path: "invalid-relative-shim",
          output_observer: observer
        )
      end
    end

    refute File.exists?(marker)
  end

  test "a dead observer is harmless and nil leaves observation disabled" do
    {pid, monitor} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    command = ["/bin/sh", "-c", "printf out; printf err >&2"]
    assert {:ok, expected} = Forcola.run(command, timeout_ms: 5_000)
    assert Forcola.run(command, timeout_ms: 5_000, output_observer: nil) == {:ok, expected}

    assert Forcola.run(command, timeout_ms: 5_000, output_observer: {pid, make_ref()}) ==
             {:ok, expected}
  end

  test "nonzero exits, signals, and spawn failures keep their existing result semantics" do
    for command <- [
          ["/bin/sh", "-c", "printf out; printf err >&2; exit 7"],
          ["/bin/sh", "-c", "printf before; kill -KILL $$"],
          ["/nonexistent/forcola-observer-command"]
        ] do
      reference = make_ref()
      expected = Forcola.run(command, timeout_ms: 5_000)
      observed = Forcola.run(command, timeout_ms: 5_000, output_observer: {self(), reference})
      assert observed == expected

      case observed do
        {:ok, result} ->
          assert drain(reference) == %{stdout: result.stdout, stderr: result.stderr}

        {:error, {:spawn, _reason}} ->
          assert drain(reference) == %{stdout: "", stderr: ""}
      end
    end
  end

  test "timeout keeps partial output and reaps the child process group", %{tmp_dir: dir} do
    pidfile = Path.join(dir, "pids")
    reference = make_ref()
    script = ~S(sleep 60 & echo "$$ $!" > "$PIDS"; printf 'partial'; printf 'warning' >&2; wait)

    assert {:error, {:timeout, %Forcola.Result{} = result}} =
             Forcola.run(["/bin/sh", "-c", script],
               timeout_ms: 300,
               kill_grace_ms: 300,
               env: [{"PIDS", pidfile}],
               output_observer: {self(), reference}
             )

    assert result.stdout == "partial"
    assert result.stderr == "warning"
    assert drain(reference) == %{stdout: "partial", stderr: "warning"}
    assert {:signal, signal} = result.status
    assert signal in [9, 15]
    [parent, child] = pidfile |> File.read!() |> String.split()
    refute alive?(parent)
    refute alive?(child)
  end

  defp await_stdout(reference, length, acc \\ "") do
    if byte_size(acc) >= length do
      acc
    else
      receive do
        {^reference, {:stdout, bytes}} -> await_stdout(reference, length, acc <> bytes)
      after
        5_000 -> flunk("observer received no output before the child finished")
      end
    end
  end

  defp drain(reference, acc \\ %{stdout: [], stderr: []}) do
    receive do
      {^reference, {channel, bytes}} when channel in [:stdout, :stderr] ->
        drain(reference, Map.update!(acc, channel, &[&1, bytes]))
    after
      0 -> Map.new(acc, fn {channel, bytes} -> {channel, IO.iodata_to_binary(bytes)} end)
    end
  end

  defp remote_pid do
    node_name = "observer-test@remote"

    :erlang.binary_to_term(
      <<131, 103, 100, byte_size(node_name)::16, node_name::binary, 1::32, 0::32, 0>>
    )
  end

  defp alive?(pid),
    do: match?({_output, 0}, System.cmd("kill", ["-0", pid], stderr_to_stdout: true))
end
