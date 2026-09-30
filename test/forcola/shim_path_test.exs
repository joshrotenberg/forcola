defmodule Forcola.ShimPathTest do
  use ExUnit.Case, async: true

  alias Forcola.Shim

  @moduletag :tmp_dir
  @moduletag :capture_log

  setup %{tmp_dir: tmp_dir} do
    {:ok, bundled} = Shim.path()
    shim_path = Path.join(tmp_dir, "forcola_shim")
    File.cp!(bundled, shim_path)
    File.chmod!(shim_path, 0o700)
    %{shim_path: shim_path}
  end

  test "default discovery and execution remain unchanged" do
    assert {:ok, path} = Shim.path()
    assert path == Application.app_dir(:forcola, "priv/forcola_shim")

    assert {:ok, %{status: 0, stdout: "bundled\n"}} =
             Forcola.run(["/bin/echo", "bundled"], timeout_ms: 5_000)
  end

  test "run uses a trusted shim override", %{shim_path: shim_path} do
    assert {:ok, ^shim_path} = Shim.path(shim_path: shim_path)

    assert {:ok, %{status: 0, stdout: "override\n"}} =
             Forcola.run(["/bin/echo", "override"],
               shim_path: shim_path,
               timeout_ms: 5_000
             )
  end

  test "stream uses a trusted shim override", %{shim_path: shim_path} do
    assert ["override"] =
             Forcola.Stream.lines(["/bin/echo", "override"],
               shim_path: shim_path,
               timeout_ms: 5_000
             )
             |> Enum.to_list()
  end

  test "duplex uses a trusted shim override", %{shim_path: shim_path} do
    assert {:ok, session} =
             Forcola.Duplex.open(["/bin/cat"], shim_path: shim_path, kill_grace_ms: 100)

    try do
      assert :ok = Forcola.Duplex.send_line(session, "override")
      assert_receive {:forcola_line, ^session, "override"}, 5_000
      assert :ok = Forcola.Duplex.send_eof(session)
      assert_receive {:forcola_exit, ^session, 0}, 5_000
    after
      Forcola.Duplex.close(session)
    end
  end

  test "daemon uses a trusted shim override", %{shim_path: shim_path} do
    assert {:ok, daemon} =
             Forcola.Daemon.start_link(
               argv: ["/bin/sh", "-c", "echo override; exec sleep 60"],
               shim_path: shim_path,
               output: {:send, self()},
               kill_grace_ms: 100
             )

    try do
      assert_receive {Forcola.Daemon, ^daemon, {:stdout, "override\n"}}, 5_000
    after
      GenServer.stop(daemon, :normal, 5_000)
    end
  end

  test "trusted symlinks are followed", %{tmp_dir: tmp_dir, shim_path: shim_path} do
    link = Path.join(tmp_dir, "shim_link")
    File.ln_s!(shim_path, link)
    assert {:ok, ^link} = Shim.path(shim_path: link)

    assert {:ok, %{status: 0}} =
             Forcola.run(["/bin/echo", "symlink"], shim_path: link, timeout_ms: 5_000)
  end

  test "invalid overrides fail without falling back in every mode", %{
    tmp_dir: tmp_dir,
    shim_path: shim_path
  } do
    # A daemon is linked to its caller, including failed starts.
    Process.flag(:trap_exit, true)
    File.chmod!(shim_path, 0o600)

    for {path, reason} <- [
          {nil, :not_a_binary},
          {~c"/tmp/forcola_shim", :not_a_binary},
          {"relative/shim", :not_absolute},
          {Path.join(tmp_dir, "missing"), :enoent},
          {tmp_dir, :not_regular},
          {shim_path, :not_executable}
        ] do
      assert_all_modes_fail(path, {:invalid_shim_path, reason})
    end
  end

  test "an executable with an invalid format fails without running the command", %{
    tmp_dir: tmp_dir
  } do
    invalid = Path.join(tmp_dir, "invalid_shim")
    File.write!(invalid, "not an executable\n")
    File.chmod!(invalid, 0o700)

    # Some port implementations report exec failure synchronously; others
    # open a port and report its exit before any shim protocol frame arrives.
    # Repeat to exercise the race between sending SPAWN and port termination.
    for _ <- 1..25 do
      assert {:error, {:spawn, reason}} =
               Forcola.run(["/bin/echo", "unexpected"], shim_path: invalid, timeout_ms: 5_000)

      case reason do
        {:shim_start_failed, os_reason} ->
          assert os_reason in [:enoexec, :eacces, :shim_exited]

        {:shim_exited, result} ->
          assert result.status == {:signal, :unconfirmed}
          assert result.stdout == ""
      end
    end
  end

  test "the guard owns the port and exits when the caller closes it", %{
    shim_path: shim_path
  } do
    owner = self()
    assert {:ok, port} = Shim.open(shim_path: shim_path)
    assert {:connected, guard} = Port.info(port, :connected)
    assert {:links, [^guard]} = Port.info(port, :links)
    refute guard == owner
    monitor = Process.monitor(guard)

    assert :ok = Shim.close(port)
    assert_receive {:DOWN, ^monitor, :process, ^guard, :normal}, 5_000
    refute Shim.send_frame(port, Shim.tag_eof())
    refute_receive {^port, _event}
  end

  test "repeated runs and streams leave no relayed messages in their caller", %{
    shim_path: shim_path
  } do
    for _ <- 1..15 do
      assert {:ok, %{stdout: "run\n"}} =
               Forcola.run(["/bin/echo", "run"], shim_path: shim_path, timeout_ms: 5_000)

      assert ["stream"] =
               Forcola.Stream.lines(["/bin/echo", "stream"],
                 shim_path: shim_path,
                 timeout_ms: 5_000
               )
               |> Enum.to_list()
    end

    {:messages, messages} = Process.info(self(), :messages)
    refute Enum.any?(messages, &match?({port, _event} when is_port(port), &1))
  end

  test "a broken shim's closed stdin cannot kill its caller", %{tmp_dir: tmp_dir} do
    broken = Path.join(tmp_dir, "closed_stdin_shim")
    # Close the read end before emitting two valid STDOUT frames. The first
    # is a barrier before the test writes; the second must arrive before the
    # resulting transport error, preserving output and error ordering.
    File.write!(broken, """
    #!/bin/sh
    exec 0<&-
    printf '\\000\\000\\000\\002\\021a\\000\\000\\000\\002\\021b'
    exec sleep 30
    """)

    File.chmod!(broken, 0o700)
    assert {:ok, port} = Shim.open(shim_path: broken)
    {:os_pid, os_pid} = Port.info(port, :os_pid)

    try do
      assert_receive {^port, {:data, <<0x11, "a">>}}, 5_000
      assert is_boolean(Shim.send_frame(port, Shim.tag_spawn(), "{}"))
      assert next_port_event(port) == {:data, <<0x11, "b">>}
      assert next_port_event(port) == {:exit_status, {:shim_error, :epipe}}
      refute Shim.send_frame(port, Shim.tag_eof())
    after
      # This deliberately invalid helper cannot perform native shim cleanup.
      System.cmd("kill", ["-KILL", Integer.to_string(os_pid)], stderr_to_stdout: true)
      Shim.close(port)
    end
  end

  defp next_port_event(port) do
    receive do
      {^port, event} -> event
    after
      5_000 -> flunk("shim did not report its next event")
    end
  end

  defp assert_all_modes_fail(path, reason) do
    assert {:error, ^reason} = Shim.open(shim_path: path)

    assert {:error, {:spawn, ^reason}} =
             Forcola.run(["/bin/echo", "unexpected"], shim_path: path, timeout_ms: 5_000)

    error =
      assert_raise Forcola.Stream.Error, fn ->
        Forcola.Stream.lines(["/bin/echo", "unexpected"],
          shim_path: path,
          timeout_ms: 5_000
        )
        |> Enum.to_list()
      end

    assert error.reason == reason
    assert {:error, ^reason} = Forcola.Duplex.open(["/bin/cat"], shim_path: path)

    assert {:error, {:spawn, ^reason}} =
             Forcola.Daemon.start_link(argv: ["/bin/cat"], shim_path: path)
  end
end
