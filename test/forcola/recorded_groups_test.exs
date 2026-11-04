defmodule Forcola.RecordedGroupsTest do
  # async: false -- these tests leak process groups on purpose and assert
  # on OS process liveness.
  use ExUnit.Case, async: false

  alias Forcola.{Duplex, Tracking}

  # Each group is recorded by one shim, which is then SIGKILLed so the group
  # leaks with nothing watching it. check/2 and stop/2 run in fresh shims
  # that hold only the recorded identities, as a restarted BEAM would.

  test "a leaked group is present, then stopped and confirmed gone" do
    {identity, _session} = leak(~S(echo ready; exec sleep 60))

    assert {:ok, [:present]} = Tracking.check([identity])

    assert {:ok, %{groups: [:gone], confirmation: :confirmed, scope: :process_group}} =
             Tracking.stop([identity], kill_grace_ms: 200)

    assert eventually(fn -> not alive?(identity.pid) end)
    assert {:ok, [:gone]} = Tracking.check([identity])
  end

  test "a leaderless group stays present until its last member is stopped" do
    # The leader exits once its stdin closes with the dead shim; the member
    # keeps the group alive without a leader.
    {identity, session} = leak(~S(sleep 60 & echo $!; read _))
    member = receive_line(session) |> String.to_integer()

    assert eventually(fn -> not alive?(identity.pid) end)
    assert alive?(member)
    assert {:ok, [:present]} = Tracking.check([identity])

    assert {:ok, %{groups: [:gone], confirmation: :confirmed}} =
             Tracking.stop([identity], kill_grace_ms: 200)

    assert eventually(fn -> not alive?(member) end)
  end

  test "stop sends INT, then TERM, then KILL to a group that ignores both" do
    {identity, session} = leak(~S(trap '' INT TERM; sleep 60 & echo $!; wait))
    member = receive_line(session) |> String.to_integer()

    started = System.monotonic_time(:millisecond)

    assert {:ok, %{groups: [:gone], confirmation: :confirmed}} =
             Tracking.stop([identity],
               interrupt_signal: :int,
               interrupt_grace_ms: 100,
               kill_grace_ms: 100
             )

    assert System.monotonic_time(:millisecond) - started >= 200
    assert eventually(fn -> not alive?(member) end)
  end

  test "a leader pid held by another process is gone and never signalled" do
    {:ok, session} =
      Duplex.open(["/bin/sh", "-c", "echo ready; exec sleep 60"], spawn_identity: true)

    try do
      identity = receive_identity(session)
      stale = %{identity | start_time: shift(identity.start_time)}

      assert {:ok, [:gone]} = Tracking.check([stale])

      assert {:ok, %{groups: [:gone], confirmation: :confirmed}} =
               Tracking.stop([stale], kill_grace_ms: 0)

      assert alive?(identity.pid), "a process with another start time was signalled"
    after
      Duplex.close(session)
    end
  end

  test "results follow input order and report identities the shim cannot judge" do
    {identity, _session} = leak(~S(echo ready; exec sleep 60))
    foreign = %{identity | start_time: foreign_start_time()}
    not_leader = %{identity | pgid: identity.pgid + 1}

    assert {:ok, [{:unknown, :foreign_identity}, :present, {:unknown, :not_a_group_leader}]} =
             Tracking.check([foreign, identity, not_leader])

    assert {:ok, %{groups: [_, :gone, _], confirmation: :unconfirmed}} =
             Tracking.stop([foreign, identity, not_leader], kill_grace_ms: 200)
  end

  test "empty input and invalid arguments" do
    assert {:ok, []} = Tracking.check([])
    assert {:ok, %{groups: [], confirmation: :confirmed}} = Tracking.stop([])

    assert_raise ArgumentError, fn -> Tracking.check(:nope) end
    assert_raise ArgumentError, fn -> Tracking.check([%{pid: 1}]) end

    assert_raise ArgumentError, fn ->
      Tracking.check([%{pid: 1, pgid: 1, start_time: %{kind: :macos, seconds: 1}}])
    end

    assert_raise ArgumentError, fn -> Tracking.stop([], kill_grace_ms: -1) end
    assert_raise ArgumentError, fn -> Tracking.stop([], interrupt_signal: :int) end

    assert {:error, {:invalid_shim_path, :not_absolute}} =
             Tracking.check([], shim_path: "relative")
  end

  # Opens a tracked session, records its root identity, then SIGKILLs the
  # shim so the group outlives it. Returns the identity and session.
  defp leak(script) do
    {:ok, session} = Duplex.open(["/bin/sh", "-c", script], spawn_identity: true)
    identity = receive_identity(session)
    shim = os_ppid(identity.pid)
    {_, 0} = System.cmd("kill", ["-KILL", shim])
    assert_receive {:forcola_exit, ^session, :shim_exited}, 10_000

    on_exit(fn ->
      System.cmd("kill", ["-KILL", "--", "-#{identity.pgid}"], stderr_to_stdout: true)
    end)

    {identity, session}
  end

  defp receive_identity(session) do
    receive do
      {:forcola_spawned, ^session, identity} -> identity
    after
      10_000 -> flunk("no spawn identity")
    end
  end

  defp receive_line(session) do
    receive do
      {:forcola_line, ^session, line} -> String.trim(line)
    after
      10_000 -> flunk("no line from the child")
    end
  end

  defp shift(%{kind: :linux, ticks: ticks} = start_time), do: %{start_time | ticks: ticks + 1}

  defp shift(%{kind: :macos, seconds: seconds} = start_time),
    do: %{start_time | seconds: seconds + 1}

  defp foreign_start_time do
    case :os.type() do
      {:unix, :darwin} -> %{kind: :linux, boot_id: "x", ticks: 1}
      _ -> %{kind: :macos, seconds: 1, microseconds: 0}
    end
  end

  defp os_ppid(pid) do
    {out, 0} = System.cmd("ps", ["-o", "ppid=", "-p", Integer.to_string(pid)])
    String.trim(out)
  end

  defp alive?(pid) do
    {output, status} =
      System.cmd("ps", ["-o", "stat=", "-p", Integer.to_string(pid)], stderr_to_stdout: true)

    status == 0 and String.trim(output) != "" and
      not String.starts_with?(String.trim(output), "Z")
  end

  defp eventually(fun, attempts \\ 250) do
    cond do
      fun.() ->
        true

      attempts > 0 ->
        Process.sleep(20)
        eventually(fun, attempts - 1)

      true ->
        false
    end
  end
end
