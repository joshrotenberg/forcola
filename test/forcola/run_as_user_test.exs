defmodule Forcola.RunAsUserTest do
  # async: false -- these spawn real processes and touch the filesystem.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  # The user/uid the test suite runs as, resolved at runtime so the happy
  # path works on any CI account rather than a hardcoded name.
  defp current_username do
    {out, 0} = System.cmd("id", ["-un"])
    String.trim(out)
  end

  defp current_uid do
    {out, 0} = System.cmd("id", ["-u"])
    String.trim(out)
  end

  describe "run/2 :user happy path (no-op drop to the current user)" do
    test "requesting the current user by name runs the command" do
      me = current_username()

      assert {:ok, %Forcola.Result{status: 0, stdout: stdout}} =
               Forcola.run(["id", "-u"], timeout_ms: 5_000, user: me)

      assert String.trim(stdout) == current_uid()
    end

    test "requesting the current user by numeric uid runs the command" do
      uid = String.to_integer(current_uid())

      assert {:ok, %Forcola.Result{status: 0, stdout: stdout}} =
               Forcola.run(["id", "-u"], timeout_ms: 5_000, user: uid)

      assert String.trim(stdout) == current_uid()
    end
  end

  describe "run/2 :user fail-closed (non-root)" do
    @describetag :non_root

    setup do
      if current_uid() == "0" do
        # Running as root, the drop would actually succeed; skip the
        # fail-closed assertions that assume no privilege.
        {:ok, skip_fail_closed: true}
      else
        {:ok, skip_fail_closed: false}
      end
    end

    test "a different user fails closed and does not run the command", ctx do
      if ctx.skip_fail_closed do
        # Root can actually drop; nothing to prove here.
        assert true
      else
        %{tmp_dir: tmp_dir} = ctx
        marker = Path.join(tmp_dir, "should-not-exist")
        refute File.exists?(marker)

        # Target root: a non-root user cannot become uid 0, so the drop must
        # fail closed before exec and the marker must never be created.
        assert {:error, {:spawn, reason}} =
                 Forcola.run(["/bin/sh", "-c", "touch #{marker}"],
                   timeout_ms: 5_000,
                   user: "root"
                 )

        assert reason
        refute File.exists?(marker), "the command ran despite the failed privilege drop"
      end
    end

    test "an unknown username is a clean spawn error, no exec", ctx do
      %{tmp_dir: tmp_dir} = ctx
      marker = Path.join(tmp_dir, "unknown-user-marker")

      assert {:error, {:spawn, reason}} =
               Forcola.run(["/bin/sh", "-c", "touch #{marker}"],
                 timeout_ms: 5_000,
                 user: "forcola-no-such-user-zzz"
               )

      assert reason
      refute File.exists?(marker)
    end

    test "an unknown group is a clean spawn error, no exec", ctx do
      %{tmp_dir: tmp_dir} = ctx
      marker = Path.join(tmp_dir, "unknown-group-marker")

      assert {:error, {:spawn, reason}} =
               Forcola.run(["/bin/sh", "-c", "touch #{marker}"],
                 timeout_ms: 5_000,
                 group: "forcola-no-such-group-zzz"
               )

      assert reason
      refute File.exists?(marker)
    end
  end

  describe "Duplex :user fail-closed" do
    test "a different user surfaces a spawn_error and does not run the command", %{
      tmp_dir: tmp_dir
    } do
      if current_uid() == "0" do
        # Root can drop; skip.
        assert true
      else
        marker = Path.join(tmp_dir, "duplex-should-not-exist")
        refute File.exists?(marker)

        {:ok, session} =
          Forcola.Duplex.open(["/bin/sh", "-c", "touch #{marker}"], user: "root")

        assert_receive {:forcola_exit, ^session, {:spawn_error, reason}}, 10_000
        assert reason
        refute File.exists?(marker), "the command ran despite the failed privilege drop"
      end
    end

    test "requesting the current user runs the command", %{tmp_dir: tmp_dir} do
      me = current_username()
      done = Path.join(tmp_dir, "duplex-ran")

      {:ok, session} =
        Forcola.Duplex.open(["/bin/sh", "-c", "touch #{done}; exit 0"],
          user: me,
          kill_grace_ms: 1_000
        )

      assert_receive {:forcola_exit, ^session, 0}, 10_000
      assert eventually(fn -> File.exists?(done) end)
    end
  end

  @tag :root_only
  test "root can actually drop to nobody", %{tmp_dir: tmp_dir} do
    if current_uid() != "0" do
      # Only meaningful as root; stays green as non-root on CI. Skip LOUDLY:
      # print a clear reason rather than passing silently.
      IO.puts("SKIP: root-only privilege-drop test; not running as root")
      assert true
    else
      out_file = Path.join(tmp_dir, "dropped-uid")

      assert {:ok, %Forcola.Result{status: 0}} =
               Forcola.run(["/bin/sh", "-c", "id -u > #{out_file}"],
                 timeout_ms: 5_000,
                 user: "nobody"
               )

      dropped = out_file |> File.read!() |> String.trim()
      refute dropped == "0", "child still ran as root after requesting nobody"
    end
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
        Process.sleep(20)
        poll(fun, deadline)
    end
  end
end
