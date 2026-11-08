defmodule Forcola.LockTest do
  use ExUnit.Case, async: true

  alias Forcola.Lock

  @moduletag :tmp_dir

  test "a second acquire on the same path reports the holder", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "layer.lock")

    assert {:ok, %Lock{os_pid: holder} = lock} = Lock.acquire(path)
    assert {:error, {:held, ^holder}} = Lock.acquire(path)

    # A different process sees the same conflict.
    task = Task.async(fn -> Lock.acquire(path) end)
    assert {:error, {:held, ^holder}} = Task.await(task)

    assert :ok = Lock.release(lock)
  end

  test "release frees the lock before returning", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "layer.lock")

    assert {:ok, first} = Lock.acquire(path)
    assert :ok = Lock.release(first)
    assert {:ok, second} = Lock.acquire(path)
    assert second.os_pid != first.os_pid
    assert :ok = Lock.release(second)

    # Releasing again is harmless.
    assert :ok = Lock.release(second)
  end

  test "the lock is released after the owning process is killed", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "layer.lock")
    test_pid = self()

    owner =
      spawn(fn ->
        send(test_pid, {:acquired, Lock.acquire(path)})
        Process.sleep(:infinity)
      end)

    assert_receive {:acquired, {:ok, %Lock{os_pid: holder, guard: guard}}}, 5_000
    assert {:error, {:held, ^holder}} = Lock.acquire(path)

    guard_monitor = Process.monitor(guard)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^guard_monitor, :process, ^guard, _reason}, 5_000

    # The shim exits on stdin EOF just after the guard closes the port.
    assert {:ok, lock} = eventually_acquire(path)
    assert :ok = Lock.release(lock)
    assert eventually(fn -> not os_alive?(holder) end)
  end

  test "the owner is told when the shim holding its lock dies", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "layer.lock")

    assert {:ok, %Lock{os_pid: holder} = lock} = Lock.acquire(path)
    {_, 0} = System.cmd("kill", ["-KILL", Integer.to_string(holder)])

    assert_receive {:forcola_lock_lost, ^lock, {:shim_exited, _status}}, 5_000
    assert :ok = Lock.release(lock)
    assert {:ok, again} = Lock.acquire(path)
    assert :ok = Lock.release(again)
  end

  test "the lock file is created once and never modified", %{tmp_dir: tmp_dir} do
    fresh = Path.join(tmp_dir, "fresh.lock")
    assert {:ok, lock} = Lock.acquire(fresh)
    assert File.regular?(fresh)
    assert :ok = Lock.release(lock)
    assert File.read!(fresh) == ""

    existing = Path.join(tmp_dir, "existing.lock")
    File.write!(existing, "pid 123\nkeep me\n")
    %File.Stat{inode: inode} = File.stat!(existing)

    assert {:ok, lock} = Lock.acquire(existing)
    assert {:error, {:held, _}} = Lock.acquire(existing)
    assert :ok = Lock.release(lock)

    assert File.read!(existing) == "pid 123\nkeep me\n"
    assert %File.Stat{inode: ^inode} = File.stat!(existing)
  end

  test "an unopenable path reports the open errno", %{tmp_dir: tmp_dir} do
    path = Path.join([tmp_dir, "missing", "layer.lock"])
    assert {:error, {:open, :enoent}} = Lock.acquire(path)
    refute File.exists?(path)
  end

  test "invalid paths and options are rejected before the shim runs" do
    assert {:error, :invalid_path} = Lock.acquire("")
    assert {:error, :invalid_path} = Lock.acquire("bad\0path")
    assert {:error, :invalid_path} = Lock.acquire(~c"charlist")
    assert_raise ArgumentError, fn -> Lock.acquire("x.lock", timeout_ms: -1) end

    assert {:error, {:invalid_shim_path, :not_absolute}} =
             Lock.acquire("x.lock", shim_path: "rel")
  end

  # A shim that answers `held <pid>` or an error exits at once, and on Linux
  # its port has been seen to close before an os pid lookup made right after
  # `Port.open/2`, leaving the answer unread. These stubs answer and exit
  # immediately; the result must not depend on whether the port is still
  # open when the guard looks at it.
  test "the answer of a shim that has already exited is still read", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "layer.lock")

    cases = [
      {"held 12345", 3, {:error, {:held, 12_345}}},
      {"error open enoent", 4, {:error, {:open, :enoent}}},
      {"error lock enolck", 5, {:error, {:lock, :enolck}}}
    ]

    for {line, status, expected} <- cases do
      shim = stub_shim(tmp_dir, "echo '#{line}'\nexit #{status}\n")

      for attempt <- 1..20 do
        assert Lock.acquire(path, shim_path: shim) == expected,
               "#{line} (attempt #{attempt})"
      end
    end
  end

  test "a shim that answers ok and exits never yields a handle without a pid",
       %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "layer.lock")
    shim = stub_shim(tmp_dir, "echo ok\nexit 0\n")

    for _attempt <- 1..20 do
      case Lock.acquire(path, shim_path: shim) do
        {:error, {:shim_exited, _status}} ->
          :ok

        {:ok, %Lock{os_pid: os_pid} = lock} ->
          assert is_integer(os_pid) and os_pid > 0
          assert_receive {:forcola_lock_lost, ^lock, {:shim_exited, _status}}, 5_000
          assert :ok = Lock.release(lock)
      end
    end
  end

  defp eventually_acquire(path, attempts \\ 100) do
    case Lock.acquire(path) do
      {:error, {:held, _}} when attempts > 0 ->
        Process.sleep(20)
        eventually_acquire(path, attempts - 1)

      result ->
        result
    end
  end

  defp eventually(fun, attempts \\ 100) do
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

  defp stub_shim(dir, body) do
    shim = Path.join(dir, "stub_shim_#{System.unique_integer([:positive])}")
    File.write!(shim, "#!/bin/sh\n" <> body)
    File.chmod!(shim, 0o755)
    shim
  end

  defp os_alive?(pid) do
    {_, status} = System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true)
    status == 0
  end
end
