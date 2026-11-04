defmodule Forcola.Lock do
  @moduledoc """
  A whole-file POSIX `fcntl` write lock held for the life of an Elixir process.

  The BEAM cannot take `fcntl` record locks itself. `acquire/2` starts
  `forcola_shim lock <path>`, which opens or creates the file and calls
  `fcntl(F_SETLK)` with an `F_WRLCK` lock over the whole file (start 0,
  length 0). The shim holds the lock for as long as it runs. Other programs
  that lock the same file with `fcntl` or `lockf` see the conflict, and so
  does a second `acquire/2` from this or another BEAM.

      {:ok, lock} = Forcola.Lock.acquire("/run/user/1000/app/layer.lock")
      {:error, {:held, os_pid}} = Forcola.Lock.acquire("/run/user/1000/app/layer.lock")
      :ok = Forcola.Lock.release(lock)

  ## Lifetime

  The lock belongs to the process that called `acquire/2`. A small guard
  process owns the shim's port and monitors that caller:

    * `release/1` asks the shim to exit and waits for it, so the lock is
      free when `release/1` returns.
    * If the owning process exits for any reason, the guard closes the
      port, the shim reads EOF on stdin and exits, and the kernel releases
      the lock. This happens shortly after the owner's exit, not before it.
    * If the BEAM itself dies, the shim's stdin closes and the lock is
      released the same way.
    * If the shim dies while the lock is held (for example, it is killed),
      the lock is gone and the owner receives
      `{:forcola_lock_lost, lock, {:shim_exited, status}}`.

  The owner is not linked to the port or the guard, so neither can crash it.

  ## The lock file

  The file is opened with `O_RDWR | O_CREAT` and mode `0600` (before umask).
  It is never truncated, removed, or replaced, and its contents are never
  read; any existing contents stay as they are. Leaving the file in place
  after release is intended: removing a lock file races with other
  processes that have it open.

  `fcntl` locks are advisory and belong to an OS process. They are not
  inherited across `fork`, and only processes that ask for a lock see them.
  """

  alias Forcola.SpawnError

  @enforce_keys [:path, :os_pid, :guard, :ref]
  defstruct @enforce_keys

  @typedoc """
  A held lock. `os_pid` is the shim process holding it, which is the pid a
  conflicting `acquire/2` reports in `{:held, os_pid}`.
  """
  @type t :: %__MODULE__{
          path: String.t(),
          os_pid: pos_integer(),
          guard: pid(),
          ref: reference()
        }

  @typedoc "Reasons `acquire/2` can fail."
  @type error ::
          {:held, pos_integer() | nil}
          | {:open, atom() | String.t()}
          | {:lock, atom() | String.t()}
          | {:shim_exited, term()}
          | {:shim_start_failed, term()}
          | {:lock_process_exited, term()}
          | {:invalid_shim_path, atom()}
          | :not_found
          | :invalid_path
          | :invalid_reply
          | :timeout

  @default_timeout_ms 5_000
  @release_timeout_ms 5_000

  @doc """
  Takes the lock on `path` without waiting, or reports why it cannot.

  Returns:

    * `{:ok, lock}` - the lock is held until `release/1` or until the
      calling process exits.
    * `{:error, {:held, os_pid}}` - another process holds a conflicting
      lock. `os_pid` comes from `F_GETLK`, and is `nil` when the holder could
      not be identified because it released the lock while being queried.
    * `{:error, {:open, reason}}` - the file could not be opened or created.
      `reason` is a POSIX error atom such as `:enoent` or `:eacces`.
    * `{:error, {:lock, reason}}` - `fcntl` failed for a reason other than a
      conflicting lock, such as `:enolck` on a file system without lock
      support (reported as a string when the name is not a known atom).
    * `{:error, :invalid_path}` - `path` is not a non-empty binary without
      NUL bytes.
    * `{:error, :timeout}` - the shim did not answer within `:timeout_ms`.
    * `{:error, :not_found | {:invalid_shim_path, reason}}` - no usable
      shim binary, as in `Forcola.Shim.path/1`.
    * `{:error, {:shim_exited, status}}` or
      `{:error, {:shim_start_failed, reason}}` - the shim could not run.

  ## Options

    * `:timeout_ms` - how long to wait for the shim's answer, a
      non-negative integer. Defaults to #{@default_timeout_ms}. Other values
      raise `ArgumentError`.
    * `:shim_path` - trusted absolute shim path, as in `Forcola.run/2`.

  A relative `path` is resolved against the BEAM's current working directory.
  """
  @spec acquire(String.t(), keyword()) :: {:ok, t()} | {:error, error()}
  def acquire(path, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)

    unless is_integer(timeout_ms) and timeout_ms >= 0 do
      raise ArgumentError,
            ":timeout_ms must be a non-negative integer, got: #{inspect(timeout_ms)}"
    end

    with :ok <- validate_path(path),
         {:ok, bin} <- Forcola.Shim.path(opts) do
      start_guard(bin, path, timeout_ms)
    end
  end

  @doc """
  Releases the lock and waits for the shim to exit.

  Call it from the process that acquired the lock. It returns `:ok` once
  the lock is free, including when the lock was already released or lost.
  It returns `{:error, :timeout}` if the shim does not exit within five
  seconds of the request; the guard then closes the port, which ends the
  shim through stdin EOF shortly afterwards.
  """
  @spec release(t()) :: :ok | {:error, :timeout}
  def release(%__MODULE__{guard: guard, ref: ref}) do
    monitor = Process.monitor(guard)
    send(guard, {:release, self(), ref})

    receive do
      {^ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^guard, _reason} ->
        :ok
    end
  end

  defp validate_path(path) when is_binary(path) and byte_size(path) > 0 do
    if String.contains?(path, <<0>>), do: {:error, :invalid_path}, else: :ok
  end

  defp validate_path(_path), do: {:error, :invalid_path}

  defp start_guard(bin, path, timeout_ms) do
    owner = self()
    ref = make_ref()
    {guard, monitor} = spawn_monitor(fn -> guard_init(owner, ref, bin, path, timeout_ms) end)

    receive do
      {^ref, {:ok, os_pid}} ->
        Process.demonitor(monitor, [:flush])
        {:ok, %__MODULE__{path: path, os_pid: os_pid, guard: guard, ref: ref}}

      {^ref, {:error, _reason} = error} ->
        Process.demonitor(monitor, [:flush])
        error

      {:DOWN, ^monitor, :process, ^guard, reason} ->
        {:error, {:lock_process_exited, reason}}
    end
  end

  # Guard process: owns the port, so the shim lives exactly as long as it does.

  defp guard_init(owner, ref, bin, path, timeout_ms) do
    Process.flag(:trap_exit, true)
    owner_monitor = Process.monitor(owner)

    case open_port(bin, path) do
      {:ok, port, os_pid} ->
        state = %{
          port: port,
          owner: owner,
          owner_monitor: owner_monitor,
          ref: ref,
          handle: %__MODULE__{path: path, os_pid: os_pid, guard: self(), ref: ref}
        }

        deadline = System.monotonic_time(:millisecond) + timeout_ms
        await_answer(state, deadline, "")

      {:error, _reason} = error ->
        send(owner, {ref, error})
    end
  end

  defp open_port(bin, path) do
    port =
      Port.open({:spawn_executable, bin}, [
        :binary,
        :exit_status,
        :use_stdio,
        :hide,
        {:line, 256},
        args: ["lock", path]
      ])

    case Port.info(port, :os_pid) do
      {:os_pid, os_pid} -> {:ok, port, os_pid}
      nil -> {:error, {:shim_exited, :unknown}}
    end
  catch
    :error, reason -> {:error, {:shim_start_failed, reason}}
  end

  defp await_answer(%{port: port} = state, deadline, partial) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, {:eol, line}}} ->
        answer(state, partial <> line)

      {^port, {:data, {:noeol, chunk}}} ->
        await_answer(state, deadline, partial <> chunk)

      {^port, {:exit_status, status}} ->
        reply(state, {:error, {:shim_exited, status}})

      {:EXIT, ^port, reason} ->
        reply(state, {:error, {:shim_exited, reason}})

      {:DOWN, monitor, :process, _owner, _reason} when monitor == state.owner_monitor ->
        # The caller is gone; exiting closes the port and ends the shim.
        :ok
    after
      remaining ->
        close_port(port)
        reply(state, {:error, :timeout})
    end
  end

  defp answer(state, "ok") do
    reply(state, {:ok, state.handle.os_pid})
    hold(state)
  end

  defp answer(state, "held " <> pid), do: reply(state, {:error, {:held, parse_pid(pid)}})
  defp answer(state, "error usage"), do: reply(state, {:error, :invalid_path})

  defp answer(state, "error open " <> name),
    do: reply(state, {:error, {:open, SpawnError.reason(name)}})

  defp answer(state, "error lock " <> name),
    do: reply(state, {:error, {:lock, SpawnError.reason(name)}})

  defp answer(state, _line), do: reply(state, {:error, :invalid_reply})

  defp parse_pid(text) do
    case Integer.parse(text) do
      {pid, ""} when pid > 0 -> pid
      _unknown -> nil
    end
  end

  defp reply(state, result), do: send(state.owner, {state.ref, result})

  defp hold(%{port: port, ref: ref} = state) do
    receive do
      {:release, from, ^ref} ->
        send(from, {ref, release_port(port)})

      {:DOWN, monitor, :process, _owner, _reason} when monitor == state.owner_monitor ->
        # Returning exits the guard, which closes the port. The shim reads
        # EOF on stdin and exits, releasing the lock.
        :ok

      {^port, {:exit_status, status}} ->
        send(state.owner, {:forcola_lock_lost, state.handle, {:shim_exited, status}})

      {:EXIT, ^port, reason} ->
        send(state.owner, {:forcola_lock_lost, state.handle, {:shim_exited, reason}})

      {^port, {:data, _unexpected}} ->
        hold(state)
    end
  end

  defp release_port(port) do
    send_release(port)

    receive do
      {^port, {:exit_status, _status}} -> :ok
      {:EXIT, ^port, _reason} -> :ok
    after
      @release_timeout_ms ->
        close_port(port)
        {:error, :timeout}
    end
  end

  defp send_release(port) do
    Port.command(port, "release\n")
  catch
    :error, :badarg -> false
  end

  defp close_port(port) do
    Port.close(port)
  catch
    :error, :badarg -> true
  end
end
