defmodule ForcolaEscript.CLI do
  @moduledoc false

  def main([private_parent]) do
    # A relocated escript has an archive entry, not an executable priv file.
    {:error, :not_found} = Forcola.Shim.path()

    with_shim(private_parent, fn shim ->
      opts = [shim_path: shim, kill_grace_ms: 1_000]
      bounded = Keyword.put(opts, :timeout_ms, 5_000)

      {:ok, %Forcola.Result{status: 0, stdout: "run\n", stderr: "err\n"}} =
        Forcola.run(["/bin/sh", "-c", "echo run; echo err >&2"], bounded)

      ["stream"] =
        ["/bin/echo", "stream"] |> Forcola.Stream.lines(bounded) |> Enum.to_list()

      duplex(opts)
      daemon(opts)
      timeout_group(private_parent, opts)
      cancel_group(private_parent, bounded)

      # An invalid explicit path must never fall back to another runner.
      {:error, {:spawn, {:invalid_shim_path, :enoent}}} =
        Forcola.run(["/bin/echo", "must not run"],
          shim_path: Path.join(private_parent, "missing"),
          timeout_ms: 5_000
        )

      IO.puts("all modes and group cleanup passed")
    end)
  end

  # This is the consumer-owned extraction pattern documented in the guide.
  defp with_shim(private_parent, fun) do
    {:ok, sections} = :escript.extract(:escript.script_name(), [])
    archive = Keyword.fetch!(sections, :archive)
    entry = ~c"forcola/priv/forcola_shim"
    {:ok, [{^entry, binary}]} = :zip.extract(archive, [:memory, {:file_list, [entry]}])

    unique = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
    directory = Path.join(private_parent, "forcola-" <> unique)
    File.mkdir!(directory)

    try do
      File.chmod!(directory, 0o700)
      shim = Path.join(directory, "forcola_shim")
      File.write!(shim, binary, [:binary, :exclusive])
      File.chmod!(shim, 0o500)
      fun.(shim)
    after
      File.rm_rf!(directory)
    end
  end

  defp duplex(opts) do
    {:ok, session} = Forcola.Duplex.open(["/bin/cat"], opts)

    try do
      :ok = Forcola.Duplex.send_line(session, "duplex")

      receive do
        {:forcola_line, ^session, "duplex"} -> :ok
      after
        5_000 -> raise "duplex did not echo"
      end
    after
      :ok = Forcola.Duplex.close(session)
    end
  end

  defp daemon(opts) do
    {:ok, daemon} =
      Forcola.Daemon.start_link(
        opts ++ [argv: ["/bin/sh", "-c", "echo daemon; read line"], output: {:send, self()}]
      )

    try do
      receive do
        {Forcola.Daemon, ^daemon, {:stdout, "daemon\n"}} -> :ok
      after
        5_000 -> raise "daemon did not start"
      end
    after
      GenServer.stop(daemon)
    end
  end

  defp timeout_group(directory, opts) do
    pid_file = Path.join(directory, "timeout-pids")
    script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; wait)

    {:error, {:timeout, %Forcola.Result{status: status}}} =
      Forcola.run(
        ["/bin/sh", "-c", script],
        opts ++ [timeout_ms: 1_000, env: [{"PID_FILE", pid_file}]]
      )

    if status == {:signal, :unconfirmed}, do: raise("timeout cleanup was unconfirmed")
    assert_group_dead(pid_file)
  end

  defp cancel_group(directory, opts) do
    pid_file = Path.join(directory, "cancel-pids")
    script = ~S(sleep 60 & echo "$$ $!" > "$PID_FILE"; echo ready; wait)

    ["ready"] =
      ["/bin/sh", "-c", script]
      |> Forcola.Stream.lines(opts ++ [env: [{"PID_FILE", pid_file}]])
      |> Enum.take(1)

    assert_group_dead(pid_file)
  end

  defp assert_group_dead(pid_file) do
    [parent, child] = pid_file |> File.read!() |> String.split()

    for pid <- [parent, child] do
      # System.cmd is only a liveness probe; every tested command uses Forcola.
      case System.cmd("kill", ["-0", pid], stderr_to_stdout: true) do
        {_, 0} -> raise "process #{pid} survived group cleanup"
        _ -> :ok
      end
    end
  end
end
