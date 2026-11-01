defmodule Forcola.SpawnErrorTest do
  use ExUnit.Case, async: false

  alias Forcola.{Daemon, Duplex, Shim, SpawnError}

  @moduletag :tmp_dir
  @moduletag :capture_log
  @fixture Path.expand("../fixtures/spawn_error_shim.py", __DIR__)
  @modes [:run, :duplex, :pull, :daemon, :daemon_ready, :stream]

  for scenario <- [:missing_cwd, :not_directory, :missing_binary, :not_executable, :unknown_user] do
    test "#{scenario} is typed in every mode with pipes and PTYs", %{tmp_dir: dir} do
      {argv, opts, stage, reason, path} = failure_case(unquote(scenario), dir)

      for pty <- [false, true], mode <- @modes do
        error = mode_error(mode, argv, [pty: pty] ++ opts)
        assert %SpawnError{stage: ^stage, reason: ^reason, path: ^path} = error
        assert error.message =~ "spawn failed: "
        assert is_nil(error.errno) or is_integer(error.errno)
      end

      refute File.exists?(Path.join(dir, "executed"))
    end
  end

  test "a failed child credential syscall is typed in every mode", %{tmp_dir: dir} do
    # gid_t(-1) is invalid even for root. Unprivileged callers can fail earlier
    # at setgroups with EPERM. Both paths run the actual child credential hook.
    for mode <- @modes, pty <- [false, true] do
      assert %SpawnError{stage: :privdrop, reason: reason, errno: errno, path: nil} =
               mode_error(
                 mode,
                 marker_argv(),
                 marker_opts(dir) ++ [group: 4_294_967_295, pty: pty]
               )

      assert reason in [:eperm, :einval]
      assert errno in [1, 22]
    end

    refute File.exists?(Path.join(dir, "executed"))
  end

  test "required cgroup setup failures are typed in every mode", %{tmp_dir: dir} do
    case Forcola.run(["/bin/true"], timeout_ms: 5_000, cgroup: :required) do
      {:error, {:spawn, _reason}} ->
        for mode <- @modes do
          assert %SpawnError{stage: :cgroup, message: message} =
                   mode_error(mode, marker_argv(), marker_opts(dir) ++ [cgroup: :required])

          assert message =~ "cgroup"
        end

        refute File.exists?(Path.join(dir, "executed"))

      {:ok, _result} ->
        IO.puts("SKIP unavailable cgroup setup: this host has a delegated cgroup")
    end
  end

  test "cgroup placement details reach every mode and pull terminal evidence", %{tmp_dir: dir} do
    # Native tests force the real placement write to fail. This fixture checks
    # that the same report survives every BEAM delivery path on all platforms.
    shim = fake_shim(dir, "cgroup_error")

    for mode <- @modes do
      assert %SpawnError{
               stage: :cgroup,
               reason: :eacces,
               errno: 13,
               path: "/test-cgroup/cgroup.procs",
               message: "spawn failed: Permission denied (os error 13)"
             } = mode_error(mode, ["unused"], shim_path: shim)
    end
  end

  test "legacy errors remain strings in every mode", %{tmp_dir: dir} do
    {argv, opts, _stage, _reason, _path} = failure_case(:missing_cwd, dir)
    typed = mode_error(:run, argv, opts)

    for mode <- @modes do
      assert mode_error(mode, argv, [structured_spawn_errors: false] ++ opts) == typed.message
    end
  end

  test "structured errors alone retain the legacy Run envelope", %{tmp_dir: dir} do
    {argv, opts, _stage, _reason, _path} = failure_case(:missing_cwd, dir)

    assert {:error, {:spawn, %SpawnError{stage: :cwd}}} =
             Forcola.run(argv, [timeout_ms: 5_000, structured_spawn_errors: true] ++ opts)

    assert {:error, {:spawn, {%SpawnError{stage: :cwd}, %Forcola.Result{}}}} =
             Forcola.run(
               argv,
               [timeout_ms: 5_000, structured_spawn_errors: true, max_output_bytes: 1024] ++ opts
             )
  end

  test "structured errors and identity compose without emitting identity on failure", %{
    tmp_dir: dir
  } do
    {argv, opts, _stage, _reason, _path} = failure_case(:missing_cwd, dir)

    for mode <- @modes do
      ref = make_ref()

      identity_opts =
        if mode in [:daemon, :daemon_ready, :stream],
          do: [spawn_observer: {self(), ref}],
          else: [spawn_identity: true]

      assert %SpawnError{stage: :cwd} = mode_error(mode, argv, identity_opts ++ opts)
      refute_receive {^ref, {:spawned, _}}, 0
      refute_receive {:forcola_spawned, _, _}, 0
    end
  end

  test "HELLO selects errors independently and alongside identity and bounded runs", %{
    tmp_dir: dir
  } do
    shim = fake_shim(dir, "cgroup_error")

    for opts <- [[], [spawn_identity: true], [spawn_identity: true, max_output_bytes: 1024]] do
      result =
        Forcola.run(
          ["unused"],
          opts ++ [timeout_ms: 5_000, structured_spawn_errors: true, shim_path: shim]
        )

      if opts[:max_output_bytes] do
        assert {:error, {:spawn, {%SpawnError{stage: :cgroup}, %Forcola.Result{}}}} = result
      else
        assert {:error, {:spawn, %SpawnError{stage: :cgroup}}} = result
      end

      hello = dir |> Path.join("hello_received") |> File.read!() |> :json.decode()
      expected = ["structured_spawn_errors_v1"]
      expected = if opts[:spawn_identity], do: ["spawn_identity_v1" | expected], else: expected
      assert Enum.sort(hello["features"]) == Enum.sort(expected)
    end
  end

  test "successful spawns compose structured errors and identity in every mode" do
    for identity? <- [false, true] do
      opts = [structured_spawn_errors: true, spawn_identity: identity?]

      assert {:ok, %Forcola.Result{status: 0, stdout: "ok\n"} = result} =
               Forcola.run(["/bin/echo", "ok"], [timeout_ms: 5_000] ++ opts)

      assert is_integer(result.pid) == identity?

      for delivery <- [:messages, :pull] do
        {:ok, session} = Duplex.open(["/bin/echo", "ok"], [delivery: delivery] ++ opts)

        try do
          if identity?, do: assert_receive({:forcola_spawned, ^session, _}, 5_000)
          if delivery == :pull, do: assert({:ok, {:stdout, "ok"}} = Duplex.recv(session, 5_000))

          assert {:ok, %Duplex.Terminal{status: 0, spawn_error: nil}} =
                   Duplex.await_terminal(session, 5_000)
        after
          Duplex.close(session)
        end
      end

      ref = make_ref()
      observer = if identity?, do: [spawn_observer: {self(), ref}], else: []

      assert ["ok"] ==
               ["/bin/echo", "ok"]
               |> Forcola.Stream.lines([timeout_ms: 5_000] ++ observer ++ opts)
               |> Enum.to_list()

      if identity?, do: assert_receive({^ref, {:spawned, _}}, 5_000)

      {:ok, daemon} =
        Daemon.start_link(
          [argv: ["/bin/echo", "ok"], output: {:send, self()}] ++ observer ++ opts
        )

      monitor = Process.monitor(daemon)
      assert_receive {Daemon, ^daemon, {:stdout, "ok\n"}}, 5_000
      if identity?, do: assert_receive({^ref, {:spawned, _}}, 5_000)
      assert_receive {:DOWN, ^monitor, :process, ^daemon, _}, 5_000
    end
  end

  test "errors-only negotiation stays within the Run and Stream startup budget", %{tmp_dir: dir} do
    shim = fake_shim(dir, "stall")
    opts = [timeout_ms: 100, structured_spawn_errors: true, shim_path: shim]

    assert {:error, {:spawn, :handshake_timeout}} = Forcola.run(["unused"], opts)

    error =
      assert_raise Forcola.Stream.Error, fn ->
        ["unused"] |> Forcola.Stream.lines(opts) |> Enum.to_list()
      end

    assert error.reason == :handshake_timeout
    refute File.exists?(Path.join(dir, "spawn_received"))
  end

  test "missing capabilities reject before SPAWN in every mode", %{tmp_dir: dir} do
    for fixture_mode <- ["legacy", "no_errors"] do
      shim = fake_shim(dir, fixture_mode)
      opts = [structured_spawn_errors: true, shim_path: shim]

      assert {:error, {:spawn, :incompatible_shim}} =
               Forcola.run(["unused"], [timeout_ms: 5_000] ++ opts)

      assert {:error, :incompatible_shim} = Duplex.open(["unused"], opts)

      error =
        assert_raise Forcola.Stream.Error, fn ->
          ["unused"]
          |> Forcola.Stream.lines([timeout_ms: 5_000] ++ opts)
          |> Enum.to_list()
        end

      assert error.reason == :incompatible_shim
      assert daemon_start_error(["unused"], opts) == {:spawn, :incompatible_shim}
      refute File.exists?(Path.join(dir, "spawn_received"))
    end
  end

  test "immediate Duplex shutdown retains a pending structured error", %{tmp_dir: dir} do
    shim = fake_shim(dir, "late_error")

    for delivery <- [:messages, :pull] do
      {:ok, session} =
        Duplex.open(["unused"],
          shim_path: shim,
          structured_spawn_errors: true,
          delivery: delivery,
          kill_grace_ms: 100
        )

      try do
        assert {:ok, %Duplex.Terminal{cause: :spawn_error}} = Duplex.shutdown(session)

        assert {:ok,
                %Duplex.Terminal{
                  cause: :spawn_error,
                  confirmation: :not_started,
                  spawn_error: %SpawnError{stage: :cgroup, reason: :eacces}
                }} = Duplex.await_terminal(session, 5_000)
      after
        Duplex.close(session)
      end
    end
  end

  test "decoder preserves unknown codes without creating atoms" do
    detail = %{stage: "exec", reason: "future_errno_103", errno: 9999, path: "binary"}
    payload = json(%{reason: "original message", spawn_error: detail})

    assert %SpawnError{
             stage: :exec,
             reason: "future_errno_103",
             errno: 9999,
             path: "binary",
             message: "original message"
           } = Shim.decode_error(payload, true)

    assert Shim.decode_error(payload) == "original message"
    assert_raise ArgumentError, fn -> String.to_existing_atom("future_errno_103") end
  end

  test "JSON null metadata becomes nil without inventing errno or path" do
    payload =
      ~s({"reason":"spawn failed: unknown user","spawn_error":{"stage":"privdrop","reason":"not_found","errno":null,"path":null}})

    assert %SpawnError{stage: :privdrop, reason: :not_found, errno: nil, path: nil} =
             Shim.decode_error(payload, true)
  end

  test "malformed structured errors are rejected without guessing from the message" do
    detail = %{stage: "cwd", reason: "enoent", errno: 2, path: "/missing"}

    for invalid <- [
          nil,
          %{},
          %{detail | stage: "future_stage_103"},
          %{detail | reason: 2},
          %{detail | errno: "ENOENT"},
          %{detail | path: 42}
        ] do
      assert Shim.decode_error(
               json(%{reason: "spawn failed: ENOENT", spawn_error: invalid}),
               true
             ) ==
               :invalid_error
    end

    assert Shim.decode_error("{broken", true) == :invalid_error
    assert Shim.decode_error(json(%{reason: 1}), true) == :invalid_error
    assert Shim.decode_error(json(%{reason: "legacy"})) == "legacy"
  end

  test "invalid options raise before a shim is opened" do
    for value <- [nil, :yes, 1, "true"] do
      opts = [structured_spawn_errors: value, shim_path: "/missing-shim"]

      assert_raise ArgumentError, ~r/structured_spawn_errors/, fn ->
        Forcola.run(["unused"], [timeout_ms: 5_000] ++ opts)
      end

      assert_raise ArgumentError, ~r/structured_spawn_errors/, fn ->
        Duplex.open(["unused"], opts)
      end

      assert_raise ArgumentError, ~r/structured_spawn_errors/, fn ->
        Daemon.start_link([argv: ["unused"]] ++ opts)
      end

      assert_raise ArgumentError, ~r/structured_spawn_errors/, fn ->
        Forcola.Stream.lines(["unused"], [timeout_ms: 5_000] ++ opts)
      end
    end
  end

  defp failure_case(:missing_cwd, dir),
    do:
      {marker_argv(), marker_opts(dir) ++ [cd: Path.join(dir, "missing")], :cwd, :enoent,
       Path.join(dir, "missing")}

  defp failure_case(:not_directory, dir) do
    path = Path.join(dir, "file")
    File.write!(path, "not a directory")
    {marker_argv(), marker_opts(dir) ++ [cd: path], :cwd, :enotdir, path}
  end

  defp failure_case(:missing_binary, dir) do
    path = Path.join(dir, "missing-binary")
    {[path], [], :exec, :enoent, path}
  end

  defp failure_case(:not_executable, dir) do
    path = Path.join(dir, "not-executable")
    File.write!(path, "#!/bin/sh\nprintf executed > \"$MARKER\"\n")
    File.chmod!(path, 0o644)
    {[path], marker_opts(dir), :exec, :eacces, path}
  end

  defp failure_case(:unknown_user, dir),
    do:
      {marker_argv(), marker_opts(dir) ++ [user: "forcola-no-such-user-103"], :privdrop,
       :not_found, nil}

  defp marker_argv, do: ["/bin/sh", "-c", ~S(printf executed > "$MARKER")]
  defp marker_opts(dir), do: [env: [{"MARKER", Path.join(dir, "executed")}]]

  defp mode_error(mode, argv, opts) do
    opts = Keyword.put_new(opts, :structured_spawn_errors, true)
    do_mode_error(mode, argv, opts)
  end

  defp do_mode_error(:run, argv, opts) do
    assert {:error, {:spawn, error}} = Forcola.run(argv, [timeout_ms: 5_000] ++ opts)
    error
  end

  defp do_mode_error(mode, argv, opts) when mode in [:duplex, :pull] do
    delivery = if mode == :pull, do: :pull, else: :messages
    assert {:ok, session} = Duplex.open(argv, [delivery: delivery] ++ opts)

    try do
      assert {:ok, %Duplex.Terminal{cause: :spawn_error, spawn_error: error}} =
               Duplex.await_terminal(session, 5_000)

      if mode == :duplex,
        do: assert_receive({:forcola_exit, ^session, {:spawn_error, ^error}}, 5_000)

      error
    after
      Duplex.close(session)
    end
  end

  defp do_mode_error(:stream, argv, opts) do
    error =
      assert_raise Forcola.Stream.Error, fn ->
        argv |> Forcola.Stream.lines([timeout_ms: 5_000] ++ opts) |> Enum.to_list()
      end

    assert is_binary(Exception.message(error))
    error.reason
  end

  defp do_mode_error(:daemon, argv, opts) do
    previous = Process.flag(:trap_exit, true)

    try do
      assert {:ok, daemon} = Daemon.start_link([argv: argv] ++ opts)
      assert_receive {:EXIT, ^daemon, {:spawn, error}}, 5_000
      error
    after
      Process.flag(:trap_exit, previous)
    end
  end

  defp do_mode_error(:daemon_ready, argv, opts) do
    assert {:exited_before_ready, {:spawn, error}} =
             daemon_start_error(argv, [ready: fn -> false end, ready_poll_ms: 10] ++ opts)

    error
  end

  defp daemon_start_error(argv, opts) do
    Task.async(fn ->
      Process.flag(:trap_exit, true)
      assert {:error, error} = Daemon.start_link([argv: argv] ++ opts)
      error
    end)
    |> Task.await(5_000)
  end

  defp fake_shim(dir, mode) do
    path = Path.join(dir, "shim.py")

    File.write!(
      path,
      String.replace(File.read!(@fixture), ~s(MODE = "cgroup_error"), ~s(MODE = "#{mode}"))
    )

    File.chmod!(path, 0o755)
    path
  end

  defp json(value), do: value |> :json.encode() |> IO.iodata_to_binary()
end
