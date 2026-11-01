defmodule Forcola.TrackingTest do
  use ExUnit.Case, async: false

  alias Forcola.{Daemon, Duplex, Result}

  @moduletag :tmp_dir
  @moduletag :capture_log
  @modes [:run, :stream, :stream_bounded, :duplex, :duplex_pull, :daemon]
  @tree Path.expand("../fixtures/tracked_tree.py", __DIR__)
  @shim Path.expand("../fixtures/tracking_shim.py", __DIR__)

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  test "every mode cleans escaped groups on cancellation, normal exit and root SIGKILL", %{
    tmp_dir: dir
  } do
    for mode <- @modes, kind <- ["setsid", "setpgid"], trigger <- [:close, :exit, :kill] do
      child = tree(dir, kind)
      ref = make_ref()

      handle =
        start(
          mode,
          child.argv,
          opts(ref, timeout_ms: if(mode == :run and trigger == :close, do: 1_500, else: 20_000))
        )

      try do
        root = await_pid(child, "root")
        descendant = await_pid(child, "descendant")
        assert_first_tracked(ref, root)
        assert_tracked(ref, descendant)
        assert alive?(descendant)
        trigger(handle, trigger, child, root)
        outcome = finish(handle, trigger)
        assert_tracking_exit(ref, scope(), :confirmed, {mode, kind, trigger})
        assert_gone(root)
        assert_gone(descendant)
        assert_history(outcome, [root, descendant])
      after
        stop(handle)
      end
    end
  end

  test "PTY cleanup reaches sampled descendant groups in every mode", %{tmp_dir: dir} do
    for mode <- @modes do
      child = tree(dir, "setsid")
      ref = make_ref()
      handle = start(mode, child.argv, opts(ref, pty: true))

      try do
        root = await_pid(child, "root")
        descendant = await_pid(child, "descendant")
        assert_first_tracked(ref, root)
        assert_tracked(ref, descendant)

        if mode == :duplex_pull do
          {:duplex, _, session} = handle
          assert {:ok, _} = Duplex.recv(session, 5_000)
        end

        trigger(handle, :exit, child, root)
        outcome = finish(handle, :exit)
        assert_tracking_exit(ref, scope(), :confirmed)
        assert_gone(root)
        assert_gone(descendant)
        assert_history(outcome, [root, descendant])
      after
        stop(handle)
      end
    end
  end

  test "new groups are reported while alive, deduplicated and retained for polling", %{
    tmp_dir: dir
  } do
    for mode <- @modes do
      child = tree(dir, "setsid")
      ref = make_ref()
      handle = start(mode, child.argv, opts(ref))

      try do
        root = await_pid(child, "root")
        descendant = await_pid(child, "descendant")
        assert_first_tracked(ref, root)
        assert_tracked(ref, descendant)
        File.write!(Path.join(child.dir, "spawn_second"), "go")
        second = await_pid(child, "second")
        assert_tracked(ref, second)
        assert alive?(second)
        assert_live_history(handle, [root, descendant, second])
        refute_receive {^ref, {:tracked, _}}, 100
        trigger(handle, :exit, child, root)
        outcome = finish(handle, :exit)
        assert_tracking_exit(ref, scope(), :confirmed)
        assert_history(outcome, [root, descendant, second])
        Enum.each([root, descendant, second], &assert_gone/1)
      after
        stop(handle)
      end
    end
  end

  test "every tracked group receives the configured INT before TERM", %{tmp_dir: dir} do
    for mode <- @modes do
      child = tree(dir, "setsid")
      ref = make_ref()

      handle =
        start(
          mode,
          child.argv,
          opts(ref,
            interrupt_signal: :int,
            interrupt_grace_ms: 150,
            timeout_ms: if(mode == :run, do: 1_500, else: 20_000)
          )
        )

      try do
        root = await_pid(child, "root")
        descendant = await_pid(child, "descendant")
        assert_first_tracked(ref, root)
        assert_tracked(ref, descendant)
        started_at = System.monotonic_time()
        trigger(handle, :close, child, root)
        finish(handle, :close)

        if mode != :run do
          assert System.monotonic_time() - started_at >=
                   System.convert_time_unit(250, :millisecond, :native)
        end

        assert_tracking_exit(ref, scope(), :confirmed, {:interrupt, mode})
        Enum.each([root, descendant], &assert_gone/1)

        for label <- ["root", "descendant"] do
          assert ["2 " <> int_at, "15 " <> term_at] =
                   child.dir
                   |> Path.join(label <> ".signals")
                   |> File.read!()
                   |> String.split("\n", trim: true)

          # Handler scheduling can shorten the observed interval between signals.
          # Check ordering here; native model tests verify per-target deadlines.
          assert String.to_integer(term_at) >= String.to_integer(int_at)
        end
      after
        stop(handle)
      end
    end
  end

  test "owner death cleans the escaped group in every mode", %{tmp_dir: dir} do
    parent = self()

    for mode <- [:run, :stream, :duplex, :daemon] do
      child = tree(dir, "setsid")
      ref = make_ref()

      owner =
        spawn(fn ->
          case mode do
            :run ->
              Forcola.run(child.argv, opts(ref, track_observer: {parent, ref}))

            :stream ->
              child.argv
              |> Forcola.Stream.lines(opts(ref, track_observer: {parent, ref}))
              |> Enum.to_list()

            :duplex ->
              {:ok, _} =
                Duplex.open(
                  child.argv,
                  Keyword.delete(opts(ref, track_observer: {parent, ref}), :timeout_ms)
                )

              Process.sleep(:infinity)

            :daemon ->
              {:ok, _} =
                Daemon.start_link(
                  [argv: child.argv] ++
                    Keyword.delete(opts(ref, track_observer: {parent, ref}), :timeout_ms)
                )

              Process.sleep(:infinity)
          end
        end)

      try do
        root = await_pid(child, "root")
        descendant = await_pid(child, "descendant")
        assert_first_tracked(ref, root)
        assert_tracked(ref, descendant)
        Process.exit(owner, :kill)
        Enum.each([root, descendant], &assert_gone/1)
      after
        Process.exit(owner, :kill)
      end
    end
  end

  test "SIGKILL of a separate BEAM cleans tracked groups in every mode", %{tmp_dir: dir} do
    for mode <- [:run, :stream, :duplex, :daemon] do
      child = tree(dir, "setsid")
      beam_file = Path.join(child.dir, "beam")
      tracked_file = Path.join(child.dir, "observed")
      options = [track_descendants: true, kill_grace_ms: 100]

      invocation =
        case mode do
          :run ->
            "Forcola.run(#{inspect(child.argv)}, opts ++ [timeout_ms: 60_000])"

          :stream ->
            "Forcola.Stream.lines(#{inspect(child.argv)}, opts ++ [timeout_ms: 60_000]) |> Enum.to_list()"

          :duplex ->
            "{:ok, _} = Forcola.Duplex.open(#{inspect(child.argv)}, opts); Process.sleep(:infinity)"

          :daemon ->
            "{:ok, _} = Forcola.Daemon.start_link(opts ++ [argv: #{inspect(child.argv)}]); Process.sleep(:infinity)"
        end

      code = """
      File.write!(#{inspect(beam_file)}, System.pid())
      ref = make_ref()
      observer = spawn(fn ->
        loop = fn loop ->
          receive do
            {^ref, {:tracked, identity}} -> File.write!(#{inspect(tracked_file)}, Integer.to_string(identity.pid) <> "\\n", [:append]); loop.(loop)
          end
        end
        loop.(loop)
      end)
      opts = #{inspect(options)} ++ [track_observer: {observer, ref}]
      #{invocation}
      """

      port =
        Port.open({:spawn_executable, System.find_executable("elixir")}, [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: [
            "--erl",
            "+S 2:2",
            "-pa",
            :code.lib_dir(:forcola) |> List.to_string() |> Path.join("ebin"),
            "-e",
            code
          ]
        ])

      try do
        root = await_pid(child, "root")
        descendant = await_pid(child, "descendant")

        assert eventually(fn ->
                 case File.read(tracked_file) do
                   {:ok, text} -> Integer.to_string(descendant) in String.split(text)
                   _ -> false
                 end
               end)

        beam = beam_file |> File.read!() |> String.trim()
        assert {_, 0} = System.cmd("kill", ["-KILL", beam], stderr_to_stdout: true)
        assert_receive {^port, {:exit_status, _}}, 5_000
        Enum.each([root, descendant], &assert_gone/1)
      after
        if File.exists?(beam_file), do: kill_owned(File.read!(beam_file), child.dir)

        if Port.info(port), do: Port.close(port)
      end
    end
  end

  test "tracking validation precedes opening the shim and defaults remain opt-in" do
    for mode <- @modes,
        invalid <- [
          [track_descendants: nil],
          [track_descendants: :yes],
          [track_observer: self()],
          [track_observer: {self(), :bad}]
        ] do
      assert_raise ArgumentError, fn ->
        options = invalid ++ [timeout_ms: 1_000, shim_path: "relative"]

        if mode == :run,
          do: Forcola.run(["unused"], options),
          else: start(mode, ["unused"], options)
      end
    end

    assert {:ok, %Result{tracked_groups: [], stdout: "plain\n"}} =
             Forcola.run(["/bin/echo", "plain"], timeout_ms: 5_000)
  end

  test "observer enables tracking and Stream also tracks without an observer", %{tmp_dir: dir} do
    for mode <- @modes do
      ref = make_ref()

      handle =
        start(
          mode,
          ["unused"],
          opts(ref, track_descendants: false, shim_path: fake_shim(dir, "valid"))
        )

      try do
        finish(handle, :exit)
        assert_first_tracked(ref, 123)
        assert_tracked(ref, 456)
        assert_tracking_exit(ref, :tree_sampled, :confirmed)
      after
        stop(handle)
      end
    end

    child = tree(dir, "setsid")

    handle =
      start(:stream, child.argv, track_descendants: true, timeout_ms: 20_000, kill_grace_ms: 100)

    try do
      root = await_pid(child, "root")
      descendant = await_pid(child, "descendant")
      # Without an observer, allow several native sampling intervals before halt.
      Process.sleep(250)
      trigger(handle, :close, child, root)
      finish(handle, :close)
      Enum.each([root, descendant], &assert_gone/1)
    after
      stop(handle)
    end
  end

  test "idle and output bounds clean tracked groups", %{tmp_dir: dir} do
    for {mode, extra, command} <- [
          {:stream, [idle_timeout_ms: 400], "quiet"},
          {:stream_bounded, [idle_timeout_ms: 400], "quiet"},
          {:stream_bounded, [max_line_bytes: 512], "flood"},
          {:duplex_pull, [max_line_bytes: 512], "flood"},
          {:run, [max_output_bytes: 1_024], "flood"}
        ] do
      child = tree(dir, "setsid")
      ref = make_ref()
      handle = start(mode, child.argv, opts(ref, extra))

      try do
        root = await_pid(child, "root")
        descendant = await_pid(child, "descendant")
        assert_first_tracked(ref, root)
        assert_tracked(ref, descendant)
        File.write!(Path.join(child.dir, command), "go")
        outcome = finish(handle, :exit)
        assert_tracking_exit(ref, scope(), :confirmed)
        assert_bound_reason(outcome, command)
        Enum.each([root, descendant], &assert_gone/1)
      after
        stop(handle)
      end
    end
  end

  test "Daemon readiness timeout drains tracking frames and cleans escaped groups", %{
    tmp_dir: dir
  } do
    child = tree(dir, "setsid")
    ref = make_ref()
    observer = self()

    task =
      Task.async(fn ->
        Process.flag(:trap_exit, true)

        Daemon.start_link(
          argv: child.argv,
          ready: fn -> false end,
          ready_timeout_ms: 1_000,
          ready_poll_ms: 20,
          track_observer: {observer, ref},
          kill_grace_ms: 100
        )
      end)

    try do
      root = await_pid(child, "root")
      descendant = await_pid(child, "descendant")
      assert_first_tracked(ref, root)
      assert_tracked(ref, descendant)
      assert {:error, :ready_timeout} = Task.await(task, 10_000)
      assert_tracking_exit(ref, scope(), :confirmed)
      Enum.each([root, descendant], &assert_gone/1)
    after
      if Process.alive?(task.pid), do: Task.shutdown(task, :brutal_kill)
    end
  end

  test "incompatible tracking capability fails before SPAWN in every mode", %{tmp_dir: dir} do
    for mode <- @modes do
      path = fake_shim(dir, "no_tracking")
      ref = make_ref()
      assert_start_error(mode, opts(ref, shim_path: path), :incompatible_shim)
      refute File.exists?(Path.join(dir, "spawn"))
    end
  end

  test "tracking composes with selected extensions and requirement-only capabilities", %{
    tmp_dir: dir
  } do
    for mode <- @modes do
      ref = make_ref()

      options =
        opts(ref,
          shim_path: fake_shim(dir, "valid"),
          spawn_identity: true,
          structured_spawn_errors: true,
          env_mode: :replace,
          env: [{"REMOVE", nil}]
        )

      options =
        if mode in [:stream, :stream_bounded],
          do: Keyword.put(options, :spawn_observer, {self(), make_ref()}),
          else: options

      handle = start(mode, ["unused"], options)

      try do
        finish(handle, :exit)
        assert_tracking_exit(ref, :tree_sampled, :confirmed)

        features =
          dir |> Path.join("hello") |> File.read!() |> :json.decode() |> Map.fetch!("features")

        assert Enum.sort(features) == [
                 "spawn_identity_v1",
                 "structured_spawn_errors_v1",
                 "track_descendants_v1"
               ]

        assert_first_tracked(ref, 123)
        assert_tracked(ref, 456)
        refute_receive {^ref, {:tracked, _}}, 20
      after
        stop(handle)
      end
    end
  end

  test "malformed or missing tracking evidence never confirms tree cleanup", %{tmp_dir: dir} do
    for mode <- @modes,
        peer <- [
          "malformed",
          "not_leader",
          "missing_root",
          "unknown_scope",
          "group_scope",
          "missing_scope"
        ] do
      ref = make_ref()
      handle = start(mode, ["unused"], opts(ref, shim_path: fake_shim(dir, peer)))

      try do
        outcome = finish(handle, :exit)
        assert_receive {^ref, {:tracking_exit, evidence}}, 5_000
        refute evidence.confirmation == :confirmed
        refute_confirmed(outcome)
      after
        stop(handle)
      end
    end
  end

  test "tracking history is bounded even when a peer exceeds the group limit", %{tmp_dir: dir} do
    for mode <- @modes do
      ref = make_ref()
      handle = start(mode, ["unused"], opts(ref, shim_path: fake_shim(dir, "overflow")))

      try do
        outcome = finish(handle, :exit)
        assert_receive {^ref, {:tracking_exit, evidence}}, 5_000
        refute evidence.confirmation == :confirmed
        assert_history_bound(outcome)
      after
        stop(handle)
      end
    end
  end

  defp assert_bound_reason({:stream_error, %{idle_timed_out: true}}, "quiet"), do: :ok
  defp assert_bound_reason({:stream_error, %{limit: {:line, :stdout, 512}}}, "flood"), do: :ok
  defp assert_bound_reason({:error, {:output_limit, %Duplex.Terminal{}}}, "flood"), do: :ok
  defp assert_bound_reason({:error, {:max_output_bytes, %Result{}}}, "flood"), do: :ok

  defp assert_bound_reason(other, command),
    do: flunk("unexpected #{command} result #{inspect(other)}")

  test "tracking control frames do not reset a backpressured Stream idle timeout", %{tmp_dir: dir} do
    shim = Path.join(dir, "tracking-idle-shim")
    File.cp!(Path.expand("../fixtures/tracking_idle_shim.py", __DIR__), shim)
    File.chmod!(shim, 0o755)
    ref = make_ref()

    error =
      assert_raise Forcola.Stream.Error, fn ->
        ["unused"]
        |> Forcola.Stream.lines(
          shim_path: shim,
          timeout_ms: 5_000,
          idle_timeout_ms: 150,
          kill_grace_ms: 10,
          backpressure: true,
          track_observer: {self(), ref}
        )
        |> Enum.to_list()
      end

    assert error.idle_timed_out
    refute error.timed_out
    assert error.scope == :tree_sampled
    assert error.confirmation == :confirmed
    assert length(error.tracked_groups) > 1
    assert_tracking_exit(ref, :tree_sampled, :confirmed)
  end

  defp opts(ref, extra \\ []),
    do:
      Keyword.merge(
        [
          track_descendants: true,
          track_observer: {self(), ref},
          timeout_ms: 20_000,
          kill_grace_ms: 100
        ],
        extra
      )

  defp start(:run, argv, opts), do: {:task, :run, Task.async(fn -> Forcola.run(argv, opts) end)}

  defp start(mode, argv, opts) when mode in [:stream, :stream_bounded] do
    opts = if mode == :stream_bounded, do: Keyword.put(opts, :backpressure, true), else: opts
    stream = Forcola.Stream.lines(argv, opts)

    task =
      Task.async(fn ->
        try do
          lines =
            stream
            |> Stream.transform(nil, fn line, state ->
              receive do
                :halt -> {:halt, state}
              after
                0 -> {[line], state}
              end
            end)
            |> Enum.to_list()

          {:lines, lines}
        rescue
          error in Forcola.Stream.Error -> {:stream_error, error}
        end
      end)

    {:task, mode, task}
  end

  defp start(mode, argv, opts) when mode in [:duplex, :duplex_pull] do
    opts = Keyword.delete(opts, :timeout_ms)
    opts = if mode == :duplex_pull, do: Keyword.put(opts, :delivery, :pull), else: opts

    case Duplex.open(argv, opts) do
      {:ok, session} -> {:duplex, mode, session}
      error -> error
    end
  end

  defp start(:daemon, argv, opts) do
    case Daemon.start_link(
           [argv: argv, output: {:send, self()}] ++ Keyword.delete(opts, :timeout_ms)
         ) do
      {:ok, daemon} -> {:daemon, daemon}
      error -> error
    end
  end

  defp trigger(_handle, :exit, child, _root),
    do: File.write!(Path.join(child.dir, "exit"), "exit")

  defp trigger(_handle, :kill, _child, root),
    do: System.cmd("kill", ["-KILL", Integer.to_string(root)], stderr_to_stdout: true)

  defp trigger({:task, :run, _}, :close, _child, _root), do: :ok
  defp trigger({:task, _, task}, :close, _child, _root), do: send(task.pid, :halt)
  defp trigger({:duplex, _, session}, :close, _child, _root), do: Duplex.close(session)

  defp trigger({:daemon, daemon}, :close, _child, _root),
    do: GenServer.stop(daemon, :normal, :infinity)

  defp finish({:task, _, task}, _trigger), do: Task.await(task, 30_000)
  defp finish({:duplex, _, _}, :close), do: :closed
  defp finish({:duplex, :duplex, session}, _), do: Duplex.await_terminal(session, 10_000)

  defp finish({:duplex, :duplex_pull, session}, _) do
    case Duplex.recv(session, 10_000) do
      {:ok, _} -> finish({:duplex, :duplex_pull, session}, :exit)
      {:done, terminal} -> {:ok, terminal}
      other -> other
    end
  end

  defp finish({:daemon, daemon}, _) do
    monitor = Process.monitor(daemon)

    receive do
      {:DOWN, ^monitor, :process, ^daemon, _} -> :closed
    after
      10_000 -> flunk("daemon remained alive")
    end
  end

  defp stop({:task, _, task}) do
    if Process.alive?(task.pid), do: Task.shutdown(task, :brutal_kill)
  end

  defp stop({:duplex, _, session}), do: Duplex.close(session)

  defp stop({:daemon, daemon}) do
    if Process.alive?(daemon), do: GenServer.stop(daemon, :normal, :infinity)
  end

  defp assert_first_tracked(ref, pid) do
    assert_receive {^ref, {:tracked, identity}}, 5_000
    assert %{pid: ^pid, pgid: ^pid, start_time: start} = identity
    assert is_map(start)
  end

  defp assert_tracked(ref, pid) do
    assert_receive {^ref, {:tracked, %{pid: ^pid, pgid: ^pid, start_time: start}}}, 5_000
    assert is_map(start)
  end

  defp assert_tracking_exit(ref, scope, confirmation, context \\ nil) do
    assert_receive {^ref, {:tracking_exit, evidence}}, 10_000

    assert evidence == %{scope: scope, confirmation: confirmation},
           "unexpected tracking evidence #{inspect(evidence)} for #{inspect(context)}"

    refute_receive {^ref, {:tracking_exit, _}}, 0
  end

  defp assert_history({:ok, %{tracked_groups: groups}}, pids), do: assert_group_pids(groups, pids)

  defp assert_history({:error, {_, %Result{tracked_groups: groups}}}, pids),
    do: assert_group_pids(groups, pids)

  defp assert_history({:stream_error, %{tracked_groups: groups}}, pids),
    do: assert_group_pids(groups, pids)

  defp assert_history(_outcome, _pids), do: :ok

  defp assert_group_pids(groups, pids),
    do: assert(Enum.sort(Enum.map(groups, & &1.pid)) == Enum.sort(pids))

  defp assert_live_history({:duplex, _, session}, pids) do
    assert {:ok, groups} = Duplex.tracked(session)
    assert_group_pids(groups, pids)
  end

  defp assert_live_history({:daemon, daemon}, pids) do
    assert {:ok, groups} = Daemon.tracked(daemon)
    assert_group_pids(groups, pids)
  end

  defp assert_live_history(_, _), do: :ok

  defp refute_confirmed({:ok, %Result{evidence: ev}}), do: refute(ev.cleanup == :confirmed)
  defp refute_confirmed({:ok, %Duplex.Terminal{confirmation: c}}), do: refute(c == :confirmed)
  defp refute_confirmed({:stream_error, %{confirmation: c}}), do: refute(c == :confirmed)

  defp refute_confirmed({:error, {_, %Result{evidence: ev}}}),
    do: refute(ev.cleanup == :confirmed)

  defp refute_confirmed(_), do: :ok

  defp assert_history_bound({:ok, %{tracked_groups: groups}}), do: assert(length(groups) <= 4_096)

  defp assert_history_bound({:stream_error, %{tracked_groups: groups}}),
    do: assert(length(groups) <= 4_096)

  defp assert_history_bound(_), do: :ok

  defp assert_start_error(mode, options, reason) do
    case start(mode, ["unused"], options) do
      {:task, :run, task} -> assert Task.await(task) == {:error, {:spawn, reason}}
      {:task, _, task} -> assert {:stream_error, %{reason: ^reason}} = Task.await(task)
      {:error, {:spawn, ^reason}} -> :ok
      {:error, ^reason} -> :ok
      other -> flunk("unexpected startup result #{inspect(other)}")
    end
  end

  defp tree(parent, kind) do
    dir = Path.join(parent, Integer.to_string(System.unique_integer([:positive])))
    File.mkdir_p!(dir)
    on_exit(fn -> cleanup_tree(dir) end)
    %{dir: dir, argv: [System.find_executable("python3"), @tree, kind, dir]}
  end

  defp cleanup_tree(dir) do
    for label <- ["root", "descendant", "second"] do
      case File.read(Path.join(dir, label <> ".pid")) do
        {:ok, text} ->
          kill_owned(text, dir)

        _ ->
          :ok
      end
    end
  end

  defp kill_owned(pid, marker) do
    pid = String.trim(pid)

    case System.cmd("ps", ["-p", pid, "-o", "command="], stderr_to_stdout: true) do
      {command, 0} ->
        if String.contains?(command, marker),
          do: System.cmd("kill", ["-KILL", pid], stderr_to_stdout: true)

      _ ->
        :ok
    end
  end

  defp await_pid(child, label) do
    path = Path.join(child.dir, label <> ".pid")

    assert eventually(fn ->
             case File.read(path) do
               {:ok, text} -> String.ends_with?(text, "\n")
               _ -> false
             end
           end),
           "child did not write #{label}.pid"

    path |> File.read!() |> String.trim() |> String.to_integer()
  end

  defp alive?(pid) do
    {_, status} = System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true)
    status == 0
  end

  defp assert_gone(pid),
    do: assert(eventually(fn -> not alive?(pid) end), "process #{pid} survived cleanup")

  defp scope, do: if(:os.type() == {:unix, :linux}, do: :tree_contained, else: :tree_sampled)
  defp eventually(fun), do: poll(fun, System.monotonic_time(:millisecond) + 10_000)

  defp poll(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(10)
        poll(fun, deadline)
    end
  end

  defp fake_shim(dir, mode) do
    File.rm(Path.join(dir, "hello"))
    File.rm(Path.join(dir, "spawn"))
    path = Path.join(dir, "shim")

    File.write!(
      path,
      @shim |> File.read!() |> String.replace(~s(MODE = "valid"), ~s(MODE = "#{mode}"))
    )

    File.chmod!(path, 0o755)
    path
  end
end
