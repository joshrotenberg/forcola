defmodule Forcola.BoundedRunTest do
  use ExUnit.Case, async: false

  @moduletag :tmp_dir
  @fixtures Path.expand("../fixtures", __DIR__)
  @json ~s({"type":"result","ok":true}\n)

  test "strict option validation happens before opening a shim" do
    for option <- [
          [idle_timeout_ms: 0],
          [idle_timeout_ms: -1],
          [idle_timeout_ms: :infinity],
          [max_output_bytes: nil],
          [max_output_bytes: 1.5],
          [max_output_bytes: 4_294_967_296],
          [env_mode: :other],
          [env_mode: :replace, env: [{"A=B", "bad"}]],
          [env_mode: :replace, env: [{"", "bad"}]],
          [env_mode: :replace, env: [{"A", <<0>>}]],
          [env_mode: :replace, env: %{"A" => "B"}],
          [env_mode: :replace, env: nil],
          [env_mode: :replace, env: [{:A, "B"}]],
          [max_output_bytes: 100, env: nil],
          [max_output_bytes: 100, timeout_ms: :infinity],
          [interrupt_signal: :term, interrupt_grace_ms: 10],
          [interrupt_signal: :int],
          [interrupt_grace_ms: 10],
          [interrupt_signal: :int, interrupt_grace_ms: 0]
        ] do
      assert_raise ArgumentError, fn ->
        Forcola.run(["/bin/true"], option ++ [timeout_ms: 1_000, shim_path: "relative"])
      end
    end
  end

  test "other modes reject unsupported requirements before opening even an older shim", %{
    tmp_dir: dir
  } do
    shim = fake_shim(dir, "legacy")
    argv = ["/bin/true"]

    for option <- [
          [env_mode: :replace],
          [env_mode: :inherit],
          [env_mode: nil],
          [interrupt_signal: :int, interrupt_grace_ms: 10],
          [interrupt_signal: nil],
          [interrupt_grace_ms: nil]
        ],
        call <- [
          fn -> Forcola.Stream.lines(argv, option ++ [timeout_ms: 1_000, shim_path: shim]) end,
          fn -> Forcola.Duplex.open(argv, option ++ [shim_path: shim]) end,
          fn ->
            Forcola.Duplex.open(
              argv,
              option ++ [delivery: :pull, max_output_bytes: 100, shim_path: shim]
            )
          end,
          fn -> Forcola.Daemon.start_link(option ++ [argv: argv, shim_path: shim]) end
        ] do
      assert_raise ArgumentError, ~r/use Forcola.run\/2/, call
    end

    for call <- [
          fn ->
            Forcola.Stream.lines(argv,
              timeout_ms: 1_000,
              max_output_bytes: 100,
              shim_path: shim
            )
          end,
          fn -> Forcola.Duplex.open(argv, idle_timeout_ms: 100, shim_path: shim) end,
          fn ->
            Forcola.Duplex.open(argv,
              delivery: :pull,
              max_output_bytes: 100,
              idle_timeout_ms: 100,
              shim_path: shim
            )
          end,
          fn -> Forcola.Daemon.start_link(argv: argv, idle_timeout_ms: 100, shim_path: shim) end,
          fn -> Forcola.Daemon.start_link(argv: argv, max_output_bytes: 100, shim_path: shim) end
        ] do
      assert_raise ArgumentError, ~r/use Forcola.run\/2/, call
    end

    assert_raise ArgumentError, ~r/output bounds require delivery: :pull/, fn ->
      Forcola.Duplex.open(argv, max_output_bytes: 100, shim_path: shim)
    end

    assert_raise ArgumentError, ~r/:max_output_bytes must be a positive integer/, fn ->
      Forcola.Duplex.open(argv, delivery: :pull, max_output_bytes: 0, shim_path: shim)
    end

    refute File.exists?(Path.join(dir, "shim_opened"))
    refute File.exists?(Path.join(dir, "spawn_received"))
  end

  test "legacy env inputs keep their original encoding and native spawn errors" do
    assert {:ok, %{stdout: "ok"}} =
             Forcola.run(["/bin/echo", "-n", "ok"], timeout_ms: 5_000, env: nil)

    for env <- [[{:FORCOLA_LEGACY_ENV, "set"}], %{"FORCOLA_LEGACY_ENV" => "set"}] do
      assert {:ok, %{stdout: "set"}} =
               Forcola.run(["/bin/sh", "-c", ~s(printf %s "$FORCOLA_LEGACY_ENV")],
                 timeout_ms: 5_000,
                 env: env
               )
    end

    for env <- [[{"BAD" <> <<0>>, "value"}], [{"BAD", "value" <> <<0>>}]] do
      assert {:error, {:spawn, reason}} =
               Forcola.run(["/bin/true"], timeout_ms: 5_000, env: env)

      assert is_binary(reason)
      assert String.starts_with?(reason, "spawn failed:")
    end
  end

  test "required cgroup alone keeps older-peer collection and slow-observer behavior", %{
    tmp_dir: dir
  } do
    observer =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> send(observer, :stop) end)
    for _ <- 1..1_024, do: send(observer, :queued)

    assert {:ok, result} =
             Forcola.run(["unused"],
               timeout_ms: 1_000,
               cgroup: :required,
               merge_stderr: true,
               output_observer: {observer, make_ref()},
               shim_path: fake_shim(dir, "legacy_cgroup")
             )

    assert result.status == 0
    assert result.stdout == "legacy output"
    assert result.evidence.reason == nil
    assert result.evidence.scope == :unknown
    assert {:message_queue_len, 1_025} = Process.info(observer, :message_queue_len)
    payload = dir |> Path.join("spawn_received") |> File.read!() |> :json.decode()
    assert payload["cgroup"] == "required"
    assert payload["merge_stderr"] == true
    refute Map.has_key?(payload, "window_bytes")
    refute Map.has_key?(payload, "strict_output")

    assert {:error, {:spawn, "fixture error"}} =
             Forcola.run(["unused"],
               timeout_ms: 1_000,
               cgroup: :required,
               shim_path: fake_shim(dir, "error")
             )
  end

  test "inherit remains default and replace removes a known inherited variable" do
    key = "FORCOLA_BOUNDED_INHERITED"
    previous = System.get_env(key)
    System.put_env(key, "inherited")

    on_exit(fn ->
      if previous, do: System.put_env(key, previous), else: System.delete_env(key)
    end)

    argv = ["/bin/sh", "-c", "printf '%s/%s' \"$FORCOLA_BOUNDED_INHERITED\" \"$EXPLICIT\""]

    assert {:ok, %{stdout: "inherited/set"}} =
             Forcola.run(argv, timeout_ms: 5_000, env: [{"EXPLICIT", "set"}])

    assert {:ok, %{stdout: "/set", evidence: %{cleanup: :confirmed}}} =
             Forcola.run(argv, timeout_ms: 5_000, env_mode: :replace, env: [{"EXPLICIT", "set"}])

    assert {:ok, %{stdout: ""}} =
             Forcola.run(["/usr/bin/env"], timeout_ms: 5_000, env_mode: :replace, env: [])
  end

  test "exact combined boundary succeeds; one extra byte retains only the prefix" do
    argv = ["/bin/sh", "-c", "printf abc; printf de >&2"]
    assert {:ok, result} = Forcola.run(argv, timeout_ms: 5_000, max_output_bytes: 5)
    assert result.stdout == "abc"
    assert result.stderr == "de"
    assert result.evidence.output_truncated == false
    assert result.evidence.child_status == 0

    assert {:error, {:max_output_bytes, partial}} =
             Forcola.run(argv, timeout_ms: 5_000, max_output_bytes: 4)

    assert byte_size(partial.stdout) + byte_size(partial.stderr) == 4
    assert String.starts_with?("abc", partial.stdout)
    assert String.starts_with?("de", partial.stderr)
    assert partial.evidence.reason == :max_output_bytes
    assert partial.evidence.output_truncated
    assert partial.evidence.cleanup == :confirmed
    assert partial.evidence.scope == :process_group
  end

  test "unterminated flood is bounded before accumulation and observation" do
    reference = make_ref()

    assert {:error, {:max_output_bytes, result}} =
             Forcola.run(
               ["/bin/sh", "-c", "while :; do printf abcdefgh; printf ijklmnop >&2; done"],
               timeout_ms: 5_000,
               kill_grace_ms: 100,
               max_output_bytes: 2_000,
               output_observer: {self(), reference}
             )

    assert byte_size(result.stdout) + byte_size(result.stderr) == 2_000
    assert observer_bytes(reference) == 2_000
    assert result.evidence.guard_close == :ok
  end

  test "silent idle timeout preserves early JSON and cannot imply process success" do
    {elapsed, outcome} =
      timed(fn ->
        Forcola.run(["/bin/sh", "-c", "printf 'early-json'; exec sleep 60"],
          timeout_ms: 10_000,
          idle_timeout_ms: 100,
          kill_grace_ms: 100
        )
      end)

    assert {:error, {:idle_timeout, result}} = outcome
    assert result.stdout == "early-json"
    assert result.evidence.reason == :idle_timeout
    assert result.evidence.cleanup == :confirmed
    assert result.evidence.child_signal in [9, 15]
    assert elapsed < 3_000
  end

  test "continuous output does not reset the whole-run deadline" do
    {elapsed, outcome} =
      timed(fn ->
        Forcola.run(["/bin/sh", "-c", "while :; do printf x; sleep 0.01; done"],
          timeout_ms: 200,
          idle_timeout_ms: 1_000,
          kill_grace_ms: 100
        )
      end)

    assert {:error, {:timeout, result}} = outcome
    assert byte_size(result.stdout) > 0
    assert elapsed < 3_000
    assert result.evidence.reason == :timeout
  end

  test "silent hang has an independent idle deadline" do
    {elapsed, outcome} =
      timed(fn ->
        Forcola.run(["/bin/sleep", "60"],
          timeout_ms: 10_000,
          idle_timeout_ms: 100,
          kill_grace_ms: 100
        )
      end)

    assert {:error, {:idle_timeout, %{stdout: "", stderr: ""}}} = outcome
    assert elapsed < 3_000
  end

  test "observer pressure has a finite bound even without a total output limit" do
    observer =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> send(observer, :stop) end)
    for _ <- 1..1_000, do: send(observer, :queued)

    {elapsed, outcome} =
      timed(fn ->
        Forcola.run(["/bin/sh", "-c", "while :; do printf x; done"],
          timeout_ms: 10_000,
          idle_timeout_ms: 5_000,
          kill_grace_ms: 100,
          output_observer: {observer, make_ref()}
        )
      end)

    assert {:error, {:observer_overflow, result}} = outcome
    assert {:message_queue_len, 1_024} = Process.info(observer, :message_queue_len)
    assert result.evidence.output_truncated
    assert elapsed < 10_000
  end

  test "flood keeps collector memory and mailbox finite", %{tmp_dir: dir} do
    pidfile = Path.join(dir, "pid")

    task =
      Task.async(fn ->
        Forcola.run(
          ["/bin/sh", "-c", "echo $$ > \"$PIDFILE\"; while :; do printf abcdefgh; done"],
          timeout_ms: 300,
          idle_timeout_ms: 1_000,
          max_output_bytes: 1_000_000,
          kill_grace_ms: 100,
          env: [{"PIDFILE", pidfile}]
        )
      end)

    {memory, messages} = sample(task.pid, 0, 0)
    assert {:error, {reason, result}} = Task.await(task, 5_000)
    assert reason in [:timeout, :max_output_bytes]
    assert byte_size(result.stdout) <= 1_000_000
    assert memory < 50_000_000
    assert messages <= 16_400
    parent = await_file(pidfile) |> String.trim()
    assert eventually(fn -> not alive?(parent) end)
  end

  test "old shim refuses strict requests before SPAWN", %{tmp_dir: dir} do
    shim = fake_shim(dir, "legacy")

    assert {:error, {:spawn, :incompatible_shim}} =
             Forcola.run(["/bin/true"], shim_path: shim, timeout_ms: 1_000, env_mode: :replace)

    refute File.exists?(Path.join(dir, "spawn_received"))
  end

  test "required cgroup with a new option still negotiates before SPAWN", %{tmp_dir: dir} do
    assert {:error, {:spawn, :incompatible_shim}} =
             Forcola.run(["unused"],
               timeout_ms: 1_000,
               cgroup: :required,
               env_mode: :replace,
               shim_path: fake_shim(dir, "legacy_cgroup")
             )

    refute File.exists?(Path.join(dir, "spawn_received"))
  end

  test "negotiation distinguishes incompatible, malformed, exited and crashed peers", %{
    tmp_dir: dir
  } do
    for {mode, reason} <- [
          {"handshake_incompatible", :incompatible_shim},
          {"handshake_no_feature", :incompatible_shim},
          {"handshake_malformed", :invalid_handshake},
          {"handshake_bad_shape", :invalid_handshake},
          {"handshake_wrong_frame", :invalid_handshake},
          {"handshake_exit", {:shim_exited, 23}},
          {"handshake_crash", {:shim_exited, 137}}
        ] do
      assert {:error, {:spawn, ^reason}} =
               Forcola.run(["unused"],
                 timeout_ms: 2_000,
                 env_mode: :replace,
                 shim_path: fake_shim(dir, mode)
               )

      refute File.exists?(Path.join(dir, "spawn_received"))
    end
  end

  test "a stalled handshake uses the caller timeout and never sends SPAWN", %{tmp_dir: dir} do
    {elapsed, outcome} =
      timed(fn ->
        Forcola.run(["unused"],
          timeout_ms: 100,
          env_mode: :replace,
          shim_path: fake_shim(dir, "handshake_timeout")
        )
      end)

    assert outcome == {:error, {:spawn, :handshake_timeout}}
    assert elapsed >= 100
    assert elapsed < 2_000
    refute File.exists?(Path.join(dir, "spawn_received"))
  end

  test "a supported shim may take longer than one second to negotiate", %{tmp_dir: dir} do
    assert {:ok, %{status: 0}} =
             Forcola.run(["unused"],
               timeout_ms: 3_000,
               env_mode: :replace,
               shim_path: fake_shim(dir, "handshake_slow")
             )
  end

  test "negotiation consumes the execution budget rather than restarting it", %{tmp_dir: dir} do
    {elapsed, outcome} =
      timed(fn ->
        Forcola.run(["unused"],
          timeout_ms: 1_000,
          env_mode: :replace,
          shim_path: fake_shim(dir, "handshake_budget")
        )
      end)

    assert {:error, {:timeout, result}} = outcome
    assert result.evidence.child_status == 0
    assert result.evidence.reason == :timeout
    assert elapsed >= 900
    assert elapsed < 1_350
    payload = dir |> Path.join("spawn_received") |> File.read!() |> :json.decode()
    assert payload["timeout_ms"] <= 600
    assert payload["timeout_ms"] > 0
  end

  test "missing or malformed EXIT retains early bytes and never confirms cleanup", %{tmp_dir: dir} do
    for mode <- ["missing", "malformed", "missing_confirmation", "bad_confirmation"] do
      shim = fake_shim(dir, mode)
      outcome = Forcola.run(["unused"], shim_path: shim, timeout_ms: 1_000, max_output_bytes: 100)

      result =
        case outcome do
          {:ok, result} -> result
          {:error, {:spawn, {_reason, result}}} -> result
        end

      assert result.stdout == @json
      assert result.status == {:signal, :unconfirmed}
      assert result.evidence.cleanup == :unknown
      assert result.evidence.scope == :unknown
      assert result.evidence.guard_close == :ok
    end
  end

  test "raw signal survives unconfirmed cleanup and strict spawn error preserves bytes", %{
    tmp_dir: dir
  } do
    assert {:ok, result} =
             Forcola.run(["unused"],
               timeout_ms: 1_000,
               max_output_bytes: 100,
               shim_path: fake_shim(dir, "signal")
             )

    assert result.status == {:signal, :unconfirmed}
    assert result.evidence.child_signal == 9
    assert result.evidence.cleanup == :unconfirmed
    assert result.evidence.scope == :process_group

    assert {:error, {:spawn, {"fixture error", partial}}} =
             Forcola.run(["unused"],
               timeout_ms: 1_000,
               max_output_bytes: 100,
               shim_path: fake_shim(dir, "error")
             )

    assert partial.stdout == @json
  end

  test "legacy missing confirmation keeps status compatibility but evidence is unknown", %{
    tmp_dir: dir
  } do
    assert {:ok, result} =
             Forcola.run(["unused"],
               timeout_ms: 1_000,
               shim_path: fake_shim(dir, "missing_confirmation")
             )

    assert result.status == 0
    assert result.evidence.cleanup == :unknown
    assert result.evidence.scope == :unknown
  end

  test "interrupt-first parent cooperates with a child in a separate group", %{tmp_dir: dir} do
    {task, parent, child} = start_tree(dir, "cooperate", idle_timeout_ms: 1_000)
    assert {:error, {:idle_timeout, result}} = Task.await(task, 5_000)
    assert result.stdout == @json
    assert result.evidence.child_status == 42
    assert result.evidence.cleanup == :confirmed
    assert result.evidence.scope == :process_group
    assert File.read!(Path.join(dir, "signals")) == "INT\n"
    assert eventually(fn -> not alive?(parent) and not alive?(child) end)
  end

  test "escaped uncooperative child survives and evidence confirms only the group", %{
    tmp_dir: dir
  } do
    {task, parent, child} = start_tree(dir, "stubborn", idle_timeout_ms: 1_000)
    assert {:error, {:idle_timeout, result}} = Task.await(task, 5_000)
    assert result.evidence.child_signal == 9
    assert result.evidence.cleanup == :confirmed
    assert result.evidence.scope == :process_group
    assert File.read!(Path.join(dir, "signals")) == "INT\nTERM\n"
    assert eventually(fn -> not alive?(parent) end)
    assert alive?(child)
    cleanup(child)
    assert eventually(fn -> not alive?(child) end)
  end

  test "strict drain can time out after root success while an escaped child holds the pipes", %{
    tmp_dir: dir
  } do
    for {reason, opts} <- [
          {:idle_timeout, [idle_timeout_ms: 500]},
          {:timeout, [timeout_ms: 500]}
        ] do
      fixture_dir = Path.join(dir, Atom.to_string(reason))
      File.mkdir!(fixture_dir)
      {task, parent, child} = start_tree(fixture_dir, "root_exit", opts)
      assert {:error, {^reason, result}} = Task.await(task, 5_000)
      assert result.stdout == @json
      assert result.evidence.child_status == 0
      assert result.evidence.reason == reason
      assert result.evidence.scope == :process_group
      assert eventually(fn -> not alive?(parent) end)
      assert alive?(child)
      cleanup(child)
      assert eventually(fn -> not alive?(child) end)
    end
  end

  test "owner death lets native EOF use the same interrupt-first sequence", %{tmp_dir: dir} do
    {task, parent, child} = start_tree(dir, "cooperate", [])
    Process.unlink(task.pid)
    Process.exit(task.pid, :kill)
    assert eventually(fn -> not alive?(parent) and not alive?(child) end)
    assert File.read!(Path.join(dir, "signals")) == "INT\n"
  end

  test "whole-run timeout invokes cooperative INT before TERM", %{tmp_dir: dir} do
    {task, parent, child} = start_tree(dir, "cooperate", timeout_ms: 1_000)
    assert {:error, {:timeout, result}} = Task.await(task, 5_000)
    assert result.evidence.child_status == 42
    assert File.read!(Path.join(dir, "signals")) == "INT\n"
    assert eventually(fn -> not alive?(parent) and not alive?(child) end)
  end

  test "SIGKILL of a separate BEAM still reaches the native INT path", %{tmp_dir: dir} do
    {:ok, shim} = Forcola.Shim.path()
    ebin = :code.which(Forcola) |> List.to_string() |> Path.dirname()
    beamfile = Path.join(dir, "beam")
    argv = [python!(), Path.join(@fixtures, "bounded_tree.py"), "cooperate", dir]

    code =
      "File.write!(#{inspect(beamfile)}, System.pid() <> \"\\n\"); " <>
        "Forcola.run(#{inspect(argv)}, timeout_ms: 60_000, shim_path: #{inspect(shim)}, " <>
        "interrupt_signal: :int, interrupt_grace_ms: 500, kill_grace_ms: 100)"

    port =
      Port.open(
        {:spawn_executable, System.find_executable("elixir")},
        [:binary, :exit_status, :stderr_to_stdout, args: ["-pa", ebin, "-e", code]]
      )

    beam = await_file(beamfile) |> String.trim()
    parent = await_file(Path.join(dir, "parent")) |> String.trim()
    child = await_file(Path.join(dir, "child")) |> String.trim()

    on_exit(fn ->
      cleanup(beam)
      cleanup(parent)
      cleanup(child)
    end)

    System.cmd("kill", ["-KILL", beam])
    assert eventually(fn -> not alive?(parent) and not alive?(child) end)
    assert File.read!(Path.join(dir, "signals")) == "INT\n"
    assert_receive {^port, {:exit_status, _status}}, 5_000
  end

  test "bounded reasons survive a missing or malformed cancellation report", %{tmp_dir: dir} do
    for mode <- ["drop_after_kill", "malformed_after_kill"] do
      assert {:error, {:idle_timeout, result}} =
               Forcola.run(["unused"],
                 timeout_ms: 2_000,
                 idle_timeout_ms: 50,
                 shim_path: fake_shim(dir, mode)
               )

      assert result.stdout == @json
      assert result.evidence.reason == :idle_timeout
      assert result.evidence.cleanup == :unknown
    end

    assert {:error, {:max_output_bytes, result}} =
             Forcola.run(["unused"],
               timeout_ms: 2_000,
               max_output_bytes: 4,
               shim_path: fake_shim(dir, "drop_after_kill")
             )

    assert result.stdout == binary_part(@json, 0, 4)
    assert result.evidence.reason == :max_output_bytes
  end

  test "bounded merged output and observer share the exact combined prefix" do
    reference = make_ref()

    assert {:error, {:max_output_bytes, result}} =
             Forcola.run(["/bin/sh", "-c", "printf abc; printf def >&2"],
               timeout_ms: 2_000,
               max_output_bytes: 5,
               merge_stderr: true,
               output_observer: {self(), reference}
             )

    assert byte_size(result.stdout) == 5
    assert result.stderr == ""
    assert observer_bytes(reference) == 5
  end

  for trigger <- [:timeout, :kill] do
    @native_opts if(trigger == :timeout, do: [timeout_ms: 1_000], else: [])
    @native_kill trigger == :kill
    @native_timed_out trigger == :timeout
    test "native #{trigger} applies INT before existing TERM/KILL", %{tmp_dir: dir} do
      {:ok, port} = Forcola.Shim.open()

      try do
        assert :ok = Forcola.Shim.negotiate(port, 5_000)
        argv = [python!(), Path.join(@fixtures, "bounded_tree.py"), "cooperate", dir]

        opts =
          @native_opts ++ [interrupt_signal: :int, interrupt_grace_ms: 500, kill_grace_ms: 100]

        Forcola.Shim.send_frame(
          port,
          Forcola.Shim.tag_spawn(),
          Forcola.Shim.encode_spawn(argv, opts)
        )

        Forcola.Shim.send_frame(port, Forcola.Shim.tag_eof())
        parent = await_file(Path.join(dir, "parent")) |> String.trim()
        child = await_file(Path.join(dir, "child")) |> String.trim()

        on_exit(fn ->
          cleanup(parent)
          cleanup(child)
        end)

        if @native_kill, do: Forcola.Shim.send_frame(port, Forcola.Shim.tag_kill())
        report = await_exit(port)
        assert report["status"] == 42
        assert report["confirmed"] == true
        assert report["timed_out"] == @native_timed_out
        assert File.read!(Path.join(dir, "signals")) == "INT\n"
        assert eventually(fn -> not alive?(parent) and not alive?(child) end)
      after
        Forcola.Shim.close(port)
      end
    end
  end

  test "guard-close failure is separate from unknown child cleanup", %{tmp_dir: dir} do
    reference = make_ref()
    observer = self()

    task =
      Task.async(fn ->
        Forcola.run(["unused"],
          shim_path: fake_shim(dir, "hold"),
          timeout_ms: 2_000,
          idle_timeout_ms: 100,
          kill_grace_ms: 100,
          output_observer: {observer, reference}
        )
      end)

    assert_receive {^reference, {:stdout, @json}}, 5_000
    {_port, guard} = Enum.find_value(Port.list(), &owned_guard(&1, task.pid))
    Process.exit(guard, :kill)
    assert {:error, {:idle_timeout, result}} = Task.await(task, 15_000)
    assert result.stdout == @json
    assert result.evidence.cleanup == :unknown
    assert result.evidence.termination == :backstop
    assert result.evidence.guard_close == {:error, :guard_close_timeout}
  end

  defp owned_guard(port, owner) do
    with {:connected, guard} <- Port.info(port, :connected),
         {:monitors, monitors} <- Process.info(guard, :monitors),
         true <- {:process, owner} in monitors do
      {port, guard}
    else
      _ -> nil
    end
  end

  defp await_exit(port) do
    receive do
      {^port, {:data, <<0x13, payload::binary>>}} -> :json.decode(payload)
      {^port, {:data, _frame}} -> await_exit(port)
    after
      5_000 -> flunk("native shim did not send EXIT")
    end
  end

  defp start_tree(dir, mode, opts) do
    task =
      Task.async(fn ->
        Forcola.run(
          [python!(), Path.join(@fixtures, "bounded_tree.py"), mode, dir],
          opts ++
            [
              timeout_ms: 10_000,
              kill_grace_ms: 100,
              interrupt_signal: :int,
              interrupt_grace_ms: 200
            ]
        )
      end)

    parent = await_file(Path.join(dir, "parent")) |> String.trim()
    child = await_file(Path.join(dir, "child")) |> String.trim()

    on_exit(fn ->
      cleanup(parent)
      cleanup(child)
    end)

    {task, parent, child}
  end

  defp fake_shim(dir, mode) do
    path = Path.join(dir, "shim_#{mode}")
    source = File.read!(Path.join(@fixtures, "evidence_shim.py"))
    File.write!(path, String.replace(source, ~s(MODE = "missing"), ~s(MODE = "#{mode}")))
    File.chmod!(path, 0o700)
    path
  end

  defp python!,
    do: System.find_executable("python3") || flunk("python3 fixture interpreter is required")

  defp observer_bytes(ref, total \\ 0) do
    receive do
      {^ref, {_channel, bytes}} -> observer_bytes(ref, total + byte_size(bytes))
    after
      0 -> total
    end
  end

  defp timed(fun) do
    start = System.monotonic_time(:millisecond)
    result = fun.()
    {System.monotonic_time(:millisecond) - start, result}
  end

  defp await_file(path) do
    assert eventually(fn ->
             case File.read(path) do
               {:ok, content} -> String.ends_with?(content, "\n")
               _ -> false
             end
           end)

    File.read!(path)
  end

  defp alive?(pid), do: match?({_, 0}, System.cmd("kill", ["-0", pid], stderr_to_stdout: true))

  defp cleanup(pid) do
    if alive?(pid), do: System.cmd("kill", ["-KILL", pid], stderr_to_stdout: true)
  end

  defp eventually(fun), do: poll(fun, System.monotonic_time(:millisecond) + 5_000)

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

  defp sample(pid, memory, messages) do
    case Process.info(pid, [:memory, :message_queue_len]) do
      nil ->
        {memory, messages}

      info ->
        Process.sleep(2)
        sample(pid, max(memory, info[:memory]), max(messages, info[:message_queue_len]))
    end
  end
end
