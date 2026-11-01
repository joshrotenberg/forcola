defmodule Forcola.ExecutionOptionsTest do
  use ExUnit.Case, async: false

  alias Forcola.{Daemon, Duplex, Shim}

  @moduletag :tmp_dir
  @moduletag :capture_log
  @modes [:run, :stream, :stream_bounded, :duplex, :duplex_pull, :daemon]
  @fixture Path.expand("../fixtures/execution_options_shim.py", __DIR__)

  setup do
    Process.flag(:trap_exit, true)
    key = "FORCOLA_UNSET_INHERITED"
    previous = System.get_env(key)
    System.put_env(key, "inherited")

    on_exit(fn ->
      if previous, do: System.put_env(key, previous), else: System.delete_env(key)
    end)

    :ok
  end

  test "nil and false remove inherited variables in every mode, with pipes and PTYs" do
    for mode <- @modes, pty <- [false, true], unset <- [nil, false] do
      output =
        output(mode, ["/usr/bin/env"],
          pty: pty,
          env: [{"FORCOLA_UNSET_INHERITED", unset}, {"FORCOLA_EXPLICIT", "kept"}]
        )

      refute output =~ "FORCOLA_UNSET_INHERITED="
      assert output =~ "FORCOLA_EXPLICIT=kept"
      refute output =~ "=nil"
    end
  end

  test "inherit and replace have the same meaning in every mode" do
    for mode <- @modes, pty <- [false, true] do
      assert output(mode, ["/usr/bin/env"], pty: pty, env_mode: :inherit) =~
               "FORCOLA_UNSET_INHERITED=inherited"

      assert output(mode, ["/usr/bin/env"], pty: pty, env_mode: :replace, env: []) == ""

      output =
        output(mode, ["/usr/bin/env"],
          pty: pty,
          env_mode: :replace,
          env: [{"ONLY", "set"}, {"MISSING", nil}, {"ALSO_MISSING", false}]
        )

      assert String.trim(output) == "ONLY=set"
    end
  end

  test "plain env retains nil, maps and atom names while validating values" do
    for mode <- @modes do
      assert output(mode, ["/usr/bin/env"], env: nil) =~ "FORCOLA_UNSET_INHERITED=inherited"

      for env <- [%{"FORCOLA_VALUE" => "set"}, [{:FORCOLA_VALUE, "set"}]] do
        assert output(mode, ["/usr/bin/env"], env: env) =~ "FORCOLA_VALUE=set"
      end

      for env <- [%{"FORCOLA_UNSET_INHERITED" => nil}, [{:FORCOLA_UNSET_INHERITED, false}]] do
        refute output(mode, ["/usr/bin/env"], env: env) =~ "FORCOLA_UNSET_INHERITED="
      end
    end
  end

  test "all other non-binary env values raise before opening a shim" do
    for mode <- @modes,
        value <- [true, :value, 42, 1.5, ~c"value", [], %{}],
        env <- [[{"NAME", value}], %{"NAME" => value}] do
      assert_raise ArgumentError, ~r/env/, fn ->
        start(mode, ["unused"], env: env, shim_path: "relative")
      end
    end
  end

  test "environment and interrupt options share Run validation in every mode" do
    for mode <- @modes,
        opts <- [
          [env_mode: nil],
          [env_mode: :other],
          [env_mode: :replace, env: nil],
          [env_mode: :inherit, env: %{"A" => "B"}],
          [env_mode: :replace, env: [{:A, "B"}]],
          [env_mode: :replace, env: [{"", false}]],
          [env_mode: :inherit, env: [{"A=B", nil}]],
          [env_mode: :replace, env: [{"A" <> <<0>>, nil}]],
          [env_mode: :inherit, env: [{"A", "B" <> <<0>>}]],
          [interrupt_signal: nil],
          [interrupt_grace_ms: nil],
          [interrupt_signal: :int],
          [interrupt_grace_ms: 100],
          [interrupt_signal: :term, interrupt_grace_ms: 100],
          [interrupt_signal: :int, interrupt_grace_ms: 0],
          [interrupt_signal: :int, interrupt_grace_ms: 4_294_967_296],
          [interrupt_signal: :int, interrupt_grace_ms: 1.5]
        ] do
      assert_raise ArgumentError, fn ->
        start(mode, ["unused"], opts ++ [shim_path: "relative"])
      end
    end
  end

  test "bounded-only negotiation keeps the empty HELLO in every mode", %{tmp_dir: dir} do
    for mode <- @modes,
        opts <- [[env_mode: :inherit], [interrupt_signal: :int, interrupt_grace_ms: 100]] do
      shim = shim(dir, "all")
      assert String.trim(output(mode, ["unused"], opts ++ [shim_path: shim])) == "ok"
      assert File.read!(Path.join(dir, "hello")) == ""
      assert File.exists?(Path.join(dir, "spawn"))
    end
  end

  test "missing bounded or unset capability fails before SPAWN in every mode", %{tmp_dir: dir} do
    for mode <- @modes,
        {peer, opts} <- [
          {"no_bounded", [env_mode: :replace]},
          {"no_bounded", [interrupt_signal: :int, interrupt_grace_ms: 100]},
          {"no_unset", [env: [{"NAME", nil}]]},
          {"no_unset", [env: [{"NAME", false}]]}
        ] do
      shim = shim(dir, peer)
      assert_start_error(mode, opts ++ [shim_path: shim], :incompatible_shim)
      refute File.exists?(Path.join(dir, "spawn"))
    end
  end

  test "removals encode null and compose with existing feature selections", %{tmp_dir: dir} do
    for mode <- @modes do
      opts = [
        env_mode: :replace,
        env: [{"NIL", nil}, {"FALSE", false}, {"SET", "value"}],
        structured_spawn_errors: true,
        spawn_identity: true,
        shim_path: shim(dir, "all")
      ]

      opts =
        if mode in [:stream, :stream_bounded],
          do: Keyword.put(opts, :spawn_observer, {self(), make_ref()}),
          else: opts

      assert String.trim(output(mode, ["unused"], opts)) == "ok"
      hello = dir |> Path.join("hello") |> File.read!() |> :json.decode()

      assert Enum.sort(hello["features"]) ==
               ["env_unset_v1", "spawn_identity_v1", "structured_spawn_errors_v1"]

      spawn = dir |> Path.join("spawn") |> File.read!() |> :json.decode()
      assert spawn["env"] == %{"NIL" => :null, "FALSE" => :null, "SET" => "value"}
      assert spawn["env_mode"] == "replace"
    end
  end

  test "unsets alone keep legacy collection and error envelopes" do
    assert {:error, {:spawn, reason}} =
             Forcola.run(["/forcola-does-not-exist"], timeout_ms: 5_000, env: [{"NAME", nil}])

    assert is_binary(reason)

    payload = Shim.encode_spawn(["/bin/true"], env: [{"A", nil}, {"B", false}]) |> :json.decode()
    assert payload["env"] == %{"A" => :null, "B" => :null}
  end

  test "Stream negotiation consumes the whole-run budget for the shared options", %{tmp_dir: dir} do
    for opts <- [[env_mode: :replace], [env: [{"NAME", false}]]] do
      assert output(
               :stream,
               ["unused"],
               opts ++ [timeout_ms: 1_000, shim_path: shim(dir, "delay")]
             ) ==
               "ok"

      spawn = dir |> Path.join("spawn") |> File.read!() |> :json.decode()
      assert spawn["timeout_ms"] > 0
      assert spawn["timeout_ms"] <= 850

      assert_start_error(
        :stream,
        opts ++ [timeout_ms: 100, shim_path: shim(dir, "stall")],
        :handshake_timeout
      )

      refute File.exists?(Path.join(dir, "spawn"))
    end
  end

  defp start(:run, argv, opts), do: Forcola.run(argv, Keyword.put_new(opts, :timeout_ms, 5_000))

  defp start(mode, argv, opts) when mode in [:stream, :stream_bounded] do
    opts = if mode == :stream_bounded, do: Keyword.put(opts, :backpressure, true), else: opts
    Forcola.Stream.lines(argv, Keyword.put_new(opts, :timeout_ms, 5_000))
  end

  defp start(mode, argv, opts) when mode in [:duplex, :duplex_pull] do
    opts = if mode == :duplex_pull, do: Keyword.put(opts, :delivery, :pull), else: opts
    Duplex.open(argv, opts)
  end

  defp start(:daemon, argv, opts),
    do: Daemon.start_link(opts ++ [argv: argv, output: {:send, self()}])

  defp output(:run, argv, opts) do
    assert {:ok, %{status: 0, stdout: stdout}} = start(:run, argv, opts)
    stdout
  end

  defp output(mode, argv, opts) when mode in [:stream, :stream_bounded],
    do: mode |> start(argv, opts) |> Enum.join("\n")

  defp output(mode, argv, opts) when mode in [:duplex, :duplex_pull] do
    assert {:ok, session} = start(mode, argv, opts)

    try do
      duplex_output(mode, session, [])
    after
      Duplex.close(session)
    end
  end

  defp output(:daemon, argv, opts) do
    assert {:ok, daemon} = start(:daemon, argv, opts)

    try do
      daemon_output(daemon, [])
    after
      if Process.alive?(daemon), do: GenServer.stop(daemon)
    end
  end

  defp duplex_output(:duplex, session, acc) do
    receive do
      {:forcola_line, ^session, line} -> duplex_output(:duplex, session, ["\n", line | acc])
      {:forcola_exit, ^session, 0} -> acc |> Enum.reverse() |> IO.iodata_to_binary()
    after
      5_000 -> flunk("Duplex did not finish printing its environment")
    end
  end

  defp duplex_output(:duplex_pull, session, acc) do
    case Duplex.recv(session, 5_000) do
      {:ok, {:stdout, line}} -> duplex_output(:duplex_pull, session, ["\n", line | acc])
      {:done, %{status: 0}} -> acc |> Enum.reverse() |> IO.iodata_to_binary()
      other -> flunk("unexpected pull result: #{inspect(other)}")
    end
  end

  defp daemon_output(daemon, acc) do
    receive do
      {Daemon, ^daemon, {:stdout, bytes}} -> daemon_output(daemon, [bytes | acc])
      {:EXIT, ^daemon, :normal} -> acc |> Enum.reverse() |> IO.iodata_to_binary()
    after
      5_000 -> flunk("Daemon did not finish printing its environment")
    end
  end

  defp assert_start_error(mode, opts, reason) when mode in [:stream, :stream_bounded] do
    error = assert_raise Forcola.Stream.Error, fn -> output(mode, ["unused"], opts) end
    assert error.reason == reason
  end

  defp assert_start_error(mode, opts, reason) when mode in [:duplex, :duplex_pull],
    do: assert(start(mode, ["unused"], opts) == {:error, reason})

  defp assert_start_error(mode, opts, reason),
    do: assert(start(mode, ["unused"], opts) == {:error, {:spawn, reason}})

  defp shim(dir, mode) do
    File.rm(Path.join(dir, "hello"))
    File.rm(Path.join(dir, "spawn"))
    path = Path.join(dir, "shim")
    source = @fixture |> File.read!() |> String.replace(~s(MODE = "all"), ~s(MODE = "#{mode}"))
    File.write!(path, source)
    File.chmod!(path, 0o755)
    path
  end
end
