defmodule Forcola.WedgedShimTest do
  # async: false -- REQUIRED here, not just convention: each test swaps
  # the :forcola code path node-globally so Forcola.Shim.path/0 resolves
  # to a fake shim; nothing else may talk to the shim while it is swapped.
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  # The Elixir-side backstop is the least-exercised path in the library:
  # it only fires when the shim speaks the protocol and then goes silent
  # (never sends EXIT/ERROR, never dies). These tests install a fake
  # shim -- a script that reads frames forever and writes nothing -- into
  # an isolated priv dir and assert that every mode's backstop returns
  # the documented shape at its computed deadline.
  #
  # The fake shim is installed by pointing the :forcola code path entry
  # at a scratch app dir (ebin symlinked to the real one, priv holding
  # the fake binary); the repo's real priv/ is never touched, and the
  # path is restored in on_exit even if the test fails.

  # Mirrors @backstop_margin_ms in Forcola, Forcola.Stream,
  # Forcola.Duplex, and Forcola.Daemon (not exported).
  @backstop_margin_ms 5_000
  @timeout_ms 200
  @kill_grace_ms 200

  setup %{tmp_dir: tmp_dir} do
    real_ebin = :code.lib_dir(:forcola) |> List.to_string() |> Path.join("ebin")

    fake_app = Path.join(tmp_dir, "forcola")
    File.mkdir_p!(Path.join(fake_app, "priv"))
    File.ln_s!(real_ebin, Path.join(fake_app, "ebin"))

    fake_shim = Path.join(fake_app, "priv/forcola_shim")
    File.write!(fake_shim, "#!/bin/sh\nexec cat > /dev/null\n")
    File.chmod!(fake_shim, 0o755)

    true = :code.replace_path(:forcola, String.to_charlist(Path.join(fake_app, "ebin")))
    on_exit(fn -> true = :code.replace_path(:forcola, String.to_charlist(real_ebin)) end)

    assert {:ok, ^fake_shim} = Forcola.Shim.path()
    :ok
  end

  test "run/2 and Stream.lines return {:signal, :unconfirmed} at the backstop deadline" do
    # Both deadlines are timeout + grace + margin; the two scenarios run
    # concurrently so the test costs one deadline, not two.
    deadline_ms = @timeout_ms + @kill_grace_ms + @backstop_margin_ms

    run_task =
      Task.async(fn ->
        timed(fn ->
          Forcola.run(["/bin/echo", "hi"],
            timeout_ms: @timeout_ms,
            kill_grace_ms: @kill_grace_ms
          )
        end)
      end)

    stream_task =
      Task.async(fn ->
        timed(fn ->
          try do
            ["/bin/echo", "hi"]
            |> Forcola.Stream.lines(timeout_ms: @timeout_ms, kill_grace_ms: @kill_grace_ms)
            |> Enum.to_list()
          rescue
            e in Forcola.Stream.Error -> {:raised, e}
          end
        end)
      end)

    # Task.await bounds the upper side: a backstop that never fires
    # fails here instead of hanging the suite.
    {run_result, run_ms} = Task.await(run_task, deadline_ms + 10_000)
    {stream_result, stream_ms} = Task.await(stream_task, deadline_ms + 10_000)

    assert {:error, {:timeout, %Forcola.Result{status: {:signal, :unconfirmed}}}} = run_result
    assert run_ms >= deadline_ms - 100, "run/2 returned before its backstop deadline"

    assert {:raised, %Forcola.Stream.Error{status: {:signal, :unconfirmed}, timed_out: true}} =
             stream_result

    assert stream_ms >= deadline_ms - 100, "Stream returned before its backstop deadline"
  end

  test "Duplex.close/1 and Daemon stop return after the shutdown backstop" do
    # Shutdown paths have no timeout_ms; their deadline is grace + margin.
    deadline_ms = @kill_grace_ms + @backstop_margin_ms

    {:ok, session} = Forcola.Duplex.open(["/bin/cat"], kill_grace_ms: @kill_grace_ms)

    # No :ready check: against a wedged shim nothing validates the spawn
    # round-trip at init, so start_link returns immediately.
    {:ok, daemon} =
      Forcola.Daemon.start_link(
        argv: ["/bin/sh", "-c", "sleep 60"],
        kill_grace_ms: @kill_grace_ms
      )

    close_task = Task.async(fn -> timed(fn -> Forcola.Duplex.close(session) end) end)

    stop_task =
      Task.async(fn -> timed(fn -> GenServer.stop(daemon, :normal, deadline_ms + 10_000) end) end)

    {close_result, close_ms} = Task.await(close_task, deadline_ms + 10_000)
    {stop_result, stop_ms} = Task.await(stop_task, deadline_ms + 10_000)

    assert close_result == :ok
    assert close_ms >= deadline_ms - 100, "Duplex.close returned before its backstop deadline"

    assert stop_result == :ok
    assert stop_ms >= deadline_ms - 100, "Daemon stop returned before its backstop deadline"
  end

  defp timed(fun) do
    t0 = System.monotonic_time(:millisecond)
    result = fun.()
    {result, System.monotonic_time(:millisecond) - t0}
  end
end
