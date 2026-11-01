defmodule ForcolaTest do
  use ExUnit.Case, async: true

  describe "run/2" do
    test "requires :timeout_ms" do
      assert_raise KeyError, fn ->
        Forcola.run(["true"], [])
      end
    end

    test "rejects a :timeout_ms outside 0..4294967295 before opening the shim" do
      # The bad shim path would surface as {:error, {:spawn, {:invalid_shim_path, _}}}
      # if validation let the call reach Forcola.Shim.open/1.
      for timeout <- [:infinity, "5000", 1.5, -1, 4_294_967_296] do
        assert_raise ArgumentError, ~r/timeout_ms must be an integer/, fn ->
          Forcola.run(["/bin/true"], timeout_ms: timeout, shim_path: "/nonexistent/shim")
        end
      end
    end
  end

  describe "Result" do
    test "status is mandatory" do
      assert_raise ArgumentError, fn ->
        struct!(Forcola.Result, stdout: "out")
      end
    end

    test "output defaults to empty binaries" do
      result = %Forcola.Result{status: 0}
      assert result.stdout == ""
      assert result.stderr == ""
    end
  end

  describe "Stream.lines/2" do
    test "requires :timeout_ms" do
      assert_raise KeyError, fn -> Forcola.Stream.lines(["true"], []) end
    end

    test "rejects a :timeout_ms outside 0..4294967295 when the stream is built" do
      for timeout <- [:infinity, "5000", 1.5, -1, 4_294_967_296] do
        assert_raise ArgumentError, ~r/timeout_ms must be an integer/, fn ->
          Forcola.Stream.lines(["/bin/true"], timeout_ms: timeout, shim_path: "/nonexistent/shim")
        end
      end
    end
  end

  describe "Shim cleanup budget" do
    test "cleanup_timeout/1 covers the shim's bounded cleanup, including a requested cgroup drain" do
      assert Forcola.Shim.cleanup_timeout([]) == 10_000
      assert Forcola.Shim.cleanup_timeout(kill_grace_ms: 200, interrupt_grace_ms: 300) == 5_500
      assert Forcola.Shim.cleanup_timeout(cgroup: false) == 10_000
      # finish_cgroup waits another kill grace plus 500 ms before EXIT.
      assert Forcola.Shim.cleanup_timeout(cgroup: true) == 15_500
      assert Forcola.Shim.cleanup_timeout(cgroup: :required, kill_grace_ms: 1_000) == 7_500
    end

    test "shutdown_timeout/1 follows the budget and saturates to :infinity" do
      assert Forcola.Shim.shutdown_timeout([]) == 16_000
      assert Forcola.Shim.shutdown_timeout(cgroup: true) == 21_500
      assert Forcola.Shim.shutdown_timeout(kill_grace_ms: 4_294_967_295) == :infinity
    end
  end

  describe "Shim" do
    test "path/0 finds the built binary" do
      assert {:ok, path} = Forcola.Shim.path()
      assert File.exists?(path)
    end

    test "decode_exit/1 defaults an older shim to confirmed" do
      payload = :json.encode(%{"status" => 0, "timed_out" => false}) |> IO.iodata_to_binary()
      assert {0, false} = Forcola.Shim.decode_exit(payload)
    end

    test "decode_exit/1 surfaces an unconfirmed teardown" do
      payload =
        :json.encode(%{
          "signal" => 9,
          "timed_out" => true,
          "confirmed" => false
        })
        |> IO.iodata_to_binary()

      assert {{:signal, :unconfirmed}, true} = Forcola.Shim.decode_exit(payload)
    end

    test "decode_exit/1 never confirms a malformed report" do
      for payload <- [
            "{bad json",
            "[]",
            "",
            ~s({"confirmed":true}),
            ~s({"status":-1,"confirmed":true}),
            ~s({"signal":0}),
            ~s({"status":"0"}),
            ~s({"status":0,"signal":15}),
            ~s({"status":0,"confirmed":"yes"}),
            ~s({"status":0,"contained":"no"})
          ] do
        assert {{:signal, :unconfirmed}, false} = Forcola.Shim.decode_exit(payload)
      end

      assert {{:signal, :unconfirmed}, true} = Forcola.Shim.decode_exit(~s({"timed_out":true}))
      assert {{:signal, :unconfirmed}, false} = Forcola.Shim.decode_exit(~s({"timed_out":"yes"}))
    end

    test "decode_exit_report/1 and decode_contained/1 are total" do
      assert %{valid: false, status: nil, confirmed: false, contained: true} =
               Forcola.Shim.decode_exit_report(~s({"confirmed":true,"contained":true}))

      assert %{valid: false, status: nil, confirmed: false, timed_out: false} =
               Forcola.Shim.decode_exit_report("{bad json")

      # A well-formed child observation survives an otherwise invalid report.
      assert %{valid: false, status: 3, confirmed: false} =
               Forcola.Shim.decode_exit_report(~s({"status":3,"confirmed":"yes"}))

      assert %{valid: true, status: {:signal, 9}, confirmed: false, output_truncated: true} =
               Forcola.Shim.decode_exit_report(
                 ~s({"signal":9,"confirmed":false,"output_truncated":true})
               )

      assert %{valid: true, status: 0, confirmed: true} =
               Forcola.Shim.decode_exit_report(~s({"status":0}))

      refute Forcola.Shim.decode_contained("{bad json")

      # The per-mode report derives its scope from the same validity.
      untracked = %Forcola.Tracking{}
      assert %{valid: false, scope: :unknown} = Forcola.Tracking.report(untracked, "{bad json")

      assert %{valid: true, scope: :process_group} =
               Forcola.Tracking.report(untracked, ~s({"status":0}))

      assert %{valid: true, scope: :active_cgroup} =
               Forcola.Tracking.report(untracked, ~s({"status":0,"contained":true}))
    end
  end
end
