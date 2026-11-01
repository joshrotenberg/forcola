defmodule ForcolaTest do
  use ExUnit.Case, async: true

  describe "run/2" do
    test "requires :timeout_ms" do
      assert_raise KeyError, fn ->
        Forcola.run(["true"], [])
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
    end
  end
end
