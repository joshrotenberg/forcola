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
  end
end
