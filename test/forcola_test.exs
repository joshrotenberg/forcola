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
