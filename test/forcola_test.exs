defmodule ForcolaTest do
  use ExUnit.Case, async: true

  describe "run/2" do
    test "requires :timeout_ms" do
      assert_raise KeyError, fn ->
        Forcola.run(["true"], [])
      end
    end

    test "raises NotImplementedError while scaffold" do
      assert_raise Forcola.NotImplementedError, ~r/Forcola\.run\/2/, fn ->
        Forcola.run(["true"], timeout_ms: 1_000)
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

  describe "scaffold stubs" do
    test "Stream.lines/2 requires :timeout_ms and raises" do
      assert_raise KeyError, fn -> Forcola.Stream.lines(["true"], []) end

      assert_raise Forcola.NotImplementedError, fn ->
        Forcola.Stream.lines(["true"], timeout_ms: 1_000)
      end
    end

    test "Duplex.open/2 raises" do
      assert_raise Forcola.NotImplementedError, fn ->
        Forcola.Duplex.open(["cat"], [])
      end
    end
  end

  describe "Shim" do
    test "path/1 reports not_found until a binary is built" do
      assert Forcola.Shim.path() == {:error, :not_found}
    end
  end
end
