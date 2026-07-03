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
  end
end
