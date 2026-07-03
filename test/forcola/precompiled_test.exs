defmodule Forcola.PrecompiledTest do
  use ExUnit.Case, async: true

  alias Forcola.Precompiled

  describe "target/1" do
    test "maps macOS arm64" do
      assert {:ok, "aarch64-apple-darwin"} = Precompiled.target("aarch64-apple-darwin24.3.0")
      assert {:ok, "aarch64-apple-darwin"} = Precompiled.target("arm64-apple-darwin23.6.0")
    end

    test "maps macOS x86_64" do
      assert {:ok, "x86_64-apple-darwin"} = Precompiled.target("x86_64-apple-darwin22.6.0")
    end

    test "maps Linux glibc" do
      assert {:ok, "x86_64-unknown-linux-gnu"} = Precompiled.target("x86_64-pc-linux-gnu")
      assert {:ok, "aarch64-unknown-linux-gnu"} = Precompiled.target("aarch64-unknown-linux-gnu")
    end

    test "maps Linux musl (Alpine)" do
      assert {:ok, "x86_64-unknown-linux-musl"} = Precompiled.target("x86_64-alpine-linux-musl")
      assert {:ok, "x86_64-unknown-linux-musl"} = Precompiled.target("x86_64-pc-linux-musl")
    end

    test "rejects unsupported architectures" do
      assert {:error, message} = Precompiled.target("i686-pc-linux-gnu")
      assert message =~ "unsupported architecture"

      assert {:error, message} = Precompiled.target("riscv64-unknown-linux-gnu")
      assert message =~ "unsupported architecture"
    end

    test "rejects unsupported operating systems" do
      assert {:error, message} = Precompiled.target("x86_64-pc-windows-msvc")
      assert message =~ "unsupported OS"
    end

    test "detects a supported target on this machine" do
      # CI and dev machines are all macOS or Linux on x86_64/aarch64.
      assert {:ok, target} = Precompiled.target()
      assert target in Precompiled.targets()
    end
  end

  describe "artifact naming" do
    test "artifact_name/2 matches the release workflow's naming" do
      assert Precompiled.artifact_name("0.1.0", "aarch64-apple-darwin") ==
               "forcola_shim-v0.1.0-aarch64-apple-darwin.tar.gz"
    end

    test "download_url/2 points at the GitHub Release for the tag" do
      assert Precompiled.download_url("0.1.0", "x86_64-unknown-linux-musl") ==
               "https://github.com/joshrotenberg/forcola/releases/download/v0.1.0/" <>
                 "forcola_shim-v0.1.0-x86_64-unknown-linux-musl.tar.gz"
    end
  end

  describe "verify/2" do
    test "accepts a matching digest" do
      body = "shim bytes"
      assert :ok = Precompiled.verify(body, Precompiled.sha256(body))
    end

    test "accepts an uppercase digest" do
      body = "shim bytes"
      assert :ok = Precompiled.verify(body, String.upcase(Precompiled.sha256(body)))
    end

    test "rejects a mismatched digest with both digests in the message" do
      expected = Precompiled.sha256("something else")
      assert {:error, message} = Precompiled.verify("shim bytes", expected)
      assert message =~ "checksum mismatch"
      assert message =~ expected
      assert message =~ Precompiled.sha256("shim bytes")
    end
  end

  describe "read_checksums/1" do
    @tag :tmp_dir
    test "reads a map of artifact name to digest", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "checksum-forcola_shim.exs")
      File.write!(path, ~s(%{"forcola_shim-v0.1.0-aarch64-apple-darwin.tar.gz" => "abc123"}\n))

      assert {:ok, %{"forcola_shim-v0.1.0-aarch64-apple-darwin.tar.gz" => "abc123"}} =
               Precompiled.read_checksums(path)
    end

    test "errors when the file does not exist" do
      assert {:error, message} = Precompiled.read_checksums("does-not-exist.exs")
      assert message =~ "not found"
    end
  end
end
