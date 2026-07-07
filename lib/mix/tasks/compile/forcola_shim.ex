defmodule Mix.Tasks.Compile.ForcolaShim do
  @moduledoc """
  Puts a `forcola_shim` binary into `priv/`.

  Resolution order:

  1. `FORCOLA_BUILD=1` (or `true`) forces a local `cargo build
     --release` from `native/forcola_shim`. This is the escape hatch
     for targets the release workflow does not cover; it raises if
     cargo is not on `PATH`.
  2. If `checksum-forcola_shim.exs` exists (it ships in the hex
     package and never in the git checkout), the precompiled binary
     for the current target is downloaded from GitHub Releases and its
     SHA256 verified against that file. A mismatch fails the compile.
  3. If `native/forcola_shim` exists and cargo is on `PATH` (git
     checkout, CI), a debug `cargo build` runs and the binary is
     copied into `priv/` when stale. This is the development path.
  4. Otherwise the compile fails with instructions.

  The binary is placed in the source `priv/` and also synced into the
  build priv (`Mix.Project.app_path/0` + `priv/`), where
  `Forcola.Shim.path/0` and `mix release` read it. Mix copies a dep's
  source priv into the build priv before this compiler runs, so the
  sync is what makes a fresh install resolve; see #47.
  """
  use Mix.Task.Compiler

  @native_dir "native/forcola_shim"
  @bin_name "forcola_shim"

  @impl true
  def run(_args) do
    cond do
      build_forced?() ->
        build_and_copy(:release)

      File.exists?(Forcola.Precompiled.checksum_file()) ->
        fetch_precompiled()

      System.find_executable("cargo") != nil and File.dir?(@native_dir) ->
        build_and_copy(:debug)

      true ->
        Mix.raise("""
        Cannot provide a forcola_shim binary: no #{Forcola.Precompiled.checksum_file()} \
        (precompiled fetch), and no cargo on PATH to build from source.

        Either install a Rust toolchain (https://rustup.rs) and recompile, \
        or recompile with FORCOLA_BUILD=1 after doing so.
        """)
    end
  end

  defp build_forced? do
    System.get_env("FORCOLA_BUILD") in ["1", "true"]
  end

  defp fetch_precompiled do
    dest = Path.join("priv", @bin_name)

    if File.exists?(dest) do
      # Even when the source-priv binary is already present and the
      # download is skipped, repair a stale or missing build priv so
      # Shim.path/0 resolves on a fresh install (see #47).
      sync_to_build_priv(dest)
    else
      version = Mix.Project.config()[:version]
      Mix.shell().info("Downloading precompiled forcola_shim v#{version}...")

      case Forcola.Precompiled.install(version, "priv") do
        {:ok, bin} ->
          sync_to_build_priv(bin)
          {:ok, []}

        {:error, message} ->
          Mix.raise("""
          Failed to fetch the precompiled forcola_shim binary: #{message}

          To build from source instead, install a Rust toolchain \
          (https://rustup.rs) and recompile with FORCOLA_BUILD=1.
          """)
      end
    end
  end

  defp build_and_copy(profile) do
    if System.find_executable("cargo") == nil do
      Mix.raise("FORCOLA_BUILD is set but cargo was not found on PATH (https://rustup.rs)")
    end

    src = Path.join([@native_dir, "target", profile_dir(profile), @bin_name])
    dest = Path.join(["priv", @bin_name])

    if profile == :release or stale?(src, dest) do
      Mix.shell().info("Building forcola_shim (#{profile})...")

      {output, exit_code} =
        System.cmd("cargo", ["build"] ++ profile_args(profile),
          cd: @native_dir,
          stderr_to_stdout: true
        )

      if exit_code != 0 do
        Mix.shell().error(output)
        Mix.raise("cargo build failed for forcola_shim (exit #{exit_code})")
      end

      File.mkdir_p!("priv")
      File.cp!(src, dest)
      File.chmod!(dest, 0o755)
      sync_to_build_priv(dest)
      {:ok, []}
    else
      # The cargo build was a noop, but the build priv may still be stale
      # or missing (fresh install), so repair it (see #47).
      sync_to_build_priv(dest)
    end
  end

  # Copies the source-priv binary into the build priv, where Shim.path/0
  # and `mix release` read it. Mix copies a dep's source priv into the
  # build priv before this compiler downloads/builds the binary and does
  # not re-sync afterward, so on a fresh install the build priv is stale
  # or missing. Runs every compile but guards on a stale/exists check so
  # it is cheap when already in sync. Uses Mix.Project.app_path/0, which
  # is correct at compile time (Application.app_dir/2 may not resolve).
  # Returns {:ok, []} when the build priv was (re)populated, {:noop, []}
  # otherwise.
  defp sync_to_build_priv(source) do
    build_dest = Path.join([Mix.Project.app_path(), "priv", @bin_name])

    if File.exists?(source) and stale?(source, build_dest) do
      File.mkdir_p!(Path.dirname(build_dest))
      File.cp!(source, build_dest)
      File.chmod!(build_dest, 0o755)
      {:ok, []}
    else
      {:noop, []}
    end
  end

  defp profile_dir(:release), do: "release"
  defp profile_dir(:debug), do: "debug"

  defp profile_args(:release), do: ["--release"]
  defp profile_args(:debug), do: []

  defp stale?(src, dest) do
    not File.exists?(dest) or
      (File.exists?(src) and mtime(src) > mtime(dest))
  end

  defp mtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> mtime
      {:error, _} -> 0
    end
  end
end
