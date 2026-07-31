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
     checkout, CI), a debug `cargo build` runs. Cargo decides whether
     Rust inputs are fresh; the resulting binary is installed only
     when its content changed. This is the development path.
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
    version = Mix.Project.config()[:version]
    Mix.shell().info("Installing precompiled forcola_shim v#{version}...")

    case Forcola.Precompiled.install_with_status(version, "priv") do
      {:ok, bin, source_status} ->
        build_status = sync_to_build_priv(bin)
        compiler_status(source_status, build_status)

      {:error, message} ->
        Mix.raise("""
        Failed to fetch the precompiled forcola_shim binary: #{message}

        To build from source instead, install a Rust toolchain \
        (https://rustup.rs) and recompile with FORCOLA_BUILD=1.
        """)
    end
  end

  @doc false
  def build_and_copy(profile, opts \\ []) do
    cargo = Keyword.get(opts, :cargo, System.find_executable("cargo"))
    native_dir = Keyword.get(opts, :native_dir, @native_dir)
    dest = Keyword.get(opts, :dest, Path.join(["priv", @bin_name]))

    if cargo == nil do
      Mix.raise("FORCOLA_BUILD is set but cargo was not found on PATH (https://rustup.rs)")
    end

    src = Path.join([native_dir, "target", profile_dir(profile), @bin_name])
    Mix.shell().info("Building forcola_shim (#{profile})...")

    {output, exit_code} =
      System.cmd(cargo, ["build"] ++ profile_args(profile),
        cd: native_dir,
        stderr_to_stdout: true
      )

    if exit_code != 0 do
      Mix.shell().error(output)
      Mix.raise("cargo build failed for forcola_shim (exit #{exit_code})")
    end

    source_status = Forcola.AtomicFile.copy_executable_if_changed(src, dest)
    build_status = sync_to_build_priv(dest, Keyword.get(opts, :build_dest))
    compiler_status(source_status, build_status)
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
  defp sync_to_build_priv(source, build_dest \\ nil) do
    build_dest = build_dest || Path.join([Mix.Project.app_path(), "priv", @bin_name])

    if File.exists?(source) do
      Forcola.AtomicFile.copy_executable_if_changed(source, build_dest)
    else
      :unchanged
    end
  end

  defp compiler_status(:unchanged, :unchanged), do: {:noop, []}
  defp compiler_status(_source_status, _build_status), do: {:ok, []}

  defp profile_dir(:release), do: "release"
  defp profile_dir(:debug), do: "debug"

  defp profile_args(:release), do: ["--release"]
  defp profile_args(:debug), do: []
end
