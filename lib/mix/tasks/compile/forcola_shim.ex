defmodule Mix.Tasks.Compile.ForcolaShim do
  @moduledoc """
  Builds the `forcola_shim` Rust binary and copies it into `priv/`.

  Development/test convenience only: it shells out to `cargo build` in
  `native/forcola_shim` and copies the resulting binary to
  `priv/forcola_shim` if the source is newer than the existing copy.
  Release builds fetch a precompiled binary instead (not yet
  implemented); this compiler is a no-op if `cargo` is not on `PATH`,
  since a real user of the published package build never has a Rust
  toolchain.
  """
  use Mix.Task.Compiler

  @native_dir "native/forcola_shim"
  @bin_name "forcola_shim"

  @impl true
  def run(_args) do
    cond do
      System.find_executable("cargo") == nil ->
        :noop

      not File.dir?(@native_dir) ->
        :noop

      true ->
        build_and_copy()
    end
  end

  defp build_and_copy do
    src = Path.join([@native_dir, "target", "debug", @bin_name])
    dest = Path.join(["priv", @bin_name])

    if stale?(src, dest) do
      Mix.shell().info("Building forcola_shim...")

      {output, exit_code} =
        System.cmd("cargo", ["build"], cd: @native_dir, stderr_to_stdout: true)

      if exit_code != 0 do
        Mix.shell().error(output)
        Mix.raise("cargo build failed for forcola_shim (exit #{exit_code})")
      end

      File.mkdir_p!("priv")
      File.cp!(src, dest)
      File.chmod!(dest, 0o755)
      {:ok, []}
    else
      {:noop, []}
    end
  end

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
