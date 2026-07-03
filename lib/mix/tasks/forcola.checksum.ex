defmodule Mix.Tasks.Forcola.Checksum do
  @shortdoc "Writes checksum-forcola_shim.exs from the release artifacts"

  @moduledoc """
  Generates the checksum file the hex package ships.

  Downloads every per-target `forcola_shim` tarball for the current
  `mix.exs` version from GitHub Releases, computes each SHA256 locally,
  and writes `#{Forcola.Precompiled.checksum_file()}` (a map of artifact
  name to hex digest).

  Run after the release workflow for the tag has finished and before
  `mix hex.build` / `mix hex.publish`. The file is gitignored; it exists
  only in the published package, which is also what switches consumer
  compiles onto the download-and-verify path.
  """
  use Mix.Task

  @impl true
  def run(_args) do
    version = Mix.Project.config()[:version]

    checksums =
      for target <- Forcola.Precompiled.targets(), into: %{} do
        name = Forcola.Precompiled.artifact_name(version, target)
        url = Forcola.Precompiled.download_url(version, target)
        Mix.shell().info("Fetching #{url}")

        case Forcola.Precompiled.download(url) do
          {:ok, body} ->
            {name, Forcola.Precompiled.sha256(body)}

          {:error, message} ->
            Mix.raise("""
            Could not fetch #{name}: #{message}

            Has the release workflow for v#{version} finished and \
            attached all targets?
            """)
        end
      end

    path = Forcola.Precompiled.checksum_file()
    File.write!(path, inspect(checksums, pretty: true, limit: :infinity) <> "\n")
    Mix.shell().info("Wrote #{path} (#{map_size(checksums)} targets)")
  end
end
