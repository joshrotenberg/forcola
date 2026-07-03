defmodule Forcola.Precompiled do
  @moduledoc """
  Downloads and verifies precompiled `forcola_shim` binaries.

  The release workflow (`.github/workflows/release.yml`) builds the shim
  for each supported target and attaches
  `forcola_shim-v<version>-<target>.tar.gz` to the GitHub Release for
  the tag. `mix forcola.checksum` fetches every tarball for the current
  version, computes SHA256 digests locally, and writes
  `checksum-forcola_shim.exs`; that file ships in the hex package (it is
  gitignored in the repo).

  At compile time the `:forcola_shim` Mix compiler calls `install/2`,
  which detects the current target, downloads the matching tarball
  (cached under the user cache dir), verifies its SHA256 against the
  checksum file, and extracts the binary into `priv/`. Any checksum
  mismatch is an error; nothing is installed from an unverified
  tarball.
  """

  @targets [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-musl"
  ]

  @checksum_file "checksum-forcola_shim.exs"
  @base_url "https://github.com/joshrotenberg/forcola/releases/download"
  @bin_name "forcola_shim"

  @doc "All targets the release workflow builds."
  @spec targets() :: [String.t()]
  def targets, do: @targets

  @doc "Name of the checksum file shipped in the hex package."
  @spec checksum_file() :: String.t()
  def checksum_file, do: @checksum_file

  @doc """
  Maps an Erlang `:system_architecture` string to a release target.

  Returns `{:error, message}` for architectures the release workflow
  does not build; callers fall back to a local `cargo build` via
  `FORCOLA_BUILD=1`.
  """
  @spec target(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def target(system_architecture \\ system_architecture()) do
    with {:ok, arch} <- parse_arch(system_architecture),
         {:ok, os} <- parse_os(system_architecture) do
      {:ok, "#{arch}-#{os}"}
    end
  end

  @doc "Release asset name for a version and target."
  @spec artifact_name(String.t(), String.t()) :: String.t()
  def artifact_name(version, target) do
    "forcola_shim-v#{version}-#{target}.tar.gz"
  end

  @doc "GitHub Release download URL for a version and target."
  @spec download_url(String.t(), String.t()) :: String.t()
  def download_url(version, target) do
    "#{@base_url}/v#{version}/#{artifact_name(version, target)}"
  end

  @doc "Verifies `body` against a lowercase hex SHA256 digest."
  @spec verify(binary(), String.t()) :: :ok | {:error, String.t()}
  def verify(body, expected_sha256) do
    actual = sha256(body)

    if actual == String.downcase(expected_sha256) do
      :ok
    else
      {:error, "checksum mismatch: expected #{expected_sha256}, got #{actual}"}
    end
  end

  @doc "Lowercase hex SHA256 digest of a binary."
  @spec sha256(binary()) :: String.t()
  def sha256(body) do
    :sha256 |> :crypto.hash(body) |> Base.encode16(case: :lower)
  end

  @doc """
  Reads the checksum file: a map of artifact name to hex SHA256 digest.
  """
  @spec read_checksums(Path.t()) :: {:ok, map()} | {:error, String.t()}
  def read_checksums(path \\ @checksum_file) do
    if File.exists?(path) do
      {checksums, _bindings} = Code.eval_file(path)
      {:ok, checksums}
    else
      {:error, "checksum file #{path} not found"}
    end
  end

  @doc """
  Downloads `url` over HTTPS with peer verification.

  Follows redirects (GitHub release assets redirect to object storage).
  """
  @spec download(String.t()) :: {:ok, binary()} | {:error, String.t()}
  def download(url) do
    {:ok, _apps} = Application.ensure_all_started([:inets, :ssl])
    request = {String.to_charlist(url), [{~c"user-agent", ~c"forcola"}]}
    http_options = [ssl: ssl_options(), timeout: 120_000, connect_timeout: 15_000]

    case :httpc.request(:get, request, http_options, body_format: :binary) do
      {:ok, {{_http, 200, _status}, _headers, body}} -> {:ok, body}
      {:ok, {{_http, status, _reason}, _headers, _body}} -> {:error, "GET #{url}: HTTP #{status}"}
      {:error, reason} -> {:error, "GET #{url} failed: #{inspect(reason)}"}
    end
  end

  @doc """
  Fetches, verifies, and installs the shim binary into `priv_dir`.

  Returns the path to the installed binary. The tarball is cached under
  the user cache dir; a cached copy is used only if its checksum still
  matches, otherwise it is re-downloaded.
  """
  @spec install(String.t(), Path.t()) :: {:ok, Path.t()} | {:error, String.t()}
  def install(version, priv_dir \\ "priv") do
    with {:ok, target} <- target(),
         {:ok, checksums} <- read_checksums() do
      name = artifact_name(version, target)

      with {:ok, expected} <- expected_checksum(checksums, name),
           {:ok, body} <- fetch_artifact(name, download_url(version, target), expected),
           :ok <- extract(body, priv_dir) do
        bin = Path.join(priv_dir, @bin_name)
        File.chmod!(bin, 0o755)
        {:ok, bin}
      end
    end
  end

  defp expected_checksum(checksums, name) do
    case Map.fetch(checksums, name) do
      {:ok, expected} -> {:ok, expected}
      :error -> {:error, "no checksum entry for #{name} in #{@checksum_file}"}
    end
  end

  defp fetch_artifact(name, url, expected) do
    cache_path = Path.join(cache_dir(), name)

    case cached(cache_path, expected) do
      {:ok, body} ->
        {:ok, body}

      :miss ->
        with {:ok, body} <- download(url),
             :ok <- verify(body, expected) do
          cache_write(cache_path, body)
          {:ok, body}
        end
    end
  end

  defp cached(cache_path, expected) do
    with {:ok, body} <- File.read(cache_path),
         :ok <- verify(body, expected) do
      {:ok, body}
    else
      # Missing or stale cache entry: fall through to a fresh download,
      # which is verified before use.
      _miss -> :miss
    end
  end

  defp cache_dir do
    :user_cache |> :filename.basedir(~c"forcola") |> List.to_string()
  end

  defp cache_write(cache_path, body) do
    # Best effort: an unwritable cache dir must not fail the install.
    with :ok <- File.mkdir_p(Path.dirname(cache_path)) do
      File.write(cache_path, body)
    end

    :ok
  end

  defp extract(body, dest_dir) do
    File.mkdir_p!(dest_dir)

    case :erl_tar.extract({:binary, body}, [:compressed, {:cwd, String.to_charlist(dest_dir)}]) do
      :ok -> :ok
      {:error, reason} -> {:error, "tarball extraction failed: #{inspect(reason)}"}
    end
  end

  defp system_architecture do
    :system_architecture |> :erlang.system_info() |> List.to_string()
  end

  defp parse_arch(system_architecture) do
    case system_architecture |> String.split("-") |> hd() do
      "x86_64" -> {:ok, "x86_64"}
      "amd64" -> {:ok, "x86_64"}
      "aarch64" -> {:ok, "aarch64"}
      "arm64" -> {:ok, "aarch64"}
      other -> {:error, "unsupported architecture #{other} (#{system_architecture})"}
    end
  end

  defp parse_os(system_architecture) do
    cond do
      String.contains?(system_architecture, "apple-darwin") ->
        {:ok, "apple-darwin"}

      String.contains?(system_architecture, "linux") ->
        if String.contains?(system_architecture, "musl") do
          {:ok, "unknown-linux-musl"}
        else
          {:ok, "unknown-linux-gnu"}
        end

      true ->
        {:error, "unsupported OS (#{system_architecture})"}
    end
  end

  defp ssl_options do
    [
      verify: :verify_peer,
      cacerts: :public_key.cacerts_get(),
      depth: 3,
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]
  end
end
