defmodule Forcola.Shim do
  @moduledoc """
  Locates and speaks to the `forcola_shim` binary.

  The shim is a Rust port program (source under `native/forcola_shim`).
  Release builds are published per target to GitHub Releases and fetched
  at compile time with SHA256 verification, so consumers need no Rust
  toolchain; a locally built binary in `priv/` takes precedence during
  development.

  The wire protocol is documented in `native/forcola_shim/src/main.rs`.
  This module is internal; use `Forcola.run/2` and friends.
  """

  @doc """
  Absolute path to the shim binary for the current target.

  Returns `{:error, :not_found}` until the build and download machinery
  lands.
  """
  @spec path() :: {:ok, Path.t()} | {:error, :not_found}
  def path do
    case Application.app_dir(:forcola, "priv/forcola_shim") do
      p when is_binary(p) ->
        if File.exists?(p), do: {:ok, p}, else: {:error, :not_found}
    end
  end
end
