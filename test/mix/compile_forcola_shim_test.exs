defmodule Mix.Tasks.Compile.ForcolaShimTest do
  # async: false -- mutates the on-disk build priv and reruns the compiler,
  # which must not race other tests that spawn the shim.
  use ExUnit.Case, async: false

  # Reproduces the fresh-install condition from #47.
  #
  # When forcola is a dependency, Mix COPIES the dep's source priv/ into
  # the build priv (`Application.app_dir(:forcola, "priv")`) before the
  # custom :forcola_shim compiler downloads/builds the binary, and does
  # not re-sync afterward. So on a fresh install the build priv is a real
  # directory missing the shim, while the source priv has it, and
  # `Forcola.Shim.path/0` (which reads the build priv) returns
  # `{:error, :not_found}`.
  #
  # In forcola's own build the build priv is instead a SYMLINK to the
  # source priv, so the two can never diverge and the bug is invisible.
  # This test replaces that symlink with a real, shim-less directory to
  # stand in for the copied dep priv, then reruns the compiler and
  # asserts it repopulates the build priv. Before the fix the compiler
  # writes only to the source priv and never touches the build priv, so
  # the assertion fails.
  test "compiler repopulates a stale build priv so Shim.path/0 resolves" do
    build_priv = Application.app_dir(:forcola, "priv")
    build_bin = Path.join(build_priv, "forcola_shim")
    source_bin = Path.expand("priv/forcola_shim")

    # A normal compile leaves the shim in the source priv; the suite needs it.
    assert File.exists?(source_bin), "expected a compiled source-priv shim before the test"

    link_target =
      case File.read_link(build_priv) do
        {:ok, target} -> target
        {:error, _} -> nil
      end

    saved_bin = File.read!(source_bin)

    on_exit(fn ->
      # Restore the original build priv (symlink to the source priv), and
      # make sure the shim is back in place for the rest of the suite.
      File.rm_rf!(build_priv)

      if link_target do
        File.ln_s!(link_target, build_priv)
      else
        File.mkdir_p!(build_priv)
      end

      File.mkdir_p!(Path.dirname(source_bin))
      File.write!(source_bin, saved_bin)
      File.chmod!(source_bin, 0o755)
    end)

    # Stand in for a freshly copied dependency priv: a real directory that
    # does not yet contain the downloaded/built shim, while the source
    # priv (left untouched) does.
    File.rm_rf!(build_priv)
    File.mkdir_p!(build_priv)

    refute File.exists?(build_bin)
    assert {:error, :not_found} = Forcola.Shim.path()

    # Re-run the compiler; it must sync the source-priv shim into the
    # build priv even though the source priv already has it.
    Mix.Task.rerun("compile.forcola_shim")

    assert File.exists?(build_bin), "compiler did not repopulate the build priv"
    assert {:ok, ^build_bin} = Forcola.Shim.path()
  end
end
