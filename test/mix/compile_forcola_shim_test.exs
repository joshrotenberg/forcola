defmodule Mix.Tasks.Compile.ForcolaShimTest do
  # async: false -- mutates the on-disk build priv and reruns the compiler,
  # which must not race other tests that spawn the shim.
  use ExUnit.Case, async: false

  alias Mix.Tasks.Compile.ForcolaShim

  @tag :tmp_dir
  test "source builds defer freshness to Cargo instead of target mtimes", %{tmp_dir: tmp_dir} do
    fixture = compiler_fixture(tmp_dir, "new shim")
    File.mkdir_p!(Path.dirname(fixture.dest))
    File.write!(fixture.dest, "stale shim")
    File.touch!(fixture.dest, System.os_time(:second) + 3_600)

    assert {:ok, []} =
             ForcolaShim.build_and_copy(:debug,
               cargo: fixture.cargo,
               native_dir: fixture.native_dir,
               dest: fixture.dest,
               build_dest: fixture.build_dest
             )

    assert File.read!(fixture.dest) == "new shim"
    assert File.read!(fixture.build_dest) == "new shim"
  end

  @tag :tmp_dir
  test "source builds report noop when installed content is unchanged", %{tmp_dir: tmp_dir} do
    fixture = compiler_fixture(tmp_dir, "same shim")
    File.mkdir_p!(Path.dirname(fixture.dest))
    File.mkdir_p!(Path.dirname(fixture.build_dest))
    File.write!(fixture.dest, "same shim")
    File.write!(fixture.build_dest, "same shim")
    File.chmod!(fixture.dest, 0o755)
    File.chmod!(fixture.build_dest, 0o755)

    destination_inode = File.stat!(fixture.dest).inode
    build_inode = File.stat!(fixture.build_dest).inode

    assert {:noop, []} =
             ForcolaShim.build_and_copy(:debug,
               cargo: fixture.cargo,
               native_dir: fixture.native_dir,
               dest: fixture.dest,
               build_dest: fixture.build_dest
             )

    assert File.stat!(fixture.dest).inode == destination_inode
    assert File.stat!(fixture.build_dest).inode == build_inode
  end

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

    saved_bin = File.read!(source_bin)
    # Keep the entire original priv entry, whether it is a symlink or a real
    # directory. Restoring an empty directory loses the built shim on runners
    # where Mix copied priv instead of linking it.
    backup =
      build_priv <>
        ".test-backup-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

    File.rename!(build_priv, backup)

    on_exit(fn ->
      # Restore the original build priv and source binary for later tests.
      File.rm_rf!(build_priv)
      File.rename!(backup, build_priv)

      File.mkdir_p!(Path.dirname(source_bin))
      File.write!(source_bin, saved_bin)
      File.chmod!(source_bin, 0o755)
    end)

    # Stand in for a freshly copied dependency priv: a real directory that
    # does not yet contain the downloaded/built shim, while the source
    # priv (left untouched) does.
    File.mkdir_p!(build_priv)

    refute File.exists?(build_bin)
    assert {:error, :not_found} = Forcola.Shim.path()

    # Re-run the compiler; it must sync the source-priv shim into the
    # build priv even though the source priv already has it.
    Mix.Task.rerun("compile.forcola_shim")

    assert File.exists?(build_bin), "compiler did not repopulate the build priv"
    assert {:ok, ^build_bin} = Forcola.Shim.path()
  end

  defp compiler_fixture(tmp_dir, target_body) do
    native_dir = Path.join(tmp_dir, "native/forcola_shim")
    target = Path.join(native_dir, "target/debug/forcola_shim")
    cargo = Path.join(tmp_dir, "cargo")

    File.mkdir_p!(Path.dirname(target))
    File.write!(target, target_body)
    File.write!(cargo, "#!/bin/sh\nexit 0\n")
    File.chmod!(cargo, 0o755)

    %{
      native_dir: native_dir,
      cargo: cargo,
      dest: Path.join(tmp_dir, "source_priv/forcola_shim"),
      build_dest: Path.join(tmp_dir, "build_priv/forcola_shim")
    }
  end
end
