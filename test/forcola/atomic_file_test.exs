defmodule Forcola.AtomicFileTest do
  use ExUnit.Case, async: true

  alias Forcola.AtomicFile

  @tag :tmp_dir
  test "replaces an executable atomically", %{tmp_dir: tmp_dir} do
    source = Path.join(tmp_dir, "source")
    destination = Path.join(tmp_dir, "destination")
    File.write!(source, "new bytes")
    File.write!(destination, "old bytes")
    File.chmod!(destination, 0o755)

    old_handle = File.open!(destination, [:read, :binary])

    try do
      assert :changed = AtomicFile.copy_executable_if_changed(source, destination)
      assert File.read!(destination) == "new bytes"
      assert IO.binread(old_handle, :eof) == "old bytes"
      assert executable_mode(destination) == 0o755
    after
      File.close(old_handle)
    end
  end

  @tag :tmp_dir
  test "does not replace an executable whose content and mode match", %{tmp_dir: tmp_dir} do
    source = Path.join(tmp_dir, "source")
    destination = Path.join(tmp_dir, "destination")
    File.write!(source, "same bytes")
    File.write!(destination, "same bytes")
    File.chmod!(destination, 0o755)
    inode = File.stat!(destination).inode

    assert :unchanged = AtomicFile.copy_executable_if_changed(source, destination)
    assert File.stat!(destination).inode == inode
  end

  @tag :tmp_dir
  test "repairs executable permissions without replacing matching content", %{tmp_dir: tmp_dir} do
    source = Path.join(tmp_dir, "source")
    destination = Path.join(tmp_dir, "destination")
    File.write!(source, "same bytes")
    File.write!(destination, "same bytes")
    File.chmod!(destination, 0o644)
    inode = File.stat!(destination).inode

    assert :changed = AtomicFile.copy_executable_if_changed(source, destination)
    assert File.stat!(destination).inode == inode
    assert executable_mode(destination) == 0o755
  end

  @tag :tmp_dir
  test "removes the temporary executable when rename fails", %{tmp_dir: tmp_dir} do
    source = Path.join(tmp_dir, "source")
    destination = Path.join(tmp_dir, "destination")
    File.write!(source, "new bytes")
    File.mkdir_p!(destination)

    assert_raise File.RenameError, fn ->
      AtomicFile.copy_executable_if_changed(source, destination)
    end

    assert Path.wildcard(Path.join(tmp_dir, ".destination.tmp-*")) == []
  end

  defp executable_mode(path), do: Bitwise.band(File.stat!(path).mode, 0o777)
end
