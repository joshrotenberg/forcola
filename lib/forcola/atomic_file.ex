defmodule Forcola.AtomicFile do
  @moduledoc false

  import Bitwise, only: [band: 2]

  @executable_mode 0o755

  @doc false
  @spec copy_executable_if_changed(Path.t(), Path.t()) :: :changed | :unchanged
  def copy_executable_if_changed(source, destination) do
    if same_contents?(source, destination) do
      repair_mode(destination)
    else
      atomic_copy(source, destination)
      :changed
    end
  end

  defp same_contents?(source, destination) do
    with {:ok, %File.Stat{size: size}} <- File.stat(source),
         {:ok, %File.Stat{size: ^size}} <- File.stat(destination),
         {:ok, source_body} <- File.read(source),
         {:ok, destination_body} <- File.read(destination) do
      source_body == destination_body
    else
      _missing_or_different -> false
    end
  end

  defp repair_mode(destination) do
    case File.stat(destination) do
      {:ok, %File.Stat{mode: mode}} when band(mode, 0o777) == @executable_mode ->
        :unchanged

      {:ok, _stat} ->
        File.chmod!(destination, @executable_mode)
        :changed
    end
  end

  defp atomic_copy(source, destination) do
    File.mkdir_p!(Path.dirname(destination))
    temporary = temporary_path(destination)

    try do
      File.cp!(source, temporary)
      File.chmod!(temporary, @executable_mode)
      File.rename!(temporary, destination)
    after
      File.rm(temporary)
    end
  end

  defp temporary_path(destination) do
    directory = Path.dirname(destination)
    basename = Path.basename(destination)
    unique = System.unique_integer([:monotonic, :positive])
    Path.join(directory, ".#{basename}.tmp-#{unique}")
  end
end
