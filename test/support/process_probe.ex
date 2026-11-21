defmodule Forcola.TestSupport.ProcessProbe do
  @moduledoc false
  # Reads a process's state, parent pid and process-group id in one `ps`
  # snapshot, using only flags that procps (Linux) and macOS `ps` share.

  @type info :: %{state: String.t(), ppid: String.t(), pgid: String.t()}

  @doc """
  Returns `{:ok, info}` or `:missing`. A `ps` failure with output, or
  unparsable output, raises. `ps` exits 1 with empty output for a missing pid.
  """
  @spec info(String.t()) :: {:ok, info} | :missing
  def info(pid) when is_binary(pid) do
    {output, status} =
      System.cmd("ps", ["-o", "stat=,ppid=,pgid=", "-p", pid], stderr_to_stdout: true)

    trimmed = String.trim(output)

    case {status, String.split(trimmed)} do
      {1, []} ->
        :missing

      {0, [state, ppid, pgid]} ->
        {:ok, %{state: state, ppid: ppid, pgid: pgid}}

      _ ->
        raise "ps failed for pid #{pid} (exit #{status}): #{inspect(output)}"
    end
  end

  @doc "A zombie counts as dead: a killed orphan stays one until its reaper runs."
  @spec alive?(String.t()) :: boolean
  def alive?(pid) do
    case info(pid) do
      {:ok, %{state: "Z" <> _}} -> false
      {:ok, _} -> true
      :missing -> false
    end
  end
end
