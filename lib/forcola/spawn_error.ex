defmodule Forcola.SpawnError do
  @moduledoc """
  A native failure before the requested command could execute.

  Enable `structured_spawn_errors: true` in any execution mode to receive
  this value in that mode's existing spawn-error envelope. The fields are:

    * `:stage` - `:cwd`, `:exec`, `:privdrop`, or `:cgroup` for a failed
      working-directory change, executable launch, privilege operation,
      or cgroup setup/placement. `:spawn` and `:pty` identify other process
      or terminal setup failures.
    * `:reason` - a known POSIX error atom, such as `:enoent`, `:enotdir`,
      `:eacces`, or `:eperm`, or a native error-kind atom such as
      `:not_found` when no OS error number exists. Unknown reason codes
      remain strings; decoding never creates atoms from wire data.
    * `:errno` - the native OS error number, or `nil` when unavailable.
      Numbers can differ between operating systems; prefer `:reason`.
    * `:path` - the relevant path when the shim has one, otherwise `nil`.
      Paths describe the failed operation, not a separate filesystem probe.
    * `:message` - the original human-readable reason string, unchanged
      from the legacy ERROR frame. `Exception.message/1` returns it.

  For example, a missing working directory has `stage: :cwd` and
  `reason: :enoent`; a missing executable has `stage: :exec` with the same
  reason. Protocol validation errors and shim-startup failures keep their
  existing string, atom, or tuple forms.
  """

  defexception [:stage, :reason, :errno, :path, :message]

  @type stage :: :cwd | :exec | :privdrop | :cgroup | :spawn | :pty
  @type t :: %__MODULE__{
          stage: stage(),
          reason: atom() | String.t(),
          errno: pos_integer() | nil,
          path: String.t() | nil,
          message: String.t()
        }

  @stages %{
    "cwd" => :cwd,
    "exec" => :exec,
    "privdrop" => :privdrop,
    "cgroup" => :cgroup,
    "spawn" => :spawn,
    "pty" => :pty
  }
  @known_reasons [
    :e2big,
    :eacces,
    :eagain,
    :ebadf,
    :ebusy,
    :eexist,
    :efault,
    :eintr,
    :einval,
    :eio,
    :eisdir,
    :eloop,
    :emfile,
    :enametoolong,
    :enfile,
    :enodev,
    :enoent,
    :enoexec,
    :enomem,
    :enospc,
    :enosys,
    :enotdir,
    :enotsup,
    :eopnotsupp,
    :eperm,
    :epipe,
    :erofs,
    :esrch,
    :etxtbsy,
    :not_found,
    :permission_denied,
    :already_exists,
    :broken_pipe,
    :would_block,
    :invalid_input,
    :invalid_data,
    :timed_out,
    :write_zero,
    :interrupted,
    :unsupported,
    :unexpected_eof,
    :out_of_memory,
    :other
  ]
  @reasons Map.new(@known_reasons, &{Atom.to_string(&1), &1})

  @doc false
  @spec from_report(term(), String.t()) :: t() | :invalid_error
  def from_report(
        %{"stage" => stage, "reason" => reason, "errno" => errno, "path" => path},
        message
      )
      when is_map_key(@stages, stage) and is_binary(reason) and byte_size(reason) > 0 and
             (errno in [nil, :null] or (is_integer(errno) and errno > 0)) and
             (path in [nil, :null] or is_binary(path)) and is_binary(message) do
    %__MODULE__{
      stage: Map.fetch!(@stages, stage),
      reason: Map.get(@reasons, reason, reason),
      errno: null_to_nil(errno),
      path: null_to_nil(path),
      message: message
    }
  end

  def from_report(_report, _message), do: :invalid_error

  # Erlang's :json decoder represents JSON null as :null, not Elixir nil.
  defp null_to_nil(:null), do: nil
  defp null_to_nil(value), do: value
end
