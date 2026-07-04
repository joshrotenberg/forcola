defmodule Forcola.Result do
  @moduledoc """
  What an OS process did: exit status plus captured output.

  `:status` is the exit code for a normal exit, or `{:signal, number}`
  when the process died from a signal, where `number` is the signal
  number (for example `{:signal, 15}` for the SIGTERM Forcola itself
  sends on timeout). The only atom case is `{:signal, :unconfirmed}`,
  which means the shim could not confirm death before its own deadline;
  treat it as leaked and investigate.
  """

  @enforce_keys [:status]
  defstruct [:status, stdout: "", stderr: ""]

  @type t :: %__MODULE__{
          status: non_neg_integer() | {:signal, atom() | non_neg_integer()},
          stdout: binary(),
          stderr: binary()
        }
end
