defmodule Forcola.Result do
  @moduledoc """
  What an OS process did: exit status plus captured output.

  `:status` is the exit code for a normal exit, or `{:signal, name}` when
  the process died from a signal (including the SIGTERM/SIGKILL Forcola
  itself sends on timeout). `{:signal, :unconfirmed}` means the shim could
  not confirm death before its own deadline; treat it as leaked and
  investigate.
  """

  @enforce_keys [:status]
  defstruct [:status, stdout: "", stderr: ""]

  @type t :: %__MODULE__{
          status: non_neg_integer() | {:signal, atom() | non_neg_integer()},
          stdout: binary(),
          stderr: binary()
        }
end
