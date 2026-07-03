defmodule Forcola.NotImplementedError do
  @moduledoc """
  Raised by scaffold stubs. Every raiser corresponds to a planned function
  documented in the module that raises it; the exception exists so that
  accidental use of the scaffold fails loudly instead of silently leaking
  processes through a fallback path.
  """

  defexception [:mfa]

  @impl true
  def exception({m, f, args}) do
    %__MODULE__{mfa: {m, f, length(args)}}
  end

  @impl true
  def message(%__MODULE__{mfa: {m, f, a}}) do
    "#{inspect(m)}.#{f}/#{a} is not implemented yet; forcola is a scaffold"
  end
end
