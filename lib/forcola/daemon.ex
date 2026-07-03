defmodule Forcola.Daemon do
  @moduledoc """
  A long-running external server process under a supervision tree.

  For processes that are supposed to outlive a single call (redis-server,
  a dev proxy) but must never outlive their supervisor. The child runs in
  its own process group; when this GenServer terminates for any reason,
  the shim kills the group.

  Planned API:

      children = [
        {Forcola.Daemon,
         argv: ["redis-server", "--port", "6399"],
         name: MyApp.Redis}
      ]

  Unlike bounded runs, a daemon has no `:timeout_ms`; its bound is its
  supervisor.
  """

  use GenServer

  @doc false
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @impl true
  def init(opts) do
    raise Forcola.NotImplementedError, {__MODULE__, :init, [opts]}
  end
end
