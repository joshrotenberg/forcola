defmodule Forcola.Tracking do
  @moduledoc false

  alias Forcola.Shim

  @limit 4_096

  defstruct enabled: false,
            observer: nil,
            groups: [],
            seen: MapSet.new(),
            valid: true,
            finished: false

  def validate_opts!(opts) do
    unless Keyword.get(opts, :track_descendants, false) in [true, false],
      do: raise(ArgumentError, "track_descendants must be a boolean")

    case opts[:track_observer] do
      nil ->
        opts

      {pid, ref} when is_pid(pid) and node(pid) == node() and is_reference(ref) ->
        Keyword.put(opts, :track_descendants, true)

      _ ->
        raise ArgumentError, "track_observer must be {local_pid, reference} or nil"
    end
  end

  def new(opts),
    do: %__MODULE__{
      enabled: Keyword.get(opts, :track_descendants, false),
      observer: opts[:track_observer]
    }

  def groups(tracking), do: Enum.reverse(tracking.groups)

  def record(%{enabled: false} = tracking, _payload), do: {tracking, nil}

  def record(tracking, payload) do
    case Shim.decode_identity(payload) do
      {:ok, %{pid: pid, pgid: pid} = identity} -> record_identity(tracking, identity)
      _ -> {%{tracking | valid: false}, nil}
    end
  end

  defp record_identity(tracking, identity) do
    cond do
      MapSet.member?(tracking.seen, identity) ->
        {tracking, nil}

      MapSet.size(tracking.seen) >= @limit ->
        {%{tracking | valid: false}, nil}

      true ->
        {%{
           tracking
           | groups: [identity | tracking.groups],
             seen: MapSet.put(tracking.seen, identity)
         }, identity}
    end
  end

  def notify_record(_tracking, nil), do: :ok
  def notify_record(tracking, identity), do: notify(tracking, {:tracked, identity})

  def report(%{enabled: false}, payload) do
    report = Shim.decode_exit_report(payload)
    Map.put(report, :scope, if(report.contained, do: :active_cgroup, else: :process_group))
  end

  def report(tracking, payload) do
    case Shim.decode_json(payload) do
      {:ok, report} when is_map(report) -> report_map(tracking, report)
      _ -> report_map(tracking, %{})
    end
  end

  def report_map(tracking, report) do
    status = raw_status(report)
    scope = scope(report["scope"])

    valid_flags =
      Enum.all?(["timed_out", "contained", "output_truncated"], fn key ->
        not Map.has_key?(report, key) or is_boolean(report[key])
      end)

    %{
      status: status,
      confirmed:
        report["confirmed"] == true and status != nil and valid_flags and
          scope != :unknown and tracking.valid and tracking.groups != [],
      scope: scope,
      timed_out: report["timed_out"] == true,
      contained: report["contained"] == true,
      output_truncated: report["output_truncated"] == true
    }
  end

  defp scope("tree_contained"), do: :tree_contained
  defp scope("tree_sampled"), do: :tree_sampled
  defp scope(_scope), do: :unknown

  defp raw_status(%{"status" => status} = report) when is_integer(status) and status >= 0 do
    if report["signal"] in [nil, :null], do: status
  end

  defp raw_status(%{"signal" => signal} = report) when is_integer(signal) and signal > 0 do
    if report["status"] in [nil, :null], do: {:signal, signal}
  end

  defp raw_status(_report), do: nil

  def status(%{confirmed: true, status: status}), do: status
  def status(_report), do: {:signal, :unconfirmed}

  def confirmation(%{confirmed: true}), do: :confirmed
  def confirmation(_report), do: :unconfirmed

  def finish(%{enabled: false} = tracking, _scope, _confirmation), do: tracking
  def finish(%{finished: true} = tracking, _scope, _confirmation), do: tracking

  def finish(tracking, scope, confirmation) do
    notify(tracking, {:tracking_exit, %{scope: scope, confirmation: confirmation}})
    %{tracking | finished: true}
  end

  def startup_failed(opts), do: opts |> new() |> finish(:unknown, :not_started)

  defp notify(%{observer: nil}, _event), do: :ok
  defp notify(%{observer: {pid, ref}}, event), do: send(pid, {ref, event})
end
