defmodule Alambique.Replica.Read do
  @moduledoc false
  # `Alambique.query/5` with `consistency: {:bounded, ms}` (design §21),
  # resolved on the calling node:
  #
  #   1. replicas are off, or the cell is awake here: the owner path;
  #   2. a ready replica here answers, if it is fresh enough for the bound;
  #   3. otherwise the owner answers, and when the cell's marker is on a
  #      replica starts in the background for the next reads.
  #
  # The owner path is the plain query request, so nothing new is on the wire.

  alias Alambique.{Cell, Config, Placement, Router, Telemetry}
  alias Alambique.Replica
  alias Alambique.Replica.{Manager, Marker}

  @spec query(map, String.t(), String.t(), list, pos_integer, timeout | nil) ::
          {:ok, map} | {:error, term}
  def query(config, cell, sql, params, bound, timeout) do
    started = System.monotonic_time()
    q = System.monotonic_time(:microsecond)
    timeout = timeout || config.call_timeout_ms

    case route(config, cell, sql, params, q, bound, timeout) do
      {:replica, reply, %{txid: txid, lag_ms: lag_ms}} ->
        Telemetry.span_stop([:replica, :read], started, %{lag_ms: lag_ms}, %{
          cell: cell,
          source: :replica,
          txid: txid
        })

        reply

      {:owner, reason} ->
        reply = Router.call(config.name, cell, {:query, sql, params, nil}, timeout)

        Telemetry.span_stop([:replica, :read], started, %{}, %{
          cell: cell,
          source: :owner,
          reason: reason
        })

        reply
    end
  end

  defp route(%{replicas: nil}, _cell, _sql, _params, _q, _bound, _timeout), do: {:owner, :off}

  defp route(config, cell, sql, params, q, bound, timeout) do
    with nil <- Cell.whereis(config, cell),
         {:ok, location} <- Placement.locate(config, cell) do
      case Registry.lookup(config.replica_registry, {location.id, cell}) do
        [{pid, _}] ->
          Replica.read(pid, sql, params, q, bound, timeout)

        [] ->
          request(config, location, cell)
          {:owner, :no_replica}
      end
    else
      pid when is_pid(pid) -> {:owner, :owner_here}
      # The owner path reports a bad id or a failing placement.
      {:error, _} -> {:owner, :placement}
    end
  end

  # Starts a replica for the next reads when the cell's marker is on.
  defp request(config, location, cell) do
    ctx = Config.repl_ctx(config, location)

    if Marker.cached?(config, ctx, location, cell),
      do: Manager.start_replica(config, cell, location)
  end
end
