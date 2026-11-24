defmodule Alambique.AckGuard do
  @moduledoc """
  Checks, at the moment a cell acknowledges a commit, that the segment of its
  TXID is in the store (design §15): a write acknowledged before its put
  landed, or after the put failed, is recorded here even when the put lands
  later and the end state looks right.

      guard = AckGuard.attach(store, prefix, &String.starts_with?(&1, "mine"))
      ...
      AckGuard.violations(guard) == []
      AckGuard.detach(guard)

  On peer nodes, `attach_peers/1` and `watch/1` do the same from the node that
  holds the stores.
  """
  alias Alambique.Replication.Keys

  @event [:alambique, :cell, :commit]

  @doc """
  Watches the commits of the cells `cell?` accepts. `store` is the
  `{module, config}` the segments are read from.
  """
  def attach({_module, _config} = store, prefix, cell?) do
    log = :ets.new(:ack_guard, [:public, :bag])
    id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach(id, @event, &__MODULE__.handle/4, {store, prefix, cell?, log})
    %{id: id, log: log}
  end

  @doc "Stops watching."
  def detach(%{id: id, log: log}) do
    :telemetry.detach(id)
    :ets.delete(log)
    :ok
  end

  @doc "Acknowledgements seen so far: `{cell, txid}`."
  def acks(%{log: log}), do: for({:ack, cell, txid} <- :ets.tab2list(log), do: {cell, txid})

  @doc "Acknowledgements whose segment was not in the store: `{cell, txid}`."
  def violations(%{log: log}),
    do: for({:early, cell, txid} <- :ets.tab2list(log), do: {cell, txid})

  @doc """
  Watches the commits of peer nodes from the node that holds the stores.
  `places` maps a location id to `{store, prefix}`. Run `watch/1` on each peer
  (again after it restarts); every acknowledgement is checked from this node
  before the peer's cell goes on.
  """
  def attach_peers(places) do
    %{
      id: {__MODULE__, make_ref()},
      log: :ets.new(:ack_guard, [:public, :bag]),
      places: places,
      node: node()
    }
  end

  @doc "Runs on a peer: sends each acknowledgement of its cells to the node of `guard`."
  def watch(%{id: id} = guard) do
    _ = :telemetry.attach(id, @event, &__MODULE__.handle_peer/4, guard)
    :ok
  end

  @doc false
  def handle(
        _event,
        _measurements,
        %{result: :ok, cell: cell, txid: txid},
        {store, prefix, cell?, log}
      ) do
    if cell?.(cell), do: check(log, store, prefix, cell, txid)
    :ok
  end

  def handle(_event, _measurements, _meta, _config), do: :ok

  @doc false
  def handle_peer(
        _event,
        _measurements,
        %{result: :ok, cell: cell, txid: txid, location: location},
        %{node: node, log: log, places: places}
      ) do
    try do
      :erpc.call(node, __MODULE__, :record, [log, places, location, cell, txid], 10_000)
    catch
      _, _ -> :ok
    end

    :ok
  end

  def handle_peer(_event, _measurements, _meta, _guard), do: :ok

  @doc false
  def record(log, places, location, cell, txid) do
    case Map.get(places, location) do
      {store, prefix} -> check(log, store, prefix, cell, txid)
      nil -> :ok
    end

    :ok
  end

  defp check(log, {module, config}, prefix, cell, txid) do
    :ets.insert(log, {:ack, cell, txid})

    case module.head(config, Keys.segment(prefix, cell, 0, txid, txid)) do
      {:ok, _etag, _meta} -> :ok
      _ -> :ets.insert(log, {:early, cell, txid})
    end
  end
end
