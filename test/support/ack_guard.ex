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

  @doc false
  def handle(
        _event,
        _measurements,
        %{result: :ok, cell: cell, txid: txid},
        {store, prefix, cell?, log}
      ) do
    if cell?.(cell) do
      {module, config} = store
      key = Keys.segment(prefix, cell, 0, txid, txid)

      :ets.insert(log, {:ack, cell, txid})

      case module.head(config, key) do
        {:ok, _etag, _meta} -> :ok
        _ -> :ets.insert(log, {:early, cell, txid})
      end
    end

    :ok
  end

  def handle(_event, _measurements, _meta, _config), do: :ok
end
