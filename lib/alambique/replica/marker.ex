defmodule Alambique.Replica.Marker do
  @moduledoc false
  # The per-cell switch of read replicas (design §21): the object
  # `<cell prefix>/replicas`. Listing a cell drops the key, so the marker is
  # not taken for a segment.

  alias Alambique.Replica.Manager
  alias Alambique.Replication.{Keys, Retry}

  @doc "Turns replicas on for `cell`. Putting it again succeeds."
  @spec enable(Alambique.Replication.ctx(), String.t()) :: :ok | {:error, term}
  def enable(ctx, cell) do
    {store, config} = ctx.store
    {clock, clock_config} = ctx.clock
    body = Jason.encode!(%{cell: cell, enabled_at_ms: clock.now_ms(clock_config), by: ctx.node})

    Retry.run(ctx, Retry.deadline_in(ctx.timeout_ms), fn ->
      case store.put(config, Keys.replicas(ctx.prefix, cell), body, :none, %{}) do
        {:ok, _etag} -> :ok
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc "Turns replicas off for `cell`. Deleting a missing marker succeeds."
  @spec disable(Alambique.Replication.ctx(), String.t()) :: :ok | {:error, term}
  def disable(ctx, cell) do
    {store, config} = ctx.store

    Retry.run(ctx, Retry.deadline_in(ctx.timeout_ms), fn ->
      case store.delete(config, Keys.replicas(ctx.prefix, cell)) do
        :ok -> :ok
        {:error, :not_found} -> :ok
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc "Whether the marker exists, retried until `deadline` (monotonic milliseconds)."
  @spec enabled?(Alambique.Replication.ctx(), String.t(), integer) ::
          {:ok, boolean} | {:error, term}
  def enabled?(ctx, cell, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.head(config, Keys.replicas(ctx.prefix, cell)) do
        {:ok, _etag, _meta} -> {:ok, true}
        {:error, :not_found} -> {:ok, false}
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc """
  Whether the marker is on, as this node last saw it. A reading older than
  `marker_ttl_ms` (positive or negative) is read again, once and without
  retries; a store error counts as off and is not remembered.
  """
  @spec cached?(map, Alambique.Replication.ctx(), Alambique.Location.t(), String.t()) ::
          boolean
  def cached?(config, ctx, location, cell) do
    key = {:marker, location.id, cell}

    case Manager.memo(config, key) do
      nil ->
        case enabled?(ctx, cell, Retry.deadline_in(0)) do
          {:ok, enabled} ->
            remember(config, location, cell, enabled)
            enabled

          {:error, _} ->
            false
        end

      enabled ->
        enabled == :on
    end
  end

  @doc "Records what this node knows about the marker of `cell`."
  @spec remember(map, Alambique.Location.t(), String.t(), boolean) :: true
  def remember(config, location, cell, enabled) do
    Manager.memo_put(
      config,
      {:marker, location.id, cell},
      if(enabled, do: :on, else: :off),
      config.replicas.marker_ttl_ms
    )
  end
end
