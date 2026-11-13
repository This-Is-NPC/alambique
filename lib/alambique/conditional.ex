defmodule Alambique.Conditional do
  @moduledoc false
  # What the lease and the node registry share: small JSON objects that a node
  # rewrites with `If-Match` while it holds them (design §5, §22). Both take a
  # context with `:store`, `:clock`, `:ttl_ms` and `:skew_margin_ms`.

  alias Alambique.Replication.Retry

  @doc "Whether `ttl_ms` is above twice `skew_margin_ms`, so a renewal fits in the TTL."
  @spec timing?(term, term) :: boolean
  def timing?(ttl_ms, skew_ms),
    do: is_integer(ttl_ms) and is_integer(skew_ms) and skew_ms >= 0 and ttl_ms > 2 * skew_ms

  @doc "A random token that identifies one acquisition."
  @spec token() :: String.t()
  def token, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  @doc "The time of the context's clock, in milliseconds."
  @spec now(map) :: integer
  def now(%{clock: {clock, config}}), do: clock.now_ms(config)

  # A request that outlives the object cannot help: it is killed after the
  # time the object can be used (`ttl - skew margin`) and answers
  # `{:error, :timeout}`, which every caller already handles as a store error (a
  # `put` may or may not have landed: callers read it back).
  defp bound(ctx), do: ctx.ttl_ms - ctx.skew_margin_ms

  @doc "One conditional `put`, bounded in time."
  @spec put(map, String.t(), iodata, Alambique.Store.condition()) ::
          {:ok, Alambique.Store.etag()} | {:error, term}
  def put(%{store: {store, config}} = ctx, key, body, condition) do
    Retry.once(ctx.store, bound(ctx), fn -> store.put(config, key, body, condition, %{}) end)
  end

  @doc "One `get`, bounded in time."
  @spec get(map, String.t()) :: {:ok, binary, Alambique.Store.etag()} | {:error, term}
  def get(%{store: {store, config}} = ctx, key) do
    case Retry.once(ctx.store, bound(ctx), fn -> store.get(config, key) end) do
      {:ok, body, etag, _meta} -> {:ok, body, etag}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "One `delete`, bounded in time. The store cannot delete conditionally."
  @spec delete(map, String.t()) :: :ok | {:error, term}
  def delete(%{store: {store, config}} = ctx, key),
    do: Retry.once(ctx.store, bound(ctx), fn -> store.delete(config, key) end)
end
