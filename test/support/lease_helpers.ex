defmodule Alambique.LeaseHelpers do
  @moduledoc "Builds lease contexts for tests: one shared store, one timeline, one clock offset per node."

  alias Alambique.Lease
  alias AlambiqueTestkit.ManualClock

  @ttl_ms 30_000
  @skew_ms 2_000

  def ttl_ms, do: @ttl_ms
  def skew_ms, do: @skew_ms

  @doc "Context for one node. Options: `:offset`, `:ttl_ms`, `:skew_margin_ms`."
  def node_ctx(store, world, prefix, opts \\ []) do
    Lease.ctx(
      store: store,
      clock: {ManualClock, ManualClock.config(world, Keyword.get(opts, :offset, 0))},
      prefix: prefix,
      ttl_ms: Keyword.get(opts, :ttl_ms, @ttl_ms),
      skew_margin_ms: Keyword.get(opts, :skew_margin_ms, @skew_ms)
    )
  end

  def unique_prefix(label),
    do: "#{label}-" <> Base.url_encode64(:crypto.strong_rand_bytes(9), padding: false)
end
