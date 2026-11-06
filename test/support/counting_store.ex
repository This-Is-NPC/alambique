defmodule Alambique.CountingStore do
  @moduledoc """
  Wraps a store and counts its requests, so tests can assert how many
  listings and reads an operation costs.
  """
  @behaviour Alambique.Store

  @doc """
  Wraps `{module, config}` and returns the `{module, config}` of the counting
  store. Call it from a test: the counts live in a supervised process that
  outlives the instances started after it.
  """
  def wrap({_module, _config} = inner) do
    holder =
      ExUnit.Callbacks.start_supervised!(
        {Agent, fn -> :ets.new(:counting_store, [:public, :duplicate_bag]) end}
      )

    {__MODULE__, %{inner: inner, table: Agent.get(holder, & &1)}}
  end

  @doc "Number of `op` requests so far (`:put`, `:get`, `:get_range`, `:head`, `:list`, `:delete`)."
  def count(%{table: table}, op), do: length(:ets.lookup(table, op))

  @doc "Keys of the `op` requests so far."
  def keys(%{table: table}, op), do: for({^op, key} <- :ets.tab2list(table), do: key)

  @doc "Prefixes of the listings so far."
  def listed(%{table: table}), do: for({:list, prefix} <- :ets.tab2list(table), do: prefix)

  @doc "Forgets the counts."
  def reset(%{table: table}), do: :ets.delete_all_objects(table)

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}, table: t}, key, body, condition, meta) do
    :ets.insert(t, {:put, key})
    m.put(c, key, body, condition, meta)
  end

  @impl true
  def get(%{inner: {m, c}, table: t}, key) do
    :ets.insert(t, {:get, key})
    m.get(c, key)
  end

  @impl true
  def get_range(%{inner: {m, c}, table: t}, key, offset, length) do
    :ets.insert(t, {:get_range, key})
    m.get_range(c, key, offset, length)
  end

  @impl true
  def head(%{inner: {m, c}, table: t}, key) do
    :ets.insert(t, {:head, key})
    m.head(c, key)
  end

  @impl true
  def list(%{inner: {m, c}, table: t}, prefix, start_after, limit) do
    :ets.insert(t, {:list, prefix})
    m.list(c, prefix, start_after, limit)
  end

  @impl true
  def delete(%{inner: {m, c}, table: t}, key) do
    :ets.insert(t, {:delete, key})
    m.delete(c, key)
  end
end
