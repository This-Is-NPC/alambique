defmodule Alambique.TracingStore do
  @moduledoc """
  Wraps a store and records when each request started and ended, so tests can
  tell which requests ran at the same time.
  """
  @behaviour Alambique.Store

  @doc "Wraps `{module, config}`; the records live in a supervised process of the test."
  def wrap({_module, _config} = inner) do
    holder =
      ExUnit.Callbacks.start_supervised!(
        {Agent, fn -> :ets.new(:tracing_store, [:public, :duplicate_bag]) end},
        id: make_ref()
      )

    {__MODULE__, %{inner: inner, table: Agent.get(holder, & &1)}}
  end

  @doc "`{op, key, started, ended}` of every request so far, in monotonic microseconds."
  def trace(%{table: table}), do: :ets.tab2list(table)

  @doc "Forgets the records."
  def reset(%{table: table}), do: :ets.delete_all_objects(table)

  @doc "Whether two records were in progress at the same moment."
  def overlap?({_, _, from_a, to_a}, {_, _, from_b, to_b}), do: from_a < to_b and from_b < to_a

  defp timed(%{table: table}, op, key, fun) do
    from = System.monotonic_time(:microsecond)
    result = fun.()
    :ets.insert(table, {op, key, from, System.monotonic_time(:microsecond)})
    result
  end

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}} = s, key, body, condition, meta),
    do: timed(s, :put, key, fn -> m.put(c, key, body, condition, meta) end)

  @impl true
  def get(%{inner: {m, c}} = s, key), do: timed(s, :get, key, fn -> m.get(c, key) end)

  @impl true
  def get_range(%{inner: {m, c}} = s, key, offset, length),
    do: timed(s, :get, key, fn -> m.get_range(c, key, offset, length) end)

  @impl true
  def head(%{inner: {m, c}} = s, key), do: timed(s, :head, key, fn -> m.head(c, key) end)

  @impl true
  def list(%{inner: {m, c}} = s, prefix, start_after, limit),
    do: timed(s, :list, prefix, fn -> m.list(c, prefix, start_after, limit) end)

  @impl true
  def delete(%{inner: {m, c}} = s, key), do: timed(s, :delete, key, fn -> m.delete(c, key) end)
end
