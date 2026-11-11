defmodule Alambique.Load.Service.DelayStore do
  @moduledoc false
  # Wraps a store: every request waits `ms` first (a sleep, which costs no
  # CPU) and is counted with its bytes. It stands for the network distance to
  # the object store; the request itself still goes over the loopback.

  @behaviour Alambique.Store

  @ops [:put, :get, :get_range, :head, :list, :delete]
  # One counter per operation, then the bytes put and the bytes read.
  @bytes_out length(@ops) + 1
  @bytes_in length(@ops) + 2

  @doc "New counters, for `wrap/3` and `counts/1`."
  def new_counters, do: :counters.new(length(@ops) + 2, [:write_concurrency])

  def wrap(inner, ms, counters), do: {__MODULE__, %{inner: inner, ms: ms, counters: counters}}

  @doc "The counts so far: trips by operation, and `bytes_put` and `bytes_got`."
  @spec counts(:counters.counters_ref()) :: map
  def counts(counters) do
    ops = for {op, i} <- Enum.with_index(@ops, 1), into: %{}, do: {op, :counters.get(counters, i)}

    Map.merge(ops, %{
      trips: ops |> Map.values() |> Enum.sum(),
      bytes_put: :counters.get(counters, @bytes_out),
      bytes_got: :counters.get(counters, @bytes_in)
    })
  end

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}} = s, key, body, condition, meta) do
    trip(s, :put)
    :counters.add(s.counters, @bytes_out, IO.iodata_length(body))
    m.put(c, key, body, condition, meta)
  end

  @impl true
  def get(%{inner: {m, c}} = s, key) do
    trip(s, :get)
    m.get(c, key) |> got(s)
  end

  @impl true
  def get_range(%{inner: {m, c}} = s, key, offset, length) do
    trip(s, :get_range)
    m.get_range(c, key, offset, length) |> got(s)
  end

  @impl true
  def head(%{inner: {m, c}} = s, key) do
    trip(s, :head)
    m.head(c, key)
  end

  @impl true
  def list(%{inner: {m, c}} = s, prefix, start_after, limit) do
    trip(s, :list)
    m.list(c, prefix, start_after, limit)
  end

  @impl true
  def delete(%{inner: {m, c}} = s, key) do
    trip(s, :delete)
    m.delete(c, key)
  end

  defp trip(%{ms: ms, counters: counters}, op) do
    :counters.add(counters, Enum.find_index(@ops, &(&1 == op)) + 1, 1)
    if ms > 0, do: Process.sleep(ms)
  end

  defp got({:ok, body, _, _} = result, %{counters: counters}) do
    :counters.add(counters, @bytes_in, byte_size(body))
    result
  end

  defp got(result, _), do: result
end
