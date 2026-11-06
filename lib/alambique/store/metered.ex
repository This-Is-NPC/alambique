defmodule Alambique.Store.Metered do
  @moduledoc false
  # Counts the requests that go through a store by operation and by kind of
  # object, in a `:counters` array shared by every process that uses the
  # wrapped store. A wake wraps the stores of its contexts with it, so the
  # tasks it starts are counted too, and reports the counts in its telemetry
  # event.

  @behaviour Alambique.Store

  @ops [:get, :head, :list, :put, :delete]
  @kinds [:lease, :frozen, :schema, :feed, :snapshot, :segment, :merged, :listing, :other]

  @doc "A new array of counters."
  @spec new() :: :counters.counters_ref()
  def new, do: :counters.new(length(@ops) * length(@kinds), [:write_concurrency])

  @doc "`store` counting into `counters`."
  @spec wrap({module, term}, :counters.counters_ref()) :: {module, map}
  def wrap(store, counters), do: {__MODULE__, %{store: store, counters: counters}}

  @doc "The requests counted: `%{{op, kind} => count}` without the zeros."
  @spec counts(:counters.counters_ref()) :: %{{atom, atom} => pos_integer}
  def counts(counters) do
    for {op, i} <- Enum.with_index(@ops),
        {kind, j} <- Enum.with_index(@kinds),
        n = :counters.get(counters, i * length(@kinds) + j + 1),
        n > 0,
        into: %{},
        do: {{op, kind}, n}
  end

  @doc "The telemetry measurements of `counts`: `trips` in all and per operation."
  @spec measurements(%{{atom, atom} => pos_integer}) :: map
  def measurements(counts) do
    per_op = for op <- @ops, into: %{}, do: {op, 0}

    per_op =
      Enum.reduce(counts, per_op, fn {{op, _kind}, n}, acc -> Map.update!(acc, op, &(&1 + n)) end)

    %{
      trips: Enum.sum(Map.values(per_op)),
      gets: per_op.get,
      heads: per_op.head,
      lists: per_op.list,
      puts: per_op.put,
      deletes: per_op.delete
    }
  end

  defp bump(%{counters: counters}, op, key) do
    index = Enum.find_index(@ops, &(&1 == op)) * length(@kinds) + kind_index(op, key) + 1
    :counters.add(counters, index, 1)
  end

  defp kind_index(op, key), do: Enum.find_index(@kinds, &(&1 == kind(op, key)))

  # The kind of the object a key (or, for a listing, a prefix) names.
  defp kind(:list, prefix) do
    cond do
      String.contains?(prefix, "/schema/") -> :schema
      String.contains?(prefix, "/feed/") -> :feed
      true -> :listing
    end
  end

  defp kind(_op, key) do
    cond do
      String.ends_with?(key, "/lease") -> :lease
      String.ends_with?(key, "/frozen") -> :frozen
      String.contains?(key, "/schema/") -> :schema
      String.contains?(key, "/feed/") -> :feed
      String.contains?(key, "/0009/") -> :snapshot
      String.contains?(key, "/0000/") -> :segment
      Regex.match?(~r|/000[1-8]/|, key) -> :merged
      true -> :other
    end
  end

  @impl true
  def location(%{store: store}), do: Alambique.Store.location(store)

  @impl true
  def put(%{store: {m, c}} = s, key, body, condition, meta) do
    bump(s, :put, key)
    m.put(c, key, body, condition, meta)
  end

  @impl true
  def get(%{store: {m, c}} = s, key) do
    bump(s, :get, key)
    m.get(c, key)
  end

  @impl true
  def get_range(%{store: {m, c}} = s, key, offset, length) do
    bump(s, :get, key)
    m.get_range(c, key, offset, length)
  end

  @impl true
  def head(%{store: {m, c}} = s, key) do
    bump(s, :head, key)
    m.head(c, key)
  end

  @impl true
  def list(%{store: {m, c}} = s, prefix, start_after, limit) do
    bump(s, :list, prefix)
    m.list(c, prefix, start_after, limit)
  end

  @impl true
  def delete(%{store: {m, c}} = s, key) do
    bump(s, :delete, key)
    m.delete(c, key)
  end
end
