defmodule Alambique.BoundedTable do
  @moduledoc false
  # Writes to a public ETS cache of at most `limit` entries (the `hint_limit`
  # option). An entry that is already stored costs a lookup, not a write. At
  # the limit a sixteenth of the table is evicted, so the entries that remain
  # keep serving and the eviction cost is shared by many writes.

  @doc "Stores `object` (a tuple whose first element is the key); returns `true`."
  @spec put(:ets.table(), pos_integer, tuple) :: true
  def put(table, limit, object) do
    key = elem(object, 0)

    case :ets.lookup(table, key) do
      [^object] ->
        true

      [_] ->
        :ets.insert(table, object)

      [] ->
        if :ets.info(table, :size) >= limit, do: evict(table, max(div(limit, 16), 1))
        :ets.insert(table, object)
    end
  end

  defp evict(table, count) do
    table |> keys(:ets.first(table), count, []) |> Enum.each(&:ets.delete(table, &1))
  end

  defp keys(_table, :"$end_of_table", _count, acc), do: acc
  defp keys(_table, _key, 0, acc), do: acc

  defp keys(table, key, count, acc),
    do: keys(table, :ets.next(table, key), count - 1, [key | acc])
end
