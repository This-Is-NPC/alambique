defmodule Alambique.BoundedTablePropertiesTest do
  @moduledoc """
  `Alambique.BoundedTable.put/3` against a model, over random writes: the
  table never passes its limit, a stored key is replaced without evicting
  anything, and a new key at the limit evicts a sixteenth of the limit (at
  least one entry) and nothing else.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.BoundedTable

  defp put_all(limit, objects, check) do
    table = :ets.new(:bounded, [:public, :set])

    try do
      Enum.each(objects, fn {key, value} = object ->
        before = Map.new(:ets.tab2list(table))
        assert BoundedTable.put(table, limit, object) == true
        after_ = Map.new(:ets.tab2list(table))

        check.(before, after_, object, limit)
        assert after_[key] == value
        assert map_size(after_) <= limit
        # No other entry changes, whatever is evicted.
        assert Enum.all?(Map.delete(after_, key), fn {k, v} -> before[k] == v end)
      end)
    after
      :ets.delete(table)
    end
  end

  defp objects, do: list_of({integer(0..60), integer(0..2)}, max_length: 150)

  property "the size never passes the limit, whatever is written" do
    check all(limit <- integer(1..50), objects <- objects(), max_runs: 300) do
      put_all(limit, objects, fn _before, _after, _object, _limit -> :ok end)
    end
  end

  property "a key already stored is replaced in place and nothing is evicted" do
    check all(limit <- integer(1..50), objects <- objects(), max_runs: 300) do
      put_all(limit, objects, fn before, after_, {key, _value}, _limit ->
        if Map.has_key?(before, key),
          do: assert(Enum.sort(Map.keys(after_)) == Enum.sort(Map.keys(before)))
      end)
    end
  end

  property "a new key at the limit evicts a sixteenth of it, at least one entry" do
    check all(limit <- integer(1..200), objects <- objects(), max_runs: 300) do
      put_all(limit, objects, fn before, after_, {key, _value}, limit ->
        cond do
          Map.has_key?(before, key) ->
            :ok

          map_size(before) >= limit ->
            evicted = Map.keys(before) -- Map.keys(after_)
            assert length(evicted) == min(max(div(limit, 16), 1), map_size(before))

          true ->
            assert Enum.sort(Map.keys(after_) -- [key]) == Enum.sort(Map.keys(before))
        end
      end)
    end
  end
end
