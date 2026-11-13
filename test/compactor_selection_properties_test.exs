defmodule Alambique.CompactorSelectionPropertiesTest do
  @moduledoc """
  Which ready cells a compactor tick takes (design §9): the cells that waited longest
  first, so that no ready cell waits more
  than ceil(ready / batch) ticks however busy the others are.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Compaction.Compactor

  @fanout 2

  # Every taken cell commits again at once, as a busy cell does. Returns the
  # tick in which each of the `n` cells was first taken; a cell that is never
  # taken is missing (the loop stops after `n` ticks).
  defp serve(table, c, n, tick \\ 1, waits \\ %{})
  defp serve(_table, _c, n, tick, waits) when tick > n, do: waits

  defp serve(table, c, n, tick, waits) do
    rows = Compactor.ready(table, c, 0)

    for {cell, _, _, _} = row <- rows do
      :ets.delete_object(table, row)
      :ets.insert(table, {cell, 1, 2, 1_000_000 + tick})
    end

    waits = Enum.reduce(rows, waits, fn {cell, _, _, _}, acc -> Map.put_new(acc, cell, tick) end)
    if map_size(waits) == n, do: waits, else: serve(table, c, n, tick + 1, waits)
  end

  property "a ready cell waits at most ceil(ready / batch) ticks" do
    check all(
            n <- integer(1..60),
            concurrency <- integer(1..4),
            batch_factor <- integer(1..4),
            dues <- list_of(integer(1..1_000), length: n),
            spans <- list_of(integer(2..20), length: n)
          ) do
      c = %{
        fanout: @fanout,
        concurrency: concurrency,
        batch_factor: batch_factor
      }

      batch = concurrency * batch_factor
      table = :ets.new(:rows, [:public, :set])
      cells = Enum.map(1..n, &"cell-#{&1}")

      for {cell, due, span} <- Enum.zip([cells, dues, spans]),
          do: :ets.insert(table, {cell, 10, 10 + span - 1, due})

      waits = serve(table, c, n)
      :ets.delete(table)

      assert map_size(waits) == n
      assert Enum.max(Map.values(waits)) <= ceil(n / batch)
    end
  end
end
