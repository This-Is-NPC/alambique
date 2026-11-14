defmodule Alambique.CompactorSelectionPropertiesTest do
  @moduledoc """
  Which ready cells a compactor tick takes (design §9): chains at the cap
  first, then the cells that waited longest, so that no ready cell waits more
  than ceil(ready / batch) ticks however busy the others are.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Compaction.Compactor

  @fanout 2
  @cap 10

  # The rows a tick takes.
  defp taken(table, c) do
    rows = Compactor.ready(table, c, 0)
    Enum.take(rows, Compactor.slots(c, rows) * c.batch_factor)
  end

  # Every taken cell commits again at once, as a busy cell does. Returns the
  # tick in which each of the `n` cells was first taken; a cell that is never
  # taken is missing (the loop stops after `n` ticks).
  defp serve(table, c, n, tick \\ 1, waits \\ %{})
  defp serve(_table, _c, n, tick, waits) when tick > n, do: waits

  defp serve(table, c, n, tick, waits) do
    rows = taken(table, c)

    for {cell, _, _, _} = row <- rows do
      :ets.delete_object(table, row)
      :ets.insert(table, {cell, 1, 2, 1_000_000 + tick})
    end

    waits = Enum.reduce(rows, waits, fn {cell, _, _, _}, acc -> Map.put_new(acc, cell, tick) end)
    if map_size(waits) == n, do: waits, else: serve(table, c, n, tick + 1, waits)
  end

  property "a ready cell waits at most ceil(ready / batch) ticks, and a chain at the cap goes first" do
    check all(
            n <- integer(1..60),
            concurrency <- integer(1..4),
            batch_factor <- integer(1..4),
            urgent <- integer(0..4),
            dues <- list_of(integer(1..1_000), length: n),
            spans <- list_of(integer(2..(@cap * 2)), length: n)
          ) do
      c = %{
        fanout: @fanout,
        max_chain_segments: @cap,
        concurrency: concurrency,
        urgent_concurrency: urgent,
        batch_factor: batch_factor
      }

      batch = concurrency * batch_factor
      table = :ets.new(:rows, [:public, :set])
      cells = Enum.map(1..n, &"cell-#{&1}")

      for {cell, due, span} <- Enum.zip([cells, dues, spans]),
          do: :ets.insert(table, {cell, 10, 10 + span - 1, due})

      forced = for {cell, span} <- Enum.zip(cells, spans), span >= @cap, do: cell
      first = taken(table, c)
      waits = serve(table, c, n)
      :ets.delete(table)

      assert map_size(waits) == n
      assert Enum.max(Map.values(waits)) <= ceil(n / batch)

      # A cell at the cap lends the tick `urgent_concurrency` more passes.
      slots = concurrency + if forced == [], do: 0, else: urgent
      assert length(first) == min(n, slots * batch_factor)

      taken = Enum.map(first, &elem(&1, 0))
      assert Enum.take(taken, min(length(forced), length(first))) |> Enum.all?(&(&1 in forced))
    end
  end

  property "a row that reaches the cap wakes the compactor, and nothing else does" do
    check all(counts <- list_of(integer(1..40), min_length: 1, max_length: 20)) do
      c = %{fanout: 4, max_chain_segments: 16, queue_limit: 1_000, max_age_ms: 300_000}
      table = :ets.new(:rows, [:public, :set])
      config = %{compactor: table, compaction: c}

      for {count, i} <- Enum.with_index(counts),
          txid <- 1..count,
          do: Compactor.notify(config, "cell-#{i}", txid)

      :ets.delete(table)
      woken = Process.info(self(), :messages) |> elem(1) |> Enum.member?(:urgent)
      flush()
      assert woken == Enum.any?(counts, &(&1 >= 16))
    end
  end

  defp flush do
    receive do
      :urgent -> flush()
    after
      0 -> :ok
    end
  end
end
