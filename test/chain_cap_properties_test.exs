defmodule Alambique.ChainCapPropertiesTest do
  @moduledoc """
  The bound the chain cap gives (design §9, "Chain cap"): while no more cells
  commit than the compactor has slots for, the TXIDs a cell has committed
  since its oldest uncompacted one never pass `max_chain_segments`, or what the
  cell commits while a pass of it runs if that is more, plus what it commits
  while a pass of it runs. The compactor's own choice of rows
  (`ready/3` and `pick/4`) runs in a model with time in whole steps.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Compaction.Compactor

  @fanout 2
  @steps 300

  # Cell `i` commits `rates[i]` TXIDs per step; a pass of it takes
  # `durations[i]` steps and compacts everything its row counted when it
  # started. Returns the longest uncompacted tail seen in any cell.
  defp simulate(c, rates, durations) do
    table = :ets.new(:rows, [:public, :set])
    cells = Enum.with_index(Enum.zip(rates, durations))
    txids = Map.new(cells, fn {_, i} -> {i, 0} end)

    {max, _} =
      Enum.reduce(1..@steps, {0, {txids, []}}, fn step, {max, {txids, running}} ->
        # Passes end, then the cells commit.
        {done, running} = Enum.split_with(running, fn {_cell, at, _row} -> at <= step end)
        _ = done

        txids =
          for {{rate, _}, i} <- cells, into: %{} do
            last = txids[i] + rate

            case :ets.lookup(table, i) do
              [{_, first, _, due}] -> :ets.insert(table, {i, first, last, due})
              [] -> :ets.insert(table, {i, txids[i] + 1, last, step + 1_000_000})
            end

            {i, last}
          end

        # The compactor starts what the slots allow.
        busy = MapSet.new(running, fn {cell, _, _} -> cell end)
        rows = Compactor.pick(Compactor.ready(table, c, step), busy, length(running), c)

        started =
          for {cell, _, _, _} = row <- rows do
            :ets.delete_object(table, row)
            {_, duration} = Enum.at(cells, cell) |> elem(0)
            {cell, step + duration, row}
          end

        running = started ++ running

        tails =
          for {_, i} <- cells do
            firsts =
              for({^i, _, {_, first, _, _}} <- running, do: first) ++
                for {_, first, _, _} <- :ets.lookup(table, i), do: first

            if firsts == [], do: 0, else: txids[i] - Enum.min(firsts) + 1
          end

        {Enum.max([max | tails]), {txids, running}}
      end)

    :ets.delete(table)
    max
  end

  property "a cell's tail stays within the cap plus what it commits while its pass runs" do
    check all(
            concurrency <- integer(1..3),
            urgent <- integer(1..3),
            cap <- integer(10..40),
            n <- integer(1..(concurrency + urgent)),
            rates <- list_of(integer(1..4), length: n),
            durations <- list_of(integer(1..8), length: n)
          ) do
      c = %{
        fanout: @fanout,
        max_chain_segments: cap,
        concurrency: concurrency,
        urgent_concurrency: urgent
      }

      max = simulate(c, rates, durations)
      # What a cell commits during a pass, with a step of lag. A row at the cap
      # waits for no slot, but a cell has one pass at a time: the row that
      # grew during a pass over the cap goes into the next one.
      during = Enum.max(rates) * (Enum.max(durations) + 1)
      bound = max(cap, during) + during
      assert max <= bound, "tail #{max} over the bound #{bound}"
    end
  end
end
