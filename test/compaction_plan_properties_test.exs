defmodule Alambique.CompactionPlanPropertiesTest do
  @moduledoc """
  Planning is stable (design §9): once a pass merged what the chain allows and
  marked the superseded files, the next pass plans nothing, however long the
  chain, so a deleted file is never written again. A pass merges every
  complete block of the chain, and only those.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Compaction.Plan

  defp seg(level, min, max),
    do: %{key: "#{level}-#{min}-#{max}", level: level, min: min, max: max}

  # What a pass does: merge level by level, then drop the inputs of every
  # merge to level 2 or above.
  defp pass(segments, fanout, max_level) do
    {segments, dead} =
      Enum.reduce(0..(max_level - 1)//1, {segments, []}, fn level, {segments, dead} ->
        index = Plan.index(segments)
        head = index |> Plan.chain(0) |> Plan.head(0)
        groups = Plan.groups(index, 0, head, fanout, level)
        merged = for g <- groups, do: seg(g.level, g.min, g.max)
        gone = if level >= 1, do: Enum.flat_map(groups, & &1.inputs), else: []
        {segments ++ merged, gone ++ dead}
      end)

    segments -- dead
  end

  defp keys(segments), do: MapSet.new(segments, &{&1.level, &1.min, &1.max})

  # The files that remain after the passes over TXIDs `1..txids`: every
  # single TXID, and from each level up the complete blocks that no complete
  # block above has taken over.
  defp remaining(txids, fanout, max_level) do
    singles = for t <- 1..txids, do: {0, t, t}

    blocks =
      for level <- 1..max_level//1,
          span = Integer.pow(fanout, level),
          j <- 0..(div(txids, span) - 1)//1,
          level == max_level or div(j, fanout) >= div(txids, span * fanout) do
        {level, j * span + 1, (j + 1) * span}
      end

    MapSet.new(singles ++ blocks)
  end

  property "a pass after a pass plans nothing" do
    check all(
            fanout <- member_of([2, 3, 4]),
            max_level <- integer(1..4),
            txids <- integer(1..200),
            passes <- integer(1..3)
          ) do
      start = for t <- 1..txids, do: seg(0, t, t)
      done = Enum.reduce(1..passes, start, fn _, acc -> pass(acc, fanout, max_level) end)

      assert keys(done) == remaining(txids, fanout, max_level)

      index = Plan.index(done)
      head = index |> Plan.chain(0) |> Plan.head(0)

      for level <- 0..(max_level - 1)//1 do
        assert Plan.groups(index, 0, head, fanout, level) == []
      end
    end
  end
end
