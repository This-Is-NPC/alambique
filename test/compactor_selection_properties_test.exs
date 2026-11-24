defmodule Alambique.CompactorSelectionPropertiesTest do
  @moduledoc """
  Which ready cells the compactor starts passes for (design §9): chains at the
  cap first, then the cells that waited longest, within the free slots, so
  that no ready cell waits more than ceil(ready / concurrency) rounds however
  busy the others are.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Compaction.Compactor

  @fanout 2
  @cap 10

  # Dues of any age, and the ones a tick before, at and after `now`.
  defp due(now),
    do: one_of([integer(1..1_000), map(member_of([-1, 0, 1]), &max(now + &1, 1))])

  # A chain of one file is short of ready, two files are ready, up to twice the cap.
  defp span, do: one_of([constant(1), integer(2..(@cap * 2))])

  # A row is ready with `fanout` files, or when its time came.
  defp ready_cells(cells, dues, spans, now) do
    for {cell, due, span} <- Enum.zip([cells, dues, spans]),
        span >= @fanout or due <= now,
        into: MapSet.new(),
        do: cell
  end

  defp table_of(cells, dues, spans) do
    table = :ets.new(:rows, [:public, :set])

    for {cell, due, span} <- Enum.zip([cells, dues, spans]),
        do: :ets.insert(table, {cell, 10, 10 + span - 1, due})

    table
  end

  # Every picked cell commits again at once, as a busy cell does. Returns the
  # round in which each of the `n` ready cells was first picked; a cell that is
  # never picked is missing (the loop stops after `n` rounds).
  defp serve(table, c, n, now, round \\ 1, waits \\ %{})
  defp serve(_table, _c, n, _now, round, waits) when round > n, do: waits

  defp serve(table, c, n, now, round, waits) do
    rows = Compactor.pick(Compactor.ready(table, c, now), MapSet.new(), 0, c)

    for {cell, _, _, _} = row <- rows do
      :ets.delete_object(table, row)
      :ets.insert(table, {cell, 1, 2, 1_000_000 + round})
    end

    waits = Enum.reduce(rows, waits, fn {cell, _, _, _}, acc -> Map.put_new(acc, cell, round) end)
    if map_size(waits) == n, do: waits, else: serve(table, c, n, now, round + 1, waits)
  end

  property "a ready cell is picked within ceil(ready / concurrency) rounds, however busy the others are" do
    check all(
            n <- integer(1..60),
            concurrency <- integer(1..4),
            urgent <- integer(0..4),
            now <- integer(0..1_000),
            dues <- list_of(due(now), length: n),
            spans <- list_of(span(), length: n)
          ) do
      c = %{
        fanout: @fanout,
        max_chain_segments: @cap,
        concurrency: concurrency,
        urgent_concurrency: urgent
      }

      cells = Enum.map(1..n, &"cell-#{&1}")
      ready = ready_cells(cells, dues, spans, now)
      table = table_of(cells, dues, spans)
      waits = serve(table, c, MapSet.size(ready), now)
      :ets.delete(table)

      assert MapSet.new(Map.keys(waits)) == ready
      assert Enum.all?(Map.values(waits), &(&1 <= ceil(MapSet.size(ready) / concurrency)))
    end
  end

  property "a pick fills the free slots, chains at the cap first, and never takes a cell that has a pass" do
    check all(
            n <- integer(1..40),
            concurrency <- integer(1..4),
            urgent <- integer(0..4),
            running <- integer(0..8),
            now <- integer(0..1_000),
            dues <- list_of(due(now), length: n),
            spans <- list_of(span(), length: n),
            busy_flags <- list_of(boolean(), length: n)
          ) do
      c = %{
        fanout: @fanout,
        max_chain_segments: @cap,
        concurrency: concurrency,
        urgent_concurrency: urgent
      }

      cells = Enum.map(1..n, &"cell-#{&1}")
      table = table_of(cells, dues, spans)
      rows = Compactor.ready(table, c, now)
      :ets.delete(table)

      # The ready rows are those with `fanout` files or whose time came, the
      # chains at the cap first and then the ones that waited longest.
      assert MapSet.new(rows, &elem(&1, 0)) == ready_cells(cells, dues, spans, now)

      assert rows ==
               Enum.sort_by(rows, fn {cell, first, last, due} ->
                 {if(last - first >= @cap - 1, do: 0, else: 1), due, cell}
               end)

      busy = for {cell, true} <- Enum.zip(cells, busy_flags), into: MapSet.new(), do: cell
      picked = Compactor.pick(rows, busy, running, c)

      eligible = Enum.reject(rows, fn {cell, _, _, _} -> cell in busy end)

      {over, rest} =
        Enum.split_with(eligible, fn {_, first, last, _} -> last - first >= @cap - 1 end)

      # The rows at the cap use `concurrency + urgent` slots, the rest
      # `concurrency`, whatever passes already run.
      from_over = min(max(concurrency + urgent - running, 0), length(over))

      from_rest =
        if from_over == length(over),
          do: min(max(concurrency - running - length(over), 0), length(rest)),
          else: 0

      assert picked == Enum.take(eligible, from_over + from_rest)
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
