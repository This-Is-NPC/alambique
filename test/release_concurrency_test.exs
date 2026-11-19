defmodule Alambique.ReleaseConcurrencyTest do
  @moduledoc """
  A node compacts at most `compaction: [release_concurrency:]` cells at once
  when it releases them; a cell that finds no place inside its budget releases
  without looking at the chain (design §9, "Release concurrency").
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, TracingStore}
  alias Alambique.Compaction.{Plan, Slots}
  alias Alambique.Replication.Keys

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @event [:alambique, :cell, :hibernate]
  @rows 60

  setup do
    capture([@event])
    setup_store()
  end

  setup ctx do
    {TracingStore, tracing} = traced = TracingStore.wrap(ctx.store)

    inst =
      start_instance(%{ctx | store: traced},
        migrations: @schema,
        retention: false,
        compaction: [
          interval_ms: 3_600_000,
          max_age_ms: 3_600_000,
          release_min_tail: 8,
          release_concurrency: 1
        ]
      )

    {:ok, inst: inst, tracing: tracing, config: Config.fetch!(inst)}
  end

  defp fill(inst, cell) do
    for n <- 1..@rows,
        do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])
  end

  defp tail_length(ctx, cell) do
    prefix = Keys.files_prefix("cells-test", cell)
    {:ok, keys, nil} = AlambiqueTestkit.MemoryStore.list(ctx.store_config, prefix, nil, 100_000)
    {segments, snapshots} = Keys.parse("cells-test", cell, keys)
    from = snapshots |> List.last() |> then(&((&1 && &1.txid) || 0))
    segments |> Plan.index() |> Plan.chain(from) |> Enum.count(&(&1.level == 0))
  end

  defp far, do: System.monotonic_time(:millisecond) + 60_000

  test "a cell that finds the node's place taken releases without a request to compact", ctx do
    cell = unique_cell()
    fill(ctx.inst, cell)
    {:ok, place} = Slots.acquire(ctx.config, 1, far())
    TracingStore.reset(ctx.tracing)

    assert :ok = Alambique.hibernate(ctx.inst, cell, release_deadline_ms: 200)

    assert {%{tail: 0}, %{reason: :request, compaction: :busy}} = await_event(@event, cell)
    assert tail_length(ctx, cell) >= @rows

    # It released the lease and nothing else: no listing, no read of a file, no
    # merged file.
    ops = for {op, key, _, _} <- TracingStore.trace(ctx.tracing), do: {op, key}
    refute Enum.any?(ops, fn {op, _} -> op in [:list, :get, :head] end)
    refute Enum.any?(ops, fn {_, key} -> key =~ ~r{/000[1-9]/} end)

    # The place is the node's, not the cell's: it is still taken, and the next
    # cell compacts once it is free.
    assert Slots.count(ctx.config) == 1
    Slots.release(ctx.config, place)

    other = unique_cell()
    fill(ctx.inst, other)
    assert :ok = Alambique.hibernate(ctx.inst, other)
    assert {%{tail: tail}, %{compaction: :done}} = await_event(@event, other)
    assert tail < 8
    assert Slots.count(ctx.config) == 0
  end

  test "a cell waits for a place that frees inside its budget and then compacts", ctx do
    cell = unique_cell()
    fill(ctx.inst, cell)
    {:ok, place} = Slots.acquire(ctx.config, 1, far())

    release =
      Task.async(fn -> Alambique.hibernate(ctx.inst, cell, release_deadline_ms: 10_000) end)

    # The holder and the cell that waits for its place.
    eventually(fn -> Slots.count(ctx.config) == 2 end)
    refute_received {:telemetry, @event, _, _}
    Slots.release(ctx.config, place)

    assert :ok = Task.await(release, 10_000)
    assert {%{tail: tail}, %{compaction: :done}} = await_event(@event, cell)
    assert tail < 8
    assert Slots.count(ctx.config) == 0
  end

  test "the place is given back when the compaction fails", ctx do
    cell = unique_cell()
    fill(ctx.inst, cell)

    AlambiqueTestkit.MemoryStore.inject(ctx.store_config,
      kind: :error,
      reason: :boom,
      ops: [:list],
      key: ~r{/cells/},
      times: :infinity
    )

    assert :ok = Alambique.hibernate(ctx.inst, cell, release_deadline_ms: 300)
    assert {_, %{compaction: compaction}} = await_event(@event, cell)
    assert compaction in [:error, :deadline_error]
    assert Slots.count(ctx.config) == 0
  end
end
