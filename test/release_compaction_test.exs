defmodule Alambique.ReleaseCompactionTest do
  @moduledoc """
  A cell that hands its lease over gracefully compacts its level-0 tail first,
  within a deadline, so the next node wakes on a short chain (design §9,
  "Compaction on release").
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Compaction.Plan
  alias Alambique.Replication.Keys
  alias Alambique.SlowStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  # A compactor that never ticks: only the release compacts.
  @idle_compactor [interval_ms: 3_600_000, max_age_ms: 3_600_000]

  setup do
    setup_store()
  end

  defp writer(ctx, extra \\ []) do
    {compaction, extra} = Keyword.pop(extra, :compaction, [])

    start_instance(
      ctx,
      Keyword.merge(
        [migrations: @schema, retention: false, compaction: @idle_compactor ++ compaction],
        extra
      )
    )
  end

  defp insert(inst, cell, range) do
    for n <- range do
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])
    end
  end

  # What a restore walks: the files from the newest snapshot, the way it picks them.
  defp chain_length(ctx, cell) do
    prefix = Keys.files_prefix("cells-test", cell)
    {:ok, keys, nil} = AlambiqueTestkit.MemoryStore.list(ctx.store_config, prefix, nil, 100_000)
    {segments, snapshots} = Keys.parse("cells-test", cell, keys)
    from = snapshots |> List.last() |> then(&((&1 && &1.txid) || 0))
    segments |> Plan.index() |> Plan.chain(from) |> length()
  end

  defp wake_gets(ctx, cell, rows) do
    capture([[:alambique, :cell, :wake]])
    reader = start_instance(ctx, migrations: @schema, retention: false)
    assert {:ok, %{rows: [[^rows]]}} = Alambique.query(reader, cell, "SELECT count(*) FROM t", [])
    {wake, _} = await_event([:alambique, :cell, :wake], cell)
    wake.gets
  end

  test "hibernate compacts a long tail, so the next node reads a short chain", ctx do
    inst = writer(ctx)
    cell = unique_cell()
    insert(inst, cell, 1..100)
    assert chain_length(ctx, cell) >= 100

    assert :ok = Alambique.hibernate(inst, cell)

    assert chain_length(ctx, cell) <= 16
    assert wake_gets(ctx, cell, 100) <= 20
  end

  test "a drain compacts the tails of the cells it releases", ctx do
    inst = writer(ctx)
    cells = for _ <- 1..3, do: unique_cell()
    for cell <- cells, do: insert(inst, cell, 1..100)

    assert {:ok, %{hibernated: 3, errors: []}} = Alambique.drain(inst)

    for cell <- cells do
      assert chain_length(ctx, cell) <= 16
      assert wake_gets(ctx, cell, 100) <= 20
    end
  end

  test "the hibernation reports what the compaction did", ctx do
    inst = writer(ctx)
    capture([[:alambique, :cell, :hibernate]])
    cell = unique_cell()
    insert(inst, cell, 1..100)
    :ok = Alambique.hibernate(inst, cell)

    assert {%{tail: tail, compact_duration: spent}, %{compaction: :done, reason: :request}} =
             await_event([:alambique, :cell, :hibernate], cell)

    assert tail >= 100
    assert spent > 0
  end

  test "a short tail is released without compacting", ctx do
    inst = writer(ctx)
    capture([[:alambique, :cell, :hibernate]])
    cell = unique_cell()
    insert(inst, cell, 1..20)
    :ok = Alambique.hibernate(inst, cell)

    assert {%{tail: tail}, %{compaction: :none}} =
             await_event([:alambique, :cell, :hibernate], cell)

    assert tail in 20..30
    assert chain_length(ctx, cell) >= 20
  end

  test "a deadline of 0 turns the compaction off", ctx do
    inst = writer(ctx, compaction: [release_deadline_ms: 0])
    capture([[:alambique, :cell, :hibernate]])
    cell = unique_cell()
    insert(inst, cell, 1..100)
    :ok = Alambique.hibernate(inst, cell)

    assert {_, %{compaction: :none}} = await_event([:alambique, :cell, :hibernate], cell)
    assert chain_length(ctx, cell) >= 100
  end

  test "the lease is handed over at the deadline of a slow store, with every write", ctx do
    slow = SlowStore.wrap(ctx.store, 40)

    inst =
      writer(%{ctx | store: slow}, compaction: [release_deadline_ms: 100, release_min_tail: 30])

    capture([[:alambique, :cell, :hibernate]])
    cell = unique_cell()
    insert(inst, cell, 1..60)

    started = System.monotonic_time(:millisecond)
    assert :ok = Alambique.hibernate(inst, cell)
    spent = System.monotonic_time(:millisecond) - started

    assert {_, %{compaction: :deadline}} = await_event([:alambique, :cell, :hibernate], cell)
    assert spent < 1_500, "the release took #{spent} ms for a deadline of 100"

    assert {:ok, %{rows: [[60]]}} =
             Alambique.query(
               start_instance(ctx, migrations: @schema),
               cell,
               "SELECT count(*) FROM t",
               []
             )
  end

  describe "when the caller gives less time than the compaction may take" do
    setup ctx do
      slow = SlowStore.wrap(ctx.store, 40)

      inst =
        writer(%{ctx | store: slow},
          compaction: [release_deadline_ms: 5_000, release_min_tail: 30, release_reserve_ms: 450]
        )

      cell = unique_cell()
      insert(inst, cell, 1..120)
      {:ok, inst: inst, cell: cell}
    end

    # The lease is free: another instance wakes the cell at once, not after
    # the lease expires.
    defp assert_taken_over(ctx, cell) do
      other = start_instance(ctx, migrations: @schema, retention: false)

      assert {:ok, %{rows: [[120]]}} =
               Alambique.query(other, cell, "SELECT count(*) FROM t", [], timeout: 2_000)
    end

    test "hibernate/3 answers :ok within its timeout and releases the lease", ctx do
      capture([[:alambique, :cell, :hibernate]])

      assert :ok = Alambique.hibernate(ctx.inst, ctx.cell, timeout: 700)

      assert {_, %{compaction: :deadline}} =
               await_event([:alambique, :cell, :hibernate], ctx.cell)

      assert_taken_over(ctx, ctx.cell)
    end

    test "a drain near its deadline releases the cell instead of timing out", ctx do
      assert {:ok, %{hibernated: 1, errors: []}} =
               Alambique.drain(ctx.inst, deadline: 700, timeout: 30_000)

      assert_taken_over(ctx, ctx.cell)
    end

    test "a call with less time than the reserve skips the compaction", ctx do
      capture([[:alambique, :cell, :hibernate]])

      assert :ok = Alambique.hibernate(ctx.inst, ctx.cell, timeout: 300)

      assert {%{tail: 0}, %{compaction: :none}} =
               await_event([:alambique, :cell, :hibernate], ctx.cell)

      assert_taken_over(ctx, ctx.cell)
    end
  end
end
