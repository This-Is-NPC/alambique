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

    assert tail == tail_length(ctx, cell)
    assert tail < 16
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

  # The level-0 files of the chain a restore walks: the tail the release reports.
  defp tail_length(ctx, cell) do
    prefix = Keys.files_prefix("cells-test", cell)
    {:ok, keys, nil} = AlambiqueTestkit.MemoryStore.list(ctx.store_config, prefix, nil, 100_000)
    {segments, snapshots} = Keys.parse("cells-test", cell, keys)
    from = snapshots |> List.last() |> then(&((&1 && &1.txid) || 0))
    segments |> Plan.index() |> Plan.chain(from) |> Enum.count(&(&1.level == 0))
  end

  # A cell with `rows` level-0 files whose lease is free, and an instance over
  # a store that takes `ms` per request to hibernate it.
  defp slow_release(ctx, ms, rows, compaction) do
    fast = writer(ctx, compaction: [release_deadline_ms: 0])
    cell = unique_cell()
    insert(fast, cell, 1..rows)
    :ok = Alambique.hibernate(fast, cell)

    inst = writer(%{ctx | store: SlowStore.wrap(ctx.store, ms)}, compaction: compaction)
    assert {:ok, %{rows: [[^rows]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t", [])
    capture([[:alambique, :cell, :hibernate], [:alambique, :compaction, :pass]])
    {inst, cell}
  end

  test "the end of the budget reports the tail that is left", ctx do
    {inst, cell} =
      slow_release(ctx, 25, 150, release_deadline_ms: 400, release_min_tail: 30)

    before = tail_length(ctx, cell)
    assert before >= 150

    assert :ok = Alambique.hibernate(inst, cell)

    assert {%{tail: tail}, %{compaction: :deadline}} =
             await_event([:alambique, :cell, :hibernate], cell)

    assert tail_length(ctx, cell) < before, "nothing was merged"

    assert tail == tail_length(ctx, cell),
           "the event reports #{tail} for a tail of #{tail_length(ctx, cell)}"

    assert {_, %{result: :ok, more: true}} = await_event([:alambique, :compaction, :pass], cell)
  end

  # A request slower than the retry backoff (50 ms) that is running at the end
  # of the budget is killed there: the pass ends with what it published.
  test "a request cut at the end of the budget keeps the merges done before", ctx do
    {inst, cell} =
      slow_release(ctx, 90, 200, release_deadline_ms: 1_000, release_min_tail: 30)

    before = tail_length(ctx, cell)
    assert before >= 200

    # The release may fail on a store the cut tripped; the compaction is the subject.
    _ = Alambique.hibernate(inst, cell)

    assert {%{tail: tail}, %{compaction: :deadline}} =
             await_event([:alambique, :cell, :hibernate], cell)

    assert {_, %{result: :ok, more: true}} = await_event([:alambique, :compaction, :pass], cell)
    assert tail_length(ctx, cell) < before, "nothing was merged"
    assert tail == tail_length(ctx, cell)
  end

  describe "a release deadline for one call" do
    # The instance gives a release 100 ms; a call may ask for more or for none.
    setup ctx do
      {inst, cell} =
        slow_release(ctx, 25, 150, release_deadline_ms: 100, release_min_tail: 30)

      {:ok, inst: inst, cell: cell}
    end

    test "replaces the deadline of the instance for that call", ctx do
      assert :ok = Alambique.hibernate(ctx.inst, ctx.cell, release_deadline_ms: 5_000)

      assert {%{tail: tail, compact_duration: spent}, %{compaction: :done}} =
               await_event([:alambique, :cell, :hibernate], ctx.cell)

      assert tail == tail_length(ctx, ctx.cell)
      assert tail < 30
      assert System.convert_time_unit(spent, :native, :millisecond) > 100
    end

    test "leaves the deadline of the instance to the other releases", ctx do
      before = tail_length(ctx, ctx.cell)

      assert {:ok, %{hibernated: 1, errors: []}} = Alambique.drain(ctx.inst)

      assert {%{tail: tail, compact_duration: spent}, %{compaction: :deadline}} =
               await_event([:alambique, :cell, :hibernate], ctx.cell)

      assert tail > 30
      assert tail <= before
      assert System.convert_time_unit(spent, :native, :millisecond) < 1_000
    end

    test "0 turns the compaction off for that call", ctx do
      assert :ok = Alambique.hibernate(ctx.inst, ctx.cell, release_deadline_ms: 0)

      assert {_, %{compaction: :none}} = await_event([:alambique, :cell, :hibernate], ctx.cell)
    end

    test "is bounded by the time the call has left", ctx do
      assert :ok =
               Alambique.hibernate(ctx.inst, ctx.cell,
                 release_deadline_ms: 60_000,
                 timeout: 2_500
               )

      assert {%{compact_duration: spent}, %{compaction: compaction}} =
               await_event([:alambique, :cell, :hibernate], ctx.cell)

      assert compaction in [:done, :deadline]
      # The reserve of 2 000 ms leaves 500 ms of the 2 500.
      assert System.convert_time_unit(spent, :native, :millisecond) < 1_500
    end

    test "must be a non-negative integer", ctx do
      for bad <- [-1, 1.5, "3000", :infinity] do
        assert_raise ArgumentError, ~r/release_deadline_ms/, fn ->
          Alambique.hibernate(ctx.inst, ctx.cell, release_deadline_ms: bad)
        end

        assert_raise ArgumentError, ~r/release_deadline_ms/, fn ->
          Alambique.move(ctx.inst, ctx.cell, node(), release_deadline_ms: bad)
        end
      end
    end
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
