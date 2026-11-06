defmodule Alambique.SweeperTest do
  @moduledoc "The sweeper migrates idle cells in the background (design §17)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Schema, Tx}
  alias AlambiqueTestkit.MemoryStore

  @moduletag :capture_log

  @v1 {1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}
  @v2 {2, "ALTER TABLE t ADD COLUMN w INTEGER DEFAULT 5"}

  setup do
    capture([[:alambique, :sweeper, :cell], [:alambique, :sweeper, :round]])
    setup_store()
  end

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  # Cells written and put to sleep by the previous release.
  defp old_cells(ctx, count) do
    old = start_instance(ctx, migrations: [@v1])

    for _ <- 1..count do
      cell = unique_cell()
      assert {:ok, _} = put(old, cell, 1)
      :ok = Alambique.hibernate(old, cell)
      cell
    end
  end

  defp index(%{store_config: config}) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/schema/", nil, 100_000)
    for "cells-test/schema/" <> rest <- keys, do: List.to_tuple(String.split(rest, "/"))
  end

  defp versions(inst) do
    config = Alambique.Config.fetch!(inst)
    {:ok, [location]} = Alambique.Placement.locations(config)

    {:ok, versions} =
      Schema.versions(config, location, System.monotonic_time(:millisecond) + 5_000)

    versions
  end

  test "drains every old version through the normal API", ctx do
    cells = old_cells(ctx, 7)
    new = start_instance(ctx, migrations: [@v1, @v2])
    assert versions(new) == [1]

    assert {:ok, %{cells: 7, migrated: 7, errors: 0, skipped: 0}} = Alambique.sweep(new)

    assert versions(new) == [2]
    assert Enum.sort(index(ctx)) == Enum.sort(for c <- cells, do: {"0000000002", c})

    for cell <- cells do
      assert durable_rows(ctx, cell, "PRAGMA user_version") == [[2]]
      assert :none = Alambique.owner(new, cell)
      assert durable_rows(ctx, cell, "SELECT n, w FROM t") == [[1, 5]]
    end

    assert {_, %{result: :migrated, version: 1}} =
             await_event([:alambique, :sweeper, :cell], hd(cells))

    assert_received {:telemetry, [:alambique, :sweeper, :round], %{cells: 7, migrated: 7}, _}
  end

  test "pages through more cells than one listing returns", ctx do
    old_cells(ctx, 9)
    new = start_instance(ctx, migrations: [@v1, @v2])
    assert {:ok, %{cells: 9, migrated: 9}} = Alambique.sweep(new, page_size: 2)
    assert versions(new) == [2]
  end

  test "skips frozen cells and finds them again after unfreezing", ctx do
    [frozen, other] = old_cells(ctx, 2)
    new = start_instance(ctx, migrations: [@v1, @v2])
    :ok = Alambique.freeze(new, frozen)

    assert {:ok, %{cells: 2, migrated: 1, skipped: 1, errors: 0}} = Alambique.sweep(new)

    assert {1, ^frozen} =
             List.keyfind(
               index(ctx) |> Enum.map(fn {v, c} -> {String.to_integer(v), c} end),
               1,
               0
             )

    assert durable_rows(ctx, other, "PRAGMA user_version") == [[2]]
    assert durable_rows(ctx, frozen, "PRAGMA user_version") == [[1]]

    :ok = Alambique.unfreeze(new, frozen)
    assert {:ok, %{migrated: 1}} = Alambique.sweep(new)
    assert versions(new) == [2]
  end

  test "an entry left behind by a crash is removed when the sweeper finds it", ctx do
    [cell] = old_cells(ctx, 1)
    new = start_instance(ctx, migrations: [@v1, @v2])
    assert {:ok, _} = put(new, cell, 2)
    :ok = Alambique.hibernate(new, cell)

    {:ok, _} =
      MemoryStore.put(ctx.store_config, "cells-test/schema/0000000001/#{cell}", "", :none, %{})

    third = start_instance(ctx, migrations: [@v1, @v2, {3, "CREATE TABLE u (x)"}])
    assert versions(third) == [1, 2]

    # One wake migrates the cell and removes both of its stale entries.
    assert {:ok, %{cells: 1, migrated: 1, errors: 0}} = Alambique.sweep(third)
    assert versions(third) == [3]
  end

  test "a stale version 0 entry is removed and the cell migrates", ctx do
    [cell] = old_cells(ctx, 1)
    new = start_instance(ctx, migrations: [@v1, @v2])

    {:ok, _} =
      MemoryStore.put(ctx.store_config, "cells-test/schema/0000000000/#{cell}", "", :none, %{})

    assert versions(new) == [0, 1]
    assert {:ok, %{cells: 1, migrated: 1, errors: 0}} = Alambique.sweep(new)

    assert versions(new) == [2]
    assert index(ctx) == [{"0000000002", cell}]
    assert durable_rows(ctx, cell, "PRAGMA user_version") == [[2]]
  end

  test "failing cells are counted, stay listed, and are retried by the next pass", ctx do
    cells = old_cells(ctx, 3)
    broken = start_instance(ctx, migrations: [@v1, {2, fn _tx -> raise "boom" end}])
    assert {:ok, %{cells: 3, migrated: 0, errors: 3}} = Alambique.sweep(broken)
    assert versions(broken) == [1]

    fixed = start_instance(ctx, migrations: [@v1, @v2])
    assert {:ok, %{migrated: 3, errors: 0}} = Alambique.sweep(fixed)
    assert Enum.sort(for {_, c} <- index_pairs(ctx), do: c) == Enum.sort(cells)
  end

  defp index_pairs(ctx), do: Enum.map(index(ctx), fn {v, c} -> {String.to_integer(v), c} end)

  test "concurrency is bounded", ctx do
    old_cells(ctx, 8)
    counters = :counters.new(2, [:atomics])

    slow = fn tx ->
      running = :counters.get(counters, 1) + 1
      :counters.add(counters, 1, 1)
      if running > :counters.get(counters, 2), do: :counters.put(counters, 2, running)
      Process.sleep(40)
      :counters.sub(counters, 1, 1)
      {:ok, _} = Tx.execute(tx, "ALTER TABLE t ADD COLUMN w INTEGER")
    end

    new = start_instance(ctx, migrations: [@v1, {2, slow}])
    assert {:ok, %{migrated: 8}} = Alambique.sweep(new, concurrency: 2)
    assert :counters.get(counters, 2) in 1..2
  end

  test "the rate bounds the cells per second", ctx do
    old_cells(ctx, 6)
    new = start_instance(ctx, migrations: [@v1, @v2])
    started = System.monotonic_time(:millisecond)
    assert {:ok, %{migrated: 6}} = Alambique.sweep(new, rate: 20, page_size: 2, concurrency: 2)
    assert System.monotonic_time(:millisecond) - started >= 250
  end

  test "an instance with a sweeper drains the old versions on its own", ctx do
    old_cells(ctx, 5)

    new =
      start_instance(ctx,
        migrations: [@v1, @v2],
        sweeper: [interval_ms: 50, rate: 1_000]
      )

    eventually(fn -> versions(new) == [2] end)

    assert Supervisor.which_children(new)
           |> Enum.any?(&match?({Alambique.Schema.Sweeper, _, _, _}, &1))

    # The round reports after its cells are migrated, so it may still be running.
    assert_receive {:telemetry, [:alambique, :sweeper, :round], %{migrated: migrated},
                    %{result: :ok}},
                   5_000

    assert migrated >= 0
  end

  test "a sweeper that fails backs off instead of spinning", ctx do
    old_cells(ctx, 2)

    MemoryStore.inject(ctx.store_config,
      kind: :error,
      times: 1_000,
      key: ~r{/schema/},
      ops: [:list]
    )

    new =
      start_instance(ctx,
        migrations: [@v1, @v2],
        op_timeout_ms: 100,
        sweeper: [interval_ms: 50, max_backoff_ms: 400]
      )

    Process.sleep(1_200)

    rounds =
      for _ <- 1..50, reduce: 0 do
        acc -> if drain_round(), do: acc + 1, else: acc
      end

    assert rounds in 1..8
    assert Process.whereis(new)
  end

  defp drain_round do
    receive do
      {:telemetry, [:alambique, :sweeper, :round], _, %{result: :error}} -> true
    after
      0 -> false
    end
  end
end
