defmodule Alambique.LocalCacheTest do
  @moduledoc """
  The files of hibernated cells are a bounded cache (design §7.1): the
  oldest are removed when the data directory exceeds `local_cache.max_bytes`,
  and never those of a cell with a process.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Cell.{Local, LocalCache}

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"}]
  @blob 200_000

  setup do
    capture([[:alambique, :local_cache, :sweep]])
    setup_store()
  end

  defp fill(inst, cell, rows \\ 1) do
    for n <- 1..rows do
      assert {:ok, _} =
               Alambique.execute(inst, cell, "INSERT INTO t (v) VALUES (zeroblob(?1))", [
                 @blob + n
               ])
    end

    :ok = Alambique.hibernate(inst, cell)
  end

  defp cell_dirs(config), do: Path.wildcard(Path.join(config.data_dir, "*.cell"))

  # The sweep may remove a directory while it is listed.
  defp size(dirs) do
    for dir <- dirs,
        {:ok, files} <- [File.ls(dir)],
        file <- files,
        {:ok, stat} <- [File.stat(Path.join(dir, file))],
        reduce: 0 do
      acc -> acc + stat.size
    end
  end

  test "a background sweep keeps the files of hibernated cells under the limit", ctx do
    inst =
      start_instance(ctx,
        migrations: @schema,
        local_cache: [max_bytes: 1_500_000, interval_ms: 50]
      )

    config = Alambique.Config.fetch!(inst)
    cells = for _ <- 1..12, do: unique_cell()
    for cell <- cells, do: fill(inst, cell)

    eventually(fn -> size(cell_dirs(config)) <= 1_500_000 end)

    # An evicted cell wakes with its data, restored from the store.
    for cell <- cells do
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    end
  end

  test "the oldest cells go first and a sweep reports what it did", ctx do
    inst = start_instance(ctx, migrations: @schema, local_cache: [max_bytes: 1_500_000])
    config = Alambique.Config.fetch!(inst)
    [old, mid, new] = for _ <- 1..3, do: unique_cell()

    for {cell, age} <- [{old, 3000}, {mid, 2000}, {new, 1000}] do
      fill(inst, cell)
      marker = Path.join(Local.dir(config, cell), "cell.db.meta")
      File.touch!(marker, System.os_time(:second) - age)
    end

    assert %{evicted: 1} = LocalCache.sweep(config)
    refute File.exists?(Local.dir(config, old))
    assert File.exists?(Local.dir(config, mid))
    assert File.exists?(Local.dir(config, new))

    assert_received {:telemetry, [:alambique, :local_cache, :sweep], %{evicted: 1, freed: freed},
                     %{instance: ^inst}}

    assert freed > 0
  end

  test "a cell with a process keeps its files", ctx do
    inst = start_instance(ctx, migrations: @schema, local_cache: [max_bytes: 1])
    config = Alambique.Config.fetch!(inst)
    cell = unique_cell()
    fill(inst, cell)
    assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (v) VALUES (x'01')")

    assert %{evicted: 0, busy: 1} = LocalCache.sweep(config)
    assert File.exists?(Local.db_path(config, cell))
    assert {:ok, %{rows: [[2]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
  end

  test "a wake that races the eviction never loses data or fails", ctx do
    inst =
      start_instance(ctx,
        migrations: @schema,
        local_cache: [max_bytes: 1, interval_ms: 3_600_000]
      )

    config = Alambique.Config.fetch!(inst)
    cell = unique_cell()
    fill(inst, cell)

    for round <- 1..40 do
      sweeper = Task.async(fn -> LocalCache.sweep(config) end)

      assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (v) VALUES (x'01')")
      Task.await(sweeper)

      assert {:ok, %{rows: [[count]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
      assert count == round + 1
      :ok = Alambique.hibernate(inst, cell)
    end
  end

  test "the limit can be turned off and is validated", ctx do
    inst = start_instance(ctx, migrations: @schema, local_cache: false)
    assert Alambique.Config.fetch!(inst).local_cache == nil

    base = [name: :bad_cache, store: ctx.store, data_dir: ctx.dir]

    for bad <- [[max_bytes: 0], [interval_ms: -1], [bogus: 1], :nope] do
      assert_raise ArgumentError, ~r/local_cache/, fn ->
        Alambique.child_spec(base ++ [local_cache: bad])
      end
    end
  end
end
