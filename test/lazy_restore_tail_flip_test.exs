defmodule Alambique.LazyRestoreTailFlipTest do
  @moduledoc """
  Two damaged ranged reads of a lazy restore that cancel in the database
  checksum (design §16). The database checksum is the XOR of the CRC-64 of
  every page. A CRC is linear: flipping the same bits at the same distance
  from the end of two pages changes both CRCs by the same amount, and the
  two changes cancel in the XOR. The last byte of a ranged read is the last
  byte of the last frame it holds, which LZ4 always stores as a literal, so
  flipping it flips the last byte of that page. Two such reads give two
  pages that differ from the store and a checksum that equals the restored
  one. The cell must refuse them all the same.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Tx
  alias AlambiqueTestkit.{CrashPoints, HostileStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  # One page per read, and no file is read whole: every page stays unchecked
  # until the end.
  @lazy [min_bytes: 0, concurrency: 2, batch_bytes: 1_024, miss_pages: 1, verify_bytes: 0]
  @all "SELECT n, v FROM t ORDER BY n"
  @row 150
  @append "UPDATE t SET v = CAST(v || 'x' AS BLOB) WHERE n = #{@row}"

  setup do
    capture([[:alambique, :replication, :hydrate]])
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # 300 rows of random 1 KB values, hibernated, so that the state is in the
  # store only. Returns the cell and its rows.
  defp seed(ctx) do
    seeder = start_instance(ctx, migrations: @schema, lazy_restore: false)
    cell = unique_cell()

    {:ok, :ok} =
      Alambique.transaction(seeder, cell, fn tx ->
        for n <- 1..300 do
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (?, randomblob(1024))", [n])
        end

        :ok
      end)

    # The feed keeps the rows until they are published and the next write
    # drops them: a cell that wakes with them would read all of them.
    eventually(fn ->
      {:ok, _} = Alambique.execute(seeder, cell, "DELETE FROM t WHERE n = 0")

      match?(
        {:ok, %{rows: [[0]]}},
        Alambique.query(seeder, cell, "SELECT count(*) FROM _alambique_feed WHERE op <> 'floor'")
      )
    end)

    {:ok, %{rows: rows}} = Alambique.query(seeder, cell, @all)
    :ok = Alambique.hibernate(seeder, cell)
    {cell, rows}
  end

  # What a new owner reads from the store: no lazy restore, so nothing of the
  # first owner's memory.
  defp reread(ctx, cell) do
    fresh = start_instance(ctx, migrations: @schema, lazy_restore: false)

    eventually(fn ->
      with {:ok, %{rows: rows}} <- Alambique.query(fresh, cell, @all), do: rows
    end)
  end

  test "two pages damaged in the same place do not pass the end of the hydration", ctx do
    {cell, rows} = seed(ctx)
    hostile = HostileStore.config(ctx.store_config)
    release = hold_hydration(cell)

    inst =
      start_instance(%{ctx | store: {HostileStore, hostile}},
        migrations: @schema,
        lazy_restore: @lazy
      )

    assert {:ok, _node} = Alambique.touch(inst, cell)
    config = Alambique.Config.fetch!(inst)

    # The last byte of the first two ranged reads that statements miss. Their
    # pages differ from the store, but the checksums add up.
    HostileStore.damage(hostile, {:flip, 99}, 2)

    eventually(fn ->
      _unchecked = Alambique.query(inst, cell, @all)
      HostileStore.damaged(hostile) == 2
    end)

    # The write waits for the end of the hydration (the write gate).
    write = Task.async(fn -> Alambique.execute(inst, cell, @append) end)
    pid = Alambique.Cell.whereis(config, cell)
    eventually(fn -> length(:sys.get_state(pid).parked) == 1 end)

    release.()
    {_, meta} = await_event([:alambique, :replication, :hydrate], cell, 30_000)

    # The damaged pages are refused, and the write that waited with them.
    assert meta.result == :error
    assert {:error, {:hydration_failed, _}} = Task.await(write, 30_000)
    assert reread(ctx, cell) == rows
  end
end
