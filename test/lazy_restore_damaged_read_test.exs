defmodule Alambique.LazyRestoreDamagedReadTest do
  @moduledoc """
  A ranged read that comes back damaged while a cell restores lazily (design
  §16). The hydrator is held, so the damaged read is the one a statement
  misses on demand. Whatever the damage, a cell whose hydration succeeds
  serves the original rows on the connection that was open all along, and
  nothing derived from a damaged page is acknowledged.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Tx
  alias AlambiqueTestkit.{CrashPoints, HostileStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  # No file is read whole: every page stays unchecked until the end.
  @lazy [min_bytes: 0, concurrency: 2, batch_bytes: 1_024, miss_pages: 4, verify_bytes: 0]
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

  # A lazy instance over a store that damages reads on command, with the
  # background reads held. The cell is awake when this returns.
  defp wake_lazily(ctx, cell) do
    hostile = HostileStore.config(ctx.store_config)
    release = hold_hydration(cell)

    inst =
      start_instance(%{ctx | store: {HostileStore, hostile}},
        migrations: @schema,
        lazy_restore: @lazy
      )

    assert {:ok, _node} = Alambique.touch(inst, cell)
    {inst, hostile, release}
  end

  # What a new owner reads from the store: no lazy restore, so nothing of the
  # first owner's memory.
  defp reread(ctx, cell) do
    fresh = start_instance(ctx, migrations: @schema, lazy_restore: false)

    # The failed owner holds its lease until it lapses: `{:unavailable, _, _}`.
    eventually(fn ->
      case Alambique.query(fresh, cell, @all) do
        {:ok, %{rows: rows}} -> rows
        {:error, {:unavailable, _, _}} -> nil
      end
    end)
  end

  defp appended(rows), do: List.update_at(rows, @row - 1, fn [n, v] -> [n, v <> "x"] end)

  test "a short fetch fails the statement; the cell then serves the original rows", ctx do
    {cell, rows} = seed(ctx)
    {inst, hostile, release} = wake_lazily(ctx, cell)

    HostileStore.damage(hostile, :short, 1)
    result = Alambique.query(inst, cell, "SELECT n, v FROM t WHERE n = #{@row}")
    assert HostileStore.damaged(hostile) == 1

    # An error, or the original row: never another answer.
    case result do
      {:error, reason} -> assert reason =~ "I/O"
      {:ok, %{rows: [row]}} -> assert row == Enum.at(rows, @row - 1)
    end

    release.()
    assert {_, %{result: :ok}} = await_event([:alambique, :replication, :hydrate], cell, 30_000)

    # The same cell, the same connection: no restart in between.
    assert {:ok, %{rows: ^rows}} = Alambique.query(inst, cell, @all)
    assert {:ok, _} = Alambique.execute(inst, cell, @append)
    assert {:ok, %{rows: after_write}} = Alambique.query(inst, cell, @all)
    assert after_write == appended(rows)

    :ok = Alambique.hibernate(inst, cell)
    assert reread(ctx, cell) == appended(rows)
  end

  test "a page that decodes damaged is never acknowledged, whatever the hydration says", ctx do
    {cell, rows} = seed(ctx)
    {inst, hostile, release} = wake_lazily(ctx, cell)
    config = Alambique.Config.fetch!(inst)

    # A byte flipped in the middle of the first ranged read of a statement.
    HostileStore.damage(hostile, {:flip, 50}, 1)
    _unchecked = Alambique.query(inst, cell, "SELECT n, v FROM t WHERE n = #{@row}")
    assert HostileStore.damaged(hostile) == 1

    # The write waits for the end of the hydration (the write gate).
    write = Task.async(fn -> Alambique.execute(inst, cell, @append) end)
    pid = Alambique.Cell.whereis(config, cell)
    eventually(fn -> length(:sys.get_state(pid).parked) == 1 end)

    release.()
    {_, meta} = await_event([:alambique, :replication, :hydrate], cell, 30_000)

    case meta.result do
      # The flip did not change a page: the file is the original.
      :ok ->
        assert {:ok, _} = Task.await(write, 30_000)
        assert {:ok, %{rows: after_write}} = Alambique.query(inst, cell, @all)
        assert after_write == appended(rows)
        :ok = Alambique.hibernate(inst, cell)
        assert reread(ctx, cell) == appended(rows)

      # A damaged page failed the checksum: the write is refused, and the
      # store still holds the original.
      :error ->
        assert {:error, {:hydration_failed, _}} = Task.await(write, 30_000)
        assert reread(ctx, cell) == rows
    end
  end
end
