defmodule Alambique.PositionTest do
  @moduledoc "position/3, isolated transactions and Tx.before/1 on one node (design §13)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Replication, Sqlite, Tx}
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    ctx = setup_store()
    %{ctx: ctx, inst: start_instance(ctx, migrations: @schema)}
  end

  defp insert(inst, cell, n, opts \\ []),
    do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n], opts)

  # The rows the store holds up to `txid`.
  defp rows_at(inst, cell, txid) do
    path =
      Path.join(
        Alambique.Config.fetch!(inst).data_dir,
        "at-#{System.unique_integer([:positive])}.db"
      )

    {:ok, %{txid: ^txid}} = Replication.restore(repl_ctx(inst, cell), cell, path, {:txid, txid})
    {:ok, conn} = Sqlite.open(path, readonly: true)
    {:ok, %{rows: rows}} = Sqlite.run(conn, "SELECT n FROM t ORDER BY n", [])
    Sqlite.close(conn)
    File.rm(path)
    List.flatten(rows)
  end

  test "an awake cell answers a cut that covers every acknowledged write", %{inst: inst} do
    cell = unique_cell()
    assert {:ok, _} = insert(inst, cell, 1)
    assert {:ok, %{txid: first, awake: true, epoch: epoch}} = Alambique.position(inst, cell)
    assert epoch >= 1
    assert rows_at(inst, cell, first) == [1]

    assert {:ok, _} = insert(inst, cell, 2)
    assert {:ok, %{txid: second}} = Alambique.position(inst, cell)
    assert second > first
    assert rows_at(inst, cell, second) == [1, 2]
  end

  test "position uploads the commits that are waiting", %{ctx: ctx, inst: inst} do
    cell = unique_cell()
    assert {:ok, _} = insert(inst, cell, 1)
    MemoryStore.inject(ctx.store_config, kind: :latency, ops: [:put], ms: 300, times: 1)

    slow = Task.async(fn -> insert(inst, cell, 2) end)
    Process.sleep(50)
    queued = Task.async(fn -> insert(inst, cell, 3) end)
    Process.sleep(20)
    {:ok, %{txid: txid}} = Alambique.position(inst, cell)

    assert [{:ok, _}, {:ok, _}] = [Task.await(slow), Task.await(queued)]
    assert rows_at(inst, cell, txid) == [1, 2, 3]
  end

  test "an asleep cell is read from the store without waking", %{inst: inst} do
    cell = unique_cell()
    assert {:ok, %{txid: 0, epoch: 0, awake: false}} = Alambique.position(inst, cell)
    assert :none = Alambique.owner(inst, cell)

    assert {:ok, _} = insert(inst, cell, 1)
    {:ok, %{txid: awake}} = Alambique.position(inst, cell)
    assert :ok = Alambique.hibernate(inst, cell)

    assert {:ok, %{txid: ^awake, awake: false, epoch: epoch}} = Alambique.position(inst, cell)
    assert epoch >= 1
    assert :none = Alambique.owner(inst, cell)
  end

  test "an invalid id is an error", %{inst: inst} do
    assert {:error, _} = Alambique.position(inst, "bad/id")
  end

  describe "isolated transactions" do
    test "own their batch: before/1 is a cut between earlier and later writes", %{ctx: ctx} do
      # Depth 1: the slow upload holds the pipeline, so A and B wait together.
      inst = start_instance(ctx, migrations: @schema, pipeline_depth: 1)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 0)
      # The slow put is a segment's, not the feed's.
      MemoryStore.inject(ctx.store_config,
        kind: :latency,
        ops: [:put],
        ms: 300,
        times: 1,
        key: ~r{^(?!.*/feed/)}
      )

      ids = fn tx -> {Tx.txid(tx), Tx.before(tx)} end

      write = fn n ->
        fn tx ->
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (?)", [n])
          ids.(tx)
        end
      end

      slow = Task.async(fn -> insert(inst, cell, 1) end)
      Process.sleep(50)

      # A and B queue behind the slow upload and share the next batch; X
      # follows them and C follows X.
      tasks =
        for {n, opts} <- [{2, []}, {3, []}, {4, [isolated: true]}, {5, []}] do
          task = Task.async(fn -> Alambique.transaction(inst, cell, write.(n), opts) end)
          Process.sleep(20)
          task
        end

      assert {:ok, _} = Task.await(slow)
      [a, b, x, c] = Enum.map(tasks, &Task.await/1)

      assert {:ok, {batch, {:error, :not_isolated}}} = a
      assert {:ok, {^batch, {:error, :not_isolated}}} = b
      assert {:ok, {x_txid, {:ok, before}}} = x
      assert {:ok, {c_txid, {:error, :not_isolated}}} = c
      assert before == batch
      assert x_txid == batch + 1
      assert c_txid == x_txid + 1

      assert rows_at(inst, cell, before) == [0, 1, 2, 3]
      assert rows_at(inst, cell, x_txid) == [0, 1, 2, 3, 4]
    end

    test "execute takes the option and the cut is the position before it", %{inst: inst} do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      {:ok, %{txid: before}} = Alambique.position(inst, cell)
      assert {:ok, _} = insert(inst, cell, 2, isolated: true)
      {:ok, %{txid: after_x}} = Alambique.position(inst, cell)

      assert after_x == before + 1
      assert rows_at(inst, cell, before) == [1]
      assert rows_at(inst, cell, after_x) == [1, 2]
    end

    test "a failed isolated transaction changes nothing", %{inst: inst} do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      {:ok, %{txid: before}} = Alambique.position(inst, cell)

      assert {:error, :nope} =
               Alambique.transaction(inst, cell, &Tx.rollback(&1, :nope), isolated: true)

      assert {:ok, %{txid: ^before}} = Alambique.position(inst, cell)
    end
  end

  describe "data TXID" do
    alias Alambique.Compaction
    alias Alambique.Replication.Objects

    defp position(inst, cell) do
      {:ok, position} = Alambique.position(inst, cell)
      position
    end

    # Wakes the cell and puts it back to sleep, as a fence per round.
    defp wake_cycles(inst, cell, rounds) do
      for _ <- 1..rounds do
        assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
        assert :ok = Alambique.hibernate(inst, cell)
      end
    end

    test "a wake takes a TXID but never moves it", %{inst: inst} do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      %{data: data, txid: txid} = position(inst, cell)
      assert data == txid
      assert :ok = Alambique.hibernate(inst, cell)

      wake_cycles(inst, cell, 3)

      assert %{data: ^data, txid: later, awake: false} = position(inst, cell)
      assert later > txid
    end

    test "every write and every DDL moves it to the new TXID", %{inst: inst} do
      cell = unique_cell()

      for n <- 1..3 do
        assert {:ok, _} = insert(inst, cell, n)
        assert %{data: data, txid: data} = position(inst, cell)
        wake_cycles(inst, cell, 1)
        assert %{data: ^data} = position(inst, cell)
      end

      assert {:ok, :ok} =
               Alambique.transaction(inst, cell, &Tx.script(&1, "CREATE TABLE extra (x INTEGER)"))

      assert %{data: data, txid: data} = position(inst, cell)
      assert :ok = Alambique.hibernate(inst, cell)
      assert %{data: ^data, awake: false} = position(inst, cell)
    end

    test "survives compaction and snapshots", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..6, do: insert(inst, cell, n)
      assert :ok = Alambique.hibernate(inst, cell)
      wake_cycles(inst, cell, 2)
      %{data: data, txid: txid} = position(inst, cell)
      assert data < txid

      ctx = repl_ctx(inst, cell)

      assert {:ok, %{merges: merges}} =
               Compaction.run(ctx, cell, fanout: 2, max_level: 3, snapshot_interval_txids: 2)

      assert merges > 0
      {:ok, {segments, snapshots}} = Objects.list_cell(ctx, cell, deadline())
      assert Enum.any?(segments, &(&1.min < &1.max))
      assert Enum.any?(snapshots, &(&1.txid > 1))

      assert %{data: ^data, txid: ^txid, awake: false} = position(inst, cell)
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      assert %{data: ^data, awake: true} = position(inst, cell)
    end

    test "an object without it counts as data at its own TXID", %{inst: inst} do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      assert :ok = Alambique.hibernate(inst, cell)
      wake_cycles(inst, cell, 2)
      %{data: data, txid: txid} = position(inst, cell)
      assert data < txid

      strip_data(repl_ctx(inst, cell), cell)

      assert %{data: ^txid, txid: ^txid} = position(inst, cell)

      assert {:error, {:data_written, ^txid}} =
               Alambique.rewind(inst, cell, {:txid, txid - 1}, if_head: {txid - 1, :data})

      # The next wake fences on top of it: the data TXID stays the old head.
      wake_cycles(inst, cell, 1)
      assert %{data: ^txid} = position(inst, cell)
    end

    defp deadline, do: System.monotonic_time(:millisecond) + 5_000

    defp strip_data(ctx, cell) do
      {:ok, {segments, snapshots}} = Objects.list_cell(ctx, cell, deadline())

      for %{key: key} <- segments ++ snapshots do
        {:ok, body, meta} = Objects.fetch(ctx, key, deadline())
        :ok = Objects.put(ctx, key, body, :none, Map.delete(meta, "data"), deadline())
      end
    end
  end
end
