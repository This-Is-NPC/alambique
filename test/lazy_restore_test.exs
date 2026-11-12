defmodule Alambique.LazyRestoreTest do
  @moduledoc "A cell serves before its file is downloaded (design §16)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Tx
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  @lazy [min_bytes: 0, concurrency: 2, batch_bytes: 1_024, miss_pages: 4]

  setup do
    capture([
      [:alambique, :replication, :hydrate],
      [:alambique, :cell, :wake],
      [:alambique, :cell, :stop]
    ])

    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # A cell with `rows` rows of 1 KB, hibernated, so that its state is in the
  # store only.
  defp seed(ctx, rows) do
    seeder = start_instance(ctx, migrations: @schema, lazy_restore: false)
    cell = unique_cell()

    {:ok, :ok} =
      Alambique.transaction(seeder, cell, fn tx ->
        for n <- 1..rows do
          {:ok, _} =
            Tx.execute(tx, "INSERT INTO t VALUES (?, randomblob(1024))", [n])
        end

        :ok
      end)

    # The change feed holds the rows until they are published and the next
    # write drops them; a cell that wakes with them would read them all.
    eventually(fn ->
      {:ok, _} = Alambique.execute(seeder, cell, "DELETE FROM t WHERE n = 0")

      match?(
        {:ok, %{rows: [[0]]}},
        Alambique.query(seeder, cell, "SELECT count(*) FROM _alambique_feed WHERE op <> 'floor'")
      )
    end)

    :ok = Alambique.hibernate(seeder, cell)
    cell
  end

  defp lazy_instance(ctx, opts \\ []) do
    start_instance(ctx, [migrations: @schema, lazy_restore: @lazy] ++ opts)
  end

  defp hydrated?(cell), do: drain([:alambique, :replication, :hydrate], cell) != []

  # Holds the background reads of every hydrator of `cell` until the returned
  # function is called, so that only the pages statements miss get filled.
  # A held read gives up when its hydrator ends.
  defp hold_hydration(cell) do
    gate = :atomics.new(1, [])

    CrashPoints.arm(
      :hydrate_before_fetch,
      fn _, %{hydrator: hydrator} -> await_gate(gate, hydrator) end,
      times: :infinity,
      when: &match?(%{cell: ^cell}, &1)
    )

    fn -> :atomics.put(gate, 1, 1) end
  end

  defp await_gate(gate, hydrator) do
    if :atomics.get(gate, 1) == 0 and Process.alive?(hydrator) do
      Process.sleep(5)
      await_gate(gate, hydrator)
    end
  end

  test "a cell answers before its file is complete and then completes it", ctx do
    cell = seed(ctx, 400)
    release = hold_hydration(cell)
    inst = lazy_instance(ctx)

    assert {:ok, %{rows: [[1, 1024]]}} =
             Alambique.query(inst, cell, "SELECT n, length(v) FROM t WHERE n = 1")

    assert {_, %{result: :ok, source: :restore}} =
             await_event([:alambique, :cell, :wake], cell, 5_000, &(&1.source == :restore))

    refute hydrated?(cell)

    release.()
    {measure, meta} = await_event([:alambique, :replication, :hydrate], cell, 30_000)
    assert meta.result == :ok
    assert measure.pages > 400

    assert {:ok, %{rows: [[400, 409_600]]}} =
             Alambique.query(inst, cell, "SELECT count(*), sum(length(v)) FROM t")
  end

  test "writes made while the file is filling are durable", ctx do
    cell = seed(ctx, 300)
    release = hold_hydration(cell)
    inst = lazy_instance(ctx)

    assert {:ok, _} =
             Alambique.execute(
               inst,
               cell,
               "UPDATE t SET v = zeroblob(10) WHERE n BETWEEN 10 AND 20"
             )

    assert {:ok, _} = Alambique.execute(inst, cell, "DELETE FROM t WHERE n > 280")
    refute hydrated?(cell)

    expected = [[280, 11]]
    sql = "SELECT count(*), count(CASE WHEN length(v) = 10 THEN 1 END) FROM t"
    assert {:ok, %{rows: ^expected}} = Alambique.query(inst, cell, sql)
    # What survives if every node dies now is the same.
    assert durable_rows(ctx, cell, sql) == expected

    release.()
    await_event([:alambique, :replication, :hydrate], cell, 30_000)
    assert {:ok, %{rows: ^expected}} = Alambique.query(inst, cell, sql)
    assert durable_rows(ctx, cell, sql) == expected
  end

  test "a cell that hibernates while the file is filling restores again", ctx do
    cell = seed(ctx, 300)
    hold_hydration(cell)
    inst = lazy_instance(ctx)
    path = Path.join([ctx.dir, Atom.to_string(inst), cell <> ".cell"])

    assert {:ok, _} = Alambique.execute(inst, cell, "DELETE FROM t WHERE n > 100")
    refute hydrated?(cell)
    :ok = Alambique.hibernate(inst, cell)

    # No marker, no file: nothing a later wake could take for complete.
    refute File.exists?(Path.join(path, "cell.db.meta"))
    refute File.exists?(Path.join(path, "cell.db"))

    drain([:alambique, :cell, :wake], cell)
    assert {:ok, %{rows: [[100]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    assert {_, %{result: :ok, source: :restore}} = await_event([:alambique, :cell, :wake], cell)
  end

  test "a cell that dies while the file is filling does not reuse it", ctx do
    cell = seed(ctx, 300)
    hold_hydration(cell)
    inst = lazy_instance(ctx)

    assert {:ok, _} = Alambique.execute(inst, cell, "DELETE FROM t WHERE n > 100")
    refute hydrated?(cell)
    config = Alambique.Config.fetch!(inst)
    pid = Alambique.Cell.whereis(config, cell)
    drain([:alambique, :cell, :wake], cell)
    Process.exit(pid, :kill)
    eventually(fn -> not Process.alive?(pid) end)

    assert {:ok, %{rows: [[100]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    assert {_, %{result: :ok, source: :restore}} = await_event([:alambique, :cell, :wake], cell)
  end

  test "a page that cannot be fetched fails the statement, not the cell", ctx do
    cell = seed(ctx, 300)
    release = hold_hydration(cell)
    inst = lazy_instance(ctx)
    config = Alambique.Config.fetch!(inst)

    # Wake the cell with the store working, then take it away.
    assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 1")
    pid = Alambique.Cell.whereis(config, cell)
    refute hydrated?(cell)

    fault =
      MemoryStore.inject(ctx.store_config,
        kind: :error,
        ops: [:get_range],
        times: :infinity
      )

    sql = "SELECT count(*), sum(length(v)) FROM t"
    assert {:error, reason} = Alambique.query(inst, cell, sql)
    assert reason =~ "I/O"
    assert Alambique.Cell.whereis(config, cell) == pid

    MemoryStore.remove(ctx.store_config, fault)
    assert {:ok, %{rows: [[300, 307_200]]}} = Alambique.query(inst, cell, sql)
    assert Alambique.Cell.whereis(config, cell) == pid
    release.()
    await_event([:alambique, :replication, :hydrate], cell, 30_000)
  end

  test "a cell with little data is downloaded at once", ctx do
    cell = seed(ctx, 20)
    inst = start_instance(ctx, migrations: @schema)

    assert {:ok, %{rows: [[20]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    refute_receive {:telemetry, [:alambique, :replication, :hydrate], _, %{cell: ^cell}}, 300
  end

  test "lazy restore is configured with validated options" do
    base = [name: :lazy_cfg, store: {AlambiqueTestkit.MemoryStore, %{}}, data_dir: "/tmp/x"]

    assert %{lazy_restore: %{min_bytes: 4_194_304, concurrency: 4}} =
             Alambique.Config.build(base)

    assert %{lazy_restore: nil} = Alambique.Config.build(base ++ [lazy_restore: false])

    assert %{lazy_restore: %{min_bytes: 0}} =
             Alambique.Config.build(base ++ [lazy_restore: [min_bytes: 0]])

    assert_raise ArgumentError, ~r/unknown :lazy_restore/, fn ->
      Alambique.Config.build(base ++ [lazy_restore: [nope: 1]])
    end

    assert_raise ArgumentError, ~r/concurrency must be a positive integer/, fn ->
      Alambique.Config.build(base ++ [lazy_restore: [concurrency: 0]])
    end
  end
end
