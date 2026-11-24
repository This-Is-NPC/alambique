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
      [:alambique, :cell, :stop],
      [:alambique, :cell, :writes_released]
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
    start_instance(ctx, Keyword.merge([migrations: @schema, lazy_restore: @lazy], opts))
  end

  defp hydrated?(cell), do: drain([:alambique, :replication, :hydrate], cell) != []

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

  describe "reads while the file is filling (verify_reads)" do
    # Reports every page a statement misses, from the hydrator that serves it.
    defp watch_misses(cell) do
      test = self()

      CrashPoints.arm(
        :hydrate_before_miss,
        fn _, %{pgno: pgno} -> send(test, {:page_miss, pgno}) end,
        times: :infinity,
        when: &match?(%{cell: ^cell}, &1)
      )
    end

    # Wakes the cell and drops the misses of the wake itself, which reads
    # pages too: what arrives afterwards belongs to the statements.
    defp wake_and_flush(inst, cell) do
      assert {:ok, _node} = Alambique.touch(inst, cell)
      refute hydrated?(cell)
      flush_misses()
    end

    defp flush_misses do
      receive do
        {:page_miss, _} -> flush_misses()
      after
        0 -> :ok
      end
    end

    test "with verify_reads a read waits for the end of the hydration, off the dirty IO", ctx do
      cell = seed(ctx, 300)
      release = hold_hydration(cell)
      watch_misses(cell)
      inst = lazy_instance(ctx, lazy_restore: @lazy ++ [verify_reads: true])
      config = Alambique.Config.fetch!(inst)
      wake_and_flush(inst, cell)

      read = Task.async(fn -> Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 5") end)

      checked =
        Task.async(fn ->
          Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 6", [], verify_lease: true)
        end)

      pid = Alambique.Cell.whereis(config, cell)
      eventually(fn -> length(:sys.get_state(pid).parked) == 2 end)
      refute hydrated?(cell)
      # No statement ran, so none waits for a page.
      assert Task.yield(read, 200) == nil
      refute_received {:page_miss, _}

      release.()
      assert {:ok, %{rows: [[5]]}} = Task.await(read, 30_000)
      assert {:ok, %{rows: [[6]]}} = Task.await(checked, 30_000)
      assert hydrated?(cell)
      refute_received {:page_miss, _}

      assert {%{count: 2}, _} = await_event([:alambique, :cell, :writes_released], cell, 5_000)
      assert :sys.get_state(pid).parked == []
    end

    test "without verify_reads a read runs at once and misses the page", ctx do
      cell = seed(ctx, 300)
      release = hold_hydration(cell)
      watch_misses(cell)
      inst = lazy_instance(ctx)
      wake_and_flush(inst, cell)

      assert {:ok, %{rows: [[5]]}} = Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 5")
      assert_received {:page_miss, _}
      refute hydrated?(cell)
      release.()
    end

    test "a read that waits gets the error of a failed hydration", ctx do
      cell = seed(ctx, 300)
      {:ok, keys, nil} = MemoryStore.list(ctx.store_config, "", nil, 10_000)

      for key <- keys, String.ends_with?(key, ".ltx") do
        {:ok, body, _etag, _meta} = MemoryStore.get(ctx.store_config, key)

        {:ok, _} =
          MemoryStore.put(
            ctx.store_config,
            key,
            Alambique.LtxTamper.alter_frames(body, :last),
            :none,
            %{}
          )
      end

      inst =
        lazy_instance(ctx, lazy_restore: @lazy ++ [verify_bytes: 0, verify_reads: true])

      assert {:error, {:hydration_failed, _}} =
               Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 5")
    end
  end

  describe "writes while the file is filling (design 16)" do
    test "a write waits for the end of the hydration and is then acknowledged", ctx do
      cell = seed(ctx, 300)
      release = hold_hydration(cell)
      inst = lazy_instance(ctx)
      config = Alambique.Config.fetch!(inst)

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 1")
      pid = Alambique.Cell.whereis(config, cell)

      write =
        Task.async(fn ->
          Alambique.execute(inst, cell, "UPDATE t SET v = zeroblob(10) WHERE n = 5")
        end)

      eventually(fn -> length(:sys.get_state(pid).parked) == 1 end)
      refute hydrated?(cell)
      # Reads are not held back.
      assert {:ok, %{rows: [[5]]}} = Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 5")

      release.()
      assert {:ok, _} = Task.await(write, 30_000)
      assert hydrated?(cell)

      assert {%{count: 1, wait_ms: wait}, _} =
               await_event([:alambique, :cell, :writes_released], cell, 5_000)

      assert wait >= 0
      assert :sys.get_state(pid).parked == []

      assert {:ok, %{rows: [[10]]}} =
               Alambique.query(inst, cell, "SELECT length(v) FROM t WHERE n = 5")
    end

    test "a write is never acknowledged on a file that fails its checksum", ctx do
      cell = seed(ctx, 300)
      {:ok, keys, nil} = MemoryStore.list(ctx.store_config, "", nil, 10_000)

      for key <- keys, String.ends_with?(key, ".ltx") do
        {:ok, body, _etag, _meta} = MemoryStore.get(ctx.store_config, key)

        {:ok, _} =
          MemoryStore.put(
            ctx.store_config,
            key,
            Alambique.LtxTamper.alter_frames(body, :last),
            :none,
            %{}
          )
      end

      # Frames only, so that the file checksum does not see the damage first.
      inst = lazy_instance(ctx, lazy_restore: @lazy ++ [verify_bytes: 0])

      assert {:error, {:hydration_failed, _}} =
               Alambique.execute(inst, cell, "UPDATE t SET v = zeroblob(10) WHERE n = 5")
    end

    test "calls that are not writes are answered while the file is filling", ctx do
      cell = seed(ctx, 300)
      release = hold_hydration(cell)
      inst = lazy_instance(ctx)
      config = Alambique.Config.fetch!(inst)

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 1")
      pid = Alambique.Cell.whereis(config, cell)
      refute hydrated?(cell)

      at = System.monotonic_time(:millisecond) + 5_000
      assert :ok = GenServer.call(pid, :touch)
      assert :ok = GenServer.call(pid, {:deadline, at, :touch})
      assert {:ok, %{awake: true}} = GenServer.call(pid, :position)
      assert Alambique.Cell.whereis(config, cell) == pid
      release.()
    end

    test "installing the change feed at a wake waits for the hydration", ctx do
      # The feed now omits a column: the wake rewrites the capture triggers.
      seeder = start_instance(ctx, migrations: @schema, lazy_restore: false)
      cell = unique_cell()

      {:ok, :ok} =
        Alambique.transaction(seeder, cell, fn tx ->
          for n <- 1..300 do
            {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (?, randomblob(1024))", [n])
          end

          :ok
        end)

      :ok = Alambique.hibernate(seeder, cell)
      release = hold_hydration(cell)
      inst = lazy_instance(ctx, feed: [tables: %{"t" => [omit: ["v"]]}])

      read = Task.async(fn -> Alambique.query(inst, cell, "SELECT count(*) FROM t") end)
      assert Task.yield(read, 300) == nil
      refute hydrated?(cell)

      release.()
      assert {:ok, %{rows: [[300]]}} = Task.await(read, 30_000)
      assert hydrated?(cell)
    end

    test "with verify_writes off the write is acknowledged at once", ctx do
      cell = seed(ctx, 300)
      hold_hydration(cell)
      inst = lazy_instance(ctx, lazy_restore: @lazy ++ [verify_writes: false])

      assert {:ok, _} = Alambique.execute(inst, cell, "UPDATE t SET v = zeroblob(10) WHERE n = 5")
      refute hydrated?(cell)
    end
  end

  test "writes made while the file is filling are durable", ctx do
    cell = seed(ctx, 300)
    release = hold_hydration(cell)
    inst = lazy_instance(ctx, lazy_restore: @lazy ++ [verify_writes: false])

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
    inst = lazy_instance(ctx, lazy_restore: @lazy ++ [verify_writes: false])
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
    inst = lazy_instance(ctx, lazy_restore: @lazy ++ [verify_writes: false])

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
    # The store keeps failing for longer than a breaker would let it be asked.
    # Frames only: a file read whole would leave no page to miss.
    inst =
      lazy_instance(ctx,
        breaker: [failures: 1_000_000],
        lazy_restore: @lazy ++ [verify_bytes: 0]
      )

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

    assert %{lazy_restore: %{verify_bytes: 1_048_576}} = Alambique.Config.build(base)

    assert %{lazy_restore: %{verify_bytes: 0}} =
             Alambique.Config.build(base ++ [lazy_restore: [verify_bytes: 0]])

    assert_raise ArgumentError, ~r/verify_bytes must be a positive integer/, fn ->
      Alambique.Config.build(base ++ [lazy_restore: [verify_bytes: -1]])
    end

    assert %{lazy_restore: %{verify_writes: true}} = Alambique.Config.build(base)

    assert_raise ArgumentError, ~r/verify_writes must be a boolean/, fn ->
      Alambique.Config.build(base ++ [lazy_restore: [verify_writes: 1]])
    end

    assert %{lazy_restore: %{verify_reads: false}} = Alambique.Config.build(base)

    assert %{lazy_restore: %{verify_reads: true}} =
             Alambique.Config.build(base ++ [lazy_restore: [verify_reads: true]])

    assert_raise ArgumentError, ~r/verify_reads must be a boolean/, fn ->
      Alambique.Config.build(base ++ [lazy_restore: [verify_reads: 1]])
    end

    assert_raise ArgumentError, ~r/unknown :lazy_restore/, fn ->
      Alambique.Config.build(base ++ [lazy_restore: [nope: 1]])
    end

    assert_raise ArgumentError, ~r/concurrency must be a positive integer/, fn ->
      Alambique.Config.build(base ++ [lazy_restore: [concurrency: 0]])
    end
  end
end
