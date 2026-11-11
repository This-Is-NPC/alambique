defmodule Alambique.CellTest do
  @moduledoc "One cell on one node: wake, serve, hibernate, local reuse, migrations, group commit (design §6, §7)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Tx
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  setup do
    capture([
      [:alambique, :cell, :wake],
      [:alambique, :cell, :hibernate],
      [:alambique, :cell, :commit],
      [:alambique, :cell, :migrate],
      [:alambique, :cell, :stop],
      [:alambique, :lease, :acquire]
    ])

    setup_store()
  end

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp numbers(inst, cell), do: Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")

  describe "read-only queries" do
    test "query refuses every statement that would write, and the cell still writes", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)

      for sql <- [
            "INSERT INTO t VALUES (7, 'x')",
            "UPDATE t SET v = 'y'",
            "DELETE FROM t",
            "CREATE TABLE u (x)",
            "DROP TABLE t",
            "PRAGMA user_version = 9",
            "PRAGMA query_only = OFF",
            "PRAGMA query_only=0",
            "BEGIN",
            "ATTACH DATABASE ':memory:' AS other",
            "-- note\nINSERT INTO t VALUES (8, 'x')"
          ] do
        assert {:error, :read_only} = Alambique.query(inst, cell, sql), sql
      end

      assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "PRAGMA user_version")

      assert {:ok, %{rows: [_]}} =
               Alambique.query(inst, cell, "PRAGMA table_info(t)")
               |> then(fn {:ok, r} -> {:ok, %{r | rows: Enum.take(r.rows, 1)}} end)

      assert {:ok, _} = put(inst, cell, 2)
      assert [[1], [2]] = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")
    end
  end

  describe "lifecycle" do
    test "a cell wakes empty, serves reads and writes, and hibernates", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      assert {:ok, %{columns: ["n"], rows: []}} = numbers(inst, cell)
      assert {:ok, %{rows: []}} = put(inst, cell, 1)
      assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)
      assert {:ok, node()} == Alambique.owner(inst, cell)

      assert {_, %{source: :empty, result: :ok, epoch: 1}} =
               await_event([:alambique, :cell, :wake], cell)

      assert :ok = Alambique.hibernate(inst, cell)
      assert :none = Alambique.owner(inst, cell)
      assert {_, %{reason: :request}} = await_event([:alambique, :cell, :hibernate], cell)
    end

    test "hibernating leaves no process, no open file and no registry row", ctx do
      inst = start_instance(ctx, migrations: @schema, idle_timeout_ms: 50)
      cells = for _ <- 1..20, do: unique_cell()
      before = length(Process.list())

      for cell <- cells, do: assert({:ok, _} = put(inst, cell, 1))
      assert Enum.any?(open_files(), &String.ends_with?(&1, "cell.db"))

      eventually(fn -> Alambique.cells(inst).count == 0 end)
      eventually(fn -> not Enum.any?(open_files(), &String.contains?(&1, ctx.dir)) end)
      assert length(Process.list()) <= before
      assert DynamicSupervisor.count_children(Module.concat(inst, CellSupervisor)).active == 0

      for cell <- cells do
        assert {_, %{reason: :idle}} = await_event([:alambique, :cell, :hibernate], cell)
        assert :none = Alambique.owner(inst, cell)
      end
    end

    test "a busy cell does not hibernate", ctx do
      inst = start_instance(ctx, migrations: @schema, idle_timeout_ms: 300)
      cell = unique_cell()

      for n <- 1..8 do
        assert {:ok, _} = put(inst, cell, n)
        Process.sleep(60)
      end

      assert {:ok, node()} == Alambique.owner(inst, cell)
    end

    test "concurrent first calls wake the cell once", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      results =
        1..40
        |> Task.async_stream(fn n -> put(inst, cell, n) end, max_concurrency: 40)
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, _}, &1))
      assert {:ok, %{rows: rows}} = numbers(inst, cell)
      assert length(rows) == 40
      assert [_] = drain([:alambique, :cell, :wake], cell)
      assert [_] = drain([:alambique, :lease, :acquire], cell)
    end

    test "an invalid cell id is refused", ctx do
      inst = start_instance(ctx)

      for id <- ["", ".", "..", "a/b", "ä", String.duplicate("a", 129), :atom, 12] do
        assert {:error, :invalid_cell_id} = Alambique.query(inst, id, "SELECT 1")
      end

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, "A_b.c-1", "SELECT 1")
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, "...", "SELECT 1")
    end

    test "a cell id received from a peer is checked too", ctx do
      inst = start_instance(ctx)
      request = {:query, "SELECT 1", [], nil}

      for id <- ["", "..", "a/b", String.duplicate("a", 129)] do
        assert {nil, {:error, :invalid_cell_id}} =
                 Alambique.Router.remote(inst, id, request, 1_000, 0)
      end
    end
  end

  describe "options" do
    test "invalid options raise with a clear message", ctx do
      base = [name: :bad, store: ctx.store, data_dir: ctx.dir]

      for {opts, message} <- [
            {Keyword.delete(base, :store), ":store or :placement is required"},
            {Keyword.delete(base, :data_dir), ":data_dir is required"},
            {base ++ [bogus: 1], "unknown options [:bogus]"},
            {base ++ [lease_ttl_ms: 1000, skew_margin_ms: 500], "twice"},
            {base ++ [migrations: [{0, "x"}]], "positive integer versions"},
            {base ++ [migrations: [{1, "a"}, {1, "b"}]], "duplicate version"},
            {base ++ [migrations: Nope], "must export migrations/0"},
            {base ++ [after_ack: :nope], ":after_ack"},
            {base ++ [idle_timeout_ms: 0], ":idle_timeout_ms"}
          ] do
        error = assert_raise ArgumentError, fn -> Alambique.child_spec(opts) end
        assert Exception.message(error) =~ message
      end
    end
  end

  describe "local file" do
    test "a clean hibernation lets the next wake reuse the file", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)

      sources = for {_, %{source: s}} <- drain([:alambique, :cell, :wake], cell), do: s
      assert sources == [:empty, :local]
    end

    test "another owner's writes force a restore", ctx do
      a = start_instance(ctx, migrations: @schema)
      b = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      assert {:ok, _} = put(a, cell, 1)
      :ok = Alambique.hibernate(a, cell)
      assert {:ok, _} = put(b, cell, 2)
      :ok = Alambique.hibernate(b, cell)
      assert {:ok, %{rows: [[1], [2]]}} = numbers(a, cell)
      :ok = Alambique.hibernate(a, cell)
      assert {:ok, %{rows: [[1], [2]]}} = numbers(b, cell)

      sources = for {_, %{source: s}} <- drain([:alambique, :cell, :wake], cell), do: s
      assert sources == [:empty, :restore, :restore, :restore]
    end

    test "a file without a marker is restored, and interrupted restores are cleaned", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)

      dir = Path.join([ctx.dir, Atom.to_string(inst), cell <> ".cell"])
      File.rm!(Path.join(dir, "cell.db.meta"))
      File.write!(Path.join(dir, "cell.db.restore-77"), "junk")
      File.write!(Path.join(dir, "cell.db"), "garbage")

      assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)

      assert Enum.sort(File.ls!(dir)) |> Enum.reject(&String.ends_with?(&1, ["-wal", "-shm"])) ==
               ["cell.db"]

      sources = for {_, %{source: s}} <- drain([:alambique, :cell, :wake], cell), do: s
      assert sources == [:empty, :restore]
    end

    test "a damaged file with a marker is restored", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)

      File.write!(
        Path.join([ctx.dir, Atom.to_string(inst), cell <> ".cell", "cell.db"]),
        "garbage"
      )

      assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)
      sources = for {_, %{source: s}} <- drain([:alambique, :cell, :wake], cell), do: s
      assert sources == [:empty, :restore]
    end

    test "a lease left behind by a dead process of this node is reclaimed", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)
      Process.exit(pid, :kill)
      eventually(fn -> not Process.alive?(pid) end)

      assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)

      assert {_, %{epoch: 2}} =
               await_event([:alambique, :cell, :wake], cell, 5_000, &(&1.epoch == 2))
    end
  end

  describe "migrations" do
    test "run once per cell, in order, and again only for newer versions", ctx do
      v1 = {1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}

      v2 =
        {2,
         fn tx -> {:ok, _} = Tx.execute(tx, "ALTER TABLE t ADD COLUMN w INTEGER DEFAULT 7") end}

      a = start_instance(ctx, migrations: [v2, v1])
      cell = unique_cell()

      assert {:ok, %{rows: [[7]]}} =
               with({:ok, _} <- put(a, cell, 1), do: Alambique.query(a, cell, "SELECT w FROM t"))

      versions = for {_, %{version: v}} <- drain([:alambique, :cell, :migrate], cell), do: v
      assert versions == [1, 2]
      :ok = Alambique.hibernate(a, cell)

      assert {:ok, _} = put(a, cell, 2)
      assert [] = drain([:alambique, :cell, :migrate], cell)
      :ok = Alambique.hibernate(a, cell)

      v3 = {3, "CREATE TABLE u (x)"}
      b = start_instance(ctx, migrations: [v1, v2, v3])
      assert {:ok, %{rows: [[0]]}} = Alambique.query(b, cell, "SELECT count(*) FROM u")
      versions = for {_, %{version: v}} <- drain([:alambique, :cell, :migrate], cell), do: v
      assert versions == [3]
      assert [[3]] = durable_rows(ctx, cell, "PRAGMA user_version")
    end

    test "migrations come from a module and are durable before the cell serves", ctx do
      defmodule Schema do
        def migrations, do: [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
      end

      inst = start_instance(ctx, migrations: Schema)
      cell = unique_cell()
      assert {:ok, %{rows: []}} = numbers(inst, cell)
      assert [[1]] = durable_rows(ctx, cell, "PRAGMA user_version")
    end

    test "a failing migration aborts the wake and leaves the cell free", ctx do
      inst = start_instance(ctx, migrations: [{1, "CREATE TABLE t (n); NOT SQL"}])
      cell = unique_cell()
      assert {:error, {:migration, 1, _}} = numbers(inst, cell)
      assert :none = Alambique.owner(inst, cell)
      assert {_, %{result: :error}} = await_event([:alambique, :cell, :wake], cell)
    end
  end

  describe "transactions" do
    test "commit, rollback and errors", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      assert {:ok, :done} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO t (n) VALUES (1)")
                 {:ok, _} = Tx.execute(tx, "INSERT INTO t (n) VALUES (2)")
                 :done
               end)

      assert {:error, :nope} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO t (n) VALUES (3)")
                 Tx.rollback(tx, :nope)
               end)

      assert {:error, {:raised, %RuntimeError{message: "boom"}, _}} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO t (n) VALUES (4)")
                 raise "boom"
               end)

      assert {:error, _} = Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (1)")

      assert {:error, :transaction_ended} =
               Alambique.transaction(inst, cell, fn tx -> Tx.execute(tx, "COMMIT") end)

      assert {:ok, %{rows: [[1], [2]]}} = numbers(inst, cell)
    end

    test "a handle is dead once its function returned", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, tx} = Alambique.transaction(inst, cell, & &1)
      assert {:error, :inactive_transaction} = Tx.query(tx, "SELECT 1")
    end

    test "a transaction learns the TXID of its batch, and after_ack reports it", ctx do
      test = self()

      inst =
        start_instance(ctx,
          migrations: @schema,
          after_ack: fn cell, txid -> send(test, {:ack, cell, txid}) end
        )

      cell = unique_cell()

      insert = fn n ->
        fn tx ->
          {:ok, _} = Tx.execute(tx, "INSERT INTO t (n) VALUES (?)", [n])
          Tx.txid(tx)
        end
      end

      # TXID 1 is the fence and 2 the migrations; the first write joins 3.
      assert {:ok, 3} = Alambique.transaction(inst, cell, insert.(1))
      assert_receive {:ack, ^cell, 2}
      assert_receive {:ack, ^cell, 3}

      assert {:ok, 4} = Alambique.transaction(inst, cell, insert.(2))
      assert_receive {:ack, ^cell, 4}
      refute_receive {:ack, ^cell, _}, 50
    end

    test "an after_ack that raises does not hurt the cell", ctx do
      inst =
        start_instance(ctx, migrations: @schema, after_ack: fn _, _ -> raise "bad callback" end)

      cell = unique_cell()
      ExUnit.CaptureLog.capture_log(fn -> assert {:ok, _} = put(inst, cell, 1) end)
      assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)
    end
  end

  describe "group commit" do
    test "writers that arrive during an upload share the next batch and wait for durability",
         ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 0)
      drain([:alambique, :cell, :commit], cell)

      MemoryStore.inject(ctx.store_config, kind: :latency, ms: 100, ops: [:put], key: ~r{/0000/})

      replies =
        1..30
        |> Task.async_stream(
          fn n ->
            {:ok, _} = put(inst, cell, n)
            # The reply came back, so the write must already be in the store.
            {n, durable_rows(ctx, cell, "SELECT n FROM t WHERE n = #{n}")}
          end,
          max_concurrency: 30,
          timeout: 20_000
        )
        |> Enum.map(fn {:ok, r} -> r end)

      assert Enum.all?(replies, fn {n, rows} -> rows == [[n]] end)

      commits = drain([:alambique, :cell, :commit], cell)
      assert Enum.sum(for {%{transactions: t}, _} <- commits, do: t) == 30
      assert length(commits) < 30
      assert Enum.any?(commits, fn {%{transactions: t}, _} -> t > 1 end)
    end

    test "a failed upload leaves every writer of the batch unknown and stops the cell", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 0)

      MemoryStore.inject(ctx.store_config, kind: :error, ops: [:put], key: ~r{/0000/})

      assert {:error, :outcome_unknown} = put(inst, cell, 1)
      assert {_, %{reason: {:sync_failed, _}}} = await_event([:alambique, :cell, :stop], cell)
      MemoryStore.clear(ctx.store_config)

      assert {:ok, %{rows: [[0]]}} = numbers(inst, cell)
    end

    test "a read behind an unacknowledged write waits for it and fails with the reason", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 0)
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

      MemoryStore.inject(ctx.store_config, kind: :error, ops: [:put], key: ~r{/0000/})

      # Queue the write and then the read while the cell is suspended, so the
      # read is handled after the local commit and before its upload.
      :sys.suspend(pid)
      write = Task.async(fn -> put(inst, cell, 1) end)
      eventually(fn -> queued(pid) == 1 end)
      read = Task.async(fn -> numbers(inst, cell) end)
      eventually(fn -> queued(pid) == 2 end)
      :sys.resume(pid)

      assert {:error, :outcome_unknown} = Task.await(write)
      assert {:error, {:sync_failed, _}} = Task.await(read)
    end
  end

  describe "ownership" do
    setup ctx do
      world = ManualClock.new(1_800_000_000_000)
      clock = fn offset -> {ManualClock, ManualClock.config(world, offset)} end
      Map.merge(ctx, %{world: world, clock: clock})
    end

    test "a cell stops serving once its lease is no longer valid, and its successor takes over",
         ctx do
      a = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      b = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      cell = unique_cell()
      assert {:ok, _} = put(a, cell, 1)

      ManualClock.advance(ctx.world, 3_000 - 500)
      assert {:error, :lease_expired} = put(a, cell, 2)
      assert {_, %{reason: :lease_expired}} = await_event([:alambique, :cell, :stop], cell)

      assert {:ok, %{rows: [[1]]}} = numbers(b, cell)
    end

    test "a cell whose lease was taken away stops at the next renewal", ctx do
      a = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      b = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      cell = unique_cell()
      assert {:ok, _} = put(a, cell, 1)

      ManualClock.advance(ctx.world, 10_000)
      assert {:ok, _} = put(b, cell, 2)

      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(a), cell)
      ref = Process.monitor(pid)
      send(pid, :renew)
      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :lease_lost}}
      assert {_, %{reason: :lease_lost}} = await_event([:alambique, :cell, :stop], cell)
      assert {:ok, %{rows: [[1], [2]]}} = numbers(b, cell)
    end

    test "an owner that was fenced out stops and tells its writers", ctx do
      a = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      b = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      cell = unique_cell()
      assert {:ok, _} = put(a, cell, 1)

      # B takes the cell as if A's lease had expired and A had not noticed.
      ManualClock.advance(ctx.world, 10_000)
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(a), cell)

      :sys.replace_state(pid, fn state ->
        %{state | lease: %{state.lease | expires_at_ms: state.lease.expires_at_ms + 60_000}}
      end)

      assert {:ok, _} = put(b, cell, 2)

      assert {:error, :fenced} = put(a, cell, 3)
      assert {_, %{reason: :fenced}} = await_event([:alambique, :cell, :stop], cell)
      assert {:ok, %{rows: [[1], [2]]}} = numbers(b, cell)
    end
  end
end
