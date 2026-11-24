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

  @doc false
  # Telemetry handler: runs in the stopping cell and holds it until `:go`.
  def hold_stopping(_event, _measurements, %{cell: cell}, {test, cell}) do
    send(test, {:stopping, self()})

    receive do
      :go -> :ok
    after
      5_000 -> :ok
    end
  end

  def hold_stopping(_event, _measurements, _meta, _config), do: :ok

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

  describe "idle page cache" do
    # The module is not async: SQLite's page cache is one pool for the whole
    # process, so a lowered limit evicts the least recently used pages of any
    # connection, and the memory assertions need no other live cell.
    @fill "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 4000) " <>
            "INSERT INTO t SELECT n, hex(randomblob(100)) FROM c"
    @scan "SELECT count(v) FROM t"

    # Reads the connection from the process of the cell, which owns it, without
    # a call: a call would restore the cache.
    defp conn(inst, cell), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

    defp cache_size(pid) do
      in_cell(pid, fn state ->
        {:ok, %{rows: [[kib]]}} = Alambique.Sqlite.run(state.conn, "PRAGMA cache_size", [])
        kib
      end)
    end

    defp cache_used(pid) do
      in_cell(pid, fn state ->
        {:ok, %{cache_used: bytes}} = Alambique.Sqlite.memory(state.conn)
        bytes
      end)
    end

    defp filled(ctx, awake) do
      inst = start_instance(ctx, migrations: @schema, awake: awake, idle_timeout_ms: 60_000)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @fill)
      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)
      {inst, cell}
    end

    test "an idle cell lowers its cache, the next call restores it, and it shrinks again",
         ctx do
      {inst, cell} = filled(ctx, cache_kib: 512, idle_cache_after_ms: 50, idle_cache_kib: 16)
      conn = conn(inst, cell)
      before = cache_used(conn)
      assert before > 300_000

      eventually(fn -> cache_size(conn) == -16 end)
      assert cache_used(conn) < before / 2

      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)
      assert cache_size(conn) == -512

      eventually(fn -> cache_size(conn) == -16 end)
    end

    test "a cell called more often than idle_cache_after_ms keeps its cache", ctx do
      {inst, cell} = filled(ctx, cache_kib: 512, idle_cache_after_ms: 200, idle_cache_kib: 16)
      conn = conn(inst, cell)

      for _ <- 1..12 do
        assert {:ok, _} = Alambique.query(inst, cell, @scan)
        assert cache_size(conn) == -512
        Process.sleep(50)
      end

      # The timer chain survived the calls.
      eventually(fn -> cache_size(conn) == -16 end)
    end

    test "nothing is armed or lowered unless idle_cache_after_ms is set", ctx do
      {inst, cell} = filled(ctx, cache_kib: 512)
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

      assert %{idle_cache: nil} = :sys.get_state(pid)
      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)
      assert %{idle_cache: nil} = :sys.get_state(pid)
      assert cache_size(pid) == -512
    end

    test "the cell still hibernates at idle_timeout_ms", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          idle_timeout_ms: 300,
          awake: [idle_cache_after_ms: 50]
        )

      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      conn = conn(inst, cell)
      eventually(fn -> cache_size(conn) == -64 end)
      assert {_, %{reason: :idle}} = await_event([:alambique, :cell, :hibernate], cell)
    end

    # Leaves several MB of garbage on the heap of the cell and returns its
    # size, so a collection that happens meanwhile does not decide the test.
    defp garbage(inst, cell, pid) do
      for _ <- 1..3 do
        assert {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT n, v FROM t")
        assert length(rows) == 4000
      end

      {:memory, before} = Process.info(pid, :memory)
      assert before > 2_000_000
      before
    end

    test "an idle cell collects the garbage of its last calls", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          awake: [cache_kib: 512, idle_cache_after_ms: 300, idle_cache_kib: 16],
          idle_timeout_ms: 60_000
        )

      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @fill)
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)
      before = garbage(inst, cell, pid)

      # Most of the heap goes, and what stays is small in absolute terms.
      eventually(fn -> elem(Process.info(pid, :memory), 1) < min(200_000, before / 10) end)
    end

    test "idle_gc: false shrinks the cache but keeps the garbage", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          awake: [cache_kib: 512, idle_cache_after_ms: 300, idle_cache_kib: 16, idle_gc: false],
          idle_timeout_ms: 60_000
        )

      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @fill)
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)
      before = garbage(inst, cell, pid)

      conn = conn(inst, cell)
      eventually(fn -> cache_size(conn) == -16 end)
      assert elem(Process.info(pid, :memory), 1) > before / 2
    end

    test "the admission total drops after the shrink", ctx do
      {inst, cell} =
        filled(ctx,
          cache_kib: 512,
          idle_cache_after_ms: 50,
          idle_cache_kib: 16,
          max_bytes: 1_000_000_000
        )

      config = Alambique.Config.fetch!(inst)
      before = Alambique.Admission.bytes(config)
      assert before > 300_000
      eventually(fn -> Alambique.Admission.bytes(config) < before / 2 end)
      assert cell
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
      # The instance holds its data directory lock for as long as it lives.
      eventually(fn ->
        not Enum.any?(
          open_files(),
          &(String.contains?(&1, ctx.dir) and not String.ends_with?(&1, "/lock"))
        )
      end)

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

      # A function cannot end its transaction: the statement is refused and
      # the transaction goes on.
      assert {:ok, {:error, :not_authorized}} =
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

    test "a read behind an unacknowledged write waits for it, even when its upload is retried",
         ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          upload: [retry_backoff_ms: 20, retry_backoff_max_ms: 50]
        )

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

      # The upload fails and is repeated: nobody is answered before it lands.
      assert Task.yield(write, 300) == nil
      assert Task.yield(read, 0) == nil
      MemoryStore.clear(ctx.store_config)

      assert {:ok, _} = await(write)
      assert {:ok, %{rows: [[0], [1]]}} = await(read)
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

    test "a cell that stopped serving is no longer reported as the owner while it exits", ctx do
      a = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      b = start_instance(ctx, migrations: @schema, clock: ctx.clock.(0))
      cell = unique_cell()
      assert {:ok, _} = put(a, cell, 1)

      ManualClock.advance(ctx.world, 10_000)
      assert {:ok, _} = put(b, cell, 2)

      # Hold the old cell right after its stop event: the process is alive,
      # as it is for the time it closes its database and releases the lease.
      id = {__MODULE__, make_ref()}

      :telemetry.attach(
        id,
        [:alambique, :cell, :stop],
        &__MODULE__.hold_stopping/4,
        {self(), cell}
      )

      on_exit(fn -> :telemetry.detach(id) end)

      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(a), cell)
      send(pid, :renew)
      assert_receive {:stopping, ^pid}
      assert Process.alive?(pid)

      assert Alambique.owner(a, cell, peers: true) == :none
      refute Alambique.owner(a, cell) == {:ok, node()}

      send(pid, :go)
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
