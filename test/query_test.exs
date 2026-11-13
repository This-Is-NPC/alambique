defmodule Alambique.QueryTest do
  @moduledoc "Cross-cell queries on read-only replicas (design §14)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{CountingStore, Query}
  alias Alambique.Query.{Cache, Row}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @count "SELECT count(*) FROM notes"

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  defp exec(inst, cell, sql, params),
    do: {:ok, _} = Alambique.execute(inst, cell, sql, params)

  defp insert(inst, cell, n),
    do:
      for(
        i <- 1..n,
        do: exec(inst, cell, "INSERT INTO notes (body) VALUES (?)", ["#{cell}-#{i}"])
      )

  # A reader instance that has no cells of its own.
  defp reader(ctx, opts \\ []),
    do: start_instance(ctx, Keyword.put_new(opts, :migrations, @schema))

  defp lease(%{store: {m, c}}, cell) do
    {:ok, body, etag, _} = m.get(c, "cells-test/cells/#{cell}/lease")
    {body, etag}
  end

  # A file that fits the first range is read by it, a larger one by a `get`.
  defp reads(cfg), do: CountingStore.count(cfg, :get) + CountingStore.count(cfg, :get_range)

  defp counts(result),
    do:
      Map.new(result.rows, fn %Row{cell: cell, txid: txid, values: [n]} -> {cell, {n, txid}} end)

  defp children(inst),
    do: DynamicSupervisor.count_children(Module.concat(inst, CellSupervisor)).active

  describe "querying cells" do
    test "reads cells that are asleep, awake here or awake elsewhere without waking or leasing",
         ctx do
      owner = start_instance(ctx, migrations: @schema)
      other = start_instance(ctx, migrations: @schema)
      asleep = for i <- 1..3, do: "asleep-#{i}"
      awake = for i <- 1..2, do: "awake-#{i}"
      elsewhere = for i <- 1..2, do: "elsewhere-#{i}"

      for cell <- asleep do
        insert(owner, cell, 2)
        :ok = Alambique.hibernate(owner, cell)
      end

      for cell <- awake, do: insert(owner, cell, 3)
      for cell <- elsewhere, do: insert(other, cell, 4)

      reader = reader(ctx)
      cells = asleep ++ awake ++ elsewhere
      leases = Map.new(cells, &{&1, lease(ctx, &1)})
      awake_before = {children(owner), children(other), children(reader)}

      result = Query.all(reader, cells, "SELECT count(*) FROM notes")

      assert result.errors == []
      counts = counts(result)
      assert Map.keys(counts) |> Enum.sort() == Enum.sort(cells)
      assert for(c <- asleep, do: elem(counts[c], 0)) == [2, 2, 2]
      assert for(c <- awake, do: elem(counts[c], 0)) == [3, 3]
      assert for(c <- elsewhere, do: elem(counts[c], 0)) == [4, 4]

      for cell <- cells do
        {_n, txid} = counts[cell]
        assert txid > 0
        assert Enum.all?(result.rows, &(&1.columns == ["count(*)"]))
      end

      assert Map.new(cells, &{&1, lease(ctx, &1)}) == leases
      assert {children(owner), children(other), children(reader)} == awake_before
    end

    test "tags every row with its cell and the TXID it was read at", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 2)
      insert(owner, "b", 1)

      reader = reader(ctx)

      rows =
        Query.all(reader, ["a", "b"], "SELECT id, body FROM notes ORDER BY id", [], ordered: true).rows

      assert [
               %Row{cell: "a", columns: ["id", "body"], values: [1, "a-1"]},
               %Row{cell: "a", values: [2, "a-2"]},
               %Row{cell: "b", values: [1, "b-1"]}
             ] = rows

      [a1, a2, b1] = rows
      assert a1.txid == a2.txid
      assert a1.txid != b1.txid or a1.cell != b1.cell
      assert durable_rows(ctx, "a", "SELECT id FROM notes") == [[1], [2]]
    end

    test "binds parameters and reads at a chosen TXID", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)

      [%Row{txid: first}] =
        Query.all(reader = reader(ctx), ["a"], "SELECT count(*) FROM notes").rows

      insert(owner, "a", 2)

      assert [%Row{values: [3], txid: latest}] =
               Query.all(reader, ["a"], "SELECT count(*) FROM notes").rows

      assert latest > first

      assert [%Row{values: [1], txid: ^first}] =
               Query.all(reader, ["a"], "SELECT count(*) FROM notes WHERE id < ?", [100],
                 at: {:txid, first}
               ).rows
    end

    test "a statement that would write is refused for every cell", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      reader = reader(ctx)

      for sql <- [
            "INSERT INTO notes (body) VALUES ('x')",
            "DELETE FROM notes",
            "DROP TABLE notes",
            "PRAGMA query_only = OFF",
            "ATTACH DATABASE ':memory:' AS m",
            "BEGIN"
          ] do
        assert %{rows: [], errors: [{"a", :read_only}]} = Query.all(reader, ["a"], sql)
      end

      assert [[1]] = durable_rows(ctx, "a", "SELECT count(*) FROM notes")
      assert [%Row{values: [1]}] = Query.all(reader, ["a"], "SELECT count(*) FROM notes").rows
    end

    test "a cell with nothing in the store and an invalid id are errors of their own", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      reader = reader(ctx)

      result = Query.all(reader, ["a", "missing", "bad/id"], "SELECT count(*) FROM notes")
      assert [%Row{cell: "a", values: [1]}] = result.rows
      assert Enum.sort(result.errors) |> Enum.map(&elem(&1, 0)) == ["bad/id", "missing"]
      assert {"bad/id", :invalid_cell} in result.errors
    end

    test "a failing cell does not stop the others", ctx do
      owner = start_instance(ctx, migrations: @schema)
      for cell <- ~w(a b c), do: insert(owner, cell, 1)
      reader = reader(ctx)

      MemoryStore.inject(ctx.store_config, kind: :error, key: ~r{/cells/b/}, ops: [:list, :get])

      result = Query.all(reader, ~w(a b c), "SELECT count(*) FROM notes", [], timeout_ms: 300)
      assert result.rows |> Enum.map(& &1.cell) |> Enum.sort() == ["a", "c"]
      assert [{"b", :injected}] = result.errors
    end

    test "a raw store that raises, exits or throws is an error", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      # Every listing crashes, more often than a breaker would let it be asked.
      reader = reader(ctx, breaker: [failures: 1_000_000])
      {_, config} = ctx.store

      for {kind, expected} <- [raise: :error, exit: :exit, throw: :throw] do
        fault = MemoryStore.inject(config, kind: kind, ops: [:list])

        result =
          Query.all(reader, ["a"], "SELECT count(*) FROM notes", [],
            location: {ctx.store, "cells-test"},
            timeout_ms: 300
          )

        assert [{"a", {:store_crashed, ^expected, _reason}}] = result.errors
        MemoryStore.remove(config, fault)
      end
    end

    test "a slow store ends in a timeout for that cell only", ctx do
      owner = start_instance(ctx, migrations: @schema)
      for cell <- ~w(a b), do: insert(owner, cell, 1)
      reader = reader(ctx)

      MemoryStore.inject(ctx.store_config,
        kind: :timeout,
        ms: 100,
        key: ~r{/cells/b/},
        ops: [:list]
      )

      result = Query.all(reader, ~w(a b), "SELECT count(*) FROM notes", [], timeout_ms: 250)
      assert [%Row{cell: "a"}] = result.rows
      assert [{"b", :timeout}] = result.errors
    end

    test "a statement that runs too long is interrupted", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      reader = reader(ctx)

      sql =
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT count(*) FROM n"

      started = System.monotonic_time(:millisecond)

      assert %{rows: [], errors: [{"a", :timeout}]} =
               Query.all(reader, ["a"], sql, [], timeout_ms: 300)

      assert System.monotonic_time(:millisecond) - started < 3_000
    end
  end

  describe "listing cells" do
    test "lists every cell id with the prefix, in store order", ctx do
      owner = start_instance(ctx, migrations: @schema)
      ids = ["org-1", "org-2", "org-10", "mail.1", "mail-2", "org"]
      for cell <- ids, do: insert(owner, cell, 1)
      reader = reader(ctx)

      assert reader |> Query.cells() |> Enum.sort() == Enum.sort(ids)
      assert reader |> Query.cells(prefix: "org-") |> Enum.sort() == ["org-1", "org-10", "org-2"]

      result = Query.all(reader, {:prefix, "mail"}, "SELECT count(*) FROM notes")
      assert result.rows |> Enum.map(& &1.cell) |> Enum.sort() == ["mail-2", "mail.1"]
      assert Query.all(reader, :all, "SELECT count(*) FROM notes").rows |> length() == 6
    end

    test "costs one request per page and skips the rest of a cell", ctx do
      counting = CountingStore.wrap(ctx.store)
      {_, %{table: _} = cfg} = counting
      owner = start_instance(%{ctx | store: counting}, migrations: @schema)

      for i <- 1..20, do: insert(owner, "small-#{String.pad_leading("#{i}", 2, "0")}", 1)
      big = "zbig"
      insert(owner, big, 30)
      reader = reader(%{ctx | store: counting})

      CountingStore.reset(cfg)
      assert reader |> Query.cells() |> Enum.count() == 21
      # 21 cells have about 100 keys: one page.
      assert CountingStore.count(cfg, :list) == 1

      CountingStore.reset(cfg)
      assert reader |> Query.cells(page_size: 10) |> Enum.count() == 21
      pages = CountingStore.count(cfg, :list)
      {:ok, all_keys, nil} = MemoryStore.list(ctx.store_config, "cells-test/cells/", nil, 10_000)
      assert pages < div(length(all_keys), 10)

      # The big cell has more than 30 keys but is passed in one request.
      CountingStore.reset(cfg)
      assert reader |> Query.cells(prefix: "z", page_size: 5) |> Enum.to_list() == [big]
      assert CountingStore.count(cfg, :list) <= 2
    end
  end

  describe "locations" do
    test "reads the store and prefix it is given, each with its own replicas", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)

      other_pid = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: :other_store))
      other = %{ctx | store: {MemoryStore, MemoryStore.config(other_pid)}}
      other_owner = start_instance(other, migrations: @schema)
      insert(other_owner, "a", 3)
      insert(other_owner, "only-there", 1)

      reader = reader(ctx)
      sql = "SELECT count(*) FROM notes"
      location = {other.store, "cells-test"}

      for _ <- 1..2 do
        assert [%Row{values: [1]}] = Query.all(reader, ["a"], sql).rows
        assert [%Row{values: [3]}] = Query.all(reader, ["a"], sql, [], location: location).rows
      end

      assert Query.cells(reader, location: location) |> Enum.sort() == ["a", "only-there"]
      assert Query.cells(reader) |> Enum.to_list() == ["a"]

      assert %{rows: [%Row{cell: "a"}, %Row{cell: "only-there"}]} =
               Query.all(reader, :all, sql, [], location: location, ordered: true)
    end
  end

  describe "the cache" do
    test "a second run applies only the new segments", ctx do
      counting = CountingStore.wrap(ctx.store)
      {_, cfg} = counting
      ctx = %{ctx | store: counting}
      owner = start_instance(ctx, migrations: @schema)
      cells = for i <- 1..3, do: "c#{i}"
      for cell <- cells, do: insert(owner, cell, 3)
      reader = reader(ctx)
      sql = "SELECT count(*) FROM notes"

      CountingStore.reset(cfg)
      first = Query.all(reader, cells, sql)
      assert first.errors == []
      cold_gets = reads(cfg)
      assert cold_gets >= 3 * 4

      # Nothing changed: only listings, no reads.
      CountingStore.reset(cfg)
      again = Query.all(reader, cells, sql)
      assert counts(again) == counts(first)
      assert reads(cfg) == 0
      assert CountingStore.count(cfg, :put) == 0
      assert CountingStore.count(cfg, :list) == 2 * length(cells)

      insert(owner, "c2", 2)
      CountingStore.reset(cfg)
      third = Query.all(reader, cells, sql)
      assert %{"c1" => {3, t1}, "c2" => {5, t2}, "c3" => {3, t3}} = counts(third)
      assert {t1, t3} == {elem(counts(first)["c1"], 1), elem(counts(first)["c3"], 1)}
      assert t2 > elem(counts(first)["c2"], 1)
      assert reads(cfg) == t2 - elem(counts(first)["c2"], 1)
    end

    test "max_age_ms answers from the cache without asking the store", ctx do
      counting = CountingStore.wrap(ctx.store)
      {_, cfg} = counting
      ctx = %{ctx | store: counting}
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      reader = reader(ctx)
      sql = "SELECT count(*) FROM notes"

      assert [%Row{values: [1]}] = Query.all(reader, ["a"], sql).rows
      insert(owner, "a", 1)

      CountingStore.reset(cfg)
      assert [%Row{values: [1]}] = Query.all(reader, ["a"], sql, [], max_age_ms: 60_000).rows
      assert CountingStore.count(cfg, :list) == 0
      assert [%Row{values: [2]}] = Query.all(reader, ["a"], sql).rows
    end

    test "a point in time is kept next to the latest replica", ctx do
      counting = CountingStore.wrap(ctx.store)
      {_, cfg} = counting
      ctx = %{ctx | store: counting}
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      reader = reader(ctx)
      sql = "SELECT count(*) FROM notes"
      [%Row{txid: old}] = Query.all(reader, ["a"], sql).rows
      insert(owner, "a", 1)
      assert [%Row{values: [2], txid: new}] = Query.all(reader, ["a"], sql).rows
      assert new > old

      CountingStore.reset(cfg)

      assert [%Row{values: [1], txid: ^old}] =
               Query.all(reader, ["a"], sql, [], at: {:txid, old}).rows

      assert [%Row{values: [1], txid: ^old}] =
               Query.all(reader, ["a"], sql, [], at: {:txid, old}).rows
    end

    test "keeps to its size bound and leaves no process or open file behind", ctx do
      owner = start_instance(ctx, migrations: @schema)
      cells = for i <- 1..24, do: "cell-#{String.pad_leading("#{i}", 2, "0")}"
      for cell <- cells, do: insert(owner, cell, 2)

      reader = reader(ctx, query: [cache_bytes: 60_000, concurrency: 4])
      dir = Cache.dir(Alambique.Config.fetch!(reader))
      processes = length(Process.list())

      result = Query.all(reader, cells, "SELECT count(*) FROM notes")
      assert result.errors == []
      assert length(result.rows) == 24

      eventually(fn -> length(Process.list()) <= processes end)
      assert Enum.filter(open_files(), &String.starts_with?(&1, dir)) == []
      sizes = for f <- Path.wildcard(Path.join(dir, "*/*")), do: File.stat!(f).size
      assert sizes != []
      assert Enum.sum(sizes) <= 60_000
      assert length(sizes) < 24
      assert children(reader) == 0

      # An evicted replica is rebuilt on the next run.
      assert Query.all(reader, cells, "SELECT count(*) FROM notes").errors == []
    end

    test "evicts the least recently used replica first", ctx do
      owner = start_instance(ctx, migrations: @schema)
      for cell <- ~w(old new), do: insert(owner, cell, 1)
      reader = reader(ctx)
      config = Alambique.Config.fetch!(reader)
      dir = Cache.dir(config)

      Query.all(reader, ["old", "new"], "SELECT 1")
      scope = :erlang.phash2({ctx.store, "cells-test"}) |> Integer.to_string(16)
      [old] = Cache.entries(dir, Cache.key("old", scope))
      [new] = Cache.entries(dir, Cache.key("new", scope))
      File.touch!(old.path, {{2020, 1, 1}, {0, 0, 0}})
      size = File.stat!(new.path).size

      assert {1, ^size} = Cache.trim(dir, size, 600)
      assert Cache.entries(dir, Cache.key("old", scope)) == []
      assert [^new] = Cache.entries(dir, Cache.key("new", scope))
    end

    test "leftover temporary files are removed once they are old", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      reader = reader(ctx)
      dir = Cache.dir(Alambique.Config.fetch!(reader))
      Query.all(reader, ["a"], "SELECT 1")

      {:ok, fresh} = Cache.tmp(dir, "a")
      {:ok, stale} = Cache.tmp(dir, "a")
      File.write!(fresh, "x")
      File.write!(stale, "x")
      File.touch!(stale, {{2020, 1, 1}, {0, 0, 0}})

      Cache.trim(dir, 1_000_000_000, 600)
      assert File.exists?(fresh)
      refute File.exists?(stale)
    end
  end

  describe "faults while building a replica" do
    defp full_dump(path) do
      {:ok, conn} = Alambique.Sqlite.open(path, readonly: true)

      {:ok, %{rows: rows}} =
        Alambique.Sqlite.run(conn, "SELECT id, body FROM notes ORDER BY id", [])

      Alambique.Sqlite.close(conn)
      rows
    end

    defp restored_dump(ctx, cell, txid) do
      repl =
        Alambique.Replication.ctx(
          store: ctx.store,
          prefix: "cells-test",
          node: "verifier",
          timeout_ms: 5_000
        )

      path = Path.join(ctx.dir, "verify-#{System.unique_integer([:positive])}.db")
      {:ok, %{txid: ^txid}} = Alambique.Replication.restore(repl, cell, path, {:txid, txid})
      full_dump(path)
    end

    defp assert_cache_consistent(ctx, reader, cells) do
      dir = Cache.dir(Alambique.Config.fetch!(reader))

      for cell <- cells, %{txid: txid, path: path} <- Cache.entries(dir, cell) do
        assert full_dump(path) == restored_dump(ctx, cell, txid)
      end
    end

    for point <- [:query_refresh_segment, :query_refresh_applied, :repl_mid_restore] do
      @point point
      test "a crash at #{point} leaves no half-applied replica", ctx do
        owner = start_instance(ctx, migrations: @schema)
        cells = ~w(a b c)
        for cell <- cells, do: insert(owner, cell, 3)
        reader = reader(ctx)
        sql = "SELECT count(*) FROM notes"

        # Cold: a restore. Warm: a refresh.
        warm? = @point != :repl_mid_restore

        if warm? do
          assert Query.all(reader, cells, sql).errors == []
          for cell <- cells, do: insert(owner, cell, 2)
        end

        for action <- [:raise, :kill] do
          CrashPoints.arm(@point, action, times: 2)
          result = Query.all(reader, cells, sql)
          CrashPoints.reset()
          assert length(result.errors) >= 1
          assert_cache_consistent(ctx, reader, cells)
        end

        final = Query.all(reader, cells, sql)
        assert final.errors == []
        assert Enum.all?(final.rows, &(&1.values == [if(warm?, do: 5, else: 3)]))
        assert_cache_consistent(ctx, reader, cells)
        dir = Cache.dir(Alambique.Config.fetch!(reader))
        assert Path.wildcard(Path.join(dir, "*/.tmp-*")) |> Enum.all?(&(File.stat!(&1).size >= 0))
      end
    end

    test "a store error during a refresh keeps the previous replica", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 2)
      reader = reader(ctx)
      sql = "SELECT count(*) FROM notes"
      assert [%Row{values: [2], txid: old}] = Query.all(reader, ["a"], sql).rows
      insert(owner, "a", 1)

      id = MemoryStore.inject(ctx.store_config, kind: :error, key: ~r{/0000/}, ops: [:get])

      assert %{rows: [], errors: [{"a", :injected}]} =
               Query.all(reader, ["a"], sql, [], timeout_ms: 200)

      MemoryStore.remove(ctx.store_config, id)

      assert [%Row{values: [2], txid: ^old}] =
               Query.all(reader, ["a"], sql, [], max_age_ms: 60_000).rows

      assert [%Row{values: [3]}] = Query.all(reader, ["a"], sql).rows
    end
  end

  describe "compaction, retention and freezing" do
    defp ltx_keys(ctx, cell) do
      {m, c} = ctx.store
      {:ok, keys, nil} = m.list(c, "cells-test/cells/#{cell}/", nil, 10_000)
      Enum.filter(keys, &String.contains?(&1, "/0000/"))
    end

    defp level_keys(ctx, cell, level) do
      {m, c} = ctx.store
      {:ok, keys, nil} = m.list(c, "cells-test/cells/#{cell}/#{level}/", nil, 10_000)
      keys
    end

    test "a frozen cell is still read, and stays asleep", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 2)
      :ok = Alambique.freeze(owner, "a")
      reader = reader(ctx)

      assert {:error, :frozen} = Alambique.query(owner, "a", "SELECT 1")
      assert [%Row{values: [2]}] = Query.all(reader, ["a"], "SELECT count(*) FROM notes").rows
      assert children(owner) == 0
    end

    test "reads on after compaction and retention removed older files", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 6)
      :ok = Alambique.hibernate(owner, "a")
      reader = reader(ctx)
      sql = "SELECT count(*) FROM notes"
      assert [%Row{values: [6]}] = Query.all(reader, ["a"], sql).rows

      insert(owner, "a", 4)
      :ok = Alambique.hibernate(owner, "a")
      repl = repl_ctx(owner, "a")
      before = length(ltx_keys(ctx, "a"))

      assert {:ok, %{snapshot: snapshot}} =
               Alambique.Compaction.run(repl, "a", fanout: 2, snapshot_interval_txids: 1)

      assert snapshot
      Process.sleep(1_100)

      assert {:ok, _} =
               Alambique.Retention.run(repl, "a",
                 pitr_window_ms: 0,
                 grace_ms: 0,
                 deletion_grace_ms: 0
               )

      assert length(ltx_keys(ctx, "a")) < before + 1

      # The cached replica is older than the snapshot: rebuilt, not patched.
      assert [%Row{values: [10]}] = Query.all(reader, ["a"], sql).rows

      File.rm_rf!(Cache.dir(Alambique.Config.fetch!(reader)))
      assert [%Row{values: [10]}] = Query.all(reader, ["a"], sql).rows
    end

    test "reads a cell compacted to level 2 from a fresh data directory", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 70)
      :ok = Alambique.hibernate(owner, "a")
      assert {:ok, _} = Alambique.Compaction.run(repl_ctx(owner, "a"), "a")
      assert Enum.any?(level_keys(ctx, "a", "0002"), &String.contains?(&1, "/0000000000000001-"))

      reader = reader(ctx)
      assert %{errors: [], rows: [%Row{values: [70]}]} = Query.all(reader, ["a"], @count)
    end

    test "a trim while replicas are being built does not break them", ctx do
      owner = start_instance(ctx, migrations: @schema)
      cells = for i <- 1..30, do: "c#{i}"
      for c <- cells, do: insert(owner, c, 20)
      reader = reader(ctx)
      dir = Cache.dir(Alambique.Config.fetch!(reader))

      trimmers =
        for _ <- 1..4 do
          Task.async(fn ->
            Enum.each(1..300, fn _ -> Cache.trim(dir, 1_000_000_000, 600) end)
          end)
        end

      result = Query.all(reader, cells, @count)
      Task.await_many(trimmers)
      assert result.errors == []
      assert length(result.rows) == 30
    end

    test "a trim leaves the directory of a temporary file", ctx do
      dir = Cache.dir(Alambique.Config.fetch!(start_instance(ctx, migrations: @schema)))
      {:ok, tmp} = Cache.tmp(dir, "a@1")
      assert {0, _} = Cache.trim(dir, 1_000_000_000, 600)
      assert File.exists?(tmp)
    end

    # Holds a restore of "a" after its first file, compacts the cell and lets
    # retention delete the files the restore listed, then lets it go on.
    defp restore_losing_files(ctx, reader_opts) do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 6)
      :ok = Alambique.hibernate(owner, "a")
      # Reading ahead would fetch the files before the pause, and then none is lost.
      reader =
        reader(ctx, [restore: [max_inflight_bytes: 1, fetch_concurrency: 1]] ++ reader_opts)

      parent = self()

      CrashPoints.arm(
        :repl_mid_restore,
        fn _, _ ->
          send(parent, {:held, self()})
          receive do: (:go -> :ok)
        end,
        times: 1
      )

      task = Task.async(fn -> Query.all(reader, ["a"], @count) end)
      assert_receive {:held, held}, 5_000
      repl = repl_ctx(owner, "a")

      assert {:ok, %{snapshot: snapshot}} =
               Alambique.Compaction.run(repl, "a", fanout: 2, snapshot_interval_txids: 1)

      assert snapshot
      Process.sleep(1_100)

      assert {:ok, _} =
               Alambique.Retention.run(repl, "a",
                 pitr_window_ms: 0,
                 grace_ms: 0,
                 deletion_grace_ms: 0
               )

      send(held, :go)
      Task.await(task, 5_000)
    end

    test "a restore that loses files to compaction and retention starts over", ctx do
      assert %{errors: [], rows: [%Row{values: [6]}]} = restore_losing_files(ctx, [])
    end

    test "restore_attempts bounds the restarts and the error is :not_found", ctx do
      assert %{rows: [], errors: [{"a", :not_found}]} =
               restore_losing_files(ctx, query: [restore_attempts: 1])
    end

    test "missing history is an error of that cell, cold or warm", ctx do
      owner = start_instance(ctx, migrations: @schema)
      for cell <- ~w(a b), do: insert(owner, cell, 4)
      for cell <- ~w(a b), do: :ok = Alambique.hibernate(owner, cell)
      reader = reader(ctx)
      sql = "SELECT count(*) FROM notes"
      assert Query.all(reader, ["a"], sql).errors == []

      # More than the tail window of files follows the hole (design §6, R3).
      for cell <- ~w(a b) do
        insert(owner, cell, 18)
        :ok = Alambique.hibernate(owner, cell)
        {m, c} = ctx.store
        keys = ltx_keys(ctx, cell)
        :ok = m.delete(c, Enum.at(keys, length(keys) - 18))
      end

      result = Query.all(reader, ["a", "b"], sql, [], ordered: true)
      assert result.rows == []
      assert [{"a", {:gap, _}}, {"b", {:gap, _}}] = result.errors

      # The previous replica is still there for an older point.
      assert [%Row{values: [4]}] = Query.all(reader, ["a"], sql, [], max_age_ms: 60_000).rows
    end
  end

  describe "a tail past a gap (design §6, R3)" do
    test "a refresh keeps the replica instead of restoring again", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 3)
      :ok = Alambique.hibernate(owner, "a")
      reader = reader(ctx)
      assert [%Row{values: [3]}] = Query.all(reader, ["a"], @count).rows

      # The owner went on, but the first of its data files is missing: what is
      # left is a tail, so the replica advances to the prefix (the fence).
      insert(owner, "a", 3)
      :ok = Alambique.hibernate(owner, "a")
      {m, c} = ctx.store
      keys = ltx_keys(ctx, "a")
      :ok = m.delete(c, Enum.at(keys, length(keys) - 3))

      capture([[:alambique, :query, :replica]])
      assert [%Row{values: [3]}] = Query.all(reader, ["a"], @count).rows

      assert_received {:telemetry, [:alambique, :query, :replica], _,
                       %{cell: "a", source: :refresh, segments: 1, result: :ok}}

      # A cold reader restores the prefix too.
      assert [%Row{values: [3]}] = Query.all(reader(ctx), ["a"], @count).rows
    end
  end

  describe "a tail right after the replica (design §6, R3)" do
    test "leaves it unchanged", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 3)
      reader = reader(ctx)
      assert [%Row{values: [3]}] = Query.all(reader, ["a"], @count).rows

      insert(owner, "a", 3)
      {m, c} = ctx.store
      keys = ltx_keys(ctx, "a")
      :ok = m.delete(c, Enum.at(keys, length(keys) - 3))

      capture([[:alambique, :query, :replica]])
      assert [%Row{values: [3]}] = Query.all(reader, ["a"], @count).rows

      assert_received {:telemetry, [:alambique, :query, :replica], _,
                       %{cell: "a", source: :unchanged, result: :ok}}
    end
  end

  describe "a zombie after the replica (design §6, R3)" do
    test "ends the refresh at the fence", ctx do
      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 3)
      :ok = Alambique.hibernate(owner, "a")
      reader = reader(ctx)
      assert [%Row{values: [3]}] = Query.all(reader, ["a"], @count).rows

      # The wake fences; an owner of epoch 0 left a file right after it.
      {:ok, _} = Alambique.touch(owner, "a")
      {:ok, %{txid: fence}} = Alambique.position(owner, "a")
      {m, c} = ctx.store
      zombie = Keys.segment("cells-test", "a", 0, fence + 1, fence + 1)
      {:ok, _} = m.put(c, zombie, "x", :if_none_match, %{"epoch" => "0"})

      capture([[:alambique, :query, :replica]])
      assert [%Row{values: [3], txid: ^fence}] = Query.all(reader, ["a"], @count).rows

      assert_received {:telemetry, [:alambique, :query, :replica], _,
                       %{cell: "a", source: :refresh, segments: 2, result: :ok}}
    end
  end

  describe "telemetry" do
    test "reports the list, each replica and each cell", ctx do
      capture([
        [:alambique, :query, :list],
        [:alambique, :query, :replica],
        [:alambique, :query, :cell],
        [:alambique, :query, :trim]
      ])

      owner = start_instance(ctx, migrations: @schema)
      insert(owner, "a", 1)
      reader = reader(ctx)

      Query.all(reader, :all, "SELECT count(*) FROM notes")
      assert_received {:telemetry, [:alambique, :query, :list], %{cells: 1}, %{result: :ok}}

      assert_received {:telemetry, [:alambique, :query, :replica], _,
                       %{cell: "a", source: :restore, result: :ok}}

      assert_received {:telemetry, [:alambique, :query, :cell], %{rows: 1},
                       %{cell: "a", result: :ok, txid: txid}}

      assert txid > 0
      assert_received {:telemetry, [:alambique, :query, :trim], %{deleted: 0}, %{}}

      insert(owner, "a", 1)
      Query.all(reader, ["a"], "SELECT count(*) FROM notes")

      assert_received {:telemetry, [:alambique, :query, :replica], %{},
                       %{cell: "a", source: :refresh, segments: 1}}
    end
  end
end
