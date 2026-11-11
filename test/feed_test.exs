defmodule Alambique.FeedTest do
  @moduledoc "Capture, publication and reading of the change feed (design §13)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{CountingStore, Feed, Tx}
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @schema [
    {1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"},
    {2, "CREATE TABLE loose (a TEXT, b INTEGER)"},
    {3, "CREATE TABLE pairs (a TEXT, b INTEGER, v TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID"}
  ]

  setup do
    capture([[:alambique, :feed, :publish]])
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  defp exec(inst, cell, sql, params \\ []),
    do: {:ok, _} = Alambique.execute(inst, cell, sql, params)

  defp feed_ctx(%{store: store}, opts \\ []),
    do: Feed.ctx(Keyword.merge([store: store, prefix: "cells-test", settle_ms: 0], opts))

  test "a store that never answers ends a read and a prune at timeout_ms", ctx do
    MemoryStore.inject(ctx.store_config, kind: :latency, ms: 60_000, ops: [:list, :get, :delete])
    feed = feed_ctx(ctx, timeout_ms: 200)
    started = System.monotonic_time(:millisecond)

    assert {:error, :timeout} = Feed.read(feed, nil, 10)
    assert {:error, :timeout} = Feed.prune(feed, 1_000)
    assert System.monotonic_time(:millisecond) - started < 1_500
  end

  # Publication follows the answer to the caller, so tests wait for it.
  defp records(ctx, count, cell \\ nil) do
    eventually(fn ->
      {:ok, records, _} = Feed.read(feed_ctx(ctx), nil, 1000)
      records = if cell, do: Enum.filter(records, &(&1.cell == cell)), else: records
      if length(records) >= count, do: records
    end)
  end

  defp summary(records), do: for(r <- records, do: {r.table, r.op, r.key, r.values})

  describe "capture" do
    test "inserts, updates and deletes on a table with a primary key", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO notes (id, body) VALUES (1, 'a')")
      exec(inst, cell, "UPDATE notes SET body = 'b' WHERE id = 1")
      exec(inst, cell, "DELETE FROM notes WHERE id = 1")

      assert summary(records(ctx, 3)) == [
               {"notes", :insert, %{"id" => 1}, %{"id" => 1, "body" => "a"}},
               {"notes", :update, %{"id" => 1}, %{"id" => 1, "body" => "b"}},
               {"notes", :delete, %{"id" => 1}, nil}
             ]
    end

    test "a table without a primary key is keyed by its rowid", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO loose VALUES ('x', 1)")
      exec(inst, cell, "UPDATE loose SET b = 2")
      exec(inst, cell, "DELETE FROM loose")

      assert summary(records(ctx, 3)) == [
               {"loose", :insert, %{"rowid" => 1}, %{"a" => "x", "b" => 1}},
               {"loose", :update, %{"rowid" => 1}, %{"a" => "x", "b" => 2}},
               {"loose", :delete, %{"rowid" => 1}, nil}
             ]
    end

    test "a WITHOUT ROWID table is keyed by its composite primary key", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO pairs VALUES ('x', 1, 'v')")
      exec(inst, cell, "UPDATE pairs SET v = 'w'")

      assert summary(records(ctx, 2)) == [
               {"pairs", :insert, %{"a" => "x", "b" => 1}, %{"a" => "x", "b" => 1, "v" => "v"}},
               {"pairs", :update, %{"a" => "x", "b" => 1}, %{"a" => "x", "b" => 1, "v" => "w"}}
             ]
    end

    test "changing a primary key is a delete of the old key and an insert of the new", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      exec(inst, cell, "UPDATE notes SET id = 2")

      assert [_, delete, insert] = summary(records(ctx, 3))
      assert delete == {"notes", :delete, %{"id" => 1}, nil}
      assert insert == {"notes", :insert, %{"id" => 2}, %{"id" => 2, "body" => "a"}}
    end

    test "REPLACE publishes the removal of the row it replaces", ctx do
      inst =
        start_instance(ctx,
          migrations: [{1, "CREATE TABLE u (id INTEGER PRIMARY KEY, k TEXT UNIQUE)"}]
        )

      cell = unique_cell()
      exec(inst, cell, "INSERT INTO u VALUES (1, 'k')")
      exec(inst, cell, "INSERT OR REPLACE INTO u VALUES (2, 'k')")

      assert [_, delete, insert] = summary(records(ctx, 3))
      assert delete == {"u", :delete, %{"id" => 1}, nil}
      assert {"u", :insert, %{"id" => 2}, _} = insert
    end

    test "a transaction with several rows is one object in commit order", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      {:ok, txid} =
        Alambique.transaction(inst, cell, fn tx ->
          for id <- 1..5, do: {:ok, _} = Tx.execute(tx, "INSERT INTO notes VALUES (?, 'x')", [id])
          {:ok, _} = Tx.execute(tx, "DELETE FROM notes WHERE id = 3")
          Tx.txid(tx)
        end)

      records = records(ctx, 6)
      assert Enum.map(records, & &1.position) == for(n <- 0..5, do: {txid, n})
      assert Enum.map(records, & &1.op) == [:insert, :insert, :insert, :insert, :insert, :delete]
      assert Enum.map(records, & &1.id) == for(n <- 0..5, do: "#{cell}:#{txid}:#{n}")
      assert Enum.uniq_by(records, & &1.at) |> length() == 1
    end

    test "a write that was not stored never reaches the feed", ctx do
      inst = start_instance(ctx, migrations: @schema, op_timeout_ms: 300)
      cell = unique_cell()
      {_, config} = ctx.store
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      [_] = records(ctx, 1)

      fault = MemoryStore.inject(config, kind: :error, ops: [:put], key: ~r{/0000/})
      assert {:error, _} = Alambique.execute(inst, cell, "INSERT INTO notes VALUES (2, 'lost')")
      MemoryStore.remove(config, fault)

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT id FROM notes")
      exec(inst, cell, "INSERT INTO notes VALUES (3, 'c')")
      assert [1, 3] = ctx |> records(2) |> Enum.map(& &1.key["id"])
    end

    test "a rolled back transaction produces no records", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      assert {:error, :nope} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO notes VALUES (1, 'lost')")
                 Tx.rollback(tx, :nope)
               end)

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      assert {:error, _} = Alambique.execute(inst, cell, "INSERT INTO notes VALUES (1, 'dup')")

      assert [record] = records(ctx, 1)
      assert record.values == %{"id" => 1, "body" => "a"}
      Process.sleep(200)
      assert [_] = records(ctx, 0)
    end

    test "values keep their types, including blobs, unicode and quotes", ctx do
      inst =
        start_instance(ctx,
          migrations: [{1, "CREATE TABLE v (id INTEGER PRIMARY KEY, a, b, c, d, e)"}]
        )

      cell = unique_cell()
      text = ~s(ünï "quoted" 'single' \\ \n)

      exec(inst, cell, "INSERT INTO v VALUES (1, ?, ?, ?, ?, ?)", [
        text,
        1.5,
        nil,
        {:blob, <<1, 2, 255>>},
        9_007_199_254_740_993
      ])

      assert [%{values: values}] = records(ctx, 1)

      assert values == %{
               "id" => 1,
               "a" => text,
               "b" => 1.5,
               "c" => nil,
               "d" => %{"$blob" => "0102ff"},
               "e" => 9_007_199_254_740_993
             }
    end

    test "internal tables and SQLite's own are never captured", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      for id <- 1..3, do: exec(inst, cell, "INSERT INTO notes VALUES (?, 'x')", [id])
      exec(inst, cell, "DELETE FROM notes")
      Alambique.hibernate(inst, cell)
      exec(inst, cell, "INSERT INTO notes VALUES (9, 'x')")

      records = records(ctx, 5)
      assert records |> Enum.map(& &1.table) |> Enum.uniq() == ["notes"]
    end

    test "a cell without tables has no feed tables and no feed objects", ctx do
      inst = start_instance(ctx)
      cell = unique_cell()

      assert {:ok, %{rows: [[0]]}} =
               Alambique.query(inst, cell, "SELECT count(*) FROM sqlite_schema")

      assert feed_keys(ctx) == []
    end

    test "feed: false captures nothing, publishes nothing and still rewinds", ctx do
      inst = start_instance(ctx, migrations: @schema, feed: false)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'x')")
      {:ok, %{txid: before}} = Alambique.position(inst, cell)
      exec(inst, cell, "UPDATE notes SET body = 'y'")
      assert {:ok, _} = Alambique.rewind(inst, cell, {:txid, before})
      assert {:ok, %{rows: [["x"]]}} = Alambique.query(inst, cell, "SELECT body FROM notes")
      Alambique.hibernate(inst, cell)

      sql = "SELECT count(*) FROM sqlite_schema WHERE name LIKE '%alambique_feed%'"
      assert {:ok, %{rows: [[0]]}} = Alambique.query(inst, cell, sql)
      assert feed_keys(ctx) == []
      assert {:ok, [], _} = Feed.read(feed_ctx(ctx), nil, 100)
    end

    test "feed: false removes the triggers of a cell that had them", ctx do
      cell = unique_cell()
      on = start_instance(ctx, migrations: @schema)
      exec(on, cell, "INSERT INTO notes VALUES (1, 'x')")
      records(ctx, 1)
      :ok = Alambique.hibernate(on, cell)
      stop_supervised!(on)

      off = start_instance(ctx, migrations: @schema, feed: false, name: on)
      exec(off, cell, "INSERT INTO notes VALUES (2, 'y')")
      Alambique.hibernate(off, cell)
      assert [_] = feed_keys(ctx)
    end

    test "batches without row changes produce no feed object", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      assert {:ok, %{rows: []}} = Alambique.query(inst, cell, "SELECT * FROM notes")
      assert {:ok, :done} = Alambique.transaction(inst, cell, fn _ -> :done end)
      Alambique.hibernate(inst, cell)
      assert {:ok, %{rows: []}} = Alambique.query(inst, cell, "SELECT * FROM notes")
      assert feed_keys(ctx) == []
    end
  end

  defp shard_prefixes,
    do: Enum.map(0..15, &Alambique.Feed.Keys.prefix("cells-test", &1)) |> Enum.sort()

  defp feed_keys(%{store: {mod, config}}) do
    {:ok, keys, _} = mod.list(config, "cells-test/feed/", nil, 1000)
    keys
  end

  defp flush_installs do
    receive do
      {:telemetry, [:alambique, :feed, :install], _, _} -> flush_installs()
    after
      0 -> :ok
    end
  end

  describe "schema changes" do
    test "the triggers are generated once, and again only when the schema changes", ctx do
      capture([[:alambique, :feed, :install]])
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      assert_received {:telemetry, [:alambique, :feed, :install], %{tables: 3}, _}
      flush_installs()
      :ok = Alambique.hibernate(inst, cell)
      File.rm_rf!(ctx.dir)
      exec(inst, cell, "INSERT INTO notes VALUES (2, 'b')")
      refute_received {:telemetry, [:alambique, :feed, :install], _, _}

      exec(inst, cell, "CREATE TABLE extra (id INTEGER PRIMARY KEY)")
      assert_received {:telemetry, [:alambique, :feed, :install], %{tables: 4}, _}
    end

    test "tables from a later migration are captured, with the rows the migration writes", ctx do
      cell = unique_cell()
      first = start_instance(ctx, migrations: Enum.take(@schema, 1))
      exec(first, cell, "INSERT INTO notes VALUES (1, 'a')")
      :ok = Alambique.hibernate(first, cell)

      second =
        start_instance(ctx,
          migrations:
            [
              {2, "CREATE TABLE later (id INTEGER PRIMARY KEY); INSERT INTO later VALUES (100)"}
            ]
            |> then(&(Enum.take(@schema, 1) ++ &1))
        )

      exec(second, cell, "INSERT INTO later VALUES (1)")

      # The row the migration inserted is on the feed too, once.
      assert summary(records(ctx, 3)) == [
               {"notes", :insert, %{"id" => 1}, %{"id" => 1, "body" => "a"}},
               {"later", :insert, %{"id" => 100}, %{"id" => 100}},
               {"later", :insert, %{"id" => 1}, %{"id" => 1}}
             ]
    end

    test "a migration's inserts, updates and backfills reach the feed exactly once", ctx do
      migrations = [
        {1,
         "CREATE TABLE seeds (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO seeds VALUES (1, 'a'); INSERT INTO seeds VALUES (2, 'b;c');"},
        {2,
         fn tx ->
           {:ok, _} = Tx.execute(tx, "ALTER TABLE seeds ADD COLUMN flag INTEGER")
           {:ok, _} = Tx.execute(tx, "UPDATE seeds SET flag = 1")
         end},
        {3, "ALTER TABLE seeds DROP COLUMN name; INSERT INTO seeds (id, flag) VALUES (3, 0)"}
      ]

      inst = start_instance(ctx, migrations: migrations)
      cell = unique_cell()
      assert {:ok, %{rows: [[3]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM seeds")

      assert summary(records(ctx, 5)) == [
               {"seeds", :insert, %{"id" => 1}, %{"id" => 1, "name" => "a"}},
               {"seeds", :insert, %{"id" => 2}, %{"id" => 2, "name" => "b;c"}},
               {"seeds", :update, %{"id" => 1}, %{"id" => 1, "name" => "a", "flag" => 1}},
               {"seeds", :update, %{"id" => 2}, %{"id" => 2, "name" => "b;c", "flag" => 1}},
               {"seeds", :insert, %{"id" => 3}, %{"id" => 3, "flag" => 0}}
             ]
    end

    test "a table created inside a transaction is captured in the same transaction", ctx do
      inst = start_instance(ctx)
      cell = unique_cell()

      {:ok, _} =
        Alambique.transaction(inst, cell, fn tx ->
          {:ok, _} = Tx.execute(tx, "CREATE TABLE made (id INTEGER PRIMARY KEY, x)")
          {:ok, _} = Tx.execute(tx, "INSERT INTO made VALUES (1, 'a')")
        end)

      exec(inst, cell, "UPDATE made SET x = 'b'")

      assert summary(records(ctx, 2)) == [
               {"made", :insert, %{"id" => 1}, %{"id" => 1, "x" => "a"}},
               {"made", :update, %{"id" => 1}, %{"id" => 1, "x" => "b"}}
             ]
    end

    test "columns can be added and dropped, and tables renamed and dropped", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      exec(inst, cell, "ALTER TABLE notes ADD COLUMN extra TEXT")
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a', 'e')")
      exec(inst, cell, "ALTER TABLE notes DROP COLUMN body")
      exec(inst, cell, "UPDATE notes SET extra = 'f'")
      exec(inst, cell, "ALTER TABLE notes RENAME TO renamed")
      exec(inst, cell, "UPDATE renamed SET extra = 'g'")
      exec(inst, cell, "DROP TABLE renamed")

      assert summary(records(ctx, 3)) == [
               {"notes", :insert, %{"id" => 1}, %{"id" => 1, "body" => "a", "extra" => "e"}},
               {"notes", :update, %{"id" => 1}, %{"id" => 1, "extra" => "f"}},
               {"renamed", :update, %{"id" => 1}, %{"id" => 1, "extra" => "g"}}
             ]
    end

    test "a rolled back DDL leaves capture as it was", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      assert {:error, :undo} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Tx.execute(tx, "CREATE TABLE ghost (id INTEGER PRIMARY KEY)")
                 {:ok, _} = Tx.execute(tx, "INSERT INTO ghost VALUES (1)")
                 Tx.rollback(tx, :undo)
               end)

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      assert [%{table: "notes"}] = records(ctx, 1)
    end
  end

  describe "Tx.script/2" do
    @script """
    CREATE TABLE made (id INTEGER PRIMARY KEY, x);
    CREATE INDEX made_x ON made (x);
    -- a trigger body keeps its semicolons
    CREATE TABLE audit (n);
    CREATE TRIGGER made_t AFTER INSERT ON made BEGIN UPDATE audit SET n = n + 1; END;
    """

    defp script(inst, cell, sql, then \\ fn _tx -> :ok end) do
      Alambique.transaction(inst, cell, fn tx ->
        with :ok <- Tx.script(tx, sql), do: then.(tx)
      end)
    end

    test "creates the schema with one capture cycle and captures at once", ctx do
      capture([[:alambique, :feed, :install]])
      inst = start_instance(ctx)
      cell = unique_cell()

      assert {:ok, :ok} =
               script(inst, cell, @script, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO made VALUES (1, 'a')")
                 :ok
               end)

      assert_received {:telemetry, [:alambique, :feed, :install], %{tables: 2}, _}
      refute_received {:telemetry, [:alambique, :feed, :install], _, _}

      exec(inst, cell, "UPDATE made SET x = 'b'")

      assert summary(records(ctx, 2) |> Enum.filter(&(&1.table == "made"))) == [
               {"made", :insert, %{"id" => 1}, %{"id" => 1, "x" => "a"}},
               {"made", :update, %{"id" => 1}, %{"id" => 1, "x" => "b"}}
             ]
    end

    test "is atomic: a failing statement leaves nothing of the script", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      assert {:ok, {:error, _}} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Tx.execute(tx, "CREATE TABLE before_script (id)")

                 Tx.script(
                   tx,
                   "CREATE TABLE one (id); CREATE TABLE notes (id); CREATE TABLE two (id)"
                 )
               end)

      assert {:ok, %{rows: [["before_script"], ["loose"], ["notes"], ["pairs"]]}} =
               Alambique.query(
                 inst,
                 cell,
                 "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY name"
               )

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      assert [%{table: "notes"}] = records(ctx, 1)
    end

    test "refuses statements that are not schema changes before running any", ctx do
      inst = start_instance(ctx)
      cell = unique_cell()

      assert {:ok, {:error, {:not_ddl, "INSERT INTO t VALUES (1);"}}} =
               script(inst, cell, "CREATE TABLE t (id); INSERT INTO t VALUES (1);")

      assert {:ok, %{rows: [[0]]}} =
               Alambique.query(inst, cell, "SELECT count(*) FROM sqlite_schema")
    end

    test "a script that ends open leaves no savepoint, and comment-only fragments are ignored",
         ctx do
      inst = start_instance(ctx)

      # An open comment is valid SQL and ignored; an open string and an
      # incomplete statement are errors. Each case uses a fresh cell.
      for {sql, ok?} <- [
            {"CREATE TABLE t (id) /* open", true},
            {"CREATE TABLE t (id); -- open", true},
            {"CREATE TABLE t (id); /* open", true},
            {"CREATE TABLE u ('open", false},
            {"CREATE TABLE u (", false}
          ] do
        assert {:ok, {outcome, {:error, "no such savepoint" <> _}}} =
                 Alambique.transaction(inst, unique_cell(), fn tx ->
                   {Tx.script(tx, sql), Tx.execute(tx, "ROLLBACK TO alambique_script")}
                 end),
               sql

        assert outcome == :ok == ok?, sql
      end
    end

    test "an empty script is a no-op, and a stale handle is refused", ctx do
      inst = start_instance(ctx)
      cell = unique_cell()

      assert {:ok, tx} = Alambique.transaction(inst, cell, fn tx -> tx end)
      assert {:ok, :ok} = script(inst, cell, " ; -- nothing\n")
      assert {:error, :inactive_transaction} = Tx.script(tx, "CREATE TABLE t (id)")
    end
  end

  describe "internal prefixes" do
    @host [
      {1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"},
      {2, "CREATE TABLE _host_jobs (id INTEGER PRIMARY KEY, state TEXT)"}
    ]

    test "host tables stay out of the feed, other tables do not", ctx do
      inst = start_instance(ctx, migrations: @host, feed: [internal_prefixes: ["_host_"]])
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO _host_jobs VALUES (1, 'new')")
      exec(inst, cell, "UPDATE _host_jobs SET state = 'done'")
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")

      assert [%{table: "notes"}] = records(ctx, 1)

      assert {:ok, %{rows: [["done"]]}} =
               Alambique.query(inst, cell, "SELECT state FROM _host_jobs")

      {:ok, :ok} =
        Alambique.transaction(inst, cell, fn tx ->
          Tx.script(tx, "CREATE TABLE _host_alarms (id); CREATE TABLE tags (id)")
        end)

      exec(inst, cell, "INSERT INTO _host_alarms VALUES (1)")
      exec(inst, cell, "INSERT INTO tags VALUES (1)")
      assert ["notes", "tags"] = ctx |> records(2) |> Enum.map(& &1.table)
    end

    test "changing the prefixes regenerates the triggers at the next wake", ctx do
      cell = unique_cell()
      inst = start_instance(ctx, migrations: @host, name: :first)
      exec(inst, cell, "INSERT INTO _host_jobs VALUES (1, 'new')")
      assert [%{table: "_host_jobs"}] = records(ctx, 1)
      stop_supervised!(inst)

      inst = start_instance(ctx, migrations: @host, feed: [internal_prefixes: ["_host_"]])
      exec(inst, cell, "INSERT INTO _host_jobs VALUES (2, 'new')")
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      assert ["_host_jobs", "notes"] = ctx |> records(2) |> Enum.map(& &1.table)
    end

    test "a cell whose only tables became internal loses its triggers", ctx do
      jobs = [{1, "CREATE TABLE _host_jobs (id INTEGER PRIMARY KEY)"}]
      cell = unique_cell()
      inst = start_instance(ctx, migrations: jobs, name: :first)
      exec(inst, cell, "INSERT INTO _host_jobs VALUES (1)")
      assert [%{table: "_host_jobs"}] = records(ctx, 1)
      stop_supervised!(inst)

      inst = start_instance(ctx, migrations: jobs, feed: [internal_prefixes: ["_host_"]])

      # The row of the first instance goes away with the first write after the
      # wake has published it again.
      {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)
      eventually(fn -> :sys.get_state(pid).feed_published > 0 end)
      exec(inst, cell, "INSERT INTO _host_jobs VALUES (2)")

      assert {:ok, %{rows: [[0]]}} =
               Alambique.query(inst, cell, "SELECT count(*) FROM _alambique_feed")
    end

    test "a function migration can create its schema with a script", ctx do
      migration =
        {1, fn tx -> :ok = Tx.script(tx, "CREATE TABLE a (id); CREATE TABLE b (id)") end}

      inst = start_instance(ctx, migrations: [migration])
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO b VALUES (1)")
      assert [%{table: "b"}] = records(ctx, 1)
    end
  end

  describe "selection" do
    @mail [
      {1,
       "CREATE TABLE messages (id INTEGER PRIMARY KEY, subject TEXT, flags INTEGER, body TEXT)"},
      {2, "CREATE TABLE parts (id INTEGER PRIMARY KEY, data TEXT)"}
    ]

    @selection [tables: %{"messages" => [omit: ["body"]], "parts" => false}]

    test "records have no omitted column and tables left out have none", ctx do
      inst = start_instance(ctx, migrations: @mail, feed: @selection)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO messages VALUES (1, 'hi', 0, 'the body')")
      exec(inst, cell, "INSERT INTO parts VALUES (1, 'data')")
      exec(inst, cell, "UPDATE messages SET body = 'other'")
      exec(inst, cell, "UPDATE messages SET flags = 1")

      assert summary(records(ctx, 2)) == [
               {"messages", :insert, %{"id" => 1}, %{"id" => 1, "subject" => "hi", "flags" => 0}},
               {"messages", :update, %{"id" => 1}, %{"id" => 1, "subject" => "hi", "flags" => 1}}
             ]
    end

    test "a new selection takes effect at the next wake", ctx do
      cell = unique_cell()
      inst = start_instance(ctx, migrations: @mail, name: :first)
      exec(inst, cell, "INSERT INTO messages VALUES (1, 'hi', 0, 'the body')")
      assert [%{values: %{"body" => "the body"}}] = records(ctx, 1)
      stop_supervised!(inst)

      inst = start_instance(ctx, migrations: @mail, feed: @selection)
      exec(inst, cell, "INSERT INTO messages VALUES (2, 'yo', 0, 'more')")
      assert [_, %{values: values}] = records(ctx, 2)
      assert values == %{"id" => 2, "subject" => "yo", "flags" => 0}
    end
  end

  describe "object format" do
    defp bodies(ctx) do
      {mod, config} = ctx.store
      for key <- feed_keys(ctx), do: elem(mod.get(config, key), 1)
    end

    test "an object is JSON compressed with zlib", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      exec(inst, cell, "INSERT INTO notes VALUES (1, '#{String.duplicate("abc ", 500)}')")

      assert [%{values: %{"id" => 1}}] = records(ctx, 1)
      assert [<<"ALF", 1, _::binary>> = body] = bodies(ctx)
      assert byte_size(body) < 500
    end
  end

  describe "publication" do
    test "a crash between the acknowledgement and the publication is repaired by the next owner",
         ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      CrashPoints.arm(:feed_before_publish, :kill, when: &match?(%{cell: ^cell}, &1))

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      eventually(fn -> Alambique.owner(inst, cell) == :none end)
      assert feed_keys(ctx) == []

      # The next call wakes the cell, which publishes the batch before serving.
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT id FROM notes")
      assert [%{values: %{"id" => 1}}] = records(ctx, 1)
      assert [_] = feed_keys(ctx)
    end

    test "a crash after the put is repaired without a second object", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      # The sender is a process of its own: the crash kills the cell from there.
      kill_cell = fn _point, %{pid: pid} -> Process.exit(pid, :kill) end
      CrashPoints.arm(:feed_after_put, kill_cell, when: &match?(%{cell: ^cell}, &1))

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      eventually(fn -> Alambique.owner(inst, cell) == :none end)
      assert [_] = feed_keys(ctx)

      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      exec(inst, cell, "INSERT INTO notes VALUES (2, 'b')")
      assert [1, 2] = ctx |> records(2) |> Enum.map(& &1.key["id"])
      assert [_, _] = feed_keys(ctx)
    end

    test "a raw store that raises, exits or throws is an error", ctx do
      feed = Feed.ctx(store: ctx.store, prefix: "cells-test", settle_ms: 0)
      {_, config} = ctx.store

      for {kind, expected} <- [raise: :error, exit: :exit, throw: :throw] do
        fault = MemoryStore.inject(config, kind: kind, ops: [:list])
        assert {:error, {:store_crashed, ^expected, _reason}} = Feed.read(feed, nil, 10)
        MemoryStore.remove(config, fault)
      end

      fault = MemoryStore.inject(config, kind: :exit, ops: [:list])
      assert {:error, {:store_crashed, :exit, _reason}} = Feed.prune(feed, 1)
      MemoryStore.remove(config, fault)
    end

    test "a lost acknowledgement of the put publishes one object", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {_, config} = ctx.store
      MemoryStore.inject(config, kind: :lost_ack, ops: [:put], key: ~r{/feed/}, times: 2)

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")

      assert [_] = records(ctx, 1)
      assert [_] = feed_keys(ctx)
    end

    test "a publication that fails is retried while the cell keeps serving", ctx do
      inst = start_instance(ctx, migrations: @schema, op_timeout_ms: 300)
      cell = unique_cell()
      {_, config} = ctx.store
      fault = MemoryStore.inject(config, kind: :error, ops: [:put], key: ~r{/feed/})

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      assert {_, %{result: :error}} = await_event([:alambique, :feed, :publish], cell)
      assert feed_keys(ctx) == []

      MemoryStore.remove(config, fault)
      assert [%{values: %{"id" => 1}}] = records(ctx, 1)
    end

    test "the publish retry delay comes from the instance option", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          op_timeout_ms: 300,
          feed: [publish_retry_ms: 5_000]
        )

      cell = unique_cell()
      {_, config} = ctx.store
      fault = MemoryStore.inject(config, kind: :error, ops: [:put], key: ~r{/feed/})

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")

      assert {_, %{result: :error, retry_in_ms: 5_000}} =
               await_event([:alambique, :feed, :publish], cell)

      MemoryStore.remove(config, fault)
    end

    test "a batch published late takes the time of its publication, after its predecessors",
         ctx do
      inst = start_instance(ctx, migrations: @schema, feed: [window_ms: 50])
      cell = unique_cell()
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      assert [_] = records(ctx, 1)

      CrashPoints.arm(:feed_before_publish, :kill, when: &match?(%{cell: ^cell}, &1))
      exec(inst, cell, "INSERT INTO notes VALUES (2, 'b')")
      eventually(fn -> Alambique.owner(inst, cell) == :none end)
      Process.sleep(150)

      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")

      assert {_, %{late: true, result: :ok}} =
               await_event([:alambique, :feed, :publish], cell, 5_000, & &1.late)

      times =
        feed_keys(ctx)
        |> Enum.map(
          &Alambique.Feed.Keys.parse("cells-test", Alambique.Feed.Keys.shard(cell, 16), &1)
        )

      assert [first, second] = times
      assert first.txid < second.txid
      assert second.time >= first.time + 150
    end

    test "rows already published are removed by the next write, and a wake does not publish them again",
         ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()

      exec(inst, cell, "INSERT INTO notes VALUES (1, 'a')")
      records(ctx, 1)
      assert [[1]] = durable_rows(ctx, cell, "SELECT count(*) FROM _alambique_feed")

      exec(inst, cell, "INSERT INTO notes VALUES (2, 'b')")
      records(ctx, 2)
      assert [[1]] = durable_rows(ctx, cell, "SELECT count(*) FROM _alambique_feed")
      assert [[1]] = durable_rows(ctx, cell, "SELECT count(*) FROM _alambique_feed_state")

      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      assert [_, _] = feed_keys(ctx)
    end
  end

  describe "reading" do
    test "positions continue where the last read stopped", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      for id <- 1..5, do: exec(inst, cell, "INSERT INTO notes VALUES (?, 'x')", [id])
      records(ctx, 5)
      feed = feed_ctx(ctx)

      {:ok, first, position} = Feed.read(feed, nil, 2)
      {:ok, second, position} = Feed.read(feed, position, 2)
      {:ok, third, position} = Feed.read(feed, position, 2)

      assert Enum.map(first ++ second ++ third, & &1.key["id"]) == [1, 2, 3, 4, 5]
      assert {:ok, [], ^position} = Feed.read(feed, position, 2)
    end

    test "objects younger than the settle time are not read yet", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'x')")
      records(ctx, 1)

      assert {:ok, [], nil} = Feed.read(feed_ctx(ctx, settle_ms: 60_000), nil, 10)
      assert {:ok, [_], _} = Feed.read(feed_ctx(ctx, settle_ms: 0), nil, 10)
    end

    test "a position can start at a time", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'x')")
      [first] = records(ctx, 1)
      Process.sleep(20)
      exec(inst, cell, "INSERT INTO notes VALUES (2, 'x')")
      records(ctx, 2)

      feed = feed_ctx(ctx)

      assert {:ok, [%{key: %{"id" => 2}}], _} =
               Feed.read(feed, Feed.position_at(feed, first.at + 1), 10)

      assert {:ok, [_, _], _} = Feed.read(feed, Feed.position_at(feed, first.at), 10)
    end

    test "a read lists each shard once however many cells exist", ctx do
      counting = CountingStore.wrap(ctx.store)
      {_, counts} = counting
      inst = start_instance(%{ctx | store: counting}, migrations: @schema, idle_timeout_ms: 200)
      cells = for _ <- 1..150, do: unique_cell()

      cells
      |> Task.async_stream(&exec(inst, &1, "INSERT INTO notes VALUES (1, 'x')"),
        max_concurrency: 32
      )
      |> Stream.run()

      feed = feed_ctx(%{ctx | store: counting})
      records = records(%{ctx | store: counting}, 150)
      assert length(records) == 150
      {:ok, _, position} = Feed.read(feed, nil, 1000)

      # Three cells write after the consumer's position.
      for cell <- Enum.take(cells, 3), do: exec(inst, cell, "INSERT INTO notes VALUES (2, 'y')")
      eventually(fn -> match?({:ok, [_, _, _], _}, Feed.read(feed, position, 1000)) end)

      CountingStore.reset(counts)
      assert {:ok, [_, _, _], position} = Feed.read(feed, position, 1000)
      assert Enum.sort(CountingStore.listed(counts)) == shard_prefixes()
      assert CountingStore.count(counts, :get) == 3

      CountingStore.reset(counts)
      assert {:ok, [], _} = Feed.read(feed, position, 1000)
      assert Enum.sort(CountingStore.listed(counts)) == shard_prefixes()
      assert CountingStore.count(counts, :get) == 0
    end
  end

  describe "retention" do
    test "prune deletes objects older than the retention", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'x')")
      records(ctx, 1)
      Process.sleep(60)
      exec(inst, cell, "INSERT INTO notes VALUES (2, 'x')")
      records(ctx, 2)

      assert {:ok, 0} = Feed.prune(feed_ctx(ctx), 60_000)
      assert {:ok, 1} = Feed.prune(feed_ctx(ctx), 50)
      assert [%{key: %{"id" => 2}}] = records(ctx, 1)
    end

    test "an instance with a retention prunes on a timer", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          feed: [retention_ms: 100, prune_interval_ms: 100]
        )

      cell = unique_cell()
      exec(inst, cell, "INSERT INTO notes VALUES (1, 'x')")
      eventually(fn -> match?([_], feed_keys(ctx)) end)
      eventually(fn -> feed_keys(ctx) == [] end)
    end
  end
end
