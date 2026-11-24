defmodule Alambique.FeedCaptureTest do
  @moduledoc """
  The stamp of the feed rows (design §13): it changes the size of nothing, so a
  large row is not written again, the rows are those an older release stamps,
  and rows stamped by an older release are still read.
  """
  use ExUnit.Case, async: true

  import Alambique.CellHelpers,
    only: [eventually: 1, setup_store: 0, start_instance: 2, unique_cell: 0]

  alias Alambique.Cell.Db
  alias Alambique.Feed.Capture
  alias Alambique.Sqlite

  setup do
    dir = Alambique.TestDirs.new("capture")
    path = Path.join(dir, "c.db")
    {:ok, conn} = Sqlite.open(path)
    on_exit(fn -> Sqlite.close(conn) end)

    :ok = Sqlite.execute(conn, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
    :ok = Capture.install(conn, Capture.everything())
    %{conn: conn, path: path}
  end

  defp rows(conn, txid, upto) do
    for [t, ts, tbl, op, _key, _vals, _seq] <- Capture.rows_from(conn, {txid + 1, -1}, upto, nil),
        do: {t, ts, tbl, op}
  end

  test "a stamped row is read with its TXID and the time of its batch", %{conn: conn} do
    :ok =
      Db.writable_script(
        conn,
        "INSERT INTO notes VALUES (1, 'a'); INSERT INTO notes VALUES (2, 'b')"
      )

    assert rows(conn, 0, 5) == []

    :ok = Capture.stamp(conn, 3, 1_000)
    assert rows(conn, 0, 5) == [{3, 1_000, "notes", "insert"}, {3, 1_000, "notes", "insert"}]
    assert rows(conn, 3, 5) == []

    :ok = Db.writable_script(conn, "INSERT INTO notes VALUES (3, 'c')")
    :ok = Capture.stamp(conn, 4, 2_000)
    assert rows(conn, 3, 4) == [{4, 2_000, "notes", "insert"}]
    assert Capture.last_time(conn) == 2_000

    {:ok, :ok} = Db.transaction(conn, 1, &Capture.cleanup(&1.conn, 3, 1_500))
    assert rows(conn, 0, 9) == [{4, 2_000, "notes", "insert"}]
  end

  test "the time of the last batch survives the removal of its rows, and an older release's is kept",
       %{conn: conn} do
    :ok = Db.writable_script(conn, "INSERT INTO notes VALUES (1, 'a')")
    :ok = Capture.stamp(conn, 3, 1_000)
    {:ok, :ok} = Db.transaction(conn, 1, &Capture.cleanup(&1.conn, 3, 1_000))
    assert rows(conn, 0, 9) == []
    assert Capture.last_time(conn) == 1_000

    # An older release saved the time in the state table.
    :ok = Db.writable_script(conn, "UPDATE _alambique_feed_state SET last_time = 5_000")
    {:ok, :ok} = Db.transaction(conn, 1, &Capture.cleanup(&1.conn, 3, 1_000))
    assert Capture.last_time(conn) == 5_000
    {:ok, :ok} = Db.transaction(conn, 1, &Capture.cleanup(&1.conn, 3, 6_000))
    assert Capture.last_time(conn) == 6_000
  end

  test "rows stamped by an older release keep their TXID and time", %{conn: conn} do
    :ok = Db.writable_script(conn, "INSERT INTO notes VALUES (1, 'a')")
    :ok = Db.writable_script(conn, "UPDATE _alambique_feed SET txid = 2, ts = 700")
    :ok = Db.writable_script(conn, "INSERT INTO notes VALUES (2, 'b')")
    :ok = Capture.stamp(conn, 3, 900)

    assert rows(conn, 0, 9) == [{2, 700, "notes", "insert"}, {3, 900, "notes", "insert"}]
    {:ok, :ok} = Db.transaction(conn, 1, &Capture.cleanup(&1.conn, 2, 700))
    assert rows(conn, 0, 9) == [{3, 900, "notes", "insert"}]
  end

  # Pages of the file that differ after `fun` ran.
  defp changed_pages(conn, path, fun) do
    {:ok, %{rows: [[size]]}} = Db.run(conn, "PRAGMA page_size", [])
    :ok = Sqlite.checkpoint(conn)
    before = File.read!(path)
    fun.()
    :ok = Sqlite.checkpoint(conn)
    after_ = File.read!(path)

    pages = fn bin -> for <<page::binary-size(^size) <- bin>>, do: page end
    {before, after_} = {pages.(before), pages.(after_)}
    differing = Enum.count(Enum.zip(before, after_), fn {a, b} -> a != b end)
    differing + abs(length(after_) - length(before))
  end

  test "a stamp writes the pages of a header, not those of a large value, whatever the TXID",
       %{conn: conn, path: path} do
    # The first TXID of each width of an integer in SQLite.
    for txid <- [5, 128, 32_768, 8_388_608, 2_147_483_648] do
      :ok = Capture.stamp(conn, txid - 1, 1_790_000_000_000)
      body = 30_000 |> :crypto.strong_rand_bytes() |> Base.encode64()
      :ok = Db.writable_script(conn, "INSERT INTO notes VALUES (#{txid}, '#{body}')")

      changed =
        changed_pages(conn, path, fn -> :ok = Capture.stamp(conn, txid, 1_790_000_000_001) end)

      # The row, its index entry and the saved pending TXID; the body is 10 pages of text that differs byte by byte.
      assert changed <= 4, "TXID #{txid}: #{changed} pages written"
    end
  end

  test "stamped rows have the TXID and the time of the batch, as an older release writes them",
       %{conn: conn} do
    :ok = Db.writable_script(conn, "INSERT INTO notes VALUES (1, 'a')")
    :ok = Capture.stamp(conn, 300, 1_790_000_000_000)

    assert {:ok, %{rows: [[300, 1_790_000_000_000]]}} =
             Db.run(conn, "SELECT txid, ts FROM _alambique_feed", [])
  end

  test "a large row is in a segment once, not again when it is stamped" do
    ctx = setup_store()

    # A long lease: the test does not depend on expiry, and a loaded host must not expire it.
    inst =
      start_instance(ctx,
        migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}],
        lease_ttl_ms: 30_000
      )

    cell = unique_cell()
    body = 60_000 |> :crypto.strong_rand_bytes() |> Base.encode64()
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO notes VALUES (1, ?)", [body])

    {mod, config} = ctx.store

    sizes = fn ->
      {:ok, keys, _} = mod.list(config, "cells-test/cells/#{cell}/0000/", nil, 100)

      for key <- keys do
        {:ok, segment, _, _} = mod.get(config, key)
        byte_size(segment)
      end
    end

    # The row and its feed copy, each about the size of the text: a stamp that
    # rewrote the feed row would add a third.
    eventually(fn -> length(sizes.()) >= 3 end)
    assert Enum.max(sizes.()) < 2.4 * byte_size(body)
  end

  describe "selection" do
    @schema """
    CREATE TABLE messages (id INTEGER PRIMARY KEY, subject TEXT, flags INTEGER, body TEXT);
    CREATE TABLE parts (id INTEGER PRIMARY KEY, data BLOB);
    CREATE TABLE labels (message_id INTEGER, label TEXT, secret TEXT, PRIMARY KEY (message_id, label)) WITHOUT ROWID;
    CREATE TABLE loose (a TEXT, b TEXT);
    """

    defp select(overrides) do
      Map.merge(Capture.everything(), Map.new(overrides))
    end

    defp captured(conn) do
      {:ok, %{rows: rows}} =
        Db.run(conn, "SELECT tbl, op, key, vals FROM _alambique_feed ORDER BY seq", [])

      for [tbl, op, key, vals] <- rows,
          do: {tbl, op, Jason.decode!(key), vals && Jason.decode!(vals)}
    end

    defp fresh(select) do
      dir = Alambique.TestDirs.new("selection")
      {:ok, conn} = Sqlite.open(Path.join(dir, "s.db"))
      on_exit(fn -> Sqlite.close(conn) end)
      :ok = Sqlite.execute(conn, @schema)
      :ok = Capture.install(conn, select)
      conn
    end

    test "an omitted column is never written to the feed table" do
      conn = fresh(select(tables: %{"messages" => {:omit, ["body"]}}))

      :ok =
        Db.writable_script(
          conn,
          "INSERT INTO messages VALUES (1, 'hi', 0, 'SECRET-BODY'); UPDATE messages SET flags = 1, body = 'SECRET-2'"
        )

      assert captured(conn) == [
               {"messages", "insert", %{"id" => 1},
                %{"id" => 1, "subject" => "hi", "flags" => 0}},
               {"messages", "update", %{"id" => 1}, %{"id" => 1, "subject" => "hi", "flags" => 1}}
             ]

      assert {:ok, %{rows: [[0]]}} =
               Db.run(
                 conn,
                 "SELECT count(*) FROM _alambique_feed WHERE instr(vals, 'SECRET') > 0",
                 []
               )
    end

    test "an update that changes only omitted columns leaves no row" do
      conn = fresh(select(tables: %{"messages" => {:omit, ["body"]}}))
      :ok = Db.writable_script(conn, "INSERT INTO messages VALUES (1, 'hi', 0, 'a')")
      :ok = Db.writable_script(conn, "UPDATE messages SET body = 'b'")
      assert length(captured(conn)) == 1
      :ok = Db.writable_script(conn, "UPDATE messages SET subject = 'yo'")
      assert length(captured(conn)) == 2
    end

    test "only keeps the key and the listed columns, also for a key that changes" do
      conn = fresh(select(tables: %{"labels" => {:only, ["label"]}, "loose" => {:only, ["a"]}}))

      :ok =
        Db.writable_script(
          conn,
          """
          INSERT INTO labels VALUES (1, 'inbox', 'x');
          INSERT INTO loose VALUES ('p', 'q');
          UPDATE labels SET message_id = 2;
          DELETE FROM loose
          """
        )

      assert captured(conn) == [
               {"labels", "insert", %{"message_id" => 1, "label" => "inbox"},
                %{"message_id" => 1, "label" => "inbox"}},
               {"loose", "insert", %{"rowid" => 1}, %{"a" => "p"}},
               {"labels", "delete", %{"message_id" => 1, "label" => "inbox"}, nil},
               {"labels", "insert", %{"message_id" => 2, "label" => "inbox"},
                %{"message_id" => 2, "label" => "inbox"}},
               {"loose", "delete", %{"rowid" => 1}, nil}
             ]
    end

    test "an excluded table has no triggers, and the rest follows other_tables" do
      conn = fresh(select(tables: %{"parts" => :skip}))

      :ok =
        Db.writable_script(
          conn,
          "INSERT INTO parts VALUES (1, x'00'); INSERT INTO loose VALUES ('a', 'b')"
        )

      assert [{"loose", "insert", _, _}] = captured(conn)

      conn = fresh(select(tables: %{"loose" => :capture}, other_tables: :skip))

      :ok =
        Db.writable_script(
          conn,
          "INSERT INTO parts VALUES (1, x'00'); INSERT INTO loose VALUES ('a', 'b'); INSERT INTO messages VALUES (1, 's', 0, 'b')"
        )

      assert [{"loose", "insert", _, _}] = captured(conn)
    end

    test "changing the selection makes the triggers out of date" do
      conn = fresh(Capture.everything())
      assert Capture.current?(conn, Capture.everything())
      refute Capture.current?(conn, select(tables: %{"messages" => {:omit, ["body"]}}))
      refute Capture.current?(conn, select(tables: %{"parts" => :skip}))
      refute Capture.current?(conn, select(other_tables: :skip))
      assert Capture.current?(conn, select(tables: %{"parts" => :capture}))
    end

    defp ops(ops, what \\ :capture), do: {:ops, ops, what}

    defp mass_delete(conn) do
      :ok =
        Db.writable_script(
          conn,
          """
          INSERT INTO messages VALUES (1, 'a', 0, 'x'), (2, 'b', 0, 'y'), (3, 'c', 0, 'z');
          INSERT INTO loose VALUES ('p', 'q'), ('r', 's'), ('t', 'u');
          DELETE FROM messages;
          DELETE FROM loose
          """
        )

      for {tbl, op, _, _} <- captured(conn), do: {tbl, op}
    end

    test "ops: [:insert] captures inserts only: deletes cost no row, other tables are untouched" do
      conn = fresh(select(tables: %{"messages" => ops([:insert])}))

      assert mass_delete(conn) ==
               List.duplicate({"messages", "insert"}, 3) ++
                 List.duplicate({"loose", "insert"}, 3) ++ List.duplicate({"loose", "delete"}, 3)

      # No delete trigger: a mass delete runs no trigger body.
      assert {:ok, %{rows: [["_alambique_feed_t_messages_i"], ["_alambique_feed_t_messages_u"]]}} =
               Db.run(
                 conn,
                 "SELECT name FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = 'messages'",
                 []
               )
    end

    test "ops: [:update] has no insert and no delete trigger" do
      conn = fresh(select(tables: %{"messages" => ops([:update])}))

      assert {:ok, %{rows: [["_alambique_feed_t_messages_u"]]}} =
               Db.run(
                 conn,
                 "SELECT name FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = 'messages'",
                 []
               )
    end

    test "ops: [:insert, :update] captures inserts and updates, not deletes" do
      conn = fresh(select(tables: %{"messages" => ops([:insert, :update])}))

      :ok =
        Db.writable_script(
          conn,
          "INSERT INTO messages VALUES (1, 'a', 0, 'x'); UPDATE messages SET flags = 1; UPDATE messages SET id = 2; DELETE FROM messages"
        )

      assert for({t, op, k, _} <- captured(conn), do: {t, op, k["id"]}) == [
               {"messages", "insert", 1},
               {"messages", "update", 1},
               {"messages", "insert", 2}
             ]
    end

    test "a key change under each filter emits only the statements whose op is listed" do
      script = """
      INSERT INTO labels VALUES (1, 'inbox', 'x');
      UPDATE labels SET message_id = 2;
      UPDATE labels SET secret = 'y'
      """

      for {list, expected} <- [
            {[:insert], ["insert", "insert"]},
            {[:update], ["update"]},
            {[:delete], ["delete"]},
            {[:insert, :delete], ["insert", "delete", "insert"]},
            {[:update, :delete], ["delete", "update"]},
            {[:insert, :update], ["insert", "insert", "update"]}
          ] do
        conn = fresh(select(tables: %{"labels" => ops(list)}))
        :ok = Db.writable_script(conn, script)
        assert Enum.map(captured(conn), &elem(&1, 1)) == expected, inspect(list)
      end
    end

    test "ops combine with omit and only" do
      conn = fresh(select(tables: %{"messages" => ops([:insert, :delete], {:omit, ["body"]})}))

      :ok =
        Db.writable_script(
          conn,
          "INSERT INTO messages VALUES (1, 'a', 0, 'SECRET'); UPDATE messages SET flags = 1; DELETE FROM messages"
        )

      assert captured(conn) == [
               {"messages", "insert", %{"id" => 1}, %{"id" => 1, "subject" => "a", "flags" => 0}},
               {"messages", "delete", %{"id" => 1}, nil}
             ]

      conn = fresh(select(tables: %{"messages" => ops([:update], {:only, ["subject"]})}))

      :ok =
        Db.writable_script(
          conn,
          "INSERT INTO messages VALUES (1, 'a', 0, 'x'); UPDATE messages SET body = 'y'; UPDATE messages SET subject = 'b'"
        )

      assert [{"messages", "update", %{"id" => 1}, %{"id" => 1, "subject" => "b"}}] =
               captured(conn)
    end

    test "ops change the fingerprint, and a selection without them keeps the old triggers" do
      conn = fresh(Capture.everything())
      before = triggers_sql(conn)

      assert Capture.current?(conn, select(tables: %{"messages" => :capture}))
      refute Capture.current?(conn, select(tables: %{"messages" => ops([:insert])}))
      refute Capture.current?(conn, select(tables: %{"messages" => ops([:delete])}))

      # The same selection, written with and without `ops`, gives the same SQL.
      :ok = Db.script(conn, "BEGIN;")
      :ok = Capture.install(conn, select(tables: %{"messages" => :capture}))
      :ok = Db.script(conn, "COMMIT;")
      assert triggers_sql(conn) == before
    end

    test "after a different selection the triggers follow it" do
      conn = fresh(select(tables: %{"messages" => ops([:insert])}))
      later = select(tables: %{"messages" => ops([:insert, :delete])})
      refute Capture.current?(conn, later)

      :ok = Db.script(conn, "BEGIN;")
      :ok = Capture.install(conn, later)
      :ok = Db.script(conn, "COMMIT;")
      assert Capture.current?(conn, later)

      :ok =
        Db.writable_script(
          conn,
          "INSERT INTO messages VALUES (1, 'a', 0, 'x'); DELETE FROM messages"
        )

      assert Enum.map(captured(conn), &elem(&1, 1)) == ["insert", "delete"]
    end

    defp triggers_sql(conn) do
      {:ok, %{rows: rows}} =
        Db.run(
          conn,
          "SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' ORDER BY name",
          []
        )

      rows
    end

    test "a column the table lacks is ignored" do
      conn = fresh(select(tables: %{"messages" => {:omit, ["nope"]}}))
      :ok = Db.writable_script(conn, "INSERT INTO messages VALUES (1, 's', 0, 'b')")
      assert [{"messages", "insert", _, %{"body" => "b", "subject" => "s"}}] = captured(conn)
    end
  end
end
