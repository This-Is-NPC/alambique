defmodule Alambique.SqliteTest do
  @moduledoc "The bundled SQLite behind `Alambique.Sqlite` and the access layer `Alambique.Cell.Db` (design §2)."
  use ExUnit.Case, async: true

  alias Alambique.Cell.Db
  alias Alambique.{Sqlite, Tx}

  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    {:ok, conn} = Sqlite.open(Path.join(dir, "t.db"))
    on_exit(fn -> Sqlite.close(conn) end)
    {:ok, conn: conn, dir: dir}
  end

  defp run(conn, sql, opts), do: Sqlite.run(conn, sql, [], opts)

  describe "values" do
    test "integers, floats, text, blobs and nil survive a round trip", %{conn: conn} do
      :ok = Sqlite.execute(conn, "CREATE TABLE v (i, f, t, b, n)")
      big = 9_223_372_036_854_775_807

      assert {:ok, _} =
               Sqlite.run(conn, "INSERT INTO v VALUES (?, ?, ?, ?, ?)", [
                 big,
                 2.5,
                 "héllo",
                 {:blob, <<0, 255, 1>>},
                 nil
               ])

      assert {:ok,
              %{
                columns: ["i", "f", "t", "b", "n"],
                rows: [[^big, 2.5, "héllo", <<0, 255, 1>>, nil]]
              }} =
               Sqlite.run(conn, "SELECT * FROM v", [])

      assert {:ok, %{rows: [["text", "blob"]]}} =
               Sqlite.run(conn, "SELECT typeof(t), typeof(b) FROM v", [])
    end

    test "iodata is bound as text, atoms by name, :undefined as null", %{conn: conn} do
      assert {:ok, %{rows: [["ab", "ok", nil, "ab"]]}} =
               Sqlite.run(conn, "SELECT ?, ?, ?, ?", [
                 ["a", ?b],
                 :ok,
                 :undefined,
                 {:blob, ["a", "b"]}
               ])
    end

    test "a wrong parameter count or type raises ArgumentError", %{conn: conn} do
      assert_raise ArgumentError, ~r/expected 1 arguments, got 0/, fn ->
        Sqlite.run(conn, "SELECT ?", [])
      end

      assert_raise ArgumentError, ~r/unsupported type/, fn ->
        Sqlite.run(conn, "SELECT ?", [{1, 2}])
      end

      assert_raise ArgumentError, fn ->
        Sqlite.run(conn, "SELECT ?", [18_446_744_073_709_551_616])
      end
    end

    test "zero-length text and blobs", %{conn: conn} do
      assert {:ok, %{rows: [["", ""]]}} = Sqlite.run(conn, "SELECT ?, ?", ["", {:blob, ""}])
    end
  end

  describe "statements" do
    test "errors carry SQLite's message", %{conn: conn} do
      assert {:error, "no such table: nope"} = Sqlite.run(conn, "SELECT * FROM nope", [])
      assert {:error, "no such table: nope"} = Sqlite.execute(conn, "DELETE FROM nope")

      :ok = Sqlite.execute(conn, "CREATE TABLE u (k PRIMARY KEY)")
      {:ok, _} = Sqlite.run(conn, "INSERT INTO u VALUES (1)", [])

      assert {:error, "UNIQUE constraint failed: u.k"} =
               Sqlite.run(conn, "INSERT INTO u VALUES (1)", [])
    end

    test "an empty statement has no rows", %{conn: conn} do
      assert {:ok, %{columns: [], rows: []}} = Sqlite.run(conn, "-- nothing", [])
    end

    test "changes and transaction status", %{conn: conn} do
      :ok = Sqlite.execute(conn, "CREATE TABLE c (a); INSERT INTO c VALUES (1), (2), (3)")
      {:ok, _} = Sqlite.run(conn, "DELETE FROM c WHERE a > 1", [])
      assert {:ok, 2} = Sqlite.changes(conn)

      assert {:ok, :idle} = Sqlite.transaction_status(conn)
      :ok = Sqlite.execute(conn, "BEGIN")
      assert {:ok, :transaction} = Sqlite.transaction_status(conn)
      :ok = Sqlite.execute(conn, "ROLLBACK")
      assert {:ok, :idle} = Sqlite.transaction_status(conn)
    end

    test "build options: no default foreign keys, math functions, no double-quoted strings", %{
      conn: conn
    } do
      assert {:ok, %{rows: [[0]]}} = Sqlite.run(conn, "PRAGMA foreign_keys", [])
      assert {:ok, %{rows: [[3.0]]}} = Sqlite.run(conn, "SELECT ceil(2.1)", [])
      assert {:error, "no such column: " <> _} = Sqlite.run(conn, ~s(SELECT "nope"), [])
    end
  end

  describe "connections" do
    test "a closed connection reports it, and closing twice is fine", %{dir: dir} do
      {:ok, conn} = Sqlite.open(Path.join(dir, "closed.db"))
      :ok = Sqlite.close(conn)
      assert :ok = Sqlite.close(conn)
      assert {:error, :connection_closed} = Sqlite.execute(conn, "SELECT 1")
      assert {:error, :connection_closed} = Sqlite.run(conn, "SELECT 1", [])
    end

    test "read-only opens need an existing file and refuse writes", %{dir: dir, conn: conn} do
      :ok = Sqlite.execute(conn, "CREATE TABLE r (a)")
      assert {:error, _} = Sqlite.open(Path.join(dir, "missing.db"), readonly: true)

      {:ok, ro} = Sqlite.open(Path.join(dir, "t.db"), readonly: true)

      assert {:error, "attempt to write a readonly database"} =
               Sqlite.run(ro, "INSERT INTO r VALUES (1)", [])

      Sqlite.close(ro)
    end

    test "interrupt stops a running statement", %{conn: conn} do
      task =
        Task.async(fn ->
          receive do
            :go -> :ok
          end

          Sqlite.run(
            conn,
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n) SELECT count(*) FROM n",
            []
          )
        end)

      :ok = Sqlite.assign(conn, task.pid)
      send(task.pid, :go)
      Process.sleep(100)
      assert :ok = Sqlite.interrupt(conn)
      assert {:error, "interrupted"} = Task.await(task, 10_000)
      :ok = Sqlite.assign(conn, self())
      assert {:ok, %{rows: [[1]]}} = Sqlite.run(conn, "SELECT 1", [])
    end

    test "closing interrupts a statement that is still running", %{dir: dir} do
      {:ok, conn} = Sqlite.open(Path.join(dir, "closing.db"))

      runner =
        Task.async(fn ->
          receive do
            :go -> :ok
          end

          Sqlite.run(
            conn,
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 300000000) SELECT count(*) FROM n",
            []
          )
        end)

      :ok = Sqlite.assign(conn, runner.pid)
      send(runner.pid, :go)
      Process.sleep(100)
      # The opener closes while the user runs the statement.
      assert :ok = Sqlite.close(conn)

      assert {:error, reason} = Task.await(runner, 2_000)
      assert reason in ["interrupted", :connection_closed]
    end

    test "ATTACH and DETACH need attach: true, and the schema is defensive", %{
      dir: dir,
      conn: conn
    } do
      assert {:error, :not_authorized} = Sqlite.run(conn, "ATTACH ':memory:' AS x", [])
      assert {:error, :not_authorized} = Sqlite.execute(conn, "ATTACH ':memory:' AS x")
      assert {:error, :not_authorized} = Sqlite.execute(conn, "VACUUM")

      :ok = Sqlite.execute(conn, "CREATE TABLE d (a)")
      :ok = Sqlite.execute(conn, "PRAGMA writable_schema = ON")
      assert {:error, _} = Sqlite.execute(conn, "UPDATE sqlite_master SET sql = sql")

      {:ok, other} = Sqlite.open(Path.join(dir, "other.db"), attach: true)
      assert :ok = Sqlite.execute(other, "ATTACH ':memory:' AS x")
      assert :ok = Sqlite.execute(other, "DETACH x")
      assert :ok = Sqlite.execute(other, "VACUUM")
      Sqlite.close(other)
    end

    test "a user's text cannot end the transaction, and the flag is per call", %{conn: conn} do
      :ok = Sqlite.execute(conn, "CREATE TABLE u (a)")
      :ok = Sqlite.execute(conn, "BEGIN")
      flag = [no_transaction_control: true]

      for sql <- ["COMMIT", "END", "end transaction", "ROLLBACK", "BEGIN", "/* c */ commit"] do
        assert {:error, :not_authorized} = run(conn, sql, flag), sql
        assert {:error, :not_authorized} = Sqlite.execute(conn, sql, flag), sql
      end

      assert {:error, :not_authorized} =
               Sqlite.execute(
                 conn,
                 "INSERT INTO u VALUES (1); COMMIT; INSERT INTO u VALUES (2)",
                 flag
               )

      assert {:ok, :transaction} = Sqlite.transaction_status(conn)

      # Savepoints are not refused, and the next call has no flag.
      assert {:ok, _} = run(conn, "SAVEPOINT s", flag)
      assert {:ok, _} = run(conn, "INSERT INTO u VALUES (3)", flag)
      assert {:ok, _} = run(conn, "ROLLBACK TO s", flag)
      assert {:ok, _} = run(conn, "RELEASE s", flag)
      assert {:ok, _} = run(conn, "COMMIT", [])
      assert {:ok, :idle} = Sqlite.transaction_status(conn)
    end

    test "text after the statement is refused unless it is blank", %{conn: conn} do
      :ok = Sqlite.execute(conn, "CREATE TABLE m (a)")

      assert {:error, {:multiple_statements, "INSERT INTO m VALUES (2)"}} =
               run(conn, "INSERT INTO m VALUES (1); INSERT INTO m VALUES (2)", [])

      assert {:error, {:multiple_statements, "COMMIT"}} = run(conn, "SELECT 1;  COMMIT ", [])

      assert {:error, {:multiple_statements, tail}} =
               run(conn, "SELECT 1; SELECT '#{String.duplicate("x", 500)}'", [])

      assert byte_size(tail) <= 103

      # Nothing ran, and the statements that are allowed still run.
      assert {:ok, %{rows: [[0]]}} = run(conn, "SELECT count(*) FROM m", [])

      for sql <- ["SELECT 1", "SELECT 1;", "SELECT 1; -- c", "SELECT 1 /* c */ ; ;", ";SELECT 1"] do
        assert {:ok, %{rows: [[1]]}} = run(conn, sql, []), sql
      end

      assert {:ok, %{rows: []}} = run(conn, " ; /* nothing */", [])

      assert {:error, {:multiple_statements, _}} =
               run(conn, "SELECT 1; SELECT 2", read_only: true)
    end

    test "a call can be read only, bounded in time and bounded in result size", %{conn: conn} do
      :ok = Sqlite.execute(conn, "CREATE TABLE b (a); INSERT INTO b VALUES (1), (2), (3)")

      assert {:error, :not_authorized} =
               run(conn, "PRAGMA recursive_triggers(1)", read_only: true)

      assert {:error, :not_authorized} = run(conn, "BEGIN", read_only: true)
      assert {:ok, %{rows: [[0]]}} = run(conn, "PRAGMA recursive_triggers", read_only: true)
      assert {:ok, %{rows: [_ | _]}} = run(conn, "PRAGMA table_info(b)", read_only: true)
      assert {:ok, %{rows: []}} = run(conn, "PRAGMA recursive_triggers(1)", [])
      assert {:ok, %{rows: [[1]]}} = run(conn, "PRAGMA recursive_triggers", [])

      assert {:error, :result_too_large} = run(conn, "SELECT a FROM b", max_rows: 2)
      assert {:ok, %{rows: [_, _, _]}} = run(conn, "SELECT a FROM b", max_rows: 3)

      assert {:error, :result_too_large} = run(conn, "SELECT zeroblob(2000)", max_bytes: 1_000)
      big = "SELECT zeroblob(500000000)"
      assert {:error, :result_too_large} = run(conn, big, max_bytes: 1_000_000, read_only: true)
      assert {:error, :result_too_large} = run(conn, "SELECT a, a, a FROM b", max_bytes: 40)

      forever =
        "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 400000000) SELECT count(*) FROM n"

      {micros, result} = :timer.tc(fn -> run(conn, forever, timeout_ms: 200) end)
      assert {:error, :statement_timeout} = result
      assert micros < 2_000_000
      assert {:ok, %{rows: [[1]]}} = run(conn, "SELECT 1", timeout_ms: 200)
    end

    test "only checkpoint/1 moves WAL frames into the file", %{dir: dir, conn: conn} do
      :ok = Sqlite.execute(conn, "PRAGMA journal_mode = WAL; CREATE TABLE w (a)")
      :ok = Sqlite.execute(conn, "INSERT INTO w VALUES (1)")
      assert {:ok, %{rows: [[0]]}} = Sqlite.run(conn, "PRAGMA wal_autocheckpoint", [])
      assert {:ok, %{rows: [["wal"]]}} = Sqlite.run(conn, "PRAGMA journal_mode = wal", [])

      for sql <- [
            "PRAGMA wal_checkpoint",
            "PRAGMA wal_checkpoint(TRUNCATE)",
            "pragma MAIN.Wal_Checkpoint(full)",
            "PRAGMA wal_autocheckpoint(10)",
            "PRAGMA wal_autocheckpoint = 10",
            "PRAGMA journal_mode(DELETE)",
            "PRAGMA journal_mode = 'memory'",
            "SELECT * FROM pragma_wal_checkpoint"
          ] do
        assert {:error, _} = Sqlite.run(conn, sql, []), sql
      end

      wal = Path.join(dir, "t.db-wal")
      assert File.stat!(wal).size > 0
      assert :ok = Sqlite.checkpoint(conn)
      assert File.stat!(wal).size == 0
    end

    test "checkpoint/1 answers busy while a reader holds the WAL and works after", %{
      dir: dir,
      conn: conn
    } do
      :ok = Sqlite.execute(conn, "PRAGMA journal_mode = WAL; CREATE TABLE w (a)")
      :ok = Sqlite.execute(conn, "INSERT INTO w VALUES (1)")

      {:ok, reader} = Sqlite.open(Path.join(dir, "t.db"))
      :ok = Sqlite.execute(reader, "BEGIN")
      assert {:ok, %{rows: [[1]]}} = Sqlite.run(reader, "SELECT a FROM w", [])

      :ok = Sqlite.execute(conn, "INSERT INTO w VALUES (2)")
      assert {:error, :busy} = Sqlite.checkpoint(conn)
      assert File.stat!(Path.join(dir, "t.db-wal")).size > 0

      :ok = Sqlite.execute(reader, "COMMIT")
      Sqlite.close(reader)
      assert :ok = Sqlite.checkpoint(conn)
      assert File.stat!(Path.join(dir, "t.db-wal")).size == 0
    end

    test "URI names open an immutable file without a wal", %{dir: dir, conn: conn} do
      :ok =
        Sqlite.execute(
          conn,
          "PRAGMA journal_mode = WAL; CREATE TABLE w (a); INSERT INTO w VALUES (7)"
        )

      # Closing does not checkpoint, and leaves the WAL and its index.
      :ok = Sqlite.checkpoint(conn)
      Sqlite.close(conn)
      for suffix <- ["-wal", "-shm"], do: File.rm!(Path.join(dir, "t.db" <> suffix))

      assert {:ok, ro} = Db.open_read_only(Path.join(dir, "t.db"))
      assert {:ok, %{rows: [[7]]}} = Db.read(ro, "SELECT a FROM w", [])
      assert {:error, :read_only} = Db.read(ro, "INSERT INTO w VALUES (8)", [])
      refute File.exists?(Path.join(dir, "t.db-wal"))
      Db.close(ro)
    end
  end

  describe "Db" do
    setup %{dir: dir} do
      {:ok, conn} = Db.open(Path.join(dir, "cell.db"))
      on_exit(fn -> Db.close(conn) end)
      {:ok, cell: conn}
    end

    test "opens in WAL mode, query only, with the cell settings", %{cell: cell} do
      assert {:ok, %{rows: [["wal"]]}} = Db.run(cell, "PRAGMA journal_mode", [])
      assert {:ok, %{rows: [[0]]}} = Db.run(cell, "PRAGMA wal_autocheckpoint", [])
      assert {:ok, %{rows: [[1]]}} = Db.run(cell, "PRAGMA synchronous", [])
      assert {:ok, %{rows: [[1]]}} = Db.run(cell, "PRAGMA recursive_triggers", [])
      assert {:ok, %{rows: [[1]]}} = Db.run(cell, "PRAGMA query_only", [])
    end

    test "read refuses everything that could write or change the session", %{cell: cell} do
      for sql <- [
            "PRAGMA query_only = OFF",
            "  /* c */ ATTACH ':memory:' AS x",
            "-- c\nBEGIN",
            "VACUUM",
            "COMMIT"
          ] do
        assert {:error, :read_only} = Db.read(cell, sql, []), sql
      end

      assert {:error, :read_only} = Db.read(cell, "CREATE TABLE a (b)", [])
      assert {:ok, %{rows: [[1]]}} = Db.read(cell, "SELECT 1", [])
    end

    test "neither reads nor transactions can checkpoint or leave WAL mode", %{cell: cell} do
      sqls = [
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "PRAGMA wal_autocheckpoint(1)",
        "PRAGMA journal_mode(DELETE)"
      ]

      for sql <- sqls do
        assert {:error, :read_only} = Db.read(cell, sql, []), sql

        assert {:error, _} =
                 Db.transaction(cell, 1, fn tx ->
                   with {:error, reason} <- Tx.execute(tx, sql), do: Tx.rollback(tx, reason)
                 end),
               sql
      end

      assert {:ok, %{rows: [["wal"]]}} = Db.run(cell, "PRAGMA journal_mode", [])
      assert {:ok, %{rows: [[0]]}} = Db.run(cell, "PRAGMA wal_autocheckpoint", [])
    end

    test "transaction commits, rolls back and reports a function that ended it", %{cell: cell} do
      :ok = Db.writable_script(cell, "CREATE TABLE d (a INTEGER PRIMARY KEY)")
      rows = fn -> Db.run(cell, "SELECT a FROM d ORDER BY a", []) end

      assert {:ok, :done} =
               Db.transaction(cell, 5, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO d VALUES (1)")
                 assert Tx.txid(tx) == 5
                 :done
               end)

      assert {:error, :nope} =
               Db.transaction(cell, 6, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO d VALUES (2)")
                 Tx.rollback(tx, :nope)
               end)

      assert {:error, {:raised, %RuntimeError{}, _}} =
               Db.transaction(cell, 6, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO d VALUES (3)")
                 raise "boom"
               end)

      assert {:ok, {:error, "UNIQUE constraint failed: d.a"}} =
               Db.transaction(cell, 6, fn tx -> Tx.execute(tx, "INSERT INTO d VALUES (1)") end)

      assert {:ok, {:error, :not_authorized}} =
               Db.transaction(cell, 6, fn tx -> Tx.execute(tx, "COMMIT") end)

      assert {:ok, %{rows: [[1]]}} = rows.()
      assert {:ok, %{rows: [[1]]}} = Db.run(cell, "PRAGMA query_only", [])
    end

    @ended "CREATE TABLE d (a PRIMARY KEY); CREATE TABLE g (x); " <>
             "CREATE TRIGGER g BEFORE INSERT ON g BEGIN SELECT RAISE(ROLLBACK, 'no'); END"
    @slow "INSERT INTO d SELECT x + 100 FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL " <>
            "SELECT x+1 FROM c WHERE x < 400000000) SELECT x FROM c)"

    test "a statement does not run once SQLite ended the transaction", %{cell: cell} do
      {:ok, _} = Db.transaction(cell, 1, fn tx -> Tx.script(tx, @ended) end, capture: false)
      {:ok, _} = Db.transaction(cell, 2, fn tx -> Tx.execute(tx, "INSERT INTO d VALUES (0)") end)

      for {name, sql, limits} <- [
            {:conflict, "INSERT OR ROLLBACK INTO d VALUES (0)", []},
            {:trigger, "INSERT INTO g VALUES (1)", []},
            {:interrupt, @slow, [timeout_ms: 100]}
          ] do
        assert {:error, :transaction_ended} =
                 Db.transaction(
                   cell,
                   3,
                   fn tx ->
                     {:ok, _} = Tx.execute(tx, "INSERT INTO d VALUES (1)")
                     assert {:error, _} = Tx.execute(tx, sql)
                     assert {:ok, :idle} = Sqlite.transaction_status(tx.conn)

                     # Neither a user statement nor the library's own runs.
                     assert {:error, :transaction_ended} =
                              Tx.execute(tx, "INSERT INTO d VALUES (2)")

                     assert {:error, :transaction_ended} =
                              Db.script(tx.conn, "PRAGMA user_version = 5")

                     assert {:error, :transaction_ended} = Db.run(tx.conn, "SELECT 1", [])
                     :returned
                   end,
                   limits: limits
                 ),
               inspect(name)

        # The check ended with the transaction: the connection is usable, and
        # nothing of the function is left.
        assert {:ok, %{rows: [[0]]}} = Db.run(cell, "SELECT a FROM d", []), inspect(name)
        assert {:ok, %{rows: [[0]]}} = Db.run(cell, "PRAGMA user_version", []), inspect(name)
        assert {:ok, %{rows: [[1]]}} = Db.run(cell, "PRAGMA query_only", []), inspect(name)
      end
    end

    test "ROLLBACK after the transaction ended is harmless, and a raise ends the check",
         %{cell: cell} do
      assert {:error, {:raised, %RuntimeError{}, _}} =
               Db.transaction(cell, 1, fn _tx -> raise "boom" end)

      assert {:ok, %{rows: [[1]]}} = Db.run(cell, "PRAGMA query_only", [])
      assert {:ok, %{rows: [[0]]}} = Db.run(cell, "SELECT count(*) FROM sqlite_master", [])
    end

    test "a handle works only inside its transaction", %{cell: cell} do
      {:ok, tx} = Db.transaction(cell, 2, & &1)
      assert {:error, :inactive_transaction} = Tx.query(tx, "SELECT 1")
    end

    test "script runs several statements and user_version reads the pragma", %{cell: cell} do
      assert {:ok, 0} = Db.user_version(cell)
      :ok = Db.writable_script(cell, "PRAGMA user_version = 4; SELECT 1")
      assert {:ok, 4} = Db.user_version(cell)
      assert {:error, _} = Db.writable_script(cell, "SELECT * FROM nope")
    end
  end

  describe "NUL bytes and scripts" do
    setup %{conn: conn} do
      :ok = Sqlite.execute(conn, "CREATE TABLE u (a)")
    end

    test "a NUL byte is refused before anything runs", %{conn: conn} do
      text = "INSERT INTO u VALUES (1)\0; DELETE FROM u"
      assert {:error, :nul_in_statement} = Sqlite.run(conn, text, [])
      assert {:error, :nul_in_statement} = Sqlite.run(conn, "SELECT 1\0", [])
      assert {:error, :nul_in_statement} = Sqlite.execute(conn, text)
      assert {:error, :nul_in_statement} = Sqlite.script(conn, "CREATE TABLE z (a)\0")
      assert {:ok, %{rows: [[0]]}} = Sqlite.run(conn, "SELECT count(*) FROM u", [])
      assert {:error, _} = Sqlite.run(conn, "SELECT * FROM z", [])
    end

    test "a script runs schema statements as SQLite delimits them", %{conn: conn} do
      bom = <<0xEF, 0xBB, 0xBF>>

      script =
        "CREATE TABLE s AS SELECT $a(') AS x; CREATE TABLE e (end, x);\n" <>
          "CREATE TRIGGER g AFTER INSERT ON e BEGIN UPDATE e SET x = new.end; SELECT 1;#{bom}END;" <>
          "#{bom}-- c\n/* c */ DROP TABLE s;#{bom}"

      assert :ok = Sqlite.script(conn, script)

      assert {:ok, %{rows: [["e"]]}} =
               Sqlite.run(conn, "SELECT name FROM sqlite_master WHERE name = 'e'", [])

      assert {:error, _} = Sqlite.run(conn, "SELECT * FROM s", [])
    end

    test "a statement that is not schema is refused before it runs, and nothing after it", %{
      conn: conn
    } do
      script =
        "CREATE TABLE s AS SELECT $a(') AS x; INSERT INTO u VALUES (7); CREATE TABLE after_ (a);\n--'"

      assert {:error, {:not_ddl, "INSERT INTO u VALUES (7);"}} = Sqlite.script(conn, script)
      assert {:ok, %{rows: [[0]]}} = Sqlite.run(conn, "SELECT count(*) FROM u", [])
      assert {:error, _} = Sqlite.run(conn, "SELECT * FROM after_", [])
    end

    test "a script refuses transaction control", %{conn: conn} do
      :ok = Sqlite.execute(conn, "BEGIN")

      for control <- ["COMMIT", "END", "ROLLBACK", "BEGIN"] do
        assert {:error, :not_authorized} =
                 Sqlite.script(conn, "DROP TABLE IF EXISTS q; #{control}")
      end

      assert {:ok, :transaction} = Sqlite.transaction_status(conn)
      :ok = Sqlite.execute(conn, "ROLLBACK")
    end

    test "while a transaction of the library is open nothing can end it", %{conn: conn} do
      {:ok, token} = Sqlite.begin_transaction(conn)

      for control <- ["COMMIT", "END", "ROLLBACK", "BEGIN"] do
        assert {:error, :not_authorized} = Sqlite.execute(conn, control)
        assert {:error, :not_authorized} = Sqlite.run(conn, control, [])
      end

      assert {:error, :not_authorized} = Sqlite.begin_transaction(conn)
      assert {:error, :not_authorized} = Sqlite.finish_transaction(conn, token + 1, :commit)
      assert {:error, :not_authorized} = Sqlite.finish_transaction(conn, token + 1, :rollback)
      assert {:ok, :transaction} = Sqlite.transaction_status(conn)
      assert :ok = Sqlite.execute(conn, "SAVEPOINT s; RELEASE s")

      assert :ok = Sqlite.finish_transaction(conn, token, :commit)
      assert {:ok, :idle} = Sqlite.transaction_status(conn)
      assert :ok = Sqlite.execute(conn, "BEGIN; ROLLBACK")
    end

    test "a checkpoint is refused while a transaction of the library is open", %{conn: conn} do
      {:ok, _} = Sqlite.run(conn, "PRAGMA journal_mode = WAL", [])
      {:ok, token} = Sqlite.begin_transaction(conn)
      :ok = Sqlite.execute(conn, "INSERT INTO u VALUES (1)")

      assert {:error, :not_authorized} = Sqlite.checkpoint(conn)
      assert {:ok, :transaction} = Sqlite.transaction_status(conn)

      assert :ok = Sqlite.finish_transaction(conn, token, :commit)
      assert :ok = Sqlite.checkpoint(conn)
    end

    test "a transaction that SQLite ended is not committed, and a rollback of it is ok", %{
      conn: conn
    } do
      :ok = Sqlite.execute(conn, "CREATE TABLE k (a PRIMARY KEY); INSERT INTO k VALUES (1)")
      {:ok, token} = Sqlite.begin_transaction(conn)
      :ok = Sqlite.execute(conn, "INSERT INTO u VALUES (1)")
      {:error, _} = Sqlite.execute(conn, "INSERT OR ROLLBACK INTO k VALUES (1)")
      assert {:ok, :idle} = Sqlite.transaction_status(conn)

      assert {:error, :transaction_ended} = Sqlite.execute(conn, "INSERT INTO u VALUES (2)")
      assert {:error, :transaction_ended} = Sqlite.finish_transaction(conn, token, :commit)
      assert :ok = Sqlite.finish_transaction(conn, token, :rollback)
      assert :ok = Sqlite.execute(conn, "INSERT INTO u VALUES (3)")
    end

    test "the WAL moves only when a commit reaches it", %{conn: conn} do
      {:ok, _} = Sqlite.run(conn, "PRAGMA journal_mode = WAL", [])
      {:ok, token} = Sqlite.begin_transaction(conn)
      :ok = Sqlite.execute(conn, "INSERT INTO u VALUES (1)")
      refute Sqlite.wal_advanced?(conn)
      :ok = Sqlite.finish_transaction(conn, token, :commit)
      assert Sqlite.wal_advanced?(conn)

      {:ok, token} = Sqlite.begin_transaction(conn)
      refute Sqlite.wal_advanced?(conn)
      :ok = Sqlite.finish_transaction(conn, token, :rollback)
    end
  end
end
