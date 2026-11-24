defmodule Alambique.ConnectionGateTest do
  @moduledoc """
  A gated connection reads outside the transaction of the library and writes
  only inside it or in a call with the secret (`Alambique.Sqlite`).
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Cell.Db
  alias Alambique.Sqlite

  @moduletag :tmp_dir

  # Statements by what a gated connection does with them:
  #   :read  - allowed in every state
  #   :write - refused unarmed, allowed armed and flagged
  #   :never - never run, in every state (no `attach: true`; the pragmas the library never allows)
  #   :txn   - transaction control: refused unarmed and armed, and in a user's
  #            text in every state; allowed flagged in the library's own text
  @kinds [
    {:read, "SELECT * FROM t"},
    {:read, "SELECT count(*) FROM sqlite_master"},
    {:read, "PRAGMA user_version"},
    {:read, "PRAGMA table_info(t)"},
    {:read, "PRAGMA cache_size"},
    {:read, "PRAGMA journal_mode"},
    {:write, "INSERT OR REPLACE INTO t VALUES (1, 2)"},
    {:write, "UPDATE t SET b = b"},
    {:write, "DELETE FROM t WHERE a = 99"},
    {:write, "DROP TABLE IF EXISTS u; CREATE TABLE u (x)"},
    {:write, "DROP INDEX IF EXISTS j; CREATE INDEX j ON t (a, b)"},
    {:write, "ANALYZE"},
    {:write, "REINDEX"},
    {:write, "PRAGMA optimize"},
    {:write, "PRAGMA incremental_vacuum"},
    {:write, "PRAGMA cache_size = -100"},
    {:write, "PRAGMA user_version = 3"},
    {:write, "PRAGMA foreign_keys = 0"},
    {:write, "SAVEPOINT s; RELEASE s"},
    {:never, "ATTACH ':memory:' AS x"},
    {:never, "DETACH x"},
    {:never, "VACUUM"},
    {:never, "PRAGMA wal_checkpoint"},
    {:never, "PRAGMA journal_mode = DELETE"},
    {:txn, "BEGIN; ROLLBACK"},
    {:txn, "BEGIN; COMMIT"},
    {:txn, "BEGIN; END"},
    {:txn, "BEGIN IMMEDIATE; ROLLBACK TRANSACTION"},
    {:txn, "BEGIN DEFERRED; COMMIT TRANSACTION"}
  ]

  setup %{tmp_dir: dir} do
    {:ok, conn} = Db.open(Path.join(dir, "g.db"))

    :ok =
      Db.writable_script(
        conn,
        "CREATE TABLE t (a INTEGER PRIMARY KEY, b); CREATE INDEX i ON t (b); INSERT INTO t VALUES (1, 1)"
      )

    on_exit(fn -> Db.close(conn) end)
    %{conn: conn}
  end

  # The result of `sql` on `conn` in `state`; `user?` runs it as a user's text
  # (`no_transaction_control`).
  defp attempt(conn, sql, :unarmed, user?),
    do: Sqlite.execute(conn, sql, no_transaction_control: user?)

  defp attempt(conn, sql, :flagged, user?),
    do: Sqlite.execute(conn, sql, write: true, no_transaction_control: user?)

  defp attempt(conn, sql, :armed, user?) do
    {:ok, result} =
      Db.transaction(conn, 1, fn tx ->
        Sqlite.execute(tx.conn, sql, no_transaction_control: user?)
      end)

    result
  end

  # What the table says of `kind` in `state`.
  defp allowed?(:read, _state, _user?), do: true
  defp allowed?(:write, state, _user?), do: state != :unarmed
  defp allowed?(:txn, state, user?), do: state == :flagged and not user?

  # `true` when the statement was not refused by the authorizer (it may fail
  # for another reason).
  defp ran?(result), do: result != {:error, :not_authorized}

  property "every statement kind is allowed or refused as the table says", %{conn: conn} do
    check all(
            {kind, sql} <- member_of(@kinds),
            state <- member_of([:armed, :unarmed, :flagged]),
            user? <- boolean(),
            max_runs: 300
          ) do
      result = attempt(conn, sql, state, user?)

      # Armed, SQLite itself refuses `VACUUM` before the authorizer is asked.
      if kind == :never,
        do: assert({:error, _} = result, "#{state} #{sql} gave #{inspect(result)}"),
        else:
          assert(
            ran?(result) == allowed?(kind, state, user?),
            "#{state} #{kind} #{sql} user text #{user?} gave #{inspect(result)}"
          )

      # Whatever ran, the connection is read-only again and the transaction is over.
      assert {:ok, :idle} = Sqlite.transaction_status(conn)
    end
  end

  test "what is not a plain write is refused unarmed and unflagged", %{conn: conn} do
    for sql <- [
          "PRAGMA optimize",
          "ANALYZE",
          "REINDEX",
          "VACUUM",
          "PRAGMA incremental_vacuum",
          "ATTACH ':memory:' AS x",
          "PRAGMA cache_size = -5",
          "PRAGMA query_only = OFF",
          "PRAGMA shrink_memory",
          "SAVEPOINT s",
          "BEGIN"
        ] do
      assert {:error, :not_authorized} = Sqlite.execute(conn, sql), sql
      assert {:error, :not_authorized} = Sqlite.run(conn, sql, []), sql
    end

    assert {:ok, %{rows: [[1, 1]]}} = Sqlite.run(conn, "SELECT a, b FROM t", [])
  end

  test "a flag needs the secret of this connection", %{conn: conn, tmp_dir: dir} do
    ref = Sqlite.ref(conn)
    sql = "INSERT INTO t VALUES (5, 5)"
    {:ok, other} = Db.open(Path.join(dir, "o.db"))
    guesses = [0, 1, other.key, :rand.uniform(0xFFFFFFFFFFFFFFFF)]

    for key <- guesses do
      forged = %Sqlite.Handle{ref: ref, key: key}
      assert {:error, :not_authorized} = Sqlite.execute(forged, sql, write: true)
      assert {:error, :not_authorized} = Sqlite.run(forged, sql, [], write: true)
      assert {:error, :not_authorized} = Sqlite.checkpoint(forged)
      assert {:error, :not_authorized} = Sqlite.begin_transaction(forged)
      assert {:error, :not_authorized} = Sqlite.assign(forged, self())
      assert {:error, :not_authorized} = Sqlite.close(forged)
    end

    assert {:error, :not_authorized} = Sqlite.execute(ref, sql, write: true)
    assert {:error, :not_authorized} = Sqlite.checkpoint(ref)
    assert {:error, :not_authorized} = Sqlite.close(ref)
    assert {:ok, %{rows: [[1]]}} = Sqlite.run(conn, "SELECT count(*) FROM t", [])
    Db.close(other)
  end

  test "while the transaction is open the secret does not close, begin, checkpoint or write outside it",
       %{conn: conn} do
    {:ok, results} =
      Db.transaction(conn, 1, fn _tx ->
        [
          Sqlite.close(conn),
          Sqlite.begin_transaction(conn),
          Sqlite.checkpoint(conn),
          Sqlite.execute(conn, "COMMIT", write: true),
          Sqlite.execute(conn, "ROLLBACK", write: true)
        ]
      end)

    assert Enum.all?(results, &(&1 == {:error, :not_authorized})), inspect(results)
    assert :ok = Sqlite.checkpoint(conn)
  end

  test "a plain connection is not gated", %{tmp_dir: dir} do
    {:ok, plain} = Sqlite.open(Path.join(dir, "p.db"))
    assert :ok = Sqlite.execute(plain, "CREATE TABLE t (a)")
    assert :ok = Sqlite.execute(plain, "PRAGMA optimize")
    Sqlite.close(plain)
  end
end
