defmodule Alambique.ReplicationHelpers do
  @moduledoc "SQLite workloads, database dumps and contexts for replication tests."

  alias Alambique.Replication
  alias Alambique.Replication.Replicator
  alias Alambique.Sqlite
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @start_ms 1_700_000_000_000

  def unique_prefix(label),
    do: "#{label}-" <> Base.url_encode64(:crypto.strong_rand_bytes(9), padding: false)

  @doc "A replication context over `store` with a manual clock; fast retries."
  def ctx(store, world, opts \\ []) do
    Replication.ctx(
      store: store,
      prefix: Keyword.get(opts, :prefix, "repl-test"),
      node: Keyword.get(opts, :node, "node-a"),
      clock: {ManualClock, ManualClock.config(world, 0)},
      backoff_ms: 1,
      max_backoff_ms: 4,
      timeout_ms: Keyword.get(opts, :timeout_ms, 40)
    )
  end

  def new_world, do: ManualClock.new(@start_ms)

  def start_store(),
    do:
      with(
        {:ok, pid} <- MemoryStore.start_link(),
        do: {pid, {MemoryStore, MemoryStore.config(pid)}}
      )

  @doc "Opens a database in WAL mode, where `VACUUM` is allowed."
  def open_db(path, page_size \\ 4096) do
    {:ok, conn} = Sqlite.open(path, attach: true)
    :ok = Sqlite.execute(conn, "PRAGMA page_size = #{page_size}")
    {:ok, _} = pragma(conn, "PRAGMA journal_mode = WAL")
    :ok = Sqlite.execute(conn, "PRAGMA synchronous = NORMAL")
    :ok = Sqlite.execute(conn, "CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v BLOB)")
    conn
  end

  def pragma(conn, sql) do
    {:ok, %{rows: rows}} = Sqlite.run(conn, sql, [])
    {:ok, rows}
  end

  def rows(conn, sql) do
    {:ok, %{rows: rows}} = Sqlite.run(conn, sql, [])
    rows
  end

  @doc "Every table's rows plus the schema, as a comparable term."
  def dump(conn) do
    schema = rows(conn, "SELECT type, name, sql FROM sqlite_master ORDER BY name")

    tables =
      for ["table", name, _] <- schema, into: %{} do
        {name, rows(conn, ~s(SELECT * FROM "#{name}" ORDER BY 1))}
      end

    {schema, tables}
  end

  @doc "Dump of the database file at `path`, read-only."
  def dump_file(path) do
    {:ok, conn} = Sqlite.open(path, readonly: true)
    result = dump(conn)
    Sqlite.close(conn)
    result
  end

  @doc "Applies one workload operation."
  def run_op(conn, {:put, k, n}),
    do: Sqlite.execute(conn, "INSERT OR REPLACE INTO kv (k, v) VALUES (#{k}, randomblob(#{n}))")

  def run_op(conn, {:del, k}), do: Sqlite.execute(conn, "DELETE FROM kv WHERE k = #{k}")

  def run_op(conn, {:txn, ops}) do
    :ok = Sqlite.execute(conn, "BEGIN")
    Enum.each(ops, &run_op(conn, &1))
    Sqlite.execute(conn, "COMMIT")
  end

  def run_op(conn, {:rollback, ops}) do
    :ok = Sqlite.execute(conn, "BEGIN")
    Enum.each(ops, &run_op(conn, &1))
    Sqlite.execute(conn, "ROLLBACK")
  end

  def run_op(conn, {:create, i}),
    do: Sqlite.execute(conn, "CREATE TABLE IF NOT EXISTS t#{i} (a INTEGER PRIMARY KEY, b TEXT)")

  def run_op(conn, {:fill, i}),
    do: Sqlite.execute(conn, "INSERT INTO t#{i} (b) SELECT hex(randomblob(50)) FROM kv LIMIT 20")

  def run_op(conn, {:drop, i}), do: Sqlite.execute(conn, "DROP TABLE IF EXISTS t#{i}")
  def run_op(conn, :vacuum), do: Sqlite.execute(conn, "VACUUM")

  def run_ops(conn, ops), do: Enum.each(ops, &run_op(conn, &1))

  @doc "Creates a table before it is filled or dropped; operations on missing tables are no-ops."
  def safe_op(conn, {:fill, i} = op) do
    run_op(conn, {:create, i})
    run_op(conn, op)
  end

  def safe_op(conn, op), do: run_op(conn, op)

  @doc "A fresh replicator for an empty store."
  def new_replicator(dir, name, epoch \\ 1, page_size \\ 4096) do
    path = Path.join(dir, name)
    conn = open_db(path, page_size)
    {conn, Replicator.new("cell", path, page_size, epoch)}
  end

  def tmp_dir, do: Alambique.TestDirs.new("repl")
end
