defmodule Alambique.SqliteMemoryTest do
  @moduledoc """
  `Alambique.Sqlite.memory/1` and the bound of `PRAGMA cache_size`.

  Not async: the bundled SQLite is built with `SQLITE_ENABLE_MEMORY_MANAGEMENT`,
  which makes every connection of the OS process share one page cache pool. The
  limit is the sum of the connections' `cache_size`, and a connection can hold
  more than its own while the others hold less. A test that measures one
  connection needs the process to itself (design §7, `cache_kib`).
  """
  use ExUnit.Case, async: false

  alias Alambique.Sqlite

  @moduletag :tmp_dir

  defp insert_rows(conn) do
    :ok =
      Sqlite.execute(
        conn,
        """
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 2000)
          INSERT INTO m SELECT i, zeroblob(500) FROM s
        """
      )
  end

  test "memory/1 reports the page cache, bounded by cache_size", %{tmp_dir: dir} do
    {:ok, conn} = Sqlite.open(Path.join(dir, "t.db"))
    on_exit(fn -> Sqlite.close(conn) end)
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = -64")
    :ok = Sqlite.execute(conn, "CREATE TABLE m (a, b)")
    assert {:ok, %{cache_used: empty, schema_used: schema}} = Sqlite.memory(conn)
    assert schema > 0

    insert_rows(conn)
    {:ok, _} = Sqlite.run(conn, "SELECT sum(length(b)) FROM m", [])

    assert {:ok, %{cache_used: used, stmt_used: 0, lookaside_used: 0}} = Sqlite.memory(conn)
    assert used > empty

    # Compare with the default cache: its absolute size varies by build and run.
    {:ok, default} = Sqlite.open(Path.join(dir, "default.db"))
    :ok = Sqlite.execute(default, "CREATE TABLE m (a, b)")
    insert_rows(default)
    {:ok, _} = Sqlite.run(default, "SELECT sum(length(b)) FROM m", [])
    assert {:ok, %{cache_used: default_used}} = Sqlite.memory(default)
    :ok = Sqlite.close(default)

    assert used * 4 < default_used
  end
end
