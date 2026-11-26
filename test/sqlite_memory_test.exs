defmodule Alambique.SqliteMemoryTest do
  @moduledoc """
  `Alambique.Sqlite.memory/1` and the bound of `PRAGMA cache_size`.

  Not async: the memory counters are those of the whole process, so a test
  that reads them needs it to itself. Each connection has a page cache of its
  own, so `cache_size` is a hard limit of that connection (design §7,
  `cache_kib`); a pooled cache would let it hold more while the others hold less.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 1]

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

  defp memory_used, do: Alambique.NativeStats.read().sqlite_memory_used

  test "lowering cache_size frees the pages of that connection, all of their memory", %{
    tmp_dir: dir
  } do
    {:ok, conn} = Sqlite.open(Path.join(dir, "shrink.db"))
    on_exit(fn -> Sqlite.close(conn) end)
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = -2048")
    :ok = Sqlite.execute(conn, "CREATE TABLE m (a, b)")
    insert_rows(conn)
    {:ok, _} = Sqlite.run(conn, "SELECT sum(length(b)) FROM m", [])
    {:ok, %{cache_used: full}} = Sqlite.memory(conn)
    assert full > 1_000_000
    held = memory_used()

    # The pool of a shared cache would keep the pages while it is not full.
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = -64")
    {:ok, %{cache_used: small}} = Sqlite.memory(conn)
    # The limit is of pages with their headers, so a page more or less.
    assert small <= 80_000
    # What the shrink evicts leaves SQLite's accounting too: a cache that kept
    # its pages in bulk blocks would free none of them.
    assert held - memory_used() >= (full - small) * 9 / 10
  end

  test "a cache of a few pages is freed whole", %{tmp_dir: dir} do
    {:ok, conn} = Sqlite.open(Path.join(dir, "few.db"))
    on_exit(fn -> Sqlite.close(conn) end)
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = -160")
    :ok = Sqlite.execute(conn, "CREATE TABLE m (a, b)")

    :ok =
      Sqlite.execute(
        conn,
        "INSERT INTO m SELECT 1, zeroblob(3000) FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)"
      )

    {:ok, _} = Sqlite.run(conn, "SELECT sum(length(b)) FROM m", [])
    {:ok, %{cache_used: used}} = Sqlite.memory(conn)
    assert used > 0
    held = memory_used()
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = 0")
    {:ok, %{cache_used: left}} = Sqlite.memory(conn)
    assert held - memory_used() >= (used - left) * 9 / 10
  end

  test "memory/1 does not wait for a statement that is running", %{tmp_dir: dir} do
    {:ok, conn} = Sqlite.open(Path.join(dir, "busy.db"))
    on_exit(fn -> Sqlite.close(conn) end)

    runner =
      Task.async(fn ->
        receive do
          :go ->
            Sqlite.run(
              conn,
              "WITH RECURSIVE c(n) AS " <>
                "(SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 2000000000) " <>
                "SELECT count(*) FROM c",
              []
            )
        end
      end)

    :ok = Sqlite.assign(conn, runner.pid)
    send(runner.pid, :go)

    # Each call returns at once; one lands while the statement holds the connection.
    eventually(fn -> Sqlite.memory(conn) == {:error, :busy} end)

    :ok = Sqlite.interrupt(conn)
    assert {:error, _} = Task.await(runner, 30_000)
    assert {:ok, %{cache_used: _}} = Sqlite.memory(conn)
  end
end
