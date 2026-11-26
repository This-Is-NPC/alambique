defmodule Alambique.PageArenaTest do
  @moduledoc """
  The page arena through the native library: where the page-cache blocks of
  a connection go, and that the memory leaves with the connection (design §2).

  Not async: the gauges are those of the whole process. The suite runs with the
  arena on (`config/config.exs`); `ALAMBIQUE_PAGE_ARENA=0` runs it off, where
  these tests check that nothing is reported or released by it.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 1]

  alias Alambique.NativeStats
  alias Alambique.Sqlite

  @moduletag :tmp_dir

  defp stats, do: NativeStats.read()
  defp arena?(stats), do: stats.page_arena_state == 1
  defp excess(s), do: s.page_arena_resident_bytes - s.page_arena_in_use_bytes

  # A database whose connection holds about `kib` KiB of page cache.
  defp filled(dir, name, kib, opts \\ []) do
    {:ok, conn} = Sqlite.open(Path.join(dir, name), opts)
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = -#{kib * 2}")
    :ok = Sqlite.execute(conn, "CREATE TABLE m (a, b)")

    :ok =
      Sqlite.execute(
        conn,
        "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < #{div(kib, 3)}) " <>
          "INSERT INTO m SELECT i, zeroblob(3000) FROM s"
      )

    {:ok, _} = Sqlite.run(conn, "SELECT sum(length(b)) FROM m", [])
    conn
  end

  test "the state and the applied settings are reported" do
    s = stats()
    assert s.page_arena_state in 0..3
    assert s.page_arena_page_hdrsz > 0

    if arena?(s) do
      assert s.page_arena_reserved_bytes >= 64 * 1024 * 1024
      assert s.page_arena_extent_kib >= 4
    else
      refute Map.has_key?(s, :page_arena_in_use_bytes)
    end
  end

  test "the blocks of a connection are in the arena and go back when it closes", %{tmp_dir: dir} do
    before = stats()
    conn = filled(dir, "a.db", 8_192)
    open = stats()

    if arena?(open) do
      assert open.page_arena_in_use_bytes - before.page_arena_in_use_bytes >= 6_000_000
      assert open.page_arena_regions - before.page_arena_regions == 1
    end

    :ok = Sqlite.close(conn)
    closed = stats()

    if arena?(closed) do
      assert closed.page_arena_in_use_bytes == before.page_arena_in_use_bytes
      assert closed.page_arena_regions == before.page_arena_regions
    end
  end

  test "pages of 64 KiB are served by the arena", %{tmp_dir: dir} do
    {:ok, conn} = Sqlite.open(Path.join(dir, "big.db"))
    :ok = Sqlite.execute(conn, "PRAGMA page_size = 65536")
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = -8192")
    :ok = Sqlite.execute(conn, "CREATE TABLE m (a, b)")
    before = stats()

    :ok =
      Sqlite.execute(
        conn,
        "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 60) " <>
          "INSERT INTO m SELECT i, zeroblob(60000) FROM s"
      )

    open = stats()

    if arena?(open),
      do: assert(open.page_arena_in_use_bytes - before.page_arena_in_use_bytes >= 40 * 65_536)

    :ok = Sqlite.close(conn)
  end

  test "closing a connection returns its memory to the system", %{tmp_dir: dir} do
    conn = filled(dir, "close.db", 12_288)
    held = stats()
    :ok = Sqlite.close(conn)

    if arena?(held) do
      # Nothing calls the library again: the close itself wakes the trim.
      assert eventually(fn -> excess(stats()) <= 1_000_000 end)
      assert stats().page_arena_released_bytes - held.page_arena_released_bytes >= 4_000_000
    end
  end

  test "a connection that is garbage collected returns its memory too", %{tmp_dir: dir} do
    held =
      Task.async(fn ->
        _conn = filled(dir, "gc.db", 12_288)
        stats()
      end)
      |> Task.await()

    # The task ended: its connection closes with its opener, and the
    # reference is collected with the task's heap.
    if arena?(held) do
      assert eventually(fn -> excess(stats()) <= 1_000_000 end)
    end
  end

  test "an abandoned connection is trimmed by its destructor", %{tmp_dir: dir} do
    # Opened here and dropped here: no process dies, so only the destructor of
    # the resource runs when the reference is collected.
    held =
      (fn ->
         _conn = filled(dir, "drop.db", 12_288)
         stats()
       end).()

    :erlang.garbage_collect()

    if arena?(held) do
      assert eventually(fn -> excess(stats()) <= 1_000_000 end)
    end
  end

  test "2 000 connections opened and closed leave no region behind", %{tmp_dir: dir} do
    before = stats()

    for i <- 1..2_000 do
      {:ok, conn} = Sqlite.open(Path.join(dir, "r#{rem(i, 5)}.db"))
      :ok = Sqlite.execute(conn, "CREATE TABLE IF NOT EXISTS t (x)")
      :ok = Sqlite.close(conn)
    end

    after_loop = stats()
    if arena?(after_loop), do: assert(after_loop.page_arena_regions == before.page_arena_regions)
    assert after_loop.connections_open == before.connections_open
  end
end
