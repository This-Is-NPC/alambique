defmodule Alambique.NativeStatsTest do
  @moduledoc "The native counters and gauges match known workloads (design §2)."
  # The counters are node-wide, so every check is a difference between two
  # reads; the module runs after the async ones and alone.
  use ExUnit.Case, async: false

  import Alambique.ReplicationHelpers, only: [open_db: 1]

  alias Alambique.NativeStats
  alias Alambique.Replication.Native
  alias Alambique.Sqlite

  @moduletag :tmp_dir

  defp stats, do: NativeStats.read()

  defp balance(s, side), do: s[:"#{side}_alloc_bytes"] - s[:"#{side}_free_bytes"]

  defp open!(dir, name, opts \\ []) do
    {:ok, conn} = Sqlite.open(Path.join(dir, name), opts)
    conn
  end

  test "open connections and files are gauges", %{tmp_dir: dir} do
    before = stats()
    conns = for i <- 1..5, do: open!(dir, "c#{i}.db", vfs: :alambique)
    for conn <- conns, do: :ok = Sqlite.execute(conn, "CREATE TABLE t (x)")

    open = stats()
    assert open.connections_open - before.connections_open == 5
    assert open.files_open - before.files_open == 5

    Enum.each(conns, &Sqlite.close/1)
    closed = stats()
    assert closed.connections_open == before.connections_open
    assert closed.files_open == before.files_open
  end

  test "a connection that is garbage collected is closed", %{tmp_dir: dir} do
    before = stats()
    open!(dir, "gc.db", vfs: :alambique)
    :erlang.garbage_collect()
    assert eventually(fn -> stats().connections_open == before.connections_open end)
    assert stats().files_open == before.files_open
  end

  test "SQLite memory comes from the BEAM and balances after close" do
    before = stats()
    total = :erlang.memory(:total)

    # An in-memory database lives in SQLite's heap: 16 MB of pages.
    {:ok, conn} = Sqlite.open(":memory:")
    :ok = Sqlite.execute(conn, "CREATE TABLE t (x)")

    :ok =
      Sqlite.execute(
        conn,
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 16) " <>
          "INSERT INTO t SELECT zeroblob(1000000) FROM n"
      )

    open = stats()
    assert open.sqlite_memory_used - before.sqlite_memory_used >= 16_000_000
    assert open.sqlite_memory_highwater >= open.sqlite_memory_used
    assert :erlang.memory(:total) - total >= 16_000_000

    # SQLite's accounting and ours agree, and nothing is left after close.
    assert balance(open, :sqlite) - balance(before, :sqlite) ==
             open.sqlite_memory_used - before.sqlite_memory_used

    Sqlite.close(conn)
    closed = stats()
    assert closed.sqlite_memory_used == before.sqlite_memory_used
    assert balance(closed, :sqlite) == balance(before, :sqlite)
  end

  test "Rust allocations are counted and freed", %{tmp_dir: dir} do
    before = stats()
    db = Path.join(dir, "r.db")
    conn = open_db(db)
    :ok = Sqlite.execute(conn, "INSERT INTO kv VALUES (1, zeroblob(2000000))")

    {:ok, segment, _, _, _, _} =
      Native.capture_full(db, db <> "-wal", 4096, 1, 0, nil)

    Sqlite.close(conn)
    done = stats()
    assert done.rust_alloc_bytes - before.rust_alloc_bytes >= 2_000_000
    assert done.rust_free_bytes - before.rust_free_bytes >= 2_000_000
    assert byte_size(segment) > 0
  end

  test "WAL frames read match the frames in the file", %{tmp_dir: dir} do
    db = Path.join(dir, "w.db")
    conn = open_db(db)
    :ok = Sqlite.execute(conn, "INSERT INTO kv VALUES (1, zeroblob(10000))")
    :ok = Sqlite.execute(conn, "INSERT INTO kv VALUES (2, zeroblob(10000))")

    wal = db <> "-wal"
    frames = div(File.stat!(wal).size - 32, 24 + 4096)
    assert frames > 4

    before = stats()
    assert {:ok, true} = Native.wal_pending(wal, 4096, {0, 0, 0, 0, 0})
    assert stats().wal_frames_read - before.wal_frames_read == frames
    Sqlite.close(conn)
  end

  test "LTX bytes encoded and decoded equal the size of the file", %{tmp_dir: dir} do
    db = Path.join(dir, "l.db")
    conn = open_db(db)
    :ok = Sqlite.execute(conn, "INSERT INTO kv VALUES (1, randomblob(50000))")

    before = stats()
    {:ok, segment, _, _, _, _} = Native.capture_full(db, db <> "-wal", 4096, 1, 0, nil)
    assert stats().ltx_bytes_encoded - before.ltx_bytes_encoded == byte_size(segment)

    before = stats()
    assert {:ok, _, _} = Native.ltx_info(segment, true)
    assert stats().ltx_bytes_decoded - before.ltx_bytes_decoded == byte_size(segment)

    # Reading the header alone decodes nothing.
    before = stats()
    assert {:ok, _, _} = Native.ltx_info(segment, false)
    assert stats().ltx_bytes_decoded == before.ltx_bytes_decoded
    Sqlite.close(conn)
  end

  test "page cache hits and misses of a controlled query", %{tmp_dir: dir} do
    path = Path.join(dir, "p.db")
    writer = open!(dir, "p.db")
    :ok = Sqlite.execute(writer, "CREATE TABLE t (x); INSERT INTO t VALUES (zeroblob(300000))")
    Sqlite.close(writer)

    conn = open!(dir, "p.db", readonly: true)
    before = stats()
    {:ok, _} = Sqlite.run(conn, "SELECT length(x) FROM t", [])
    cold = stats()
    assert cold.page_cache_misses - before.page_cache_misses > 0

    {:ok, _} = Sqlite.run(conn, "SELECT length(x) FROM t", [])
    warm = stats()
    assert warm.page_cache_misses == cold.page_cache_misses
    assert warm.page_cache_hits - cold.page_cache_hits > 0
    Sqlite.close(conn)
    assert File.exists?(path)
  end

  test "dirty NIF calls are counted and timed", %{tmp_dir: dir} do
    before = stats()
    conn = open!(dir, "d.db")
    :ok = Sqlite.execute(conn, "CREATE TABLE t (x)")
    {:ok, _} = Sqlite.run(conn, "INSERT INTO t VALUES (1)", [])
    Sqlite.close(conn)
    done = stats()

    # open, execute, run and close; `run` is one call.
    assert done.sqlite_calls - before.sqlite_calls == 4
    assert done.sqlite_call_ns_total > before.sqlite_call_ns_total

    assert done.sqlite_call_ns_max >=
             div(done.sqlite_call_ns_total - before.sqlite_call_ns_total, 4)
  end

  test "emit sends the event and works as a poller measurement" do
    ref = make_ref()
    me = self()

    :telemetry.attach(
      {__MODULE__, ref},
      [:alambique, :native, :stats],
      &__MODULE__.forward/4,
      {me, ref}
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

    assert :ok = NativeStats.emit()
    assert_receive {^ref, measurements, %{}}
    assert Map.keys(measurements) |> Enum.sort() == Map.keys(stats()) |> Enum.sort()
    assert Enum.all?(measurements, fn {_, v} -> is_integer(v) end)

    {:ok, poller} =
      :telemetry_poller.start_link(
        measurements: [{NativeStats, :emit, []}],
        period: 20,
        name: :native_stats_test_poller
      )

    assert_receive {^ref, _, _}, 2_000
    assert_receive {^ref, _, _}, 2_000
    GenServer.stop(poller)
  end

  def forward(_event, measurements, meta, {pid, ref}), do: send(pid, {ref, measurements, meta})

  defp eventually(fun, tries \\ 100) do
    cond do
      fun.() -> true
      tries == 0 -> false
      true -> Process.sleep(20) && eventually(fun, tries - 1)
    end
  end
end
