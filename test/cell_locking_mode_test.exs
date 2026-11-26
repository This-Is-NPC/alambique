defmodule Alambique.CellLockingModeTest do
  @moduledoc """
  `sql: [locking_mode: :exclusive]` (design §15): the cell's connection takes
  SQLite's lock on the file and keeps the WAL's index in memory, so a second
  connection that honours SQLite's locks is refused up front. The lock is a
  POSIX lock: it must survive everything the library does with the file.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Feed, Replication.Native, Sqlite}

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @sqlite_busy 5

  # Another OS process asks for a lock on the range of SQLite's SHARED lock
  # (`SHARED_FIRST`, 510 bytes). A shared request is refused only while an
  # exclusive lock is held there.
  @probe """
  import fcntl, os, sys
  fd = os.open(sys.argv[1], os.O_RDWR)
  try:
      fcntl.lockf(fd, fcntl.LOCK_SH | fcntl.LOCK_NB, 510, 0x40000002, os.SEEK_SET)
      print("free")
  except OSError:
      print("held")
  """

  setup do
    setup_store()
  end

  defp boot(env, mode, opts \\ []) do
    sql = [locking_mode: mode] ++ Keyword.get(opts, :sql, [])

    inst =
      start_instance(
        env,
        [migrations: @schema, sql: sql, lazy_restore: false, compaction: false] ++
          Keyword.delete(opts, :sql)
      )

    cell = unique_cell()
    for n <- [0, 1], do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (#{n})")
    %{inst: inst, cell: cell, env: env}
  end

  defp pid(c), do: Cell.whereis(Config.fetch!(c.inst), c.cell)
  defp state(c), do: :sys.get_state(pid(c))
  defp path(c), do: state(c).path

  defp rows(c) do
    {:ok, %{rows: rows}} = Alambique.query(c.inst, c.cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end

  defp store(c), do: c.env |> durable_rows(c.cell, "SELECT n FROM t ORDER BY n") |> List.flatten()

  defp locking_mode(c) do
    {:ok, %{rows: [[mode]]}} = Alambique.query(c.inst, c.cell, "PRAGMA locking_mode")
    mode
  end

  defp lock_state(path) do
    {out, 0} = System.cmd("python3", ["-I", "-c", @probe, path])
    String.trim(out)
  end

  defp asleep?(c), do: eventually(fn -> Alambique.owner(c.inst, c.cell) == :none end)

  test "an exclusive cell keeps its WAL index in memory", env do
    c = boot(env, :exclusive)
    assert locking_mode(c) == "exclusive"
    assert File.exists?(path(c) <> "-wal")
    refute File.exists?(path(c) <> "-shm")

    n = boot(env, :normal)
    assert locking_mode(n) == "normal"
    assert File.exists?(path(n) <> "-shm")
  end

  test "another connection is refused while the cell is awake, and not after", env do
    c = boot(env, :exclusive)
    n = boot(env, :normal)

    assert Native.foreign_exec(path(c), "SELECT count(*) FROM t") == @sqlite_busy
    assert Native.foreign_exec(path(n), "SELECT count(*) FROM t") == 0

    assert :ok = Alambique.hibernate(c.inst, c.cell)
    assert Native.foreign_exec(path(c), "SELECT count(*) FROM t") == 0
  end

  test "the lock is held in the other process's eyes, through snapshots and wakes", env do
    c = boot(env, :exclusive)
    n = boot(env, :normal)
    assert lock_state(path(c)) == "held"
    assert lock_state(path(n)) == "free"

    # The snapshot of the file reads it through the connection's own file.
    snapshot = fn s ->
      Sqlite.capture_full(s.conn, s.path <> "-wal", s.repl.page_size, 1, 0, nil)
    end

    assert {:ok, _, _, _, _, pages} = in_cell(pid(c), snapshot)
    assert pages >= 2
    assert lock_state(path(c)) == "held"

    assert :ok = Alambique.hibernate(c.inst, c.cell)
    assert asleep?(c)
    assert rows(c) == [0, 1]
    assert lock_state(path(c)) == "held"
  end

  test "a statement cannot change the mode", env do
    c = boot(env, :exclusive)

    assert {:error, _} = Alambique.execute(c.inst, c.cell, "PRAGMA locking_mode = NORMAL")

    result =
      Alambique.transaction(c.inst, c.cell, fn tx ->
        case Alambique.Tx.execute(tx, "PRAGMA locking_mode = NORMAL") do
          {:ok, _} -> :changed
          {:error, reason} -> Alambique.Tx.rollback(tx, reason)
        end
      end)

    assert {:error, _} = result
    assert locking_mode(c) == "exclusive"
  end

  test "writes, reads, sleep, wake and the feed agree on an exclusive cell", env do
    c = boot(env, :exclusive)
    {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
    assert {:ok, _} = Alambique.position(c.inst, c.cell)
    assert rows(c) == [0, 1, 2]
    assert store(c) == [0, 1, 2]

    assert :ok = Alambique.checkpoint(c.inst, c.cell)
    {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (3)")
    assert :ok = Alambique.hibernate(c.inst, c.cell)
    assert asleep?(c)

    # No orphan stop on the way: the cell wakes with what the store holds.
    assert rows(c) == [0, 1, 2, 3]
    assert store(c) == [0, 1, 2, 3]

    ctx = Feed.ctx(store: env.store, prefix: "cells-test", settle_ms: 0)

    assert eventually(fn ->
             {:ok, records, _} = Feed.read(ctx, nil, 1_000)
             keys = for r <- records, r.cell == c.cell, r.table == "t", do: r.key["n"]
             if 3 in keys, do: keys
           end) == [0, 1, 2, 3]
  end

  test "a failed commit leaves nothing on an exclusive cell", env do
    c = boot(env, :exclusive)
    vfs_fault(path(c) <> "-wal", :commit_frame, :ioerr_write)

    assert {:error, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (6)")
    assert Native.vfs_fault_hits() == 1
    assert {:ok, _} = Alambique.position(c.inst, c.cell)
    assert rows(c) == [0, 1]
    assert :ok = Alambique.hibernate(c.inst, c.cell)
    assert store(c) == [0, 1]
    assert rows(c) == [0, 1]
  end

  describe "configuration" do
    defp build(env, opts) do
      Config.build(
        [
          name: :locking_mode_config,
          store: env.store,
          prefix: "locking-mode",
          data_dir: Path.join(env.dir, "locking-mode"),
          node_id: "locking-mode",
          lease_ttl_ms: 3_000,
          skew_margin_ms: 500,
          idle_timeout_ms: 60_000,
          op_timeout_ms: 2_000
        ] ++ opts
      )
    end

    test "the mode is normal or exclusive", env do
      assert build(env, []).sql.locking_mode == :normal

      assert build(env, sql: [locking_mode: :exclusive], lazy_restore: false).sql.locking_mode ==
               :exclusive

      assert_raise ArgumentError, ~r/:sql locking_mode must be one of/, fn ->
        build(env, sql: [locking_mode: :fast])
      end
    end

    test "exclusive is refused where a second connection or a closed descriptor would break it",
         env do
      assert_raise ArgumentError, ~r/does not work with feed hand_over: :sender/, fn ->
        build(env,
          sql: [locking_mode: :exclusive],
          lazy_restore: false,
          feed: [hand_over: :sender]
        )
      end

      assert_raise ArgumentError, ~r/needs lazy_restore: false/, fn ->
        build(env, sql: [locking_mode: :exclusive])
      end

      assert build(env, feed: [hand_over: :sender]).sql.locking_mode == :normal
    end
  end
end
