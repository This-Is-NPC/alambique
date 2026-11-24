defmodule Alambique.ForeignWriterTest do
  @moduledoc """
  A second connection on the file of a cell (design §15): refused when the
  library can see it, found by the wal-index header when it cannot. The
  writers here are what the table of held files cannot see: a connection
  opened past it (`Alambique.Foreign`), as another process would.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Feed, Foreign, Sqlite, Tx}
  alias Alambique.Cell.Db
  alias Alambique.Replication.Native

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @page 4096
  @frame @page + 24

  setup do
    setup_store()
  end

  defp boot(env, mode, opts \\ []) do
    inst =
      start_instance(
        env,
        Keyword.merge([migrations: @schema, sql: [orphan_frames: mode], compaction: false], opts)
      )

    cell = unique_cell()
    for n <- [0, 1], do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (#{n})")
    %{inst: inst, cell: cell, env: env, mode: mode}
  end

  defp pid(%{inst: inst, cell: cell}), do: Cell.whereis(Config.fetch!(inst), cell)
  defp state(c), do: :sys.get_state(pid(c))
  defp path(c), do: state(c).repl.db_path
  defp conn_ref(c), do: Sqlite.ref(state(c).conn)

  defp rows(c) do
    {:ok, %{rows: rows}} = Alambique.query(c.inst, c.cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end

  defp store(c),
    do: c.env |> durable_rows(c.cell, "SELECT n FROM t ORDER BY n") |> List.flatten()

  defp asleep?(c), do: eventually(fn -> Alambique.owner(c.inst, c.cell) == :none end, 30_000)

  defp feed_now(c) do
    ctx = Feed.ctx(store: c.env.store, prefix: "cells-test", settle_ms: 0)
    {:ok, records, _} = Feed.read(ctx, nil, 10_000)
    for r <- records, r.cell == c.cell, r.table == "t", do: r.key["n"]
  end

  defp wal_frames(file) do
    case File.stat(file <> "-wal") do
      {:ok, %{size: size}} when size > 32 -> div(size - 32, @frame)
      _ -> 0
    end
  end

  defp exec!(c, sql), do: 0 = Foreign.exec(path(c), sql)

  # New frames until the WAL holds `known`: a row takes up to three (its
  # page and those of the feed), a change of `user_version` takes one.
  defp fill(c, known, n) do
    case known - wal_frames(path(c)) do
      0 ->
        :ok

      missing when missing >= 3 ->
        exec!(c, "INSERT INTO t VALUES (#{n})")
        fill(c, known, n + 1)

      _ ->
        exec!(c, "PRAGMA user_version = #{n}")
        fill(c, known, n + 1)
    end
  end

  # The frames are gone from the WAL, or cannot be trusted: the cell stops
  # whatever `orphan_frames` says, and the next wake has what the store held.
  defp assert_stopped_at_the_cut(c) do
    assert Alambique.position(c.inst, c.cell) == {:error, :orphan_frames}
    assert asleep?(c)
    assert store(c) == [0, 1]
    assert feed_now(c) == [0, 1]
    assert rows(c) == [0, 1]
    assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
    assert store(c) == [0, 1, 2]
  end

  describe "a foreign commit with the frames still in the WAL" do
    test "is dropped at the next cut under :stop", env do
      c = boot(env, :stop)
      exec!(c, "INSERT INTO t VALUES (50)")
      assert rows(c) == [0, 1, 50]
      assert store(c) == [0, 1]

      assert Alambique.position(c.inst, c.cell) == {:error, :orphan_frames}
      assert asleep?(c)
      assert store(c) == [0, 1]
      assert feed_now(c) == [0, 1]
      assert rows(c) == [0, 1]
    end

    test "is replicated at the next cut under :replicate", env do
      c = boot(env, :replicate)
      exec!(c, "INSERT INTO t VALUES (50)")

      assert {:ok, _} = Alambique.position(c.inst, c.cell)
      assert store(c) == [0, 1, 50]
      assert Enum.count(eventually(fn -> feed_now(c) end), &(&1 == 50)) == 1
      assert rows(c) == [0, 1, 50]

      # The header is accounted for: the cell goes on.
      assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
      assert store(c) == [0, 1, 2, 50]
      assert {:ok, _} = Alambique.position(c.inst, c.cell)
    end

    test "found by the write that follows: it does not run, under :stop", env do
      c = boot(env, :stop)
      exec!(c, "INSERT INTO t VALUES (50)")

      assert Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (7)") ==
               {:error, :orphan_frames}

      assert asleep?(c)
      assert store(c) == [0, 1]
      assert feed_now(c) == [0, 1]
      assert rows(c) == [0, 1]
    end

    test "found by the write that follows: the frames are replicated, then it runs, under :replicate",
         env do
      c = boot(env, :replicate)
      exec!(c, "INSERT INTO t VALUES (50)")

      assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (7)")
      assert store(c) == [0, 1, 7, 50]
      assert rows(c) == [0, 1, 7, 50]
      assert Enum.count(eventually(fn -> feed_now(c) end), &(&1 == 50)) == 1
    end

    for mode <- [:stop, :replicate] do
      test "a commit right after the check is refused by the write lock (#{mode})", env do
        c = boot(env, unquote(mode))
        :ok = Native.set_foreign_commit(conn_ref(c), path(c), "INSERT INTO t VALUES (60)")

        assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (7)")

        assert rows(c) == [0, 1, 7]
        assert store(c) == [0, 1, 7]
        assert Enum.count(eventually(fn -> feed_now(c) end), &(&1 == 7)) == 1
        refute 60 in feed_now(c)
      end
    end
  end

  describe "a foreign checkpoint that empties the WAL" do
    for mode <- [:stop, :replicate] do
      test "after an insert, over a WAL of no frames (#{mode})", env do
        c = boot(env, unquote(mode))
        assert Alambique.checkpoint(c.inst, c.cell) == :ok
        assert wal_frames(path(c)) == 0

        exec!(c, "INSERT INTO t VALUES (50); PRAGMA wal_checkpoint(TRUNCATE)")

        # The count of frames is what it was, and the row is in the file.
        assert wal_frames(path(c)) == 0
        assert rows(c) == [0, 1, 50]
        assert_stopped_at_the_cut(c)
      end

      test "after an insert, with as many frames as before (#{mode})", env do
        c = boot(env, unquote(mode))
        known = wal_frames(path(c))
        assert known > 0

        exec!(c, "INSERT INTO t VALUES (50); PRAGMA wal_checkpoint(TRUNCATE)")

        fill(c, known, 51)

        assert wal_frames(path(c)) == known
        assert 50 in rows(c)
        assert_stopped_at_the_cut(c)
      end

      test "found by the write that follows: it does not run (#{mode})", env do
        c = boot(env, unquote(mode))
        exec!(c, "INSERT INTO t VALUES (50); PRAGMA wal_checkpoint(TRUNCATE)")

        assert Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (7)") ==
                 {:error, :orphan_frames}

        assert asleep?(c)
        assert store(c) == [0, 1]
        assert feed_now(c) == [0, 1]
        assert rows(c) == [0, 1]
      end

      test "found when the cell is asked to hibernate (#{mode})", env do
        c = boot(env, unquote(mode))
        exec!(c, "INSERT INTO t VALUES (50); PRAGMA wal_checkpoint(TRUNCATE)")

        assert Alambique.hibernate(c.inst, c.cell) == {:error, :orphan_frames}
        assert asleep?(c)
        assert store(c) == [0, 1]
        assert rows(c) == [0, 1]
      end
    end
  end

  describe "a header that cannot be read" do
    for mode <- [:stop, :replicate] do
      test "counts as a foreign writer at the next cut (#{mode})", env do
        c = boot(env, unquote(mode))
        :ok = Native.set_header_unreadable(conn_ref(c), true)
        assert_stopped_at_the_cut(c)
      end

      test "counts as a foreign writer at the begin of a write (#{mode})", env do
        c = boot(env, unquote(mode))
        :ok = Native.set_header_unreadable(conn_ref(c), true)

        assert Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (7)") ==
                 {:error, :orphan_frames}

        assert asleep?(c)
        assert store(c) == [0, 1]
        assert rows(c) == [0, 1]
      end
    end
  end

  describe "the baseline of a connection" do
    @describetag :tmp_dir

    # The WAL that a closed connection leaves is not a writer.
    test "a file whose WAL holds frames at open is not a foreign write", %{tmp_dir: dir} do
      file = Path.join(dir, "cell.db")
      {:ok, conn} = Db.open(file)
      {:ok, _} = Db.transaction(conn, 1, &create_and_insert/1)
      :ok = Sqlite.close(conn)
      assert File.stat!(file <> "-wal").size > 0

      {:ok, conn} = Db.open(file)
      assert Sqlite.foreign_write(conn) == :none
      assert {:ok, _} = Db.transaction(conn, 2, &insert/1)
      assert Sqlite.foreign_write(conn) == :none
      :ok = Sqlite.checkpoint(conn)
      assert Sqlite.foreign_write(conn) == :none
      assert {:ok, _} = Db.transaction(conn, 3, &insert/1)
      Sqlite.close(conn)
    end

    test "a connection that opens a held file read-write is refused, and the answer is the same asleep",
         %{tmp_dir: dir} do
      file = Path.join(dir, "cell.db")
      {:ok, conn} = Db.open(file)
      assert Db.open(file) == {:error, :held_by_cell}
      Sqlite.close(conn)
      assert {:ok, again} = Db.open(file)
      Sqlite.close(again)
    end

    defp create_and_insert(tx) do
      {:ok, _} = Tx.execute(tx, "CREATE TABLE IF NOT EXISTS t (n INTEGER PRIMARY KEY)", [])
      insert(tx)
    end

    defp insert(tx), do: {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (NULL)", [])
  end

  describe "a second read-write connection on the file of an awake cell" do
    @describetag :tmp_dir

    test "is refused through the file, a hard link, no VFS, a VFS URI; reading is not", env do
      c = boot(env, :stop)
      file = path(c)
      link = file <> ".link"
      :ok = File.ln(file, link)

      assert Sqlite.open(file) == {:error, :held_by_cell}
      assert Sqlite.open(file, vfs: :alambique) == {:error, :held_by_cell}
      assert Sqlite.open(link) == {:error, :held_by_cell}
      assert Sqlite.open("file:#{file}?vfs=alambique") == {:error, :held_by_cell}
      assert Sqlite.open("file:#{link}?vfs=unix") == {:error, :held_by_cell}

      assert {:ok, reader} = Sqlite.open(file, readonly: true)
      assert {:ok, %{rows: [[2]]}} = Sqlite.run(reader, "SELECT count(*) FROM t", [])
      Sqlite.close(reader)

      assert rows(c) == [0, 1]

      # Asleep, the file is free again.
      assert Alambique.hibernate(c.inst, c.cell) == :ok
      assert asleep?(c)
      assert {:ok, conn} = Sqlite.open(file)
      Sqlite.close(conn)
      assert {:ok, conn} = Sqlite.open(link)
      Sqlite.close(conn)
    end

    # The VFS refuses it when it opens the decoded name.
    test "ATTACH through an escaped URI is refused by the VFS", %{tmp_dir: dir} = env do
      c = boot(env, :stop)
      escaped = String.replace(path(c), "/", "%2F")
      {:ok, other} = Sqlite.open(Path.join(dir, "other.db"), attach: true, vfs: :alambique)

      assert {:error, _} =
               Sqlite.execute(
                 other,
                 "ATTACH DATABASE 'file:#{escaped}' AS cell; CREATE TABLE cell.u (a)"
               )

      Sqlite.close(other)
      assert rows(c) == [0, 1]
      assert {:ok, _} = Alambique.position(c.inst, c.cell)
    end

    # The spelling of the name must not matter: SQLite decodes a `file:` URI
    # before it opens the file, and the file is what is held.
    defp attach(%{tmp_dir: dir}, name, c) do
      {:ok, other} = Sqlite.open(Path.join(dir, "other.db"), attach: true)

      result =
        with :ok <-
               Sqlite.execute(other, "ATTACH DATABASE '#{name}' AS x",
                 no_transaction_control: true
               ) do
          {:attached, Sqlite.execute(other, "CREATE TABLE x.u (a)")}
        end

      Sqlite.close(other)
      assert rows(c) == [0, 1]
      result
    end

    defp escape(string), do: URI.encode(string, &URI.char_unreserved?/1)
    defp escape_one(string), do: String.replace(string, "c", "%63", global: false)

    test "ATTACH of the file through the default VFS is refused in every spelling of its name",
         %{tmp_dir: dir} = env do
      c = boot(env, :stop)
      file = path(c)
      link = file <> ".link"
      symlink = file <> ".symlink"
      :ok = File.ln(file, link)
      :ok = File.ln_s(file, symlink)
      dots = String.duplicate("../", length(Path.split(File.cwd!())) - 1)
      relative = dots <> String.trim_leading(file, "/")

      refused = [
        file,
        "file:" <> file,
        "file:" <> escape(file),
        "file:" <> escape_one(file),
        "file:" <> escape(file) <> "?cache=shared",
        "file://localhost" <> file,
        "file://localhost" <> escape(file),
        "file:" <> file <> "%00junk",
        "file:" <> file <> "?mode=rw",
        "file:" <> file <> "?mode=ro&mode=rw",
        "file:" <> file <> "?immutable=0&immutable=1",
        "file:" <> file <> "?immutable=0",
        relative,
        "file:" <> relative,
        "file:" <> escape(relative),
        link,
        "file:" <> escape(link),
        symlink,
        "file:" <> escape(symlink)
      ]

      for name <- refused do
        result = attach(%{tmp_dir: dir}, name, c)
        assert match?({:error, _}, result), "#{name} gave #{inspect(result)}"
      end

      assert {:ok, _} = Alambique.position(c.inst, c.cell)
    end

    test "ATTACH of the file through the default VFS stays allowed read-only",
         %{tmp_dir: dir} = env do
      c = boot(env, :stop)
      file = path(c)

      for name <- [
            "file:" <> file <> "?mode=ro",
            "file:" <> escape(file) <> "?mode=ro",
            "file:" <> escape(file) <> "?mode=%72o",
            "file:" <> file <> "?mode=rw&mode=ro",
            "file:" <> file <> "?immutable=1",
            "file://localhost" <> file <> "?immutable=1"
          ] do
        result = attach(%{tmp_dir: dir}, name, c)
        assert match?({:attached, {:error, _}}, result), "#{name} gave #{inspect(result)}"
      end
    end

    for vfs <- [:alambique, :default] do
      test "ATTACH of the file from another connection is refused (#{vfs})",
           %{tmp_dir: dir} = env do
        c = boot(env, :stop)
        file = path(c)
        opts = if unquote(vfs) == :alambique, do: [vfs: :alambique], else: []
        {:ok, other} = Sqlite.open(Path.join(dir, "other.db"), [attach: true] ++ opts)

        assert {:error, _} =
                 Sqlite.execute(
                   other,
                   "ATTACH DATABASE '#{file}' AS cell; CREATE TABLE cell.u (a)"
                 )

        # A read-only ATTACH is not a writer.
        assert :ok =
                 Sqlite.execute(other, "ATTACH DATABASE 'file:#{file}?mode=ro' AS ro",
                   no_transaction_control: true
                 )

        Sqlite.close(other)
        assert rows(c) == [0, 1]
        assert {:ok, _} = Alambique.position(c.inst, c.cell)
      end
    end
  end

  # A wake reuses the local file only when its marker names the head of the
  # store, and that file is closed and checkpointed; a restore removes any
  # `-wal`. So a wake reaches `Db.open` with frames only when a WAL was left
  # beside a file that equals the store: a crash that recovered a WAL, which
  # the test makes by keeping the files of a live connection.
  describe "a wake whose local file has frames in its WAL" do
    for mode <- [:stop, :replicate] do
      test "is not judged a foreign write (#{mode})", env do
        c = boot(env, unquote(mode))
        file = path(c)
        assert Alambique.hibernate(c.inst, c.cell) == :ok
        assert asleep?(c)
        leave_frames(file, 60)
        assert wal_frames(file) > 0

        # The wake reuses the file and recovers its WAL.
        assert {:ok, _} = Alambique.touch(c.inst, c.cell)
        awake = pid(c)
        assert File.exists?(file <> "-wal")

        assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
        assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (3)")
        assert pid(c) == awake
        assert {:ok, _} = Alambique.position(c.inst, c.cell)
        assert pid(c) == awake

        assert rows(c) == [0, 1, 2, 3, 60]
        assert store(c) == rows(c)
        assert Enum.sort(feed_now(c)) == rows(c)
      end
    end

    # A committed row in a WAL beside the database file as it was before: the
    # state a crash leaves, made from the files of a live connection.
    defp leave_frames(file, n) do
      {:ok, other} = Foreign.open(file)
      :ok = Sqlite.execute(other, "INSERT INTO t VALUES (#{n})")
      for part <- ["", "-wal"], do: File.cp!(file <> part, file <> part <> ".copy")
      Sqlite.close(other)

      for part <- ["", "-wal"] do
        File.rename!(file <> part <> ".copy", file <> part)
      end

      File.rm(file <> "-shm")
    end
  end
end
