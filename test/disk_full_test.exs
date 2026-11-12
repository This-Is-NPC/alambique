defmodule Alambique.DiskFullTest do
  @moduledoc "A full local disk is `{:error, :disk_full}` for the caller, and the cell keeps its acknowledged data (design §6.4)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, DiskFull, Fault, Sqlite, Tx}

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  @blob "INSERT INTO t (n, v) VALUES (?, zeroblob(200000))"

  setup do
    setup_store()
  end

  # SQLite stops growing the file at this many pages: what a full disk does,
  # without filling the disk of the host.
  defp limit_pages(inst, cell, pages),
    do: {:ok, _} = Alambique.execute(inst, cell, "PRAGMA max_page_count = #{pages}")

  describe "statements" do
    test "a write that needs a page the disk lacks is :disk_full and the cell keeps serving",
         ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (1, 'kept')")
      limit_pages(inst, cell, 10)

      assert {:error, :disk_full} = Alambique.execute(inst, cell, @blob, [2])
      assert {:error, :disk_full} = Alambique.execute(inst, cell, @blob, [3])

      # Awake, with what was acknowledged and nothing of the refused writes.
      assert Cell.whereis(Config.fetch!(inst), cell)

      assert {:ok, %{rows: [[1, "kept"]]}} =
               Alambique.query(inst, cell, "SELECT n, v FROM t ORDER BY n")

      # Room again: the next write goes through and is durable.
      limit_pages(inst, cell, 1000)
      assert {:ok, _} = Alambique.execute(inst, cell, @blob, [4])
      :ok = Alambique.hibernate(inst, cell)

      assert {:ok, %{rows: [[1], [4]]}} =
               Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")
    end

    test "a statement of a transaction returns :disk_full and the transaction rolls back", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (1, 'kept')")
      limit_pages(inst, cell, 10)

      assert {:error, :disk_full} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Tx.execute(tx, "INSERT INTO t (n, v) VALUES (2, 'small')")

                 case Tx.execute(tx, @blob, [3]) do
                   {:ok, _} -> :ok
                   {:error, reason} -> Tx.rollback(tx, reason)
                 end
               end)

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t")
    end
  end

  describe "wake" do
    test "migrations that fill the disk fail the wake with :disk_full", ctx do
      inst =
        start_instance(ctx,
          migrations: [
            {1,
             "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB);" <>
               "PRAGMA max_page_count = 10;" <>
               "INSERT INTO t (n, v) VALUES (1, zeroblob(200000))"}
          ]
        )

      assert {:error, :disk_full} = Alambique.query(inst, unique_cell(), "SELECT 1")
    end

    test "a restore that cannot write the file fails the wake with :disk_full", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (1, 'kept')")
      :ok = Alambique.hibernate(inst, cell)
      File.rm_rf!(Path.join(Config.fetch!(inst).data_dir, "#{cell}.cell"))

      Fault.install(fn
        :restore_install, _ -> {:error, :enospc}
        _, _ -> :ok
      end)

      on_exit(&Fault.uninstall/0)

      assert {:error, :disk_full} = Alambique.query(inst, cell, "SELECT 1")
      Fault.uninstall()
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t")
    end
  end

  describe "the classifier" do
    test "names a full disk by its code and leaves other reasons alone" do
      for reason <- [
            :disk_full,
            :enospc,
            :edquot,
            {:restore, :enospc},
            {:migration, 3, :disk_full}
          ] do
        assert DiskFull.classify(reason) == :disk_full
      end

      for reason <- [:timeout, {:restore, :enoent}, "database or disk is full", {:io, "x"}] do
        assert DiskFull.classify(reason) == reason
      end
    end

    test "Sqlite maps SQLITE_FULL by code", ctx do
      {:ok, conn} = Sqlite.open(Path.join(ctx.dir, "full.db"))
      {:ok, _} = Sqlite.run(conn, "CREATE TABLE t (v BLOB)", [])
      {:ok, _} = Sqlite.run(conn, "PRAGMA max_page_count = 3", [])

      assert {:error, :disk_full} =
               Sqlite.run(conn, "INSERT INTO t VALUES (zeroblob(100000))", [])

      assert {:error, :disk_full} =
               Sqlite.execute(conn, "INSERT INTO t VALUES (zeroblob(100000))")
    end
  end
end
