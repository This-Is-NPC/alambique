defmodule Alambique.CellMarkerTest do
  @moduledoc """
  A cell killed while it writes the complete-file marker leaves either a
  complete file without a marker or a marker that names it; both wakes serve.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, ClusterFuns}
  alias AlambiqueTestkit.CrashPoints

  @schema [
    {1, "CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER NOT NULL, v BLOB)"}
  ]

  setup do
    ctx = setup_store()
    on_exit(&CrashPoints.reset/0)
    ctx
  end

  for point <- [:local_before_mark, :local_after_mark] do
    test "a kill at #{point} still serves the cell", ctx do
      inst = start_instance(ctx, migrations: @schema, lazy_restore: false)
      cell = unique_cell()
      :ok = ClusterFuns.lazy_updates(inst, cell, 20, 10)
      :ok = Alambique.hibernate(inst, cell)

      config = Alambique.Config.fetch!(inst)
      dir = Cell.Local.dir(config, cell)
      marker = Path.join(dir, "cell.db.meta")

      # Take the first marker at the wake.
      assert {:ok, %{rows: [[20]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM log")
      refute File.exists?(marker)

      test = self()

      CrashPoints.arm(unquote(point), fn _, _ ->
        send(test, {:mark, self()})
        receive do: (:go -> :ok)
      end)

      task = Task.async(fn -> Alambique.hibernate(inst, cell) end)
      assert_receive {:mark, cell_pid}, 10_000

      if unquote(point) == :local_after_mark,
        do: assert(File.exists?(marker)),
        else: refute(File.exists?(marker))

      Process.exit(cell_pid, :kill)
      _ = Task.yield(task, 5_000)
      CrashPoints.reset()

      assert {:ok, %{rows: [[20]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM log")
    end
  end

  describe "the data directory" do
    test "cells named like work directories keep their files across a restart", ctx do
      first = start_instance(ctx, name: :marker_restart, migrations: @schema)
      cells = ["rewind-x", "relocate-1", "scrub-2"]

      for cell <- cells do
        assert {:ok, _} = Alambique.execute(first, cell, "INSERT INTO log (n) VALUES (1)")
        :ok = Alambique.hibernate(first, cell)
      end

      config = Alambique.Config.fetch!(first)
      paths = for cell <- cells, do: Cell.Local.db_path(config, cell)
      assert Enum.all?(paths, &File.exists?/1)

      stop_supervised!(:marker_restart)
      start_instance(ctx, name: :marker_restart, migrations: @schema)

      assert Enum.all?(paths, &File.exists?/1)
    end

    test "an instance removes the work directories a crash left", ctx do
      data_dir = Path.join(ctx.dir, "work_restart")

      for path <- ["work/scrub-1/cell.db", "work/rewind-7/target.db"] do
        File.mkdir_p!(Path.dirname(Path.join(data_dir, path)))
        File.write!(Path.join(data_dir, path), "x")
      end

      start_instance(ctx, name: :work_restart, migrations: @schema)

      refute File.exists?(Path.join(data_dir, "work"))
    end

    test "the directories of a cell are private to the owner", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO log (n) VALUES (1)")
      config = Alambique.Config.fetch!(inst)

      for dir <- [config.data_dir, Cell.Local.dir(config, cell)] do
        assert Bitwise.band(File.stat!(dir).mode, 0o077) == 0, dir
      end

      # SQLite creates the -wal and -shm files itself, with the mode of the database.
      assert File.exists?(Cell.Local.db_path(config, cell) <> "-wal")
    end
  end
end
