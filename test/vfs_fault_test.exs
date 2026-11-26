defmodule Alambique.VfsFaultTest do
  @moduledoc """
  The test-only fault of the VFS: a write or a sync of one file of one cell
  fails with the error the test asks for, after the bytes reached the file.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config}
  alias Alambique.Replication.{Native, Replicator}

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    setup_store()
  end

  defp boot(env) do
    inst = start_instance(env, migrations: @schema, compaction: false)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (0)")
    %{inst: inst, cell: cell}
  end

  defp wal(c),
    do:
      Cell.whereis(Config.fetch!(c.inst), c.cell)
      |> :sys.get_state()
      |> Map.fetch!(:path)
      |> Kernel.<>("-wal")

  defp insert(c, n), do: Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (#{n})")

  for rc <- [:ioerr_write, :full] do
    test "a fault on the commit frame fails the commit once (#{rc})", env do
      c = boot(env)
      vfs_fault(wal(c), :commit_frame, unquote(rc))

      assert {:error, _} = insert(c, 1)
      assert Native.vfs_fault_hits() == 1
      # It disarmed itself: the next write goes through.
      assert {:ok, _} = insert(c, 2)
      assert Native.vfs_fault_hits() == 1
    end
  end

  test "a fault matches the whole path, not its end", env do
    c = boot(env)
    wal = wal(c)

    vfs_fault(
      Path.join(Path.dirname(wal), "x" <> Path.basename(wal)),
      :commit_frame,
      :ioerr_write
    )

    assert {:ok, _} = insert(c, 1)
    assert Native.vfs_fault_hits() == 0
  end

  test "skip lets selected frames pass", env do
    c = boot(env)
    # A write is two commits, the rows and the stamp of the feed: the first
    # passes, the second fails.
    vfs_fault(wal(c), :commit_frame, :ioerr_write, skip: 1)

    assert {:error, {:stamp_failed, _}} = insert(c, 1)
    assert Native.vfs_fault_hits() == 1
  end

  test "a commit that writes no frame does not meet a commit-frame fault", env do
    c = boot(env)
    noop = "UPDATE t SET n = n WHERE n = 99"
    assert {:ok, _} = Alambique.execute(c.inst, c.cell, noop)

    # The write that cleans the feed rows of the batches published has
    # frames; the next one, with nothing left to clean, has none.
    pid = Cell.whereis(Config.fetch!(c.inst), c.cell)

    assert eventually(fn ->
             state = :sys.get_state(pid)

             state.feed.published == state.feed.cleaned and
               not in_cell(pid, &Replicator.unreplicated?(&1.repl, &1.conn))
           end)

    vfs_fault(wal(c), :commit_frame, :ioerr_write)
    assert {:ok, _} = Alambique.execute(c.inst, c.cell, noop)
    assert Native.vfs_fault_hits() == 0
    # The fault is still armed for the first commit that has a frame.
    assert {:error, _} = insert(c, 1)
    assert Native.vfs_fault_hits() == 1
  end

  test "a sync fault fails a checkpoint and leaves the cell serving", env do
    c = boot(env)
    {:ok, _} = insert(c, 1)
    vfs_fault(wal(c), :sync, :ioerr_fsync)

    assert {:error, _} = Alambique.checkpoint(c.inst, c.cell)
    assert Native.vfs_fault_hits() == 1
    assert {:ok, _} = insert(c, 2)
    assert :ok = Alambique.checkpoint(c.inst, c.cell)
  end

  test "clear disarms the fault", env do
    c = boot(env)
    vfs_fault(wal(c), :commit_frame, :ioerr_write)
    :ok = Native.vfs_fault_clear()

    assert {:ok, _} = insert(c, 1)
    assert Native.vfs_fault_hits() == 0
  end
end
