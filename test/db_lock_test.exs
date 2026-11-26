defmodule Alambique.DbLockTest do
  @moduledoc """
  The locks SQLite holds on the file of an awake cell stay held. They are
  POSIX locks, which belong to the process: closing any other descriptor of
  the file drops them, so the library must never open the live file by path
  and close it (the first snapshot of a new cell reads it).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config}

  @moduletag capture_log: true

  # Another OS process asks for a write lock on the range of the SHARED lock
  # of SQLite (`SHARED_FIRST`, 510 bytes): refused while a connection of the
  # cell holds its read lock there.
  @probe """
  import fcntl, os, sys
  fd = os.open(sys.argv[1], os.O_RDWR)
  try:
      fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 510, 0x40000002, os.SEEK_SET)
      print("free")
  except OSError:
      print("held")
  """

  setup do
    setup_store()
  end

  defp db_path(inst, cell),
    do: Cell.whereis(Config.fetch!(inst), cell) |> :sys.get_state() |> Map.fetch!(:path)

  defp lock_state(path) do
    {out, 0} = System.cmd("python3", ["-I", "-c", @probe, path])
    String.trim(out)
  end

  test "a new cell's first snapshot leaves the lock of its connection held", env do
    inst = start_instance(env, migrations: [{1, "CREATE TABLE t (n INTEGER)"}], compaction: false)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")

    assert lock_state(db_path(inst, cell)) == "held"
  end
end
