defmodule Alambique.LazyDbLockTest do
  @moduledoc """
  The end of a lazy restore leaves the locks of the connections on the
  database file held. They are POSIX locks, which belong to the process:
  closing any descriptor of the file drops them, so the descriptor of the
  restore plan is closed only when SQLite has no file open on the path.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 1]
  import Alambique.CompactionHelpers

  alias Alambique.{NativeStats, Replication, Sqlite}
  alias Alambique.Replication.{Hydrator, Lazy}

  # Another OS process asks for a write lock on the range of the SHARED lock
  # of SQLite (`SHARED_FIRST`, 510 bytes): refused while a connection holds
  # its read lock there.
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
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    history(env, new_owner(env, "owner.db"), 4)
    env
  end

  defp lock_state(path) do
    {out, 0} = System.cmd("python3", ["-I", "-c", @probe, path])
    String.trim(out)
  end

  test "a connection opened after the hydration keeps its lock when the plan is dropped", env do
    path = Path.join(env.dir, "lazy.db")
    parent = self()
    %{lazy_plans: plans, lazy_fds_parked: parked} = NativeStats.read()

    # The process that restored owns the plan; the connection below never
    # holds it, because the hydration was over when it opened the file.
    {pid, ref} =
      spawn_monitor(fn ->
        {:ok, result} =
          Replication.restore(env.ctx, cell(), path, :latest,
            lazy: %{min_bytes: 0, wait_ms: 1_000}
          )

        lazy = Lazy.new(result, env.ctx, miss_pages: 2, batch_bytes: 256)
        {:ok, hydrator} = Hydrator.start_link(lazy, concurrency: 2, fetch_ms: 1_000, poll_ms: 10)

        receive do
          {:hydrated, ^hydrator} -> send(parent, :hydrated)
        end

        receive do
          :stop -> lazy
        end
      end)

    assert_receive :hydrated, 10_000
    {:ok, conn} = Sqlite.open(path, vfs: :alambique)
    on_exit(fn -> Sqlite.close(conn) end)
    assert {:ok, %{rows: [[_]]}} = Sqlite.run(conn, "SELECT count(*) FROM kv", [])
    assert lock_state(path) == "held"

    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    # The plan is released as the process ends. Once it is gone its
    # descriptor is parked, and the lock must stay.
    eventually(fn -> NativeStats.read().lazy_plans == plans end)
    assert NativeStats.read().lazy_fds_parked == parked + 1
    assert lock_state(path) == "held"
  end
end
