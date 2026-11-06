defmodule Alambique.NativePanicTest do
  @moduledoc """
  A Rust panic gives the caller an error and leaves the VM running (design
  §2.1). The panicking NIFs and the panic probe exist only in the test build
  (cargo feature `test-panic`). The panics print to stderr.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 1]

  alias Alambique.Replication.Native
  alias Alambique.Sqlite

  @moduletag :tmp_dir

  # Callbacks SQLite calls: the VFS, the allocator and the authorizer.
  @callbacks ~w(x_open x_close x_read x_write x_truncate x_sync x_file_size x_lock x_unlock
                x_check_reserved_lock x_file_control x_sector_size x_device_characteristics
                x_shm_map x_shm_lock x_shm_unmap x_shm_barrier x_malloc x_realloc x_free
                x_size x_roundup authorize)

  # Callbacks whose panic must show as a failed call, not only as survival.
  @failing ~w(x_open x_read x_write x_lock x_shm_map authorize x_malloc)

  # The VM still runs NIFs, including SQLite.
  defp assert_vm_works(dir) do
    {:ok, conn} = Sqlite.open(Path.join(dir, "after.db"), vfs: :alambique)
    assert :ok = Sqlite.execute(conn, "CREATE TABLE t (a); INSERT INTO t VALUES (1)")
    assert {:ok, %{rows: [[1]]}} = Sqlite.run(conn, "SELECT a FROM t", [])
    Sqlite.close(conn)
  end

  for nif <- [:panic_normal, :panic_dirty_cpu, :panic_dirty_io] do
    test "#{nif} raises in the caller only", %{tmp_dir: dir} do
      assert :nif_panicked = catch_error(apply(Native, unquote(nif), []))

      {pid, ref} = spawn_monitor(fn -> apply(Native, unquote(nif), []) end)
      assert_receive {:DOWN, ^ref, :process, ^pid, {:nif_panicked, _stacktrace}}, 5_000

      assert_vm_works(dir)
    end
  end

  test "a panic in a destructor does not stop the VM", %{tmp_dir: dir} do
    before = Native.panic_drops()
    task = Task.async(fn -> is_reference(Native.panic_resource()) end)
    assert Task.await(task)

    # The resource is destroyed when its owner's heap is collected.
    eventually(fn -> Native.panic_drops() == before + 1 end)
    assert_vm_works(dir)
  end

  for site <- @callbacks do
    test "a panic in #{site} becomes an SQLite error code", %{tmp_dir: dir} do
      {open, exec} = Native.panic_in_callback(unquote(site), Path.join(dir, "probe.db"))

      if unquote(site) in @failing do
        assert open != 0 or exec != 0
      end

      assert_vm_works(dir)
    end
  end
end
