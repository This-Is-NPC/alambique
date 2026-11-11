defmodule Alambique.DataDirLockTest do
  @moduledoc "One live instance per data directory, (design §1)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Replica

  setup do
    %{ctx: setup_store()}
  end

  defp opts(ctx, name, dir) do
    [
      name: name,
      store: ctx.store,
      prefix: "cells-test",
      data_dir: dir,
      node_id: Atom.to_string(name)
    ]
  end

  defp try_start(ctx, name, dir) do
    start_supervised(Supervisor.child_spec({Alambique, opts(ctx, name, dir)}, id: name))
  end

  test "a second instance on one directory is refused and leaves the first's files alone", %{
    ctx: ctx
  } do
    dir = Path.join(ctx.dir, "shared")
    first = start_instance(ctx, data_dir: dir)

    # What the first instance owns; a start that cleaned the directory would remove it.
    replicas = Replica.dir(Alambique.Config.fetch!(first))
    File.mkdir_p!(replicas)
    File.write!(Path.join(replicas, "x"), "x")
    File.mkdir_p!(Path.join(dir, "work/rewind-1"))

    assert {:error, {{:data_dir_in_use, ^dir}, _}} = try_start(ctx, :second, dir)

    assert File.exists?(Path.join(replicas, "x"))
    assert File.dir?(Path.join(dir, "work/rewind-1"))
    assert Process.alive?(Process.whereis(first))
  end

  test "the directory is free again after a clean stop", %{ctx: ctx} do
    dir = Path.join(ctx.dir, "reused")
    first = start_instance(ctx, data_dir: dir)
    assert {:error, _} = try_start(ctx, :second, dir)

    :ok = stop_supervised(first)

    assert {:ok, _} = try_start(ctx, :third, dir)
  end

  test "a lock held by another OS process refuses the start until that process exits", %{
    ctx: ctx
  } do
    dir = Path.join(ctx.dir, "foreign")
    File.mkdir_p!(dir)
    ebin = Application.app_dir(:alambique, "ebin")

    code = """
    {:ok, _lock} = Alambique.Replication.Native.dir_lock(Path.join("#{dir}", "lock"))
    IO.puts("locked")
    IO.gets("")
    """

    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :stderr_to_stdout,
        args: ["-pa", ebin, "-e", code]
      ])

    assert_receive {^port, {:data, "locked\n"}}, 30_000

    assert {:error, {{:data_dir_in_use, ^dir}, _}} = try_start(ctx, :second, dir)

    # Closing the port ends the process's input; it exits and the kernel frees the lock.
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    Port.close(port)
    await_exit(os_pid)

    assert {:ok, _} = try_start(ctx, :third, dir)
  end

  defp await_exit(os_pid) do
    deadline = System.monotonic_time(:millisecond) + 30_000

    Stream.repeatedly(fn ->
      alive = File.exists?("/proc/#{os_pid}")
      unless alive, do: :done, else: Process.sleep(20) && :alive
    end)
    |> Enum.find(&(&1 == :done or System.monotonic_time(:millisecond) > deadline))
  end
end
