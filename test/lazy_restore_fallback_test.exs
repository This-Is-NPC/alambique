defmodule Alambique.LazyRestoreFallbackTest do
  @moduledoc """
  A lazy page fetch that fails discards the partial file and restores the
  chain in full, so the wake still serves.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{ClusterFuns, Config, Placement, Replication}
  alias Alambique.Replication.Native
  alias AlambiqueTestkit.{CrashPoints, LazyFailStore}

  @schema [
    {1, "CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER NOT NULL, v BLOB)"}
  ]

  setup do
    ctx = setup_store()
    on_exit(&CrashPoints.reset/0)
    ctx
  end

  test "a failed page-1 range read falls back to a full restore", ctx do
    seeder = start_instance(ctx, migrations: @schema, lazy_restore: false)
    cell = unique_cell()
    :ok = ClusterFuns.lazy_updates(seeder, cell, 20, 30)
    :ok = Alambique.hibernate(seeder, cell)

    # Find the object and frame the lazy plan uses for page 1.
    config = Config.fetch!(seeder)
    {:ok, location} = Placement.locate(config, cell, fresh: true)
    repl_ctx = Config.repl_ctx(config, location)
    probe = Path.join(ctx.dir, "probe.db")

    {:ok, result} =
      Replication.restore(repl_ctx, cell, probe, :latest, lazy: %{min_bytes: 0, wait_ms: 5_000})

    :ok = Native.lazy_ready(result.lazy)
    {file, offset, _len, _pages} = Native.lazy_miss_run(result.lazy, 1, 16)
    key = Enum.at(result.files, file)
    :ok = Native.lazy_release(result.lazy)
    for suffix <- ["", "-wal", "-shm"], do: File.rm(probe <> suffix)

    failing = {LazyFailStore, %{inner: ctx.store_config, key: key, offset: offset}}

    waker =
      start_instance(%{ctx | store: failing},
        migrations: @schema,
        lazy_restore: [min_bytes: 0, batch_bytes: 1_024, miss_pages: 4],
        op_timeout_ms: 300
      )

    test = self()

    CrashPoints.arm(:wake_lazy_fallback, fn _, %{cell: fallback_cell} ->
      send(test, {:fallback, fallback_cell})
    end)

    assert {:ok, %{rows: [[20]]}} = Alambique.query(waker, cell, "SELECT count(*) FROM log")
    assert_receive {:fallback, ^cell}, 5_000
  end
end
