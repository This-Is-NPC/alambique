defmodule Alambique.PageSidecarConfigTest do
  @moduledoc "The `:page_sidecar` setting of the lazy restore (design §16)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, Placement, Tx}
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  @lazy [min_bytes: 0, verify_bytes: 0, concurrency: 2, batch_bytes: 1_024, miss_pages: 4]

  setup do
    capture([[:alambique, :replication, :hydrate], [:alambique, :replication, :sidecar]])
    setup_store()
  end

  defp ctx_of(inst, cell) do
    config = Config.fetch!(inst)
    {:ok, location} = Placement.locate(config, cell)
    Config.repl_ctx(config, location)
  end

  defp sidecars(ctx) do
    {mod, cfg} = ctx.store
    {:ok, keys, nil} = mod.list(cfg, "cells-test/", nil, 10_000)
    Enum.filter(keys, &String.ends_with?(&1, ".ltx.pages"))
  end

  describe "the setting" do
    test "is off by default and writes no sidecar", ctx do
      inst = start_instance(ctx, migrations: @schema, lazy_restore: @lazy)
      cell = unique_cell()
      assert Config.fetch!(inst).lazy_restore.page_sidecar == false
      assert ctx_of(inst, cell).page_sidecar == nil

      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1, x'00')")
      assert sidecars(ctx) == []
    end

    test "gives the context the size above which an object gets one", ctx do
      inst = start_instance(ctx, migrations: @schema, lazy_restore: @lazy ++ [page_sidecar: true])
      assert ctx_of(inst, unique_cell()).page_sidecar == 0

      inst =
        start_instance(ctx,
          migrations: @schema,
          lazy_restore: Keyword.put(@lazy, :verify_bytes, 5_000) ++ [page_sidecar: true]
        )

      assert ctx_of(inst, unique_cell()).page_sidecar == 5_000
    end

    test "is off with the lazy restore", ctx do
      inst = start_instance(ctx, migrations: @schema, lazy_restore: false)
      assert ctx_of(inst, unique_cell()).page_sidecar == nil
    end

    test "must be a boolean", ctx do
      assert_raise ArgumentError, ~r/:page_sidecar must be a boolean/, fn ->
        start_instance(ctx, migrations: @schema, lazy_restore: [page_sidecar: 1])
      end
    end
  end

  test "a commit is acknowledged before its sidecar is written", ctx do
    inst = start_instance(ctx, migrations: @schema, lazy_restore: @lazy ++ [page_sidecar: true])
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1, x'00')")

    # The put of a sidecar takes longer than the call may: on the path of
    # the acknowledgment the next commit would fail.
    MemoryStore.inject(ctx.store_config,
      kind: :latency,
      ms: 5_000,
      ops: [:put],
      key: ~r/\.pages$/
    )

    assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (2, x'00')")
    assert {:ok, %{rows: [[2]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")

    # The sidecar gave up at its deadline, and reported it.
    assert_receive {:telemetry, [:alambique, :replication, :sidecar], _,
                    %{result: :write_failed}},
                   15_000
  end

  describe "a cell that wrote sidecars" do
    defp seed(ctx, rows) do
      seeder =
        start_instance(ctx, migrations: @schema, lazy_restore: @lazy ++ [page_sidecar: true])

      cell = unique_cell()

      {:ok, :ok} =
        Alambique.transaction(seeder, cell, fn tx ->
          for n <- 1..rows do
            {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (?, randomblob(1024))", [n])
          end

          :ok
        end)

      eventually(fn ->
        {:ok, _} = Alambique.execute(seeder, cell, "DELETE FROM t WHERE n = 0")

        match?(
          {:ok, %{rows: [[0]]}},
          Alambique.query(
            seeder,
            cell,
            "SELECT count(*) FROM _alambique_feed WHERE op <> 'floor'"
          )
        )
      end)

      :ok = Alambique.hibernate(seeder, cell)
      cell
    end

    test "wakes without reading its objects whole", ctx do
      cell = seed(ctx, 300)
      assert sidecars(ctx) != []
      inst = start_instance(ctx, migrations: @schema, lazy_restore: @lazy)

      assert {:ok, %{rows: [[300]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")

      assert {%{check_covered: covered, check_objects: 0}, %{result: :ok}} =
               await_event([:alambique, :replication, :hydrate], cell, 30_000)

      assert covered > 0
    end

    test "still holds a write until the hydration ends, with every file covered", ctx do
      cell = seed(ctx, 300)
      release = hold_hydration(cell)
      inst = start_instance(ctx, migrations: @schema, lazy_restore: @lazy)
      assert {:ok, _node} = Alambique.touch(inst, cell)

      write =
        Task.async(fn ->
          Alambique.execute(inst, cell, "INSERT INTO t VALUES (100000, x'00')")
        end)

      pid = Alambique.Cell.whereis(Config.fetch!(inst), cell)
      eventually(fn -> length(:sys.get_state(pid).parked) == 1 end)
      assert Task.yield(write, 200) == nil

      release.()
      assert {:ok, _} = Task.await(write, 30_000)

      assert {%{check_covered: covered, check_objects: 0}, %{result: :ok}} =
               await_event([:alambique, :replication, :hydrate], cell, 30_000)

      assert covered > 0
    end
  end
end
