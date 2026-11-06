defmodule Alambique.SchemaTest do
  @moduledoc "Versioned migrations: the window rule and the schema index (design §17)."
  use ExUnit.Case, async: false

  @moduletag :capture_log

  import Alambique.CellHelpers

  alias Alambique.Schema
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @v1 {1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}
  @v2 {2, "ALTER TABLE t ADD COLUMN w INTEGER"}
  @v3 {3, "ALTER TABLE t ADD COLUMN x INTEGER"}
  @v4 {4, "ALTER TABLE t ADD COLUMN y INTEGER"}

  setup do
    on_exit(&CrashPoints.reset/0)
    capture([[:alambique, :schema, :index], [:alambique, :cell, :migrate]])
    setup_store()
  end

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp version(inst, cell) do
    {:ok, %{rows: [[version]]}} = Alambique.query(inst, cell, "PRAGMA user_version")
    version
  end

  # The index as {version, cell} pairs.
  defp index(%{store_config: config}) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/schema/", nil, 100_000)

    for "cells-test/schema/" <> rest <- keys do
      [version, cell] = String.split(rest, "/")
      {String.to_integer(version), cell}
    end
  end

  describe "window" do
    test "serves N-1, N and N+1, and refuses N+2", ctx do
      cell = unique_cell()
      code2 = start_instance(ctx, migrations: [@v1, @v2])

      # N+1: migrated by newer code, served without migrating.
      newer = start_instance(ctx, migrations: [@v1, @v2, @v3])
      assert {:ok, _} = put(newer, cell, 1)
      :ok = Alambique.hibernate(newer, cell)
      drain([:alambique, :cell, :migrate], cell)

      assert {:ok, _} = put(code2, cell, 2)
      assert version(code2, cell) == 3
      assert [] = drain([:alambique, :cell, :migrate], cell)
      assert index(ctx) == [{3, cell}]
      :ok = Alambique.hibernate(code2, cell)

      # N+2: refused, and the cell stays free and untouched.
      newest = start_instance(ctx, migrations: [@v1, @v2, @v3, @v4])
      assert version(newest, cell) == 4
      :ok = Alambique.hibernate(newest, cell)

      assert {:error, {:schema_too_new, 4}} = put(code2, cell, 3)
      assert :none = Alambique.owner(code2, cell)
      assert durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n") == [[1], [2]]
      assert index(ctx) == [{4, cell}]

      # N-1 is migrated on wake.
      old = start_instance(ctx, migrations: [@v1])
      other = unique_cell()
      assert {:ok, _} = put(old, other, 1)
      :ok = Alambique.hibernate(old, other)
      assert version(code2, other) == 2
    end

    test "the window is an option", ctx do
      cell = unique_cell()
      code2 = start_instance(ctx, migrations: [@v1, @v2], migration_window: 2)
      newest = start_instance(ctx, migrations: [@v1, @v2, @v3, @v4])
      assert {:ok, _} = put(newest, cell, 1)
      :ok = Alambique.hibernate(newest, cell)

      assert {:ok, _} = put(code2, cell, 2)
      assert version(code2, cell) == 4
    end

    test "an instance without migrations does not check", ctx do
      cell = unique_cell()
      versioned = start_instance(ctx, migrations: [@v1, @v2, @v3])
      assert {:ok, _} = put(versioned, cell, 1)
      :ok = Alambique.hibernate(versioned, cell)

      bare = start_instance(ctx, [])
      assert {:ok, %{rows: [[1]]}} = Alambique.query(bare, cell, "SELECT count(*) FROM t")
    end

    test "versions must be contiguous from 1", ctx do
      assert_raise ArgumentError, ~r/contiguous/, fn ->
        Alambique.child_spec(store: ctx.store, data_dir: ctx.dir, migrations: [@v1, @v3])
      end

      assert_raise ArgumentError, ~r/contiguous/, fn ->
        Alambique.child_spec(store: ctx.store, data_dir: ctx.dir, migrations: [@v2])
      end
    end
  end

  describe "index" do
    test "a new cell is listed at the code's version, and moves when it migrates", ctx do
      cell = unique_cell()
      old = start_instance(ctx, migrations: [@v1])
      assert {:ok, _} = put(old, cell, 1)
      assert index(ctx) == [{1, cell}]
      :ok = Alambique.hibernate(old, cell)

      new = start_instance(ctx, migrations: [@v1, @v2, @v3])
      assert version(new, cell) == 3
      assert index(ctx) == [{3, cell}]

      assert [{%{}, %{op: :put, version: 3, result: :ok}}, {_, %{op: :delete, version: 1}}] =
               drain([:alambique, :schema, :index], cell) |> Enum.take(-2)
    end

    test "a wake that reuses the local file touches nothing", ctx do
      cell = unique_cell()
      inst = start_instance(ctx, migrations: [@v1, @v2])
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      drain([:alambique, :schema, :index], cell)

      assert {:ok, _} = put(inst, cell, 2)
      assert [] = drain([:alambique, :schema, :index], cell)
      assert index(ctx) == [{2, cell}]
    end

    test "a wake after a restore writes the entry again", ctx do
      cell = unique_cell()
      inst = start_instance(ctx, migrations: [@v1])
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)

      # The index was lost (or never existed for a cell from before it).
      :ok = MemoryStore.delete(ctx.store_config, "cells-test/schema/0000000001/#{cell}")
      assert index(ctx) == []

      other = start_instance(ctx, migrations: [@v1], data_dir: Path.join(ctx.dir, "other"))
      assert {:ok, _} = put(other, cell, 2)
      assert index(ctx) == [{1, cell}]
    end

    test "the index never shows the cell nowhere when a wake crashes mid-migration", ctx do
      cell = unique_cell()
      old = start_instance(ctx, migrations: [@v1])
      assert {:ok, _} = put(old, cell, 1)
      :ok = Alambique.hibernate(old, cell)

      new = start_instance(ctx, migrations: [@v1, @v2])

      for point <- [:schema_before_put, :schema_after_put] do
        CrashPoints.arm(point, :raise)
        assert {:error, _} = put(new, cell, 2)
        refute index(ctx) == []
        assert {1, cell} in index(ctx)
        CrashPoints.disarm(point)
      end

      # Put but not uploaded: both versions listed; the next wake repairs it.
      assert {:ok, _} = put(new, cell, 2)
      assert index(ctx) == [{2, cell}]
      assert version(new, cell) == 2
    end

    test "a failed index write fails the wake before the migrations are uploaded", ctx do
      cell = unique_cell()
      old = start_instance(ctx, migrations: [@v1])
      assert {:ok, _} = put(old, cell, 1)
      :ok = Alambique.hibernate(old, cell)

      new = start_instance(ctx, migrations: [@v1, @v2], op_timeout_ms: 300)

      MemoryStore.inject(ctx.store_config,
        kind: :error,
        times: 1_000,
        key: ~r{/schema/},
        ops: [:put]
      )

      assert {:error, _} = put(new, cell, 2)
      assert durable_rows(ctx, cell, "PRAGMA user_version") == [[1]]
      assert index(ctx) == [{1, cell}]
    end

    test "keys sort by number and the oldest version is the first key" do
      assert Schema.key("p", 9, "c") < Schema.key("p", 10, "c")
      assert Schema.key("p", 2, "c") == "p/schema/0000000002/c"
    end
  end
end
