defmodule Alambique.LazyWholeModeTest do
  @moduledoc "The lazy restore mode: `:ranged`, or `:whole` to read the chain before the first statement (design §16)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, CountingStore, Tx}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]

  setup do
    capture([[:alambique, :replication, :hydrate]])
    env = setup_store()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  # A cell with `rows` rows of 1 KB, hibernated, so that its state is in the
  # store only.
  defp seed(ctx, rows) do
    seeder = start_instance(ctx, migrations: @schema, lazy_restore: false)
    cell = unique_cell()

    {:ok, :ok} =
      Alambique.transaction(seeder, cell, fn tx ->
        for n <- 1..rows do
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (?, randomblob(1024))", [n])
        end

        :ok
      end)

    # The change feed holds the rows until they are published and the next
    # write drops them; a cell that wakes with them would read them all.
    eventually(fn ->
      {:ok, _} = Alambique.execute(seeder, cell, "DELETE FROM t WHERE n = 0")

      match?(
        {:ok, %{rows: [[0]]}},
        Alambique.query(seeder, cell, "SELECT count(*) FROM _alambique_feed WHERE op <> 'floor'")
      )
    end)

    :ok = Alambique.hibernate(seeder, cell)
    cell
  end

  # An instance that reads through a counting store.
  defp counted(ctx, lazy_restore) do
    {_, counter} = counted = CountingStore.wrap(ctx.store)

    inst =
      start_instance(%{ctx | store: counted}, migrations: @schema, lazy_restore: lazy_restore)

    {inst, counter}
  end

  test "whole mode reads the chain without a ranged read", ctx do
    cell = seed(ctx, 200)
    {inst, counter} = counted(ctx, min_bytes: 0, mode: :whole)

    assert {:ok, %{rows: [[200]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    assert CountingStore.count(counter, :get_range) == 0
    assert CountingStore.count(counter, :get) > 0
    assert drain([:alambique, :replication, :hydrate], cell) == []
  end

  test "ranged mode, the default, reads by range", ctx do
    cell = seed(ctx, 200)
    {inst, counter} = counted(ctx, min_bytes: 0)

    assert {:ok, %{rows: [[200]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    assert CountingStore.count(counter, :get_range) > 0
    assert {_, %{result: :ok}} = await_event([:alambique, :replication, :hydrate], cell, 30_000)
  end

  describe "per cell" do
    defmodule Mode do
      @moduledoc false
      def options(_cell), do: [lazy_restore: [mode: :whole]]
    end

    test "a cell that asks for :whole reads its chain without a ranged read", ctx do
      cell = seed(ctx, 200)
      {_, counter} = counted = CountingStore.wrap(ctx.store)

      inst =
        start_instance(%{ctx | store: counted},
          migrations: @schema,
          lazy_restore: [min_bytes: 0],
          cell_options: {Mode, :options}
        )

      assert {:ok, %{rows: [[200]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
      assert CountingStore.count(counter, :get_range) == 0
      assert drain([:alambique, :replication, :hydrate], cell) == []
    end
  end

  describe "the setting" do
    defp config(ctx, lazy_restore) do
      Config.build(
        name: :lazy_mode,
        store: ctx.store,
        prefix: "lazy-mode",
        data_dir: Path.join(ctx.dir, "lazy-mode"),
        node_id: "lazy-mode",
        lease_ttl_ms: 3_000,
        skew_margin_ms: 500,
        idle_timeout_ms: 60_000,
        op_timeout_ms: 2_000,
        lazy_restore: lazy_restore
      )
    end

    test "is :ranged by default, :whole when the lazy restore is off or says so", ctx do
      assert Config.lazy_mode(config(ctx, []), nil) == :ranged
      assert Config.lazy_mode(config(ctx, mode: :whole), nil) == :whole
      assert Config.lazy_mode(config(ctx, false), nil) == :whole
    end

    test "of a cell overrides the instance's, unless the lazy restore is off", ctx do
      assert Config.lazy_mode(config(ctx, []), :whole) == :whole
      assert Config.lazy_mode(config(ctx, mode: :whole), :ranged) == :ranged
      assert Config.lazy_mode(config(ctx, false), :ranged) == :whole
    end

    test "must be :ranged or :whole", ctx do
      assert_raise ArgumentError, ~r/:mode must be :ranged or :whole/, fn ->
        config(ctx, mode: :eager)
      end
    end
  end
end
