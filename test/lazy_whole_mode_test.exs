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

  # What a cold wake of the cell asks of the store: the number of reads of each
  # segment, whole and by range. The instance leaves the cell so the next one
  # can take it, and does not compact, so that the chain stays as it was.
  defp store_calls({ctx, {_, counter} = counted}, cell, lazy_restore, opts \\ []) do
    CountingStore.reset(counter)
    {hydrates?, opts} = Keyword.pop(opts, :hydrates, false)

    inst =
      start_instance(
        %{ctx | store: counted},
        [migrations: @schema, lazy_restore: lazy_restore, compaction: false] ++ opts
      )

    assert {:ok, %{rows: [[200]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")

    # A lazy wake that leaves before its hydration ends has read less.
    if hydrates?,
      do: {_, %{result: :ok}} = await_event([:alambique, :replication, :hydrate], cell, 30_000)

    :ok = Alambique.hibernate(inst, cell)

    for op <- [:get, :get_range],
        key <- CountingStore.keys(counter, op),
        String.ends_with?(key, ".ltx"),
        reduce: %{},
        do: (calls -> Map.update(calls, {op, key}, 1, &(&1 + 1)))
  end

  # The reads of the same chain by a full restore (`lazy_restore: false`),
  # which also asks for the first range of each segment: it says the size of
  # the segment, and is the whole of a small one (`Restore.fetch_bounded/2`).
  # The next wake reads the fence the one before wrote, so only the segments
  # both read are compared.
  defp same_as_full_restore(store, cell, whole) do
    full = store_calls(store, cell, false)
    assert Enum.any?(full, &match?({{:get_range, _}, _}, &1))
    assert Map.take(full, Map.keys(whole)) == whole
  end

  test "whole mode makes the store calls of a full restore", ctx do
    cell = seed(ctx, 200)
    store = {ctx, CountingStore.wrap(ctx.store)}
    whole = store_calls(store, cell, min_bytes: 0, mode: :whole)
    same_as_full_restore(store, cell, whole)
    assert drain([:alambique, :replication, :hydrate], cell) == []
  end

  test "ranged mode, the default, reads pages by range", ctx do
    cell = seed(ctx, 200)
    store = {ctx, CountingStore.wrap(ctx.store)}
    ranged = store_calls(store, cell, [min_bytes: 0], hydrates: true)
    assert Enum.any?(ranged, fn {{op, _key}, n} -> op == :get_range and n > 1 end)
  end

  describe "per cell" do
    defmodule Mode do
      @moduledoc false
      def options(_cell), do: [lazy_restore: [mode: :whole]]
    end

    test "a cell that asks for :whole makes the store calls of a full restore", ctx do
      cell = seed(ctx, 200)

      store = {ctx, CountingStore.wrap(ctx.store)}
      whole = store_calls(store, cell, [min_bytes: 0], cell_options: {Mode, :options})
      same_as_full_restore(store, cell, whole)
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
