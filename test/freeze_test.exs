defmodule Alambique.FreezeTest do
  @moduledoc "Freezing cells (design §10)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  setup do
    capture([
      [:alambique, :cell, :wake],
      [:alambique, :cell, :hibernate],
      [:alambique, :cell, :freeze],
      [:alambique, :cell, :unfreeze]
    ])

    setup_store()
  end

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp numbers(inst, cell), do: Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")

  test "a frozen cell hibernates at once and refuses to wake", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    assert {:ok, _} = put(inst, cell, 1)
    assert {:ok, false} = Alambique.frozen?(inst, cell)

    assert :ok = Alambique.freeze(inst, cell)
    assert {:ok, true} = Alambique.frozen?(inst, cell)
    assert :none = Alambique.owner(inst, cell)
    assert {_, %{reason: :request}} = await_event([:alambique, :cell, :hibernate], cell)

    assert {:error, :frozen} = numbers(inst, cell)
    assert {:error, :frozen} = put(inst, cell, 2)

    assert {_, %{result: :error, reason: :frozen}} =
             await_event([:alambique, :cell, :wake], cell, 5_000, &(&1.result == :error))

    # The refused wake gave the lease back and wrote nothing.
    assert :none = Alambique.owner(inst, cell)
    assert durable_rows(ctx, cell, "SELECT n FROM t") == [[1]]
  end

  test "unfreezing lets the cell wake with its data", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    assert {:ok, _} = put(inst, cell, 1)
    assert :ok = Alambique.freeze(inst, cell)
    assert :ok = Alambique.unfreeze(inst, cell)
    assert {:ok, false} = Alambique.frozen?(inst, cell)
    assert {:ok, %{rows: [[1]]}} = numbers(inst, cell)
    assert {:ok, _} = put(inst, cell, 2)
  end

  test "freezing and unfreezing are idempotent and work on cells that never woke", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    assert :ok = Alambique.unfreeze(inst, cell)
    assert :ok = Alambique.freeze(inst, cell)
    assert :ok = Alambique.freeze(inst, cell)
    assert {:error, :frozen} = numbers(inst, cell)
    assert :ok = Alambique.unfreeze(inst, cell)
    assert {:ok, _} = numbers(inst, cell)
  end

  test "an awake cell finds the marker at its next lease renewal", ctx do
    inst = start_instance(ctx, migrations: @schema, lease_ttl_ms: 600, skew_margin_ms: 100)
    cell = unique_cell()
    assert {:ok, _} = put(inst, cell, 1)

    # What `mix alambique.freeze` does: write the marker, nothing else.
    key = Keys.frozen("cells-test", cell)
    {:ok, _} = MemoryStore.put(ctx.store_config, key, "{}", :none, %{})
    assert {:ok, _} = numbers(inst, cell)

    eventually(fn -> Alambique.owner(inst, cell) == :none end)
    assert {_, %{reason: :frozen}} = await_event([:alambique, :cell, :hibernate], cell)
    assert {:error, :frozen} = numbers(inst, cell)
    assert durable_rows(ctx, cell, "SELECT n FROM t") == [[1]]
  end

  test "a cell frozen with writes in flight loses none of them", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    assert {:ok, _} = put(inst, cell, 0)

    writers = for n <- 1..20, do: Task.async(fn -> put(inst, cell, n) end)
    assert :ok = Alambique.freeze(inst, cell)
    results = Task.await_many(writers)

    acked = [0 | for({{:ok, _}, n} <- Enum.zip(results, 1..20), do: n)]
    assert Enum.all?(results, &(match?({:ok, _}, &1) or &1 == {:error, :frozen}))
    assert durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n") == Enum.map(acked, &[&1])
  end

  test "a frozen cell cannot be moved", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    assert {:ok, _} = put(inst, cell, 1)
    assert :ok = Alambique.freeze(inst, cell)
    assert {:error, :frozen} = Alambique.move(inst, cell, node())
  end

  test "a wake that cannot read the marker fails instead of guessing", ctx do
    inst = start_instance(ctx, migrations: @schema, op_timeout_ms: 100)
    cell = unique_cell()

    MemoryStore.inject(ctx.store_config,
      kind: :error,
      ops: [:head],
      key: ~r{/frozen$},
      times: :infinity
    )

    assert {:error, {:frozen_check, _}} = numbers(inst, cell)
  end

  test "freeze and unfreeze emit events", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    :ok = Alambique.freeze(inst, cell)
    :ok = Alambique.unfreeze(inst, cell)
    assert {%{duration: _}, %{result: :ok}} = await_event([:alambique, :cell, :freeze], cell)
    assert {%{duration: _}, %{result: :ok}} = await_event([:alambique, :cell, :unfreeze], cell)
  end

  test "bad cell ids are rejected", ctx do
    inst = start_instance(ctx, migrations: @schema)
    assert {:error, :invalid_cell_id} = Alambique.freeze(inst, "a/b")
    assert {:error, :invalid_cell_id} = Alambique.unfreeze(inst, "")
    assert {:error, :invalid_cell_id} = Alambique.frozen?(inst, "a b")
  end
end
