defmodule Alambique.WakeLeaseFirstTest do
  @moduledoc """
  A wake reads nothing of the cell from the store before it holds the lease
  (design §7): without a lease there is no restore cost.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Config, Lease, Placement, TracingStore}
  alias AlambiqueTestkit.MemoryStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  setup do
    capture([[:alambique, :cell, :wake]])
    ctx = setup_store()
    {TracingStore, tracing} = traced = TracingStore.wrap(ctx.store)
    seeder = start_instance(ctx, migrations: @schema, compaction: false)
    reader = start_instance(ctx, migrations: @schema, compaction: false, store: traced)
    Map.merge(ctx, %{seeder: seeder, reader: reader, tracing: tracing})
  end

  # A hibernated cell with data in the store.
  defp seed(%{seeder: seeder}) do
    cell = unique_cell()
    {:ok, _} = Alambique.execute(seeder, cell, "INSERT INTO t VALUES (1, 'x')", [])
    :ok = Alambique.hibernate(seeder, cell)
    drain([:alambique, :cell, :wake], cell)
    cell
  end

  defp hold_elsewhere(ctx, cell) do
    config = Config.fetch!(ctx.reader)
    {:ok, location} = Placement.locate(config, cell)
    {:ok, _lease} = Lease.acquire(Config.lease_ctx(config, location), cell, "other-node")
  end

  # One wake of `cell` with `outcome`; returns the result of the call and the
  # requests of the wake, `{op, key, from, to}`, in the order they started.
  defp wake(ctx, cell, outcome) do
    TracingStore.reset(ctx.tracing)

    case outcome do
      :held ->
        hold_elsewhere(ctx, cell)

      :lease_put_fails ->
        MemoryStore.inject(ctx.store_config,
          kind: :error,
          ops: [:put],
          key: ~r|/lease$|,
          reason: :down
        )

      :free ->
        :ok
    end

    result = Alambique.touch(ctx.reader, cell)
    MemoryStore.clear(ctx.store_config)
    {result, ctx.tracing |> TracingStore.trace() |> Enum.sort_by(&elem(&1, 2))}
  end

  defp lease?({_, key, _, _}), do: key =~ ~r|/lease$|

  # Requests other than the lease's and the frozen marker's.
  defp cell_data(trace),
    do: Enum.reject(trace, fn {_, key, _, _} -> key =~ ~r"/(lease|frozen)$" end)

  defp assert_lease_first(trace, :free) do
    {:put, _, _, lease_end} = Enum.find(trace, &(lease?(&1) and elem(&1, 0) == :put))
    data = cell_data(trace)
    assert data != []
    assert Enum.all?(data, fn {_, _, from, _} -> from >= lease_end end)
  end

  defp assert_lease_first(trace, _failed), do: assert(cell_data(trace) == [])

  test "a lease held by another node costs no read of the cell", ctx do
    cell = seed(ctx)

    assert {{:error, _}, trace} = wake(ctx, cell, :held)
    assert cell_data(trace) == []
  end

  test "a lease that cannot be written costs no read of the cell", ctx do
    cell = seed(ctx)

    assert {{:error, _}, trace} = wake(ctx, cell, :lease_put_fails)
    assert cell_data(trace) == []
  end

  property "no request for the cell precedes the lease put that succeeded", ctx do
    check all(outcome <- member_of([:free, :held, :lease_put_fails]), max_runs: 12) do
      cell = seed(ctx)
      {_result, trace} = wake(ctx, cell, outcome)
      assert_lease_first(trace, outcome)
    end
  end
end
