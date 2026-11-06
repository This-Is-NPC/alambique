defmodule Alambique.ExclusiveTest do
  @moduledoc "The lease of an operation that replaces a cell's state (design §18, §20)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, Exclusive, Lease, Placement}
  alias AlambiqueTestkit.MemoryStore

  @moduletag :capture_log

  setup do
    ctx = setup_store()
    inst = start_instance(ctx)
    config = Config.fetch!(inst)
    cell = unique_cell()
    {:ok, location} = Placement.locate(config, cell)
    Map.merge(ctx, %{config: config, cell: cell, lease_ctx: Config.lease_ctx(config, location)})
  end

  defp deadline(ms), do: System.monotonic_time(:millisecond) + ms

  test "a store error is returned as it is, not waited out", ctx do
    MemoryStore.inject(ctx.store_config, kind: :error, ops: [:get, :put], key: ~r/lease/)

    assert {:error, :injected} =
             Exclusive.take(ctx.config, ctx.cell, ctx.lease_ctx, "op", 10, deadline(1_000))
  end

  test "the lease of an operation that died is released and taken again", ctx do
    {:ok, stale} = Lease.acquire(ctx.lease_ctx, ctx.cell, "op")

    assert {:ok, lease} =
             Exclusive.take(ctx.config, ctx.cell, ctx.lease_ctx, "op", 10, deadline(1_000))

    assert lease.owner == "op"
    assert lease.epoch >= stale.epoch
    assert {:ok, %Lease{owner: "op", released: false}} = Lease.read(ctx.lease_ctx, ctx.cell)
  end

  test "a lease held by someone else that does not end before the deadline is reported", ctx do
    {:ok, _} = Lease.acquire(ctx.lease_ctx, ctx.cell, "other")

    assert {:error, {:owned, "other", _}} =
             Exclusive.take(ctx.config, ctx.cell, ctx.lease_ctx, "op", 10, deadline(-1))
  end
end
