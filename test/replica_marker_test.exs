defmodule Alambique.ReplicaMarkerTest do
  @moduledoc "The switch of read replicas when the store fails (design §21)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, Placement, Replica}
  alias AlambiqueTestkit.MemoryStore

  @moduletag capture_log: true

  setup do
    ctx = setup_store()
    inst = start_instance(ctx, op_timeout_ms: 200, replicas: [marker_ttl_ms: 60_000])
    Map.merge(ctx, %{inst: inst, cell: unique_cell()})
  end

  defp fail_markers(ctx, ops),
    do: MemoryStore.inject(ctx.store_config, kind: :error, key: ~r{/replicas$}, ops: ops)

  test "a switch that cannot be written is an error and stays off", ctx do
    fault = fail_markers(ctx, [:put])
    assert {:error, :injected} = Alambique.replicas(ctx.inst, ctx.cell, true)

    MemoryStore.remove(ctx.store_config, fault)
    assert {:ok, false} = Alambique.replicas(ctx.inst, ctx.cell)
  end

  test "a switch that cannot be removed is an error and stays on", ctx do
    :ok = Alambique.replicas(ctx.inst, ctx.cell, true)
    fault = fail_markers(ctx, [:delete])
    assert {:error, :injected} = Alambique.replicas(ctx.inst, ctx.cell, false)

    MemoryStore.remove(ctx.store_config, fault)
    assert {:ok, true} = Alambique.replicas(ctx.inst, ctx.cell)
  end

  test "turning off a switch that is not there succeeds", ctx do
    assert :ok = Alambique.replicas(ctx.inst, ctx.cell, false)
    assert {:ok, false} = Alambique.replicas(ctx.inst, ctx.cell)
  end

  test "an unreadable switch is an error, not off", ctx do
    fail_markers(ctx, [:head])
    assert {:error, :injected} = Alambique.replicas(ctx.inst, ctx.cell)
  end

  test "a read that fails counts as off and is not remembered", ctx do
    :ok = Alambique.replicas(ctx.inst, ctx.cell, true)
    config = Config.fetch!(ctx.inst)
    {:ok, location} = Placement.locate(config, ctx.cell)
    repl = Config.repl_ctx(config, location)
    # The answer of the call above is forgotten, as after the ttl.
    :ets.delete_all_objects(config.replica_memo)

    fault = fail_markers(ctx, [:head])
    refute Replica.Marker.cached?(config, repl, location, ctx.cell)

    MemoryStore.remove(ctx.store_config, fault)
    assert Replica.Marker.cached?(config, repl, location, ctx.cell)
  end
end
