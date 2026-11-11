defmodule Alambique.MovedCleanupPropertiesTest do
  @moduledoc """
  The cleanup of what a moved cell left behind (design §18) over random
  sequences of writes, moves, restarts of the node and time. The invariants:
  no object of a location is deleted before `moved_at_ms + moved_grace_ms +
  skew_margin_ms`, whatever restarted meanwhile, no cleanup touches the data
  of the live location, and once the grace passed every location the cell
  left is cleaned without anyone asking for it.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Config, Lease, Location}
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @moduletag capture_log: true

  @grace_ms 1_000
  @skew_ms 500
  @cell "home-a"
  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  defp op do
    frequency([
      {3, constant(:write)},
      {3, constant(:move)},
      {2, constant(:restart)},
      {1, constant(:pause)},
      {4, tuple({constant(:advance), integer(0..2_500)})}
    ])
  end

  property "a restart neither deletes early nor forgets a pending cleanup" do
    base = setup_store()

    check all(ops <- list_of(op(), min_length: 1, max_length: 10), max_runs: 25) do
      run = System.unique_integer([:positive])
      ids = for n <- [:a, :b, :table], into: %{}, do: {n, {:store, run, n}}

      stores =
        for {n, id} <- ids, into: %{} do
          pid = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: id))
          {n, MemoryStore.config(pid)}
        end

      locations = [
        %Location{id: "a", store: {MemoryStore, stores.a}, prefix: "pa"},
        %Location{id: "b", store: {MemoryStore, stores.b}, prefix: "pb"}
      ]

      world = ManualClock.new(1_700_000_000_000)

      opts = [
        name: :"moved_cleanup_#{run}",
        placement:
          {AlambiqueTestkit.Placement,
           locations: locations, table: {MemoryStore, stores.table}, initial: %{@cell => "a"}},
        migrations: @schema,
        clock: {ManualClock, ManualClock.config(world, 0)},
        moved_grace_ms: @grace_ms,
        skew_margin_ms: @skew_ms,
        lease_ttl_ms: 1_000_000_000,
        deletion_grace_ms: 0,
        compaction: [interval_ms: 20, moved_scan_ms: 20]
      ]

      inst = start_instance(base, opts)
      ctx = %{inst: inst, base: base, opts: opts, locations: locations, world: world}

      try do
        final = Enum.reduce(ops, %{writes: 0}, &step(&1, &2, ctx))
        settle(final, ctx)
      after
        stop_supervised(inst)
        for {_, id} <- ids, do: stop_supervised(id)
      end
    end
  end

  defp step(:write, model, ctx) do
    {:ok, _} = Alambique.execute(ctx.inst, @cell, "INSERT INTO t (n) VALUES (?)", [model.writes])
    verify(%{model | writes: model.writes + 1}, ctx)
  end

  defp step(:move, model, ctx) do
    {:ok, here} = Alambique.Placement.locate(Config.fetch!(ctx.inst), @cell, fresh: true)
    # Refusals (not cleaned yet, settling) are part of the scenario.
    _ = Alambique.relocate(ctx.inst, @cell, Enum.find(ctx.locations, &(&1.id != here.id)))
    verify(model, ctx)
  end

  defp step(:restart, model, ctx) do
    stop_supervised!(ctx.inst)
    start_instance(ctx.base, ctx.opts)
    verify(model, ctx)
  end

  defp step(:pause, model, ctx) do
    Process.sleep(60)
    verify(model, ctx)
  end

  defp step({:advance, ms}, model, ctx) do
    ManualClock.advance(ctx.world, ms)
    verify(model, ctx)
  end

  # The data of the cell is intact, and a location the cell left still holds
  # its objects while the grace runs.
  defp verify(model, ctx) do
    assert {:ok, %{rows: rows}} = Alambique.query(ctx.inst, @cell, "SELECT n FROM t ORDER BY n")
    assert List.flatten(rows) == Enum.to_list(0..(model.writes - 1)//1)

    now = ManualClock.world_ms(ctx.world)

    for location <- ctx.locations do
      case moved_lease(ctx, location) do
        %{moved_at_ms: at} when at != nil ->
          if now < at + @grace_ms + @skew_ms,
            do: assert(objects(ctx, location) != [], "#{location.id} cleaned early")

        _ ->
          :ok
      end
    end

    model
  end

  # Past the grace, every location the cell left ends up cleaned.
  defp settle(model, ctx) do
    ManualClock.advance(ctx.world, @grace_ms + @skew_ms + 1)

    for location <- ctx.locations, moved_lease(ctx, location) != nil do
      eventually(fn -> objects(ctx, location) == [] end, 10_000)
    end

    verify(model, ctx)
  end

  defp moved_lease(ctx, location) do
    case Lease.read(Config.lease_ctx(Config.fetch!(ctx.inst), location), @cell) do
      {:ok, %{moved_to: to} = lease} when to != nil -> lease
      _ -> nil
    end
  end

  defp objects(ctx, location) do
    repl = Config.repl_ctx(Config.fetch!(ctx.inst), location)
    {store, config} = repl.store
    {:ok, keys, nil} = store.list(config, "#{location.prefix}/cells/#{@cell}/", nil, 100_000)
    Enum.reject(keys, &String.ends_with?(&1, "/lease"))
  end
end
