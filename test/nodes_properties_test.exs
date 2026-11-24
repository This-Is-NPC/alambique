defmodule Alambique.NodesPropertiesTest do
  @moduledoc "Invariants of the node registry (design §22)."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.LeaseHelpers, only: [unique_prefix: 1]

  alias Alambique.Nodes
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @start 1_000_000_000
  @ttl 10_000
  @skew 1_000

  setup do
    {:ok, pid} = start_supervised(MemoryStore, restart: :temporary)
    {:ok, store: {MemoryStore, MemoryStore.config(pid)}}
  end

  defp ctx(store, world, prefix) do
    Nodes.ctx(
      store: store,
      clock: {ManualClock, ManualClock.config(world, 0)},
      prefix: prefix,
      ttl_ms: @ttl,
      skew_margin_ms: @skew
    )
  end

  # Distinct node ids, at least one.
  defp ids(max) do
    map(list_of(member_of(Enum.take(~w(a b c d e f), max)), min_length: 1), &Enum.uniq/1)
  end

  defp live(ctx) do
    {:ok, entries} = Nodes.list(ctx)
    Enum.map(entries, &{&1.node_id, &1.incarnation})
  end

  property "an expired entry leaves the listing, a live one stays", %{store: {_, conf} = store} do
    check all(
            ids <- ids(5),
            gaps <- list_of(integer(0..(2 * @ttl)), length: length(ids)),
            wait <- integer(0..(3 * @ttl)),
            max_runs: 100
          ) do
      MemoryStore.clear(conf)
      world = ManualClock.new(@start)
      ctx = ctx(store, world, unique_prefix("exp"))

      registered_at =
        for {id, gap} <- Enum.zip(ids, gaps) do
          ManualClock.advance(world, gap)
          {:ok, _} = Nodes.register(ctx, id, "i", "adv")
          {id, ManualClock.world_ms(world)}
        end

      ManualClock.advance(world, wait)
      now = ManualClock.world_ms(world)
      expected = for {id, at} <- registered_at, now <= at + @ttl + @skew, do: {id, "i"}

      assert live(ctx) == Enum.sort(expected)
    end
  end

  # `held` is the last registration that succeeded, and whether it was released.
  # The entry is live until its expiry plus the skew margin, boundary included.
  property "one node id is never live under two incarnations", %{store: {_, conf} = store} do
    op =
      one_of([
        tuple({constant(:register), member_of(~w(i1 i2 i3)), member_of([:restart, :free])}),
        tuple({constant(:advance), integer(0..@ttl), constant(nil)}),
        # Another incarnation registers a tick before, at and after the end of the entry.
        tuple({constant(:edge), member_of([-1, 0, 1]), member_of([:restart, :free])}),
        tuple({constant(:deregister), constant(nil), constant(nil)})
      ])

    check all(ops <- list_of(op, min_length: 1, max_length: 25), max_runs: 100) do
      MemoryStore.clear(conf)
      world = ManualClock.new(@start)
      ctx = ctx(store, world, unique_prefix("inc"))

      Enum.reduce(ops, nil, &step(&1, &2, ctx, world))
    end
  end

  defp step({:register, incarnation, takeover}, held, ctx, world) do
    now = ManualClock.world_ms(world)
    answer = Nodes.register(ctx, "n", incarnation, "adv", takeover: takeover)
    holding = holder(held, now)

    held =
      if holding && holding.incarnation != incarnation && takeover == :free do
        assert answer == {:error, {:held, holding.incarnation, holding.expires_at_ms}}
        held
      else
        assert {:ok, entry} = answer
        assert entry.incarnation == incarnation
        %{entry: entry, released?: false}
      end

    assert live(ctx) == listed(held, now)
    held
  end

  defp step({:advance, ms, _}, held, ctx, world) do
    ManualClock.advance(world, ms)
    assert live(ctx) == listed(held, ManualClock.world_ms(world))
    held
  end

  defp step({:edge, offset, takeover}, held, ctx, world) do
    if held do
      delta = held.entry.expires_at_ms + @skew + offset - ManualClock.world_ms(world)
      if delta > 0, do: ManualClock.advance(world, delta)
    end

    step({:register, "i9", takeover}, held, ctx, world)
  end

  defp step({:deregister, _, _}, %{released?: false} = held, ctx, world) do
    assert :ok = Nodes.deregister(ctx, held.entry)
    held = %{held | released?: true}
    assert live(ctx) == listed(held, ManualClock.world_ms(world))
    held
  end

  defp step({:deregister, _, _}, held, _ctx, _world), do: held

  defp holder(nil, _now), do: nil
  defp holder(%{released?: true}, _now), do: nil

  defp holder(%{entry: entry}, now),
    do: if(now <= entry.expires_at_ms + @skew, do: entry)

  defp listed(held, now) do
    case holder(held, now) do
      nil -> []
      entry -> [{"n", entry.incarnation}]
    end
  end

  property "deregister leaves the listing at once, the others stay", %{store: {_, conf} = store} do
    check all(
            ids <- ids(6),
            leaving <- list_of(boolean(), length: length(ids)),
            max_runs: 100
          ) do
      MemoryStore.clear(conf)
      world = ManualClock.new(@start)
      ctx = ctx(store, world, unique_prefix("dereg"))

      entries = for id <- ids, do: {id, elem(Nodes.register(ctx, id, "i", "adv"), 1)}

      for {{_id, entry}, true} <- Enum.zip(entries, leaving),
          do: assert(:ok = Nodes.deregister(ctx, entry))

      staying = for {{id, _}, false} <- Enum.zip(entries, leaving), do: {id, "i"}
      assert live(ctx) == Enum.sort(staying)
    end
  end
end
