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

  property "one node id is never live under two incarnations", %{store: {_, conf} = store} do
    op =
      one_of([
        tuple({constant(:register), member_of(~w(i1 i2 i3)), member_of([:restart, :free])}),
        tuple({constant(:advance), integer(0..@ttl), constant(nil)}),
        tuple({constant(:deregister), constant(nil), constant(nil)})
      ])

    check all(ops <- list_of(op, min_length: 1, max_length: 25), max_runs: 100) do
      MemoryStore.clear(conf)
      world = ManualClock.new(@start)
      ctx = ctx(store, world, unique_prefix("inc"))

      Enum.reduce(ops, nil, fn
        {:register, incarnation, takeover}, held ->
          case Nodes.register(ctx, "n", incarnation, "adv", takeover: takeover) do
            {:ok, entry} -> assert_one(ctx, entry)
            {:error, {:held, _, _}} -> assert_one(ctx, held)
          end

        {:advance, ms, _}, held ->
          ManualClock.advance(world, ms)
          held

        {:deregister, _, _}, held ->
          if held, do: Nodes.deregister(ctx, held)
          held
      end)
    end
  end

  # At most one live entry for the id, and it is the last one written.
  defp assert_one(ctx, entry) do
    live = live(ctx)
    assert length(live) <= 1
    if entry && live != [], do: assert(live == [{"n", entry.incarnation}])
    entry
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
