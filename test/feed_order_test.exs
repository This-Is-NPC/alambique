defmodule Alambique.FeedOrderTest do
  @moduledoc """
  The keys of the feed objects of a cell follow the commit order, across late
  publications, an emptied queue and a clock that moves back (design §13, F1).

  The test pins the interleaving that broke it: a batch published late, then
  a batch stamped earlier whose acknowledgement came after the queue emptied.
  The property drives commits, held uploads and the clock at random.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Feed.Keys
  alias AlambiqueTestkit.{GatedStore, ManualClock}

  @schema [{1, "CREATE TABLE log (id INTEGER PRIMARY KEY)"}]
  @start 1_800_000_000_000

  setup do
    base = setup_store()
    {:ok, gate} = GatedStore.start_link()
    world = ManualClock.new(@start)
    Map.merge(base, %{gate: gate, world: world, gated: GatedStore.wrap(gate, base.store)})
  end

  defp start(ctx, prefix, feed) do
    start_instance(%{ctx | store: ctx.gated},
      prefix: prefix,
      migrations: @schema,
      pipeline_depth: 16,
      lease_ttl_ms: 20_000,
      compaction: false,
      clock: {ManualClock, ManualClock.config(ctx.world, 0)},
      feed: feed
    )
  end

  defp exec(inst, cell, n),
    do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO log VALUES (#{n})")

  defp held(ctx, kind) do
    pattern = if kind == :feed, do: "/feed/", else: ".ltx"
    Enum.filter(GatedStore.held(ctx.gate), &String.contains?(&1, pattern))
  end

  defp feed_keys(ctx, prefix) do
    {mod, config} = ctx.store
    {:ok, keys, _} = mod.list(config, "#{prefix}/feed/", nil, 1000)
    keys
  end

  # TXIDs of the objects of a cell in the order of their keys.
  defp txids_by_key(ctx, prefix, cell) do
    shard = Keys.shard(cell, 16)

    ctx
    |> feed_keys(prefix)
    |> Enum.map(&Keys.parse(prefix, shard, &1))
    |> Enum.sort_by(&{&1.time, &1.txid})
    |> Enum.map(& &1.txid)
  end

  test "a batch stamped before a late one does not take a key before it", ctx do
    prefix = "order"
    inst = start(ctx, prefix, window_ms: 1)
    cell = unique_cell()
    GatedStore.hold(ctx.gate, ~r{/feed/})

    # Z is stamped now and its object is held; A is stamped 100 ms later.
    exec(inst, cell, 0)
    eventually(fn -> length(held(ctx, :feed)) == 1 end)
    ManualClock.advance(ctx.world, 100)
    exec(inst, cell, 1)

    # B is stamped 2 ms after A, but its segment is held: it is acknowledged
    # only after A was published.
    GatedStore.hold(ctx.gate, ~r{\.ltx$})
    ManualClock.advance(ctx.world, 2)
    b = Task.async(fn -> exec(inst, cell, 2) end)
    eventually(fn -> length(held(ctx, :segment)) == 1 end)

    # A is 3 ms old when the sender reaches it: late, so it takes the time now.
    ManualClock.advance(ctx.world, 1)
    [z] = held(ctx, :feed)
    GatedStore.release(ctx.gate, z)
    eventually(fn -> length(held(ctx, :feed)) == 1 end)
    [a] = held(ctx, :feed)
    GatedStore.release(ctx.gate, a)
    eventually(fn -> length(feed_keys(ctx, prefix)) == 2 end)

    # B is 1 ms old: on time under its own key, which is before A's.
    [segment] = held(ctx, :segment)
    GatedStore.release(ctx.gate, segment)
    Task.await(b)
    eventually(fn -> length(held(ctx, :feed)) == 1 end)
    GatedStore.open(ctx.gate)
    eventually(fn -> length(feed_keys(ctx, prefix)) == 3 end)

    txids = txids_by_key(ctx, prefix, cell)
    assert txids == Enum.sort(txids)
  end

  # Every commit inserts a fresh id: a repeated one fails the primary key.
  # Every put of the cell is held; the script moves the clock (also back) and
  # lets uploads of segments and feed objects land one at a time.
  defp step(ctx, inst, cell, :commit, tasks) do
    n = System.unique_integer([:positive])
    before = length(held(ctx, :segment))
    task = Task.async(fn -> exec(inst, cell, n) end)
    eventually(fn -> length(held(ctx, :segment)) > before or Task.yield(task, 0) != nil end)
    [task | tasks]
  end

  defp step(ctx, _inst, _cell, {:clock, ms}, tasks) do
    ManualClock.advance(ctx.world, ms)
    tasks
  end

  defp step(ctx, _inst, _cell, {:land, kind}, tasks) do
    case held(ctx, kind) do
      [key | _] -> GatedStore.release(ctx.gate, key)
      [] -> :ok
    end

    tasks
  end

  defp op do
    one_of([
      constant(:commit),
      tuple({constant(:clock), integer(-40..60)}),
      tuple({constant(:land), member_of([:segment, :feed])})
    ])
  end

  property "keys follow the commit order whatever the delays and the clock", ctx do
    check all(
            ops <- list_of(op(), min_length: 6, max_length: 14),
            window_ms <- integer(1..20),
            max_runs: 100,
            max_shrinking_steps: 0
          ) do
      prefix = "order#{System.unique_integer([:positive])}"
      inst = start(ctx, prefix, window_ms: window_ms)
      cell = unique_cell()
      exec(inst, cell, 0)
      eventually(fn -> feed_keys(ctx, prefix) != [] end)
      GatedStore.hold(ctx.gate, ~r{/feed/})
      GatedStore.hold(ctx.gate, ~r{\.ltx$})

      tasks = Enum.reduce(ops, [], &step(ctx, inst, cell, &1, &2))
      commits = length(tasks) + 1

      GatedStore.open(ctx.gate)
      Enum.each(tasks, &Task.await/1)
      eventually(fn -> length(feed_keys(ctx, prefix)) == commits end)

      txids = txids_by_key(ctx, prefix, cell)
      assert txids == Enum.sort(txids)
      stop_supervised!(inst)
    end
  end
end
