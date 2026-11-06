defmodule Alambique.FeedPropertiesTest do
  @moduledoc """
  Invariant F1 of the change feed (design §13) on a cluster of peer nodes.

  Random scenarios mix writes from random nodes with hibernations and moves,
  node kills and restarts, store faults (including lost acknowledgements of
  feed objects), partitions, clock skew within the margin and crashes of a
  cell between its acknowledgement and the publication of the feed. The feed
  puts a random number of batches (1 to 8) in an object. After the
  cluster is healed, for every cell:

    * the feed holds every durable write exactly once after deduplication by
      idempotency key, and nothing else, so no record exists for a write that
      was not stored;
    * the first occurrences come in commit order;
    * duplicates are identical.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.{ClusterFuns, Feed}
  alias AlambiqueTestkit.{CrashPoints, Cluster}

  @moduletag :cluster
  @moduletag timeout: 900_000

  @names [:n1, :n2, :n3]
  @cells ["c0", "c1", "c2"]
  @ttl_ms 2_000
  @skew_ms 400
  @call_timeout_ms 3_000
  @settle_within_ms @ttl_ms + @skew_ms + 15_000
  @crash_points [
    :feed_before_publish,
    :feed_before_group_put,
    :feed_before_stop_flush,
    :feed_after_put,
    :feed_before_cleanup,
    :repl_after_put
  ]

  defp op do
    node = member_of(@names)
    cell = member_of(@cells)

    frequency([
      {20, tuple({constant(:write), node, cell})},
      {2, tuple({constant(:hibernate), node, cell})},
      {2, tuple({constant(:move), node, cell, node})},
      {2, tuple({constant(:kill), node})},
      {3, tuple({constant(:restart), node})},
      {2,
       tuple(
         {constant(:store_fault), node, member_of([:error, :timeout, :lost_ack]), integer(1..4)}
       )},
      {3, tuple({constant(:feed_fault), node, member_of([:error, :lost_ack]), integer(1..3)})},
      {3, tuple({constant(:crash_point), node, member_of(@crash_points)})},
      {1, tuple({constant(:partition_store), node})},
      {1, tuple({constant(:heal_store), node})},
      {1, tuple({constant(:skew), node, integer(-div(@skew_ms, 2)..div(@skew_ms, 2))})},
      {4, tuple({constant(:pass), integer(100..1500)})}
    ])
  end

  property "every durable write appears on the feed exactly once, in commit order" do
    check all(
            ops <- resize(list_of(op(), min_length: 20, max_length: 45), 45),
            object_batches <- integer(1..8),
            linger_ms <- member_of([0, 5, 20]),
            max_runs: 12,
            max_shrinking_steps: 0
          ) do
      {:ok, cluster} =
        Cluster.start_link(
          names: @names,
          ttl_ms: @ttl_ms,
          skew_margin_ms: @skew_ms,
          op_timeout_ms: 1_500,
          migrations: ClusterFuns.feed_schema(),
          instance_opts: [
            feed: [window_ms: 500, object_batches: object_batches, linger_ms: linger_ms]
          ]
        )

      try do
        run = Enum.reduce(ops, %{seq: 0, dead: MapSet.new()}, &step(cluster, &1, &2))
        Cluster.heal_all(cluster)
        for node <- run.dead, do: Cluster.restart(cluster, node)
        verify(cluster)
      after
        Process.unlink(cluster)
        Cluster.stop(cluster)
      end
    end
  end

  # Scenario

  defp step(cluster, {:write, node, cell}, run) do
    n = run.seq + 1
    inst = Cluster.instance(cluster)
    args = [inst, cell, "INSERT INTO log (n) VALUES (?)", [n], [timeout: @call_timeout_ms]]
    Cluster.call(cluster, node, Alambique, :execute, args, @call_timeout_ms + 2_000)
    %{run | seq: n}
  end

  defp step(cluster, {:hibernate, node, cell}, run) do
    args = [Cluster.instance(cluster), cell, [timeout: @call_timeout_ms]]
    Cluster.call(cluster, node, Alambique, :hibernate, args, @call_timeout_ms + 2_000)
    run
  end

  defp step(cluster, {:move, node, cell, target}, run) do
    args = [
      Cluster.instance(cluster),
      cell,
      Cluster.node(cluster, target),
      [timeout: @call_timeout_ms]
    ]

    Cluster.call(cluster, node, Alambique, :move, args, @call_timeout_ms + 2_000)
    run
  end

  defp step(cluster, {:kill, node}, run) do
    unless node in run.dead, do: Cluster.kill(cluster, node)
    %{run | dead: MapSet.put(run.dead, node)}
  end

  defp step(cluster, {:restart, node}, run) do
    if node in run.dead, do: Cluster.restart(cluster, node)
    %{run | dead: MapSet.delete(run.dead, node)}
  end

  defp step(cluster, {:store_fault, node, kind, times}, run) do
    unless node in run.dead,
      do:
        Cluster.inject(cluster, node, kind: kind, times: times, ms: 700, ops: [:put, :get, :list])

    run
  end

  defp step(cluster, {:feed_fault, node, kind, times}, run) do
    unless node in run.dead,
      do: Cluster.inject(cluster, node, kind: kind, times: times, key: ~r{/feed/}, ops: [:put])

    run
  end

  defp step(cluster, {:crash_point, node, point}, run) do
    unless node in run.dead,
      do: Cluster.call(cluster, node, CrashPoints, :arm, [point, :kill, [times: 1]])

    run
  end

  defp step(cluster, {:partition_store, node}, run),
    do: tap(run, fn _ -> if node not in run.dead, do: Cluster.partition_store(cluster, node) end)

  defp step(cluster, {:heal_store, node}, run),
    do: tap(run, fn _ -> if node not in run.dead, do: Cluster.heal_store(cluster, node) end)

  defp step(cluster, {:skew, node, ms}, run),
    do: tap(run, fn _ -> Cluster.set_clock_offset(cluster, node, ms) end)

  defp step(_cluster, {:pass, ms}, run), do: tap(run, fn _ -> Process.sleep(ms) end)

  # Checks

  defp verify(cluster) do
    inst = Cluster.instance(cluster)
    {store, config} = Cluster.store(cluster)

    stored = for cell <- @cells, into: %{}, do: {cell, durable_numbers(cluster, inst, cell)}
    ctx = Feed.ctx(store: {store, config}, prefix: "cluster", settle_ms: 0)

    # Cells that stayed awake publish again on their retry timer; the reads
    # above woke the rest, which publish what the last owner left.
    try do
      eventually(fn -> feed_numbers(ctx) == stored end, @settle_within_ms)
    rescue
      RuntimeError -> :timeout
    end

    check_feed(ctx, stored)
  end

  defp durable_numbers(cluster, inst, cell) do
    eventually(
      fn ->
        Enum.find_value(@names, fn node ->
          args = [inst, cell, "SELECT n FROM log ORDER BY id", [], [timeout: @call_timeout_ms]]

          case Cluster.call(cluster, node, Alambique, :query, args, @call_timeout_ms + 2_000) do
            {:ok, {:ok, %{rows: rows}}} -> List.flatten(rows)
            _ -> nil
          end
        end)
      end,
      @settle_within_ms
    )
  end

  defp check_feed(ctx, stored) do
    all = read_all(ctx)

    for cell <- @cells do
      mine = Enum.filter(all, &(&1.cell == cell and &1.table == "log"))
      firsts = Enum.uniq_by(mine, & &1.id)

      assert Enum.map(firsts, & &1.values["n"]) == stored[cell],
             "cell #{cell}: feed #{inspect(Enum.map(firsts, & &1.values["n"]))} stored #{inspect(stored[cell])}"

      positions = Enum.map(firsts, & &1.position)
      assert positions == Enum.sort(positions), "cell #{cell}: first occurrences out of order"

      for {id, copies} <- Enum.group_by(mine, & &1.id), length(copies) > 1 do
        assert copies |> Enum.map(&Map.delete(&1, :at)) |> Enum.uniq() |> length() == 1,
               "cell #{cell}: duplicates of #{id} differ"
      end
    end

    assert Enum.all?(all, &(&1.table == "log")), "records of an unexpected table"
  end

  defp feed_numbers(ctx) do
    ctx
    |> read_all()
    |> Enum.uniq_by(& &1.id)
    |> Enum.group_by(& &1.cell, & &1.values["n"])
    |> then(fn by_cell ->
      for cell <- @cells, into: %{}, do: {cell, Map.get(by_cell, cell, [])}
    end)
  end

  defp read_all(ctx, position \\ nil, acc \\ []) do
    {:ok, records, next} = Feed.read(ctx, position, 100)
    if records == [], do: acc, else: read_all(ctx, next, acc ++ records)
  end
end
