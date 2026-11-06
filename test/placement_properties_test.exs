defmodule Alambique.PlacementPropertiesTest do
  @moduledoc """
  Guarantee P1 of the placement (design §18) on a cluster of peer nodes.

  Random scenarios mix writes from every node with relocations of cells to
  either location (so cells move back and forth), cleanups of the location a
  cell left, crashes at each step of both, node kills and restarts, faults in
  both stores, store partitions and clock skew. Nodes keep the placement
  answer they cached before a move (a stale view). While the scenario runs, a
  sampler checks that no cell is ever writable in both locations. After the
  cluster is healed:

    * every relocation that was started can be run again to completion, and
      the location a cell left can be cleaned up;
    * no acknowledged write is lost, and nothing was stored that was not
      attempted;
    * the epochs of a cell's commits never decrease over time, and no epoch
      committed in both locations (fencing across any sequence of moves);
    * the change feed, read from both locations, holds every stored row exactly
      once after deduplication, in commit order (F1).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.{ClusterFuns, Feed, Lease, Location}
  alias AlambiqueTestkit.{Cluster, CrashPoints, MemoryStore}
  alias AlambiqueTestkit.Placement, as: TestPlacement

  @moduletag :cluster
  @moduletag timeout: 900_000

  @names [:n1, :n2, :n3]
  @cells ["c0", "c1", "c2"]
  @ttl_ms 2_000
  @skew_ms 400
  @call_timeout_ms 3_000
  @relocate_timeout_ms 8_000
  @moved_grace_ms 1_000
  # Longer than the gap before a cell may move back to a cleaned location.
  @settle_ms 13_000
  @settle_within_ms @ttl_ms + @skew_ms + 15_000
  # Both the immediate deletion and a grace that keeps the marked objects
  # pending while cells move back.
  @deletion_graces [0, 60_000]
  @points [
    :relocate_after_acquire,
    :relocate_after_install,
    :relocate_after_snapshot,
    :relocate_before_mark,
    :relocate_after_mark,
    :relocate_after_commit,
    :relocate_before_cleanup,
    :trash_before_mark,
    :trash_after_mark,
    :trash_before_delete,
    :trash_after_delete,
    :cleanup_before_mark,
    :repl_after_put,
    :lease_after_put
  ]

  defp op do
    node = member_of(@names)
    cell = member_of(@cells)

    frequency([
      {16, tuple({constant(:write), node, cell})},
      {7, tuple({constant(:relocate), node, cell, member_of(["a", "b"])})},
      {4, tuple({constant(:cleanup), node, cell, member_of(["a", "b"])})},
      {5, tuple({constant(:crash_point), node, member_of(@points)})},
      {2, tuple({constant(:hibernate), node, cell})},
      {2, tuple({constant(:kill), node})},
      {3, tuple({constant(:restart), node})},
      {2,
       tuple(
         {constant(:fault), constant(:a), node, member_of([:error, :lost_ack]), integer(1..4)}
       )},
      {2,
       tuple(
         {constant(:fault), constant(:b), node, member_of([:error, :lost_ack]), integer(1..4)}
       )},
      {1, tuple({constant(:partition_store), node})},
      {1, tuple({constant(:heal_store), node})},
      {1, tuple({constant(:skew), node, integer(-div(@skew_ms, 2)..div(@skew_ms, 2))})},
      {5, tuple({constant(:round_trip), node, cell})},
      {4, tuple({constant(:pass), integer(100..1200)})}
    ])
  end

  property "one location accepts the writes of a cell, and no acknowledged write is lost" do
    check all(
            ops <- resize(list_of(op(), min_length: 20, max_length: 40), 40),
            max_runs: 4,
            max_shrinking_steps: 0
          ) do
      # Every generated scenario runs with deletion grace 0 and with a grace
      # that leaves the marked objects pending through the moves back.
      for grace <- @deletion_graces, do: run_scenario(ops, grace)
    end
  end

  defp run_scenario(ops, grace) do
    {:ok, b_pid} = MemoryStore.start_link()
    {:ok, table_pid} = MemoryStore.start_link()
    b = MemoryStore.config(b_pid)
    table = MemoryStore.config(table_pid)

    {:ok, cluster} =
      Cluster.start_link(
        names: @names,
        ttl_ms: @ttl_ms,
        skew_margin_ms: @skew_ms,
        op_timeout_ms: 1_500,
        migrations: ClusterFuns.feed_schema(),
        placement: &placement(&1, b, table),
        instance_opts: [
          feed: [window_ms: 500],
          moved_grace_ms: @moved_grace_ms,
          deletion_grace_ms: grace
        ]
      )

    locations = locations(Cluster.store(cluster), b)
    sampler = start_sampler(locations, table)

    try do
      # Every node learns the first answer, so later moves meet stale views.
      warm_caches(cluster)

      run =
        Enum.reduce(
          ops,
          %{seq: 0, dead: MapSet.new(), acked: [], tried: [], moving: %{}, b: b, grace: grace},
          &step(cluster, &1, &2)
        )

      violations = stop_sampler(sampler)

      assert violations == [],
             "a cell was writable in both locations with grace #{grace}: #{inspect(violations)}"

      Cluster.heal_all(cluster)
      MemoryStore.clear(b)
      for node <- run.dead, do: Cluster.restart(cluster, node)
      verify(cluster, run, locations)
    after
      Process.unlink(cluster)
      Cluster.stop(cluster)
      CrashPoints.reset()
    end
  end

  # Locations: `a` is the cluster's store, `b` has a store of its own.
  defp locations({store, config}, b) do
    [
      %Location{id: "a", store: {store, config}, prefix: "pa"},
      %Location{id: "b", store: {MemoryStore, b}, prefix: "pb"}
    ]
  end

  defp placement(shared, b, table),
    do: {TestPlacement, placement_opts(locations(shared, b), table)}

  defp placement_opts(locations, table),
    do: [locations: locations, table: {MemoryStore, table}, initial: Map.new(@cells, &{&1, "a"})]

  # Scenario

  defp write(cluster, node, cell, n) do
    inst = Cluster.instance(cluster)
    args = [inst, cell, "INSERT INTO log (n) VALUES (?)", [n], [timeout: @call_timeout_ms]]
    Cluster.call(cluster, node, Alambique, :execute, args, @call_timeout_ms + 2_000)
  end

  defp warm_caches(cluster) do
    inst = Cluster.instance(cluster)

    for node <- @names, cell <- @cells do
      args = [inst, cell, "SELECT 1", [], [timeout: @call_timeout_ms]]
      Cluster.call(cluster, node, Alambique, :query, args, @call_timeout_ms + 2_000)
    end
  end

  defp step(cluster, {:write, node, cell}, run) do
    n = run.seq + 1

    case write(cluster, node, cell, n) do
      {:ok, {:ok, _}} ->
        %{run | seq: n, acked: [{cell, n} | run.acked], tried: [{cell, n} | run.tried]}

      _ ->
        %{run | seq: n, tried: [{cell, n} | run.tried]}
    end
  end

  defp step(cluster, {:relocate, node, cell, dest}, run) do
    args = [Cluster.instance(cluster), cell, dest, [timeout: @relocate_timeout_ms]]
    Cluster.call(cluster, node, Alambique, :relocate, args, @relocate_timeout_ms + 4_000)
    %{run | moving: Map.put(run.moving, cell, dest)}
  end

  # There and back with the writes and the cleanup a move back needs, so every
  # scenario has a chance to move a cell twice.
  defp step(cluster, {:round_trip, node, cell}, run) do
    [
      {:relocate, node, cell, "b"},
      {:pass, 600},
      {:cleanup, node, cell, "a"},
      {:write, node, cell},
      {:pass, @settle_ms},
      {:relocate, node, cell, "a"},
      {:write, node, cell},
      {:pass, 600},
      {:cleanup, node, cell, "b"}
    ]
    |> Enum.reduce(run, &step(cluster, &1, &2))
  end

  defp step(cluster, {:cleanup, node, cell, from}, run) do
    args = [Cluster.instance(cluster), cell, from, [grace_ms: 0]]
    Cluster.call(cluster, node, Alambique, :cleanup_moved, args, @relocate_timeout_ms + 4_000)
    run
  end

  defp step(cluster, {:crash_point, node, point}, run) do
    unless node in run.dead,
      do: Cluster.call(cluster, node, CrashPoints, :arm, [point, :kill, [times: 1]])

    run
  end

  defp step(cluster, {:hibernate, node, cell}, run) do
    args = [Cluster.instance(cluster), cell, [timeout: @call_timeout_ms]]
    Cluster.call(cluster, node, Alambique, :hibernate, args, @call_timeout_ms + 2_000)
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

  defp step(cluster, {:fault, :a, node, kind, times}, run) do
    unless node in run.dead,
      do:
        Cluster.inject(cluster, node, kind: kind, times: times, ms: 700, ops: [:put, :get, :list])

    run
  end

  defp step(cluster, {:fault, :b, node, kind, times}, run) do
    unless node in run.dead do
      MemoryStore.inject(run.b,
        kind: kind,
        times: times,
        node: Cluster.node(cluster, node),
        ms: 700,
        ops: [:put, :get, :list]
      )
    end

    run
  end

  defp step(cluster, {:partition_store, node}, run),
    do: tap(run, fn _ -> if node not in run.dead, do: Cluster.partition_store(cluster, node) end)

  defp step(cluster, {:heal_store, node}, run),
    do: tap(run, fn _ -> if node not in run.dead, do: Cluster.heal_store(cluster, node) end)

  defp step(cluster, {:skew, node, ms}, run),
    do: tap(run, fn _ -> Cluster.set_clock_offset(cluster, node, ms) end)

  defp step(_cluster, {:pass, ms}, run), do: tap(run, fn _ -> Process.sleep(ms) end)

  # The sampler reads the placement, then the lease of the other location,
  # then the placement again. If the placement named the same location both
  # times, the other lease must not be one that anyone can hold: absent, a
  # moved marker, or a free lease installed by a move that did not commit yet.
  # (A move completes within a placement write, far longer than the sampling
  # step, so the placement does not flip twice between the reads.)
  defp start_sampler([a, b] = locations, table) do
    opts = placement_opts(locations, table)

    leases =
      for location <- [a, b], into: %{} do
        {location.id,
         Lease.ctx(
           store: location.store,
           clock: {Alambique.Clock.System, nil},
           prefix: location.prefix,
           ttl_ms: @ttl_ms,
           skew_margin_ms: @skew_ms
         )}
      end

    spawn_link(fn -> sample(opts, leases, []) end)
  end

  defp sample(opts, leases, violations) do
    receive do
      {:stop, from} -> send(from, {:done, violations})
    after
      25 ->
        found =
          for cell <- @cells,
              {:ok, %{id: id}} <- [located(opts, cell)],
              other = if(id == "a", do: "b", else: "a"),
              {:ok, %{moved_to: nil, moved_from: nil}} <- [Lease.read(leases[other], cell)],
              {:ok, %{id: ^id}} <- [located(opts, cell)],
              do: {cell, id}

        sample(opts, leases, found ++ violations)
    end
  end

  defp located(opts, cell), do: TestPlacement.locate(opts, cell)

  defp stop_sampler(pid) do
    send(pid, {:stop, self()})

    receive do
      {:done, violations} -> Enum.uniq(violations)
    after
      5_000 -> flunk("sampler did not stop")
    end
  end

  # Checks

  defp verify(cluster, run, locations) do
    inst = Cluster.instance(cluster)

    for {cell, dest} <- run.moving, do: settle_move(cluster, inst, cell, dest)

    stored = for cell <- @cells, into: %{}, do: {cell, durable_numbers(cluster, inst, cell)}

    for {cell, n} <- run.acked do
      assert n in stored[cell], "acknowledged write #{n} of #{cell} is lost"
    end

    tried = Enum.group_by(run.tried, &elem(&1, 0), &elem(&1, 1))

    for cell <- @cells do
      extra = stored[cell] -- [0 | Map.get(tried, cell, [])]
      assert extra == [], "cell #{cell} holds rows nobody wrote: #{inspect(extra)}"
    end

    check_commit_order(cluster)
    clean_up_all(cluster, inst, locations)

    assert stored ==
             for(cell <- @cells, into: %{}, do: {cell, durable_numbers(cluster, inst, cell)})

    check_feed(locations, stored)
  end

  # Runs the last relocation asked for until it returns. A move left half done
  # towards the other location is finished first.
  defp settle_move(cluster, inst, cell, dest) do
    eventually(
      fn ->
        Enum.find_value(@names, fn node ->
          case relocate(cluster, node, inst, cell, dest) do
            {:ok, {:ok, %Location{id: ^dest}}} ->
              true

            {:ok, {:error, {:move_pending, other}}} ->
              relocate(cluster, node, inst, cell, other)
              nil

            # Back to a location the cell left: it must be cleaned up first,
            # also when a cleanup was lost with a node. A cleaned one only
            # needs time (`:destination_settling`; cleaning again would
            # restart it).
            {:ok, {:error, reason}}
            when reason in [:destination_not_empty, :destination_not_cleaned] ->
              args = [inst, cell, dest, [grace_ms: 0]]
              Cluster.call(cluster, node, Alambique, :cleanup_moved, args, 20_000)
              nil

            _ ->
              nil
          end
        end)
      end,
      120_000
    )
  end

  defp relocate(cluster, node, inst, cell, dest) do
    args = [inst, cell, dest, [timeout: 15_000]]
    Cluster.call(cluster, node, Alambique, :relocate, args, 20_000)
  end

  # Fencing across moves: epochs only grow along a cell's commits in time, and
  # no epoch was ever committed in both locations.
  defp check_commit_order(cluster) do
    commits =
      for %{event: [:alambique, :cell, :commit], meta: %{result: :ok} = meta, at: at} <-
            Cluster.events(cluster),
          do: {meta.cell, at, meta.epoch, meta.location}

    for {cell, entries} <- Enum.group_by(commits, &elem(&1, 0)) do
      sorted = Enum.sort_by(entries, &elem(&1, 1))
      epochs = Enum.map(sorted, &elem(&1, 2))
      assert epochs == Enum.sort(epochs), "cell #{cell} committed with epochs #{inspect(epochs)}"

      for {epoch, group} <- Enum.group_by(sorted, &elem(&1, 2)) do
        assert group |> Enum.map(&elem(&1, 3)) |> Enum.uniq() |> length() == 1,
               "cell #{cell} committed epoch #{epoch} in both locations"
      end
    end
  end

  # Every location a cell is not in can be cleaned up once healed, and the
  # cell still reads what was acknowledged afterwards.
  defp clean_up_all(cluster, inst, locations) do
    for cell <- @cells, %Location{id: from} <- locations do
      eventually(
        fn ->
          Enum.find_value(@names, fn node ->
            args = [inst, cell, from, [grace_ms: 0]]

            case Cluster.call(cluster, node, Alambique, :cleanup_moved, args, 20_000) do
              {:ok, {:ok, %{result: :cleaned}}} -> true
              {:ok, {:ok, %{result: :waiting}}} -> nil
              {:ok, {:error, reason}} when reason in [:live_location, :not_moved] -> true
              _ -> nil
            end
          end)
        end,
        60_000
      )
    end
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

  defp check_feed(locations, stored) do
    ctx = Feed.ctx(locations: locations, settle_ms: 0)

    try do
      eventually(fn -> feed_numbers(ctx) == stored end, @settle_within_ms)
    rescue
      RuntimeError -> :timeout
    end

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
