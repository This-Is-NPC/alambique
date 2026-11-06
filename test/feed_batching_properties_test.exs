defmodule Alambique.FeedBatchingPropertiesTest do
  @moduledoc """
  A feed object per group of batches (design §13) on one node over the memory
  store. Random commits of varied sizes, bursts and pauses, with random
  `object_batches` and `linger_ms`, slow feed puts, crashes of the sender
  after a put and restarts of it, and random reads with resumes from saved
  positions. For every cell:

    * the feed holds every durable row exactly once after deduplication by
      idempotency key, and nothing else, in commit order;
    * a resume from any saved position loses nothing;
    * there are no more objects than batches.

  A second property stops cells in the middle of a linger (hibernation, idle,
  shutdown), crashes the cell or the sender during the stop flush and moves
  cells to another owner. Every acknowledged commit is in the feed once, in
  commit order, and a clean stop has already published its cell.

  Plain tests pin the key rule of a group and the reading of a feed that mixes both object shapes.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Feed
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @schema [{1, "CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER)"}]
  @cells ["c0", "c1"]

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # Helpers

  defp start(ctx, prefix, feed, extra \\ []),
    do: start_instance(ctx, [migrations: @schema, prefix: prefix, feed: feed] ++ extra)

  defp feed_ctx(%{store: store}, prefix),
    do: Feed.ctx(store: store, prefix: prefix, settle_ms: 0)

  # Inserts `k` rows in one transaction; returns their numbers.
  defp commit(inst, seq, cell, k) do
    ns = Agent.get_and_update(seq, fn n -> {Enum.to_list((n + 1)..(n + k)//1), n + k} end)
    sql = "INSERT INTO log (n) VALUES " <> Enum.map_join(ns, ",", fn _ -> "(?)" end)
    {:ok, _} = Alambique.execute(inst, cell, sql, ns)
    ns
  end

  defp durable(inst, cell) do
    {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT n FROM log ORDER BY id")
    List.flatten(rows)
  end

  # Pages of up to `limit` objects after `position`: `{records, next}` each.
  defp pages(ctx, limit, position \\ nil, acc \\ []) do
    {:ok, records, next} = Feed.read(ctx, position, limit)

    if records == [],
      do: Enum.reverse(acc),
      else: pages(ctx, limit, next, [{records, next} | acc])
  end

  defp flat(pages), do: Enum.flat_map(pages, &elem(&1, 0))

  defp numbers(records, cell) do
    records
    |> Enum.filter(&(&1.cell == cell and &1.table == "log"))
    |> Enum.uniq_by(& &1.id)
    |> Enum.map(& &1.values["n"])
  end

  defp feed_keys(%{store: {mod, config}}, prefix) do
    {:ok, keys, _} = mod.list(config, prefix <> "/feed/", nil, 10_000)
    keys
  end

  defp bodies(%{store: {mod, config}} = ctx, prefix) do
    for key <- feed_keys(ctx, prefix) do
      {:ok, body, _, _} = mod.get(config, key)
      {:ok, json} = Alambique.Feed.Record.json(body, Alambique.Feed.Record.default_max_bytes())
      Jason.decode!(json)
    end
  end

  # Kills the sender at `point`, once `extra` more rows were committed
  # meanwhile, so the next sender regroups with a longer group.
  # Returns a flag that is set once the crash happened.
  defp crash_after_put(inst, seq, cell, point, extra) do
    fired = :atomics.new(1, [])

    CrashPoints.arm(
      point,
      fn _point, _context ->
        commit(inst, seq, cell, extra)
        :atomics.put(fired, 1, 1)
        Process.exit(self(), :kill)
      end,
      when: &match?(%{cell: ^cell}, &1)
    )

    fired
  end

  # Properties

  defp op do
    cell = member_of(@cells)

    frequency([
      {8, tuple({constant(:commit), cell, integer(0..6)})},
      {4, tuple({constant(:burst), cell, list_of(integer(1..4), min_length: 2, max_length: 6)})},
      {3, tuple({constant(:pause), integer(0..40)})},
      {2, tuple({constant(:crash), cell, member_of([:feed_after_put, :feed_before_group_put])})},
      {2, tuple({constant(:read), integer(1..5)})}
    ])
  end

  property "every durable row appears once in commit order, whatever the grouping", ctx do
    runs = :counters.new(1, [])

    check all(
            ops <- list_of(op(), min_length: 8, max_length: 30),
            object_batches <- integer(1..8),
            linger_ms <- integer(0..20),
            put_ms <- member_of([0, 0, 15]),
            limit <- integer(1..6),
            max_runs: 25,
            max_shrinking_steps: 0
          ) do
      :counters.add(runs, 1, 1)
      prefix = "batching-#{:counters.get(runs, 1)}-#{System.unique_integer([:positive])}"
      feed = [object_batches: object_batches, linger_ms: linger_ms, window_ms: 60_000]
      inst = start(ctx, prefix, feed)
      seq = start_supervised!({Agent, fn -> 0 end}, id: make_ref())
      {_, store_config} = ctx.store

      if put_ms > 0,
        do:
          MemoryStore.inject(store_config,
            kind: :latency,
            ms: put_ms,
            ops: [:put],
            key: ~r{#{prefix}/feed/}
          )

      try do
        reads =
          Enum.reduce(ops, [], fn
            {:commit, cell, k}, reads ->
              if k > 0, do: commit(inst, seq, cell, k)
              reads

            {:burst, cell, ks}, reads ->
              Enum.each(ks, &commit(inst, seq, cell, &1))
              reads

            {:pause, ms}, reads ->
              Process.sleep(ms)
              reads

            {:crash, cell, point}, reads ->
              fired = crash_after_put(inst, seq, cell, point, 1)
              commit(inst, seq, cell, 1)
              eventually(fn -> :atomics.get(fired, 1) == 1 end)
              reads

            {:read, n}, reads ->
              [pages(feed_ctx(ctx, prefix), n) | reads]
          end)

        verify(ctx, prefix, inst, reads, limit, objects_at_most(ops))
      after
        stop_supervised!(inst)
      end
    end
  end

  # A commit makes one batch. A crash adds two commits and may leave one
  # object that a longer regrouped one repeats.
  defp objects_at_most(ops) do
    Enum.sum(
      for op <- ops do
        case op do
          {:commit, _, k} -> min(k, 1)
          {:burst, _, ks} -> length(ks)
          {:crash, _, _} -> 3
          _ -> 0
        end
      end
    )
  end

  defp settled?(reader, limit, stored) do
    records = reader |> pages(limit) |> flat()
    Enum.all?(@cells, &(numbers(records, &1) == stored[&1]))
  end

  defp verify(ctx, prefix, inst, reads, limit, at_most) do
    reader = feed_ctx(ctx, prefix)
    stored = for cell <- @cells, into: %{}, do: {cell, durable(inst, cell)}

    try do
      eventually(fn -> settled?(reader, limit, stored) end, 15_000)
    rescue
      RuntimeError ->
        records = reader |> pages(limit) |> flat()

        flunk(
          "feed #{inspect(for c <- @cells, do: numbers(records, c))} stored #{inspect(stored)}"
        )
    end

    full = pages(reader, limit)

    for cell <- @cells do
      mine = full |> flat() |> Enum.filter(&(&1.cell == cell))
      positions = mine |> Enum.uniq_by(& &1.id) |> Enum.map(& &1.position)
      assert positions == Enum.sort(positions), "cell #{cell}: first occurrences out of order"
    end

    assert length(feed_keys(ctx, prefix)) <= at_most

    # Resume from every saved position of the random reads and of the last one.
    for sequence <- [full | reads],
        {{_, position}, index} <- Enum.with_index(sequence) do
      delivered = sequence |> Enum.take(index + 1) |> flat()
      rest = reader |> pages(limit, position) |> flat()

      for cell <- @cells do
        assert numbers(delivered ++ rest, cell) == stored[cell],
               "cell #{cell}: resume from #{inspect(position)} lost rows"
      end
    end
  end

  # Stops in the middle of a linger

  @idle_ms 300
  @stop_points [:feed_before_stop_flush, :feed_before_group_put, :feed_after_put]

  defp stop_op do
    cell = member_of(@cells)

    frequency([
      {8, tuple({constant(:commit), cell, integer(1..4)})},
      {2, tuple({constant(:pause), integer(0..40)})},
      {3, tuple({constant(:hibernate), cell})},
      {2, tuple({constant(:idle), cell})},
      {2, tuple({constant(:shutdown), member_of([:a, :b])})},
      {2, tuple({constant(:stop_crash), cell, member_of(@stop_points)})},
      {3, tuple({constant(:takeover), cell})}
    ])
  end

  property "a stop publishes every acknowledged commit, whatever the linger", ctx do
    runs = :counters.new(1, [])

    check all(
            ops <- list_of(stop_op(), min_length: 6, max_length: 20),
            object_batches <- integer(1..8),
            linger_ms <- member_of([0, 20, 60_000]),
            put_ms <- member_of([0, 15]),
            max_runs: 20,
            max_shrinking_steps: 200
          ) do
      :counters.add(runs, 1, 1)
      prefix = "stops-#{:counters.get(runs, 1)}-#{System.unique_integer([:positive])}"
      feed = [object_batches: object_batches, linger_ms: linger_ms, window_ms: 60_000]
      {_, store_config} = ctx.store

      if put_ms > 0,
        do:
          MemoryStore.inject(store_config,
            kind: :latency,
            ms: put_ms,
            ops: [:put],
            key: ~r{#{prefix}/feed/}
          )

      run = %{
        ctx: ctx,
        prefix: prefix,
        feed: feed,
        seq: start_supervised!({Agent, fn -> 0 end}, id: make_ref()),
        insts: %{a: start_stops(ctx, prefix, feed), b: start_stops(ctx, prefix, feed)},
        owners: Map.new(@cells, &{&1, :a}),
        acked: Map.new(@cells, &{&1, []}),
        dirty: MapSet.new()
      }

      try do
        ops |> Enum.reduce(run, &stop_step/2) |> verify_stops()
      after
        CrashPoints.reset()
      end
    end
  end

  defp start_stops(ctx, prefix, feed),
    do: start(ctx, prefix, feed, idle_timeout_ms: @idle_ms)

  defp inst(run, cell), do: run.insts[run.owners[cell]]

  # A cell that was killed holds its lease until it expires, for a new owner.
  defp ready(run, cell),
    do: eventually(fn -> match?({:ok, _}, Alambique.touch(inst(run, cell), cell)) end, 10_000)

  defp write(run, cell, k) do
    ready(run, cell)
    ns = commit(inst(run, cell), run.seq, cell, k)
    %{run | acked: Map.update!(run.acked, cell, &(&1 ++ ns))}
  end

  defp awake?(inst, cell), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell) != nil

  # A stop with a valid lease and no fault leaves the feed complete before any
  # wake. A cell that was hurt keeps rows for the next wake.
  defp assert_published(run, cells) do
    records = run.ctx |> feed_ctx(run.prefix) |> pages(100) |> flat()

    for cell <- cells, not MapSet.member?(run.dirty, cell) do
      assert numbers(records, cell) == run.acked[cell],
             "cell #{cell}: not published at the stop, feed #{inspect(numbers(records, cell))}"
    end
  end

  defp stop_step({:commit, cell, k}, run), do: write(run, cell, k)

  defp stop_step({:pause, ms}, run) do
    Process.sleep(ms)
    run
  end

  defp stop_step({:hibernate, cell}, run) do
    ready(run, cell)
    :ok = Alambique.hibernate(inst(run, cell), cell)
    assert_published(run, [cell])
    %{run | dirty: MapSet.delete(run.dirty, cell)}
  end

  defp stop_step({:idle, cell}, run) do
    run = write(run, cell, 1)
    eventually(fn -> not awake?(inst(run, cell), cell) end, 10_000)
    assert_published(run, [cell])
    %{run | dirty: MapSet.delete(run.dirty, cell)}
  end

  # The instance stops all its cells; a replacement restores them from the store.
  defp stop_step({:shutdown, role}, run) do
    stop_supervised!(run.insts[role])
    assert_published(run, for({cell, ^role} <- run.owners, do: cell))
    %{run | insts: %{run.insts | role => start_stops(run.ctx, run.prefix, run.feed)}}
  end

  # The cell or the sender dies at the point, which the stop may not reach.
  defp stop_step({:stop_crash, cell, point}, run) do
    run = write(run, cell, 1)
    CrashPoints.arm(point, :kill, when: &match?(%{cell: ^cell}, &1))

    try do
      Alambique.hibernate(inst(run, cell), cell)
    catch
      :exit, _ -> :ok
    end

    CrashPoints.disarm(point)
    %{run | dirty: MapSet.put(run.dirty, cell)}
  end

  defp stop_step({:takeover, cell}, run) do
    target = if run.owners[cell] == :a, do: :b, else: :a
    {:ok, _} = Alambique.take_over(run.insts[target], cell, :property)
    # The old owner learns of it and leaves without publishing.
    eventually(fn -> not awake?(inst(run, cell), cell) end)
    %{run | owners: Map.put(run.owners, cell, target)}
  end

  # Wakes every cell for its rows, stops them again, and compares the feed.
  defp verify_stops(run) do
    for cell <- @cells do
      ready(run, cell)
      assert durable(inst(run, cell), cell) == run.acked[cell]
    end

    for cell <- @cells,
        awake?(inst(run, cell), cell),
        do: :ok = Alambique.hibernate(inst(run, cell), cell)

    reader = feed_ctx(run.ctx, run.prefix)
    records = reader |> pages(100) |> flat()

    for cell <- @cells do
      assert numbers(records, cell) == run.acked[cell], "cell #{cell}: feed differs from commits"
      mine = Enum.filter(records, &(&1.cell == cell))
      positions = mine |> Enum.uniq_by(& &1.id) |> Enum.map(& &1.position)
      assert positions == Enum.sort(positions), "cell #{cell}: first occurrences out of order"
    end
  end

  # Plain tests

  describe "a stop" do
    @lingering [object_batches: 16, linger_ms: 60_000, window_ms: 120_000]

    defp written(ctx, prefix, cell),
      do: numbers(ctx |> feed_ctx(prefix) |> pages(10) |> flat(), cell)

    defp write_three(inst, cell) do
      seq = start_supervised!({Agent, fn -> 0 end}, id: make_ref())
      for _ <- 1..3, do: commit(inst, seq, cell, 1)
    end

    test "hibernation publishes a lingering queue at once", ctx do
      inst = start(ctx, "hib", @lingering)
      cell = unique_cell()
      write_three(inst, cell)
      assert written(ctx, "hib", cell) == []

      :ok = Alambique.hibernate(inst, cell)

      assert written(ctx, "hib", cell) == [1, 2, 3]
      assert [_] = feed_keys(ctx, "hib")
    end

    test "idle hibernation publishes a lingering queue", ctx do
      inst = start(ctx, "idle", @lingering, idle_timeout_ms: 200)
      cell = unique_cell()
      write_three(inst, cell)

      eventually(fn -> written(ctx, "idle", cell) == [1, 2, 3] end)
      refute awake?(inst, cell)
    end

    test "a shutdown publishes a lingering queue", ctx do
      inst = start(ctx, "down", @lingering)
      cell = unique_cell()
      write_three(inst, cell)

      stop_supervised!(inst)

      assert written(ctx, "down", cell) == [1, 2, 3]
    end

    test "stop_publish_ms 0 keeps the rows for the next wake", ctx do
      inst = start(ctx, "off", @lingering ++ [stop_publish_ms: 0])
      cell = unique_cell()
      write_three(inst, cell)

      :ok = Alambique.hibernate(inst, cell)

      assert written(ctx, "off", cell) == []
    end

    test "a sender that cannot put does not hold the stop for long", ctx do
      {_, store_config} = ctx.store
      feed = @lingering ++ [stop_publish_ms: 300, publish_retry_ms: 10]
      inst = start(ctx, "stuck", feed)
      cell = unique_cell()
      write_three(inst, cell)
      MemoryStore.inject(store_config, kind: :error, ops: [:put], key: ~r{stuck/feed/})

      started = System.monotonic_time(:millisecond)
      :ok = Alambique.hibernate(inst, cell)

      assert System.monotonic_time(:millisecond) - started < 3_000
      assert written(ctx, "stuck", cell) == []
    end

    test "a crash before the flush leaves the rows to the next wake", ctx do
      inst = start(ctx, "crash", @lingering)
      cell = unique_cell()
      write_three(inst, cell)
      CrashPoints.arm(:feed_before_stop_flush, :kill, when: &match?(%{cell: ^cell}, &1))

      spawn(fn -> Alambique.hibernate(inst, cell) end)
      eventually(fn -> not awake?(inst, cell) end)
      assert written(ctx, "crash", cell) == []

      {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      :ok = Alambique.hibernate(inst, cell)
      assert written(ctx, "crash", cell) == [1, 2, 3]
    end
  end

  describe "a group of batches" do
    test "a crash after the put regroups under the key of the last batch", ctx do
      inst = start(ctx, "regroup", object_batches: 4, linger_ms: 100, window_ms: 60_000)
      seq = start_supervised!({Agent, fn -> 0 end})
      cell = unique_cell()
      crash_after_put(inst, seq, cell, :feed_after_put, 1)

      for _ <- 1..3, do: commit(inst, seq, cell, 1)

      reader = feed_ctx(ctx, "regroup")

      eventually(fn ->
        numbers(reader |> pages(10) |> flat(), cell) == [1, 2, 3, 4]
      end)
    end

    test "a crash before the put regroups with the rows committed meanwhile", ctx do
      inst = start(ctx, "before", object_batches: 4, linger_ms: 100, window_ms: 60_000)
      seq = start_supervised!({Agent, fn -> 0 end})
      cell = unique_cell()
      crash_after_put(inst, seq, cell, :feed_before_group_put, 1)

      for _ <- 1..3, do: commit(inst, seq, cell, 1)

      reader = feed_ctx(ctx, "before")

      eventually(fn ->
        numbers(reader |> pages(10) |> flat(), cell) == [1, 2, 3, 4]
      end)
    end

    test "a feed with both shapes reads in commit order", ctx do
      cell = unique_cell()
      seq = start_supervised!({Agent, fn -> 0 end})
      first = start(ctx, "mixed", window_ms: 60_000)
      for _ <- 1..2, do: commit(first, seq, cell, 1)
      reader = feed_ctx(ctx, "mixed")
      eventually(fn -> length(feed_keys(ctx, "mixed")) == 2 end)
      stop_supervised!(first)

      second = start(ctx, "mixed", object_batches: 4, linger_ms: 200, window_ms: 60_000)
      for _ <- 1..4, do: commit(second, seq, cell, 1)

      eventually(fn ->
        records = reader |> pages(10) |> flat()
        numbers(records, cell) == [1, 2, 3, 4, 5, 6]
      end)

      shapes = ctx |> bodies("mixed") |> Enum.map(&Map.has_key?(&1, "batches"))
      assert Enum.count(shapes, & &1) >= 1
      assert Enum.count(shapes, & &1) < length(shapes)
    end

    test "queued batches share one object", ctx do
      inst = start(ctx, "grouped", object_batches: 4, linger_ms: 300, window_ms: 60_000)
      seq = start_supervised!({Agent, fn -> 0 end})
      cell = unique_cell()
      for _ <- 1..4, do: commit(inst, seq, cell, 2)

      eventually(fn -> feed_keys(ctx, "grouped") != [] end)
      assert [%{"batches" => batches}] = bodies(ctx, "grouped")
      assert length(batches) == 4
    end
  end
end
