defmodule Alambique.FeedBatchingPropertiesTest do
  @moduledoc """
  A feed object per group of batches (design §13) on one node over the memory
  store. Random commits of varied sizes, bursts and pauses, with random
  `object_batches`, `linger_ms` and cleanup mode, feed puts that fail, lose
  their acknowledgement or are slow, crashes of the sender after a put and
  restarts of it, a write that cleans up while a publication is held and the
  cell is killed, and random reads with resumes from saved positions. Every
  write answer is kept. For every cell, with the faults cleared:

    * the rows the store restores are the rows of the acknowledged writes;
    * the feed holds every one of them exactly once after deduplication by
      idempotency key, and nothing else, in commit order;
    * a resume from any saved position loses nothing;
    * there are no more objects than batches.

  A second property stops cells in the middle of a linger (hibernation, idle,
  shutdown), crashes the cell or the sender during the stop flush, fails and
  slows feed puts, kills a cell while a publication is held after a cleanup
  and moves cells to another owner. Every acknowledged commit is in the feed
  once, in commit order, and a clean stop has already published its cell. A
  cell that was hurt, or whose feed puts failed, has published a prefix of its
  commits until its next clean stop.

  Plain tests pin the key rule of a group and the reading of a feed that mixes both object shapes.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Feed, FeedProbe}
  alias AlambiqueTestkit.{CrashPoints, GatedStore, MemoryStore}

  @schema [{1, "CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER)"}]
  @cells ["c0", "c1"]
  # Quick retries of a feed put, and of the lease of a cell that was killed.
  @feed [window_ms: 60_000, publish_retry_ms: 10, publish_retry_max_ms: 40]
  @fast [lease_ttl_ms: 1_000, skew_margin_ms: 200, retry: [max_attempts: 3]]

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # Helpers

  defp start(ctx, prefix, feed, extra \\ []),
    do: start_instance(ctx, [migrations: @schema, prefix: prefix, feed: feed] ++ extra)

  defp feed_ctx(%{store: store}, prefix),
    do: Feed.ctx(store: store, prefix: prefix, settle_ms: 0)

  # The numbers given to rows and, per cell, those of the acknowledged writes.
  defp start_ledger,
    do: start_supervised!({Agent, fn -> %{next: 0, acked: %{}} end}, id: make_ref())

  defp acked(ledger, cell), do: Agent.get(ledger, &Map.get(&1.acked, cell, []))

  # A cell that was killed holds its lease until it expires, for a new owner.
  defp awaken(inst, cell),
    do: eventually(fn -> match?({:ok, _}, Alambique.touch(inst, cell)) end, 15_000)

  # Inserts `k` rows in one transaction and keeps the answer: the rows of an
  # acknowledged write are in the ledger; returns their numbers.
  defp commit(inst, ledger, cell, k) do
    ns =
      Agent.get_and_update(ledger, fn %{next: n} = state ->
        {Enum.to_list((n + 1)..(n + k)//1), %{state | next: n + k}}
      end)

    awaken(inst, cell)
    sql = "INSERT INTO log (n) VALUES " <> Enum.map_join(ns, ",", fn _ -> "(?)" end)
    assert {:ok, _} = Alambique.execute(inst, cell, sql, ns)

    Agent.update(ledger, fn state ->
      %{state | acked: Map.update(state.acked, cell, ns, &(&1 ++ ns))}
    end)

    ns
  end

  # The rows the store holds for the cell: what survives if every node dies.
  defp stored_rows(_ctx, _prefix, _cell, []), do: []

  defp stored_rows(ctx, prefix, cell, _acked) do
    ctx |> durable_rows(cell, "SELECT n FROM log ORDER BY id", prefix) |> List.flatten()
  end

  defp feed_fault(ctx, prefix, kind, opts) do
    {_, config} = ctx.store
    MemoryStore.inject(config, [kind: kind, ops: [:put], key: ~r{#{prefix}/feed/}] ++ opts)
  end

  # Faults cleared and held puts released.
  defp calm(run) do
    {_, config} = run.ctx.store
    MemoryStore.clear(config)
    GatedStore.open(run.gate)
  end

  # Waits until the feed holds the rows of the cell's acknowledged writes.
  defp await_feed(reader, cell, written) do
    eventually(fn -> sorted_numbers(reader, cell) == Enum.sort(written) end, 20_000)
  rescue
    RuntimeError ->
      flunk(
        "cell #{cell}: feed #{inspect(sorted_numbers(reader, cell))}, writes #{inspect(written)}"
      )
  end

  defp sorted_numbers(reader, cell),
    do: reader |> pages(100) |> flat() |> numbers(cell) |> Enum.sort()

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
  defp crash_after_put(inst, ledger, cell, point, extra) do
    fired = :atomics.new(1, [])

    CrashPoints.arm(
      point,
      fn _point, _context ->
        commit(inst, ledger, cell, extra)
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
      {2, tuple({constant(:read), integer(1..5)})},
      # Fewer failures than the attempts of one put, and more.
      {2, tuple({constant(:fail), integer(1..7)})},
      {2, tuple({constant(:lose_ack), integer(1..4)})},
      {1, tuple({constant(:slow), integer(20..80)})},
      {3, tuple({constant(:race), cell, member_of([:kill, :stay])})}
    ])
  end

  property "every durable row appears once in commit order, whatever the grouping", ctx do
    runs = :counters.new(1, [])

    check all(
            ops <- list_of(op(), min_length: 8, max_length: 30),
            object_batches <- integer(1..8),
            linger_ms <- integer(0..20),
            cleanup <- member_of([:in_write, :idle_or_write, :idle]),
            put_ms <- member_of([0, 0, 15]),
            limit <- integer(1..6),
            max_runs: 25,
            max_shrinking_steps: 0
          ) do
      :counters.add(runs, 1, 1)
      prefix = "batching-#{:counters.get(runs, 1)}-#{System.unique_integer([:positive])}"
      feed = [object_batches: object_batches, linger_ms: linger_ms, cleanup: cleanup] ++ @feed
      {:ok, gate} = GatedStore.start_link()
      gated = %{ctx | store: GatedStore.wrap(gate, ctx.store)}
      inst = start(gated, prefix, feed, @fast)
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
        inst: inst,
        gate: gate,
        ledger: start_ledger(),
        reads: []
      }

      try do
        run = Enum.reduce(ops, run, &batch_step/2)
        verify(run, limit, objects_at_most(ops))
      after
        MemoryStore.clear(store_config)
        GatedStore.open(gate)
        stop_supervised!(inst)
        GenServer.stop(gate)
      end
    end
  end

  defp batch_step({:commit, cell, k}, run) do
    if k > 0, do: commit(run.inst, run.ledger, cell, k)
    run
  end

  defp batch_step({:burst, cell, ks}, run) do
    Enum.each(ks, &commit(run.inst, run.ledger, cell, &1))
    run
  end

  defp batch_step({:pause, ms}, run) do
    Process.sleep(ms)
    run
  end

  defp batch_step({:crash, cell, point}, run) do
    fired = crash_after_put(run.inst, run.ledger, cell, point, 1)
    commit(run.inst, run.ledger, cell, 1)
    eventually(fn -> :atomics.get(fired, 1) == 1 end)
    run
  end

  defp batch_step({:read, n}, run),
    do: %{run | reads: [pages(feed_ctx(run.ctx, run.prefix), n) | run.reads]}

  defp batch_step({:fail, times}, run), do: batch_fault(run, :error, times: times)
  defp batch_step({:lose_ack, times}, run), do: batch_fault(run, :lost_ack, times: times)
  defp batch_step({:slow, ms}, run), do: batch_fault(run, :latency, ms: ms, times: 4)

  # A write cleans up the cell while the next batch is held on its put and
  # unpublished; the cell may be killed then.
  defp batch_step({:race, cell, after_race}, run) do
    run = batch_settle(run, cell)
    FeedProbe.race(run.gate, run.inst, cell, run, &write_one(&1, cell))

    if after_race == :kill, do: FeedProbe.kill(run.inst, cell)
    GatedStore.open(run.gate)
    run
  end

  defp batch_fault(run, kind, opts) do
    feed_fault(run.ctx, run.prefix, kind, opts)
    run
  end

  defp write_one(run, cell) do
    commit(run.inst, run.ledger, cell, 1)
    run
  end

  # Faults cleared and puts released, the feed holds every acknowledged row of
  # the cell, so that no put is waiting on the gate or on a fault.
  defp batch_settle(run, cell) do
    calm(run)
    awaken(run.inst, cell)
    await_feed(feed_ctx(run.ctx, run.prefix), cell, acked(run.ledger, cell))
    run
  end

  # A commit makes one batch. A crash adds two commits and may leave one
  # object that a longer regrouped one repeats; a race adds three batches and
  # a kill may regroup them.
  defp objects_at_most(ops) do
    Enum.sum(
      for op <- ops do
        case op do
          {:commit, _, k} -> min(k, 1)
          {:burst, _, ks} -> length(ks)
          {:crash, _, _} -> 3
          {:race, _, _} -> 5
          _ -> 0
        end
      end
    )
  end

  defp settled?(reader, limit, stored) do
    records = reader |> pages(limit) |> flat()
    Enum.all?(@cells, &(numbers(records, &1) == stored[&1]))
  end

  # The store holds exactly the acknowledged rows, and once the faults are
  # cleared the feed holds them once each, in the order of the store.
  defp verify(run, limit, at_most) do
    calm(run)
    reader = feed_ctx(run.ctx, run.prefix)

    stored =
      for cell <- @cells, into: %{} do
        written = acked(run.ledger, cell)
        if written != [], do: awaken(run.inst, cell)
        rows = stored_rows(run.ctx, run.prefix, cell, written)

        assert Enum.sort(rows) == Enum.sort(written),
               "cell #{cell}: the store holds #{inspect(rows)}, the writes answered #{inspect(written)}"

        {cell, rows}
      end

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

    assert length(feed_keys(run.ctx, run.prefix)) <= at_most

    # Resume from every saved position of the random reads and of the last one.
    for sequence <- [full | run.reads],
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
      {3, tuple({constant(:takeover), cell})},
      {2, tuple({constant(:fail), integer(1..7)})},
      {2, tuple({constant(:lose_ack), integer(1..4)})},
      {1, tuple({constant(:slow), integer(20..80)})},
      {3, tuple({constant(:race), cell, member_of([:kill, :stay])})}
    ])
  end

  property "a stop publishes every acknowledged commit, whatever the linger", ctx do
    runs = :counters.new(1, [])

    check all(
            ops <- list_of(stop_op(), min_length: 6, max_length: 20),
            object_batches <- integer(1..8),
            linger_ms <- member_of([0, 20, 60_000]),
            cleanup <- member_of([:in_write, :idle_or_write, :idle]),
            put_ms <- member_of([0, 15]),
            max_runs: 20,
            max_shrinking_steps: 200
          ) do
      :counters.add(runs, 1, 1)
      prefix = "stops-#{:counters.get(runs, 1)}-#{System.unique_integer([:positive])}"
      feed = [object_batches: object_batches, linger_ms: linger_ms, cleanup: cleanup] ++ @feed
      {_, store_config} = ctx.store
      {:ok, gate} = GatedStore.start_link()
      gated = %{ctx | store: GatedStore.wrap(gate, ctx.store)}

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
        gated: gated,
        gate: gate,
        prefix: prefix,
        feed: feed,
        ledger: start_ledger(),
        insts: %{a: start_stops(gated, prefix, feed), b: start_stops(gated, prefix, feed)},
        owners: Map.new(@cells, &{&1, :a}),
        lingers?: linger_ms > 1_000,
        faulty: false,
        dirty: MapSet.new()
      }

      try do
        ops |> Enum.reduce(run, &stop_step/2) |> verify_stops()
      after
        CrashPoints.reset()
        MemoryStore.clear(store_config)
        GatedStore.open(gate)
      end
    end
  end

  defp start_stops(ctx, prefix, feed),
    do: start(ctx, prefix, feed, idle_timeout_ms: @idle_ms, retry: [max_attempts: 3])

  defp inst(run, cell), do: run.insts[run.owners[cell]]

  defp ready(run, cell), do: awaken(inst(run, cell), cell)

  defp write(run, cell, k) do
    commit(inst(run, cell), run.ledger, cell, k)
    run
  end

  defp awake?(inst, cell), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell) != nil

  # A stop with a valid lease and no fault leaves the feed complete before any
  # wake. A cell that was hurt keeps rows for the next wake, and the puts of
  # one that met a fault may not have been retried yet: it has published the
  # first of its commits, without a gap.
  defp assert_published(run, cells) do
    records = run.ctx |> feed_ctx(run.prefix) |> pages(100) |> flat()

    for cell <- cells do
      written = acked(run.ledger, cell)
      published = numbers(records, cell)

      if MapSet.member?(run.dirty, cell) do
        assert published == Enum.take(written, length(published)),
               "cell #{cell}: feed #{inspect(published)} is no prefix of #{inspect(written)}"
      else
        assert published == written,
               "cell #{cell}: not published at the stop, feed #{inspect(published)}"
      end
    end
  end

  # A stop that met no fault leaves its cell clean.
  defp clean(run, cell),
    do: if(run.faulty, do: run.dirty, else: MapSet.delete(run.dirty, cell))

  defp stop_step({:commit, cell, k}, run), do: write(run, cell, k)

  defp stop_step({:pause, ms}, run) do
    Process.sleep(ms)
    run
  end

  defp stop_step({:hibernate, cell}, run) do
    ready(run, cell)
    :ok = Alambique.hibernate(inst(run, cell), cell)
    assert_published(run, [cell])
    %{run | dirty: clean(run, cell)}
  end

  defp stop_step({:idle, cell}, run) do
    run = write(run, cell, 1)
    eventually(fn -> not awake?(inst(run, cell), cell) end, 10_000)
    assert_published(run, [cell])
    %{run | dirty: clean(run, cell)}
  end

  # The instance stops all its cells; a replacement restores them from the store.
  defp stop_step({:shutdown, role}, run) do
    stop_supervised!(run.insts[role])
    assert_published(run, for({cell, ^role} <- run.owners, do: cell))
    %{run | insts: %{run.insts | role => start_stops(run.gated, run.prefix, run.feed)}}
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

  defp stop_step({:fail, times}, run), do: stop_fault(run, :error, times: times)
  defp stop_step({:lose_ack, times}, run), do: stop_fault(run, :lost_ack, times: times)
  defp stop_step({:slow, ms}, run), do: stop_fault(run, :latency, ms: ms, times: 4)

  # A write cleans up the cell while the next batch is held on its put and
  # unpublished; the cell may be killed then. A lingering cell does not put
  # before its linger ends, so nothing is held.
  defp stop_step({:race, _cell, _after_race}, %{lingers?: true} = run), do: run

  defp stop_step({:race, cell, after_race}, run) do
    run = stop_settle(run, cell)
    run = FeedProbe.race(run.gate, inst(run, cell), cell, run, &write(&1, cell, 1))

    if after_race == :kill, do: FeedProbe.kill(inst(run, cell), cell)
    GatedStore.open(run.gate)
    if after_race == :kill, do: %{run | dirty: MapSet.put(run.dirty, cell)}, else: run
  end

  # A fault hurts the publication of every cell until it is cleared.
  defp stop_fault(run, kind, opts) do
    feed_fault(run.ctx, run.prefix, kind, opts)
    %{run | faulty: true, dirty: MapSet.union(run.dirty, MapSet.new(@cells))}
  end

  # Faults cleared and puts released, the feed holds every acknowledged row of
  # the cell.
  defp stop_settle(run, cell) do
    calm(run)
    ready(run, cell)
    await_feed(feed_ctx(run.ctx, run.prefix), cell, acked(run.ledger, cell))
    %{run | faulty: false, dirty: MapSet.delete(run.dirty, cell)}
  end

  # Wakes every cell for its rows, stops them again, and compares the feed,
  # once the faults are cleared.
  defp verify_stops(run) do
    calm(run)

    for cell <- @cells do
      ready(run, cell)
      written = acked(run.ledger, cell)

      assert stored_rows(run.ctx, run.prefix, cell, written) == written,
             "cell #{cell}: the store differs from the acknowledged writes"
    end

    for cell <- @cells,
        awake?(inst(run, cell), cell),
        do: :ok = Alambique.hibernate(inst(run, cell), cell)

    reader = feed_ctx(run.ctx, run.prefix)
    records = reader |> pages(100) |> flat()

    for cell <- @cells do
      assert numbers(records, cell) == acked(run.ledger, cell),
             "cell #{cell}: feed differs from commits"

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
      ledger = start_ledger()
      for _ <- 1..3, do: commit(inst, ledger, cell, 1)
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
      # The flush precedes the checkpoint, the release and the exit.
      eventually(fn -> not awake?(inst, cell) end)
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

      assert System.monotonic_time(:millisecond) - started < 15_000
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
      ledger = start_ledger()
      cell = unique_cell()
      crash_after_put(inst, ledger, cell, :feed_after_put, 1)

      for _ <- 1..3, do: commit(inst, ledger, cell, 1)

      reader = feed_ctx(ctx, "regroup")

      eventually(fn ->
        numbers(reader |> pages(10) |> flat(), cell) == [1, 2, 3, 4]
      end)
    end

    test "a crash before the put regroups with the rows committed meanwhile", ctx do
      inst = start(ctx, "before", object_batches: 4, linger_ms: 100, window_ms: 60_000)
      ledger = start_ledger()
      cell = unique_cell()
      crash_after_put(inst, ledger, cell, :feed_before_group_put, 1)

      for _ <- 1..3, do: commit(inst, ledger, cell, 1)

      reader = feed_ctx(ctx, "before")

      eventually(fn ->
        numbers(reader |> pages(10) |> flat(), cell) == [1, 2, 3, 4]
      end)
    end

    test "a feed with both shapes reads in commit order", ctx do
      cell = unique_cell()
      ledger = start_ledger()
      first = start(ctx, "mixed", window_ms: 60_000)
      for _ <- 1..2, do: commit(first, ledger, cell, 1)
      reader = feed_ctx(ctx, "mixed")
      eventually(fn -> length(feed_keys(ctx, "mixed")) == 2 end)
      stop_supervised!(first)

      second = start(ctx, "mixed", object_batches: 4, linger_ms: 200, window_ms: 60_000)
      for _ <- 1..4, do: commit(second, ledger, cell, 1)

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
      ledger = start_ledger()
      cell = unique_cell()
      for _ <- 1..4, do: commit(inst, ledger, cell, 2)

      eventually(fn -> feed_keys(ctx, "grouped") != [] end)
      assert [%{"batches" => batches}] = bodies(ctx, "grouped")
      assert length(batches) == 4
    end
  end
end
