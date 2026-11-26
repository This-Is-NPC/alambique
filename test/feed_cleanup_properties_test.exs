defmodule Alambique.FeedCleanupPropertiesTest do
  @moduledoc """
  The feed under every cleanup mode and under feed puts that fail (design
  §13, §15). Random scenarios mix writes of random sizes, publications held
  and released, feed puts that fail, lose their acknowledgement or are slow,
  quiet periods, kills and hibernations of one cell, and a write that cleans
  while an earlier batch is still unpublished, with the cleanup bounded or not
  and the hand-over to the sender in pieces or not, by the cell, by the sender
  or by the library, and a reader of the sender that closes after each read
  or after a burst of them. Once the faults are
  cleared, the records of the feed, deduplicated by id, are the rows of the
  state restored from the store: every acknowledged write, each row once and
  in order, and no row that no write made. A repeated record is identical to
  its first, and no change has two ids.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Feed, FeedProbe, Sqlite, Tx}
  alias Alambique.Cell.Db
  alias Alambique.Feed.Capture
  alias AlambiqueTestkit.{CrashPoints, GatedStore, MemoryStore}

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @cell "cleanup-cell"

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  defp op do
    frequency([
      {8, tuple({constant(:write), integer(1..6)})},
      {2, constant(:hold)},
      {2, constant(:release)},
      {4, constant(:idle)},
      {1, constant(:kill)},
      {2, constant(:hibernate)},
      {2, tuple({constant(:pass), integer(0..30)})},
      # Fewer failures than the attempts of one put, and more.
      {2, tuple({constant(:fail), integer(1..7)})},
      {1, constant(:fail_all)},
      {2, tuple({constant(:lose_ack), integer(1..4)})},
      {1, tuple({constant(:slow), integer(20..80)})},
      {2, constant(:clear)},
      {3, tuple({constant(:race), member_of([:kill, :hibernate, :stay])})}
    ])
  end

  property "every captured change is on the feed once, in order, whatever the cleanup", ctx do
    runs = :counters.new(1, [])

    check all(
            cleanup <- member_of([:in_write, :idle, :after_publish, :idle_or_write]),
            pieces <- member_of([nil, 2, 3]),
            hand_over <- member_of([:cell, :sender, :native]),
            close <- member_of([:after_read, :after_burst]),
            rows <- member_of([nil, 1, 2, 5]),
            first <- integer(1..6),
            ops <- list_of(op(), min_length: 6, max_length: 16),
            max_runs: 20,
            max_shrinking_steps: 0
          ) do
      scenario(ctx, runs, {cleanup, rows, pieces, {hand_over, close}}, [{:write, first} | ops])
    end
  end

  # Every run holds a kill at a random place and a crash of the cell between
  # the local commit of a cleanup and the upload of its batch.
  property "a kill anywhere, also inside a cleanup, loses and repeats nothing", ctx do
    runs = :counters.new(1, [])

    check all(
            cleanup <- member_of([:idle, :after_publish, :idle_or_write]),
            rows <- member_of([nil, 1, 2]),
            first <- integer(1..6),
            pieces <- member_of([nil, 2, 3]),
            hand_over <- member_of([:cell, :sender, :native]),
            close <- member_of([:after_read, :after_burst]),
            ops <- list_of(op(), min_length: 6, max_length: 12),
            kill_at <- integer(0..12),
            crash_at <- integer(0..12),
            max_runs: 12,
            max_shrinking_steps: 0
          ) do
      ops =
        ops
        |> List.insert_at(kill_at, :kill)
        |> List.insert_at(crash_at, :crash_in_cleanup)

      scenario(ctx, runs, {cleanup, rows, pieces, {hand_over, close}}, [{:write, first} | ops])
    end
  end

  property "a bounded cleanup removes whole batches, as many as fit, and always progresses" do
    dir = Alambique.TestDirs.new("cutoff")
    {:ok, conn} = Sqlite.open(Path.join(dir, "c.db"))
    on_exit(fn -> Sqlite.close(conn) end)
    :ok = Sqlite.execute(conn, "CREATE TABLE notes (id INTEGER PRIMARY KEY)")
    :ok = Capture.install(conn, Capture.everything())

    check all(
            from <- integer(0..3),
            sizes <- list_of(integer(0..6), min_length: 1, max_length: 8),
            bound <- integer(1..10)
          ) do
      :ok = Db.writable_script(conn, "DELETE FROM _alambique_feed")
      txids = Enum.with_index(sizes, from + 1)

      for {size, txid} <- txids, _ <- 1..size//1 do
        :ok =
          Db.writable_script(
            conn,
            "INSERT INTO _alambique_feed (txid, ts, tbl, op, key) VALUES (#{txid}, 1, 'notes', 'insert', '{}')"
          )
      end

      upto = from + length(sizes)
      cut = Capture.cutoff(conn, from, upto, bound)
      {:ok, :ok} = Db.transaction(conn, 1, &Capture.cleanup(&1.conn, cut, 1))

      left = Map.new(txids, fn {_size, txid} -> {txid, count(conn, txid)} end)
      sizes_by_txid = Map.new(txids, fn {size, txid} -> {txid, size} end)

      assert cut >= from and cut <= upto

      # A batch is removed whole or not at all.
      for {txid, size} <- sizes_by_txid, do: assert(left[txid] in [0, size])

      deleted = for {txid, size} <- sizes_by_txid, left[txid] == 0, do: size
      total = Enum.sum(sizes)

      # Progress, and no more than the bound unless one batch alone is more.
      if total > 0, do: assert(Enum.sum(deleted) > 0)
      if length(Enum.filter(deleted, &(&1 > 0))) > 1, do: assert(Enum.sum(deleted) <= bound)

      # Nothing more fits: the next batch would pass the bound.
      next = for {txid, size} <- Enum.sort(sizes_by_txid), left[txid] == size, size > 0, do: size

      case next do
        [following | _] -> assert Enum.sum(deleted) + following > bound
        [] -> :ok
      end
    end
  end

  defp count(conn, txid) do
    {:ok, %{rows: [[n]]}} =
      Sqlite.run(conn, "SELECT count(*) FROM _alambique_feed WHERE txid = ?", [txid])

    n
  end

  defp scenario(ctx, runs, {cleanup, rows, pieces, {hand_over, close}}, ops) do
    :counters.add(runs, 1, 1)
    n = :counters.get(runs, 1)
    {:ok, gate} = GatedStore.start_link()
    store = GatedStore.wrap(gate, ctx.store)
    {_, mem} = ctx.store

    inst =
      start_instance(%{ctx | store: store},
        name: :"cleanup_run_#{n}",
        prefix: "cleanup-#{n}",
        migrations: @schema,
        lease_ttl_ms: 1_000,
        skew_margin_ms: 200,
        idle_timeout_ms: 60_000,
        retry: [max_attempts: 3],
        feed: [
          cleanup: cleanup,
          cleanup_after_ms: 40,
          cleanup_rows: rows,
          hand_over_rows: pieces,
          hand_over: hand_over,
          reader_close: if(hand_over == :sender, do: close),
          publish_retry_ms: 10,
          publish_retry_max_ms: 40,
          stop_publish_ms: 200,
          window_ms: 60_000
        ]
      )

    run = %{
      inst: inst,
      cleanup: cleanup,
      gate: gate,
      mem: mem,
      store: ctx.store,
      n: n,
      next: 1,
      acked: [],
      unknown: [],
      hurt: false
    }

    try do
      run = Enum.reduce(ops, run, &step/2)
      verify(ctx, run)
    after
      MemoryStore.clear(mem)
      GatedStore.open(gate)
      stop_supervised!(inst)
      GenServer.stop(gate)
    end
  end

  # A cell that was killed keeps its lease until it expires.
  defp ready(run),
    do: eventually(fn -> match?({:ok, _}, Alambique.touch(run.inst, @cell)) end, 15_000)

  # The answer of a write is `{:ok, :ok}` unless a kill or a crash met it; a
  # write with another answer may or may not be durable, as a whole.
  defp write(run, k) do
    ready(run)
    ids = Enum.to_list(run.next..(run.next + k - 1))

    answer =
      try do
        Alambique.transaction(run.inst, @cell, fn tx ->
          for id <- ids, do: {:ok, _} = Tx.execute(tx, "INSERT INTO notes VALUES (?, 'x')", [id])
          :ok
        end)
      catch
        :exit, reason -> {:exit, reason}
      end

    run = %{run | next: run.next + k}

    case answer do
      {:ok, :ok} ->
        %{run | acked: run.acked ++ ids}

      other ->
        assert run.hurt, "a write was not acknowledged without a kill: #{inspect(other)}"
        %{run | unknown: [ids | run.unknown]}
    end
  end

  defp step({:write, k}, run), do: write(run, k)

  defp step(:hold, run) do
    GatedStore.hold(run.gate, ~r{/feed/})
    run
  end

  defp step(:release, run) do
    GatedStore.open(run.gate)
    run
  end

  defp step(:idle, run) do
    Process.sleep(120)
    run
  end

  defp step(:kill, run) do
    kill(run)
    %{run | hurt: true}
  end

  defp step(:crash_in_cleanup, run) do
    CrashPoints.arm(:feed_after_cleanup, :kill, when: &match?(%{cell: @cell}, &1))
    %{run | hurt: true}
  end

  defp step(:hibernate, run) do
    ready(run)
    _ = Alambique.hibernate(run.inst, @cell)
    run
  end

  defp step({:pass, ms}, run) do
    Process.sleep(ms)
    run
  end

  defp step({:fail, times}, run), do: inject(run, :error, times: times)
  defp step(:fail_all, run), do: inject(run, :error, [])
  defp step({:lose_ack, times}, run), do: inject(run, :lost_ack, times: times)
  defp step({:slow, ms}, run), do: inject(run, :latency, ms: ms, times: 4)

  defp step(:clear, run) do
    MemoryStore.clear(run.mem)
    run
  end

  # A batch is published and the next one is held on its put, which the cell
  # has handed over but not published, when a write cleans up the cell: the
  # rows of the held batch stay, whatever happens to the cell next.
  defp step({:race, after_race}, run) do
    run = settle(run)
    run = FeedProbe.race(run.gate, run.inst, @cell, run, &write(&1, 1))

    case after_race do
      :kill ->
        kill(run)
        %{run | hurt: true}

      :hibernate ->
        _ = Alambique.hibernate(run.inst, @cell)
        run

      :stay ->
        run
    end
  end

  defp inject(run, kind, opts) do
    MemoryStore.inject(
      run.mem,
      [kind: kind, ops: [:put], key: ~r{cleanup-#{run.n}/feed/}] ++ opts
    )

    run
  end

  defp kill(run), do: FeedProbe.kill(run.inst, @cell)

  # `nil` when the cell is not running.
  defp published(run), do: FeedProbe.published(run.inst, @cell)

  # Faults cleared, puts released, every acknowledged row on the feed and the
  # cell told of its publication.
  defp settle(run) do
    MemoryStore.clear(run.mem)
    GatedStore.open(run.gate)
    ready(run)

    try do
      eventually(fn -> caught_up?(run) end, 20_000)
    rescue
      RuntimeError ->
        records = feed_records(run)

        flunk(
          "feed stalled: acked #{inspect(run.acked)} feed #{inspect(ids(records))} " <>
            "published #{inspect(published(run))}"
        )
    end

    run
  end

  # A crash armed earlier may stop the cell again: a call wakes it. A cell
  # that woke after its rows were cleaned has published nothing itself.
  defp caught_up?(run) do
    with {:ok, _} <- Alambique.touch(run.inst, @cell),
         {:ok, %{rows: [[waiting]]}} <-
           Alambique.query(
             run.inst,
             @cell,
             "SELECT count(*) FROM _alambique_feed WHERE op != 'floor' AND txid > ?",
             [published(run) || 0]
           ) do
      run.acked -- ids(feed_records(run)) == [] and waiting == 0
    else
      # Down right after a kill: not caught up yet.
      {:error, {:cell_down, _}} -> false
    end
  end

  defp feed_records(run) do
    feed = Feed.ctx(store: run.store, prefix: "cleanup-#{run.n}", settle_ms: 0)
    {:ok, all, _} = Feed.read(feed, nil, 10_000)
    Enum.filter(all, &(&1.cell == @cell))
  end

  defp ids(records), do: records |> Enum.uniq_by(& &1.id) |> Enum.map(& &1.key["id"])

  # The rows the store holds for the cell: what survives if every node dies.
  defp stored(ctx, run) do
    eventually(fn ->
      read = fn ->
        durable_rows(ctx, @cell, "SELECT id FROM notes ORDER BY id", "cleanup-#{run.n}")
      end

      first = read.()
      Process.sleep(50)
      first == read.() and List.flatten(first)
    end)
  end

  defp verify(ctx, run) do
    # A crash armed and not yet fired would kill the cell after this point,
    # and a dead cell publishes nothing until the next call wakes it.
    CrashPoints.disarm(:feed_after_cleanup)
    MemoryStore.clear(run.mem)
    GatedStore.open(run.gate)
    ready(run)
    stored = stored(ctx, run)

    assert run.acked != []
    assert run.acked -- stored == [], "an acknowledged row is not in the store"
    unknown = List.flatten(run.unknown)
    assert stored -- (run.acked ++ unknown) == [], "the store holds a row no write made"

    for ids <- run.unknown do
      assert length(ids -- stored) in [0, length(ids)],
             "a write is durable in part: #{inspect(ids)}"
    end

    records = eventually(fn -> (r = feed_records(run)) && ids(r) == stored && r end, 20_000)
    first = Enum.uniq_by(records, & &1.id)

    assert Enum.map(first, & &1.key["id"]) == stored
    assert Enum.map(first, & &1.position) == Enum.sort(Enum.map(first, & &1.position))

    for {id, same} <- Enum.group_by(records, & &1.id) do
      assert same |> Enum.map(&Map.delete(&1, :at)) |> Enum.uniq() |> length() == 1,
             "#{id} was published twice with different content"
    end

    # Published rows leave the cell, a batch at a time however small the
    # bound, once the cell cleans on its own.
    if run.cleanup != :in_write, do: assert_cleaned(run, 100)

    # The positions of a batch run from 0 without a gap.
    for {_txid, batch} <- Enum.group_by(first, &elem(&1.position, 0)) do
      assert batch |> Enum.map(&elem(&1.position, 1)) |> Enum.sort() ==
               Enum.to_list(0..(length(batch) - 1)//1)
    end
  end

  # The idle modes clean in quiet time: a poll slower than the quiet period
  # leaves it.
  defp assert_cleaned(_run, 0), do: flunk("published rows were never removed from the cell")

  defp assert_cleaned(run, tries) do
    {:ok, %{rows: [[left]]}} =
      Alambique.query(run.inst, @cell, "SELECT count(*) FROM _alambique_feed WHERE op != 'floor'")

    if left > 0 do
      Process.sleep(200)
      assert_cleaned(run, tries - 1)
    end
  end
end
