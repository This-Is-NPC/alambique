defmodule Alambique.FeedCleanupPropertiesTest do
  @moduledoc """
  The feed under every cleanup mode (design §13). Random scenarios mix writes
  of random sizes, publications held and released, quiet periods, kills and
  hibernations of one cell, with the cleanup bounded or not and the hand-over
  to the sender in pieces or not, read by the cell or by the sender. After the cell has
  published what it holds, the records, deduplicated by id, are every captured
  change exactly once and in order, a repeated record is identical to its
  first, and no change has two ids.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Feed, Sqlite, Tx}
  alias Alambique.Cell.Db
  alias Alambique.Feed.Capture
  alias AlambiqueTestkit.{CrashPoints, GatedStore}

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
      {2, tuple({constant(:pass), integer(0..30)})}
    ])
  end

  property "every captured change is on the feed once, in order, whatever the cleanup", ctx do
    runs = :counters.new(1, [])

    check all(
            cleanup <- member_of([:in_write, :idle, :after_publish, :idle_or_write]),
            rows <- member_of([nil, 2, 5]),
            pieces <- member_of([nil, 2, 3]),
            hand_over <- member_of([:cell, :sender]),
            ops <- list_of(op(), min_length: 6, max_length: 16),
            max_runs: 20,
            max_shrinking_steps: 0
          ) do
      scenario(ctx, runs, {cleanup, rows, pieces, hand_over}, ops)
    end
  end

  # Every run holds a kill at a random place and a crash of the cell between
  # the local commit of a cleanup and the upload of its batch.
  property "a kill anywhere, also inside a cleanup, loses and repeats nothing", ctx do
    runs = :counters.new(1, [])

    check all(
            cleanup <- member_of([:idle, :after_publish, :idle_or_write]),
            rows <- member_of([nil, 2]),
            pieces <- member_of([nil, 2, 3]),
            hand_over <- member_of([:cell, :sender]),
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

      scenario(ctx, runs, {cleanup, rows, pieces, hand_over}, ops)
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

  defp scenario(ctx, runs, {cleanup, rows, pieces, hand_over}, ops) do
    :counters.add(runs, 1, 1)
    n = :counters.get(runs, 1)
    {:ok, gate} = GatedStore.start_link()
    store = GatedStore.wrap(gate, ctx.store)

    inst =
      start_instance(%{ctx | store: store},
        name: :"cleanup_run_#{n}",
        prefix: "cleanup-#{n}",
        migrations: @schema,
        lease_ttl_ms: 1_000,
        skew_margin_ms: 200,
        idle_timeout_ms: 60_000,
        feed: [
          cleanup: cleanup,
          cleanup_after_ms: 40,
          cleanup_rows: rows,
          hand_over_rows: pieces,
          hand_over: hand_over,
          publish_retry_ms: 10,
          publish_retry_max_ms: 40,
          stop_publish_ms: 200,
          window_ms: 60_000
        ]
      )

    try do
      run = Enum.reduce(ops, %{inst: inst, gate: gate, next: 1}, &step/2)
      GatedStore.open(gate)
      verify(ctx, n, run)
    after
      GatedStore.open(gate)
      stop_supervised!(inst)
      GenServer.stop(gate)
    end
  end

  # A cell that was killed keeps its lease until it expires.
  defp ready(run),
    do: eventually(fn -> match?({:ok, _}, Alambique.touch(run.inst, @cell)) end, 15_000)

  defp step({:write, k}, run) do
    ready(run)
    ids = Enum.to_list(run.next..(run.next + k - 1))

    # A write that meets a kill has no answer: the table says if it is durable.
    _ =
      Alambique.transaction(run.inst, @cell, fn tx ->
        for id <- ids, do: {:ok, _} = Tx.execute(tx, "INSERT INTO notes VALUES (?, 'x')", [id])
        :ok
      end)

    %{run | next: run.next + k}
  end

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
    case Alambique.Cell.whereis(Alambique.Config.fetch!(run.inst), @cell) do
      nil -> :ok
      pid -> Process.exit(pid, :kill)
    end

    run
  end

  # The cell dies after a cleanup committed locally, before its batch is sent.
  defp step(:crash_in_cleanup, run) do
    CrashPoints.arm(:feed_after_cleanup, :kill, when: &match?(%{cell: @cell}, &1))
    run
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

  defp verify(ctx, n, run) do
    ready(run)
    # A crash armed earlier can still kill the cell under this read.
    expected =
      eventually(fn ->
        case Alambique.query(run.inst, @cell, "SELECT id FROM notes ORDER BY id") do
          {:ok, %{rows: rows}} -> {:ids, List.flatten(rows)}
          _ -> false
        end
      end)
      |> elem(1)

    feed = Feed.ctx(store: ctx.store, prefix: "cleanup-#{n}", settle_ms: 0)

    records =
      eventually(
        fn ->
          {:ok, all, _} = Feed.read(feed, nil, 10_000)
          all = Enum.filter(all, &(&1.cell == @cell))
          ids = all |> Enum.uniq_by(& &1.id) |> Enum.map(& &1.key["id"])
          ids == expected and all
        end,
        20_000
      )

    first = Enum.uniq_by(records, & &1.id)
    assert Enum.map(first, & &1.key["id"]) == expected

    for {id, same} <- Enum.group_by(records, & &1.id) do
      assert same |> Enum.map(&Map.delete(&1, :at)) |> Enum.uniq() |> length() == 1,
             "#{id} was published twice with different content"
    end

    # The positions of a batch run from 0 without a gap.
    for {_txid, batch} <- Enum.group_by(first, &elem(&1.position, 0)) do
      assert batch |> Enum.map(&elem(&1.position, 1)) |> Enum.sort() ==
               Enum.to_list(0..(length(batch) - 1)//1)
    end
  end
end
