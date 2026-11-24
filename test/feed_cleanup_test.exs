defmodule Alambique.FeedCleanupTest do
  @moduledoc """
  When the rows of published batches leave the cell's file (`feed: [cleanup:]`,
  design §13): in the next user write, in a transaction of the cell's own once
  it is idle, or as soon as the publication is acknowledged.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{CountingStore, Feed}
  alias AlambiqueTestkit.{CrashPoints, GatedStore}

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @rows "SELECT count(*) FROM _alambique_feed WHERE op <> 'floor'"
  @quiet 150

  setup do
    capture([[:alambique, :feed, :publish]])
    on_exit(&CrashPoints.reset/0)
    ctx = setup_store()
    {_, counts} = store = CountingStore.wrap(ctx.store)
    Map.merge(ctx, %{store: store, counts: counts})
  end

  defp start(ctx, feed, opts \\ []) do
    feed = Keyword.merge([cleanup_after_ms: @quiet], feed)
    start_instance(ctx, [migrations: @schema, feed: feed] ++ opts)
  end

  defp gated(ctx) do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    {gate, %{ctx | store: GatedStore.wrap(gate, ctx.store)}}
  end

  defp insert(inst, cell, id),
    do: {:ok, _} = Alambique.execute(inst, cell, ~s|INSERT INTO notes VALUES (?, 'x')|, [id])

  defp cell_pid(inst, cell), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

  # Reads the state without being a call: the cell stays idle.
  defp state(inst, cell), do: :sys.get_state(cell_pid(inst, cell))

  # Every acknowledged batch is published.
  defp published(inst, cell) do
    eventually(fn ->
      s = state(inst, cell)
      (s.feed_published > 0 and s.feed_published == s.feed_sent and s.feed_backlog == 0) && s
    end)
  end

  defp cleaned(inst, cell) do
    eventually(fn ->
      s = state(inst, cell)
      s.feed_published > 0 and s.feed_cleaned == s.feed_published and s
    end)
  end

  defp local_rows(inst, cell) do
    {:ok, %{rows: [[n]]}} = Alambique.query(inst, cell, @rows)
    n
  end

  defp stored_rows(ctx, cell), do: ctx |> durable_rows(cell, @rows) |> hd() |> hd()

  defp segments(%{counts: counts}),
    do: counts |> CountingStore.keys(:put) |> Enum.count(&String.contains?(&1, "/0000/"))

  defp records(ctx, cell) do
    {:ok, records, _} =
      Feed.read(Feed.ctx(store: ctx.store, prefix: "cells-test", settle_ms: 0), nil, 1_000)

    Enum.filter(records, &(&1.cell == cell))
  end

  defp keys(records), do: Enum.map(records, & &1.key["id"])

  # Full collections of the traced cell until `:cleanup` arrives.
  defp collections_until(:cleanup) do
    receive do
      :cleanup -> []
      {:trace, _, :gc_major_start, _} = event -> [event | collections_until(:cleanup)]
      {:trace, _, _, _} -> collections_until(:cleanup)
    after
      5_000 -> flunk("no cleanup")
    end
  end

  # Full collections of the traced cell until it has shrunk its cache.
  defp collections_until(:shrunk, pid) do
    eventually(fn -> :sys.get_state(pid).idle_cache == :shrunk end)
    drain_collections([])
  end

  defp drain_collections(acc) do
    receive do
      {:trace, _, :gc_major_start, _} = event -> drain_collections([event | acc])
      {:trace, _, _, _} -> drain_collections(acc)
    after
      50 -> acc
    end
  end

  # Time enough for a cleanup that would run to have run.
  defp quiet, do: Process.sleep(4 * @quiet)

  describe ":in_write" do
    test "leaves the rows until the next write, which removes them", ctx do
      inst = start(ctx, cleanup: :in_write)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)
      before = segments(ctx)

      quiet()
      assert %{feed_cleaned: 0} = state(inst, cell)
      assert segments(ctx) == before
      assert stored_rows(ctx, cell) == 1

      insert(inst, cell, 2)
      assert %{feed_cleaned: cleaned} = state(inst, cell)
      assert cleaned > 0
      assert local_rows(inst, cell) == 1
      assert segments(ctx) == before + 1
    end
  end

  describe ":idle" do
    test "cleans once after a quiet time, in a batch of its own", ctx do
      inst = start(ctx, cleanup: :idle)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)
      before = segments(ctx)
      assert local_rows(inst, cell) == 1

      state = cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
      assert segments(ctx) == before + 1
      assert state.feed_cleaned == state.feed_published

      # Nothing is left to clean, so nothing more is uploaded.
      quiet()
      assert segments(ctx) == before + 1
      assert [%{key: %{"id" => 1}}] = records(ctx, cell)
    end

    test "a write that finds the rows does not remove them", ctx do
      inst = start(ctx, cleanup: :idle, cleanup_after_ms: 1_000)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)

      insert(inst, cell, 2)
      assert %{feed_cleaned: 0} = state(inst, cell)
      assert local_rows(inst, cell) == 2

      state = cleaned(inst, cell)
      assert state.feed_published >= 2
      assert local_rows(inst, cell) == 0
    end

    test "a call postpones the cleanup, a read too", ctx do
      inst = start(ctx, cleanup: :idle, cleanup_after_ms: 400)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)

      for _ <- 1..8 do
        Process.sleep(100)
        {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      end

      assert %{feed_cleaned: 0} = state(inst, cell)
      cleaned(inst, cell)
    end
  end

  describe ":after_publish" do
    test "cleans as soon as the publication is acknowledged, with no call", ctx do
      inst = start(ctx, cleanup: :after_publish, cleanup_after_ms: 60_000 - 1)
      cell = unique_cell()
      insert(inst, cell, 1)
      before = segments(ctx)

      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
      assert segments(ctx) == before + 1
    end
  end

  describe ":idle_or_write" do
    test "cleans an idle cell", ctx do
      inst = start(ctx, cleanup: :idle_or_write)
      cell = unique_cell()
      insert(inst, cell, 1)
      before = segments(ctx)
      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
      assert segments(ctx) == before + 1
    end

    test "a write that finds the rows removes them", ctx do
      inst = start(ctx, cleanup: :idle_or_write, cleanup_after_ms: 10_000)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)
      before = segments(ctx)

      insert(inst, cell, 2)
      assert %{feed_cleaned: cleaned} = state(inst, cell)
      assert cleaned >= 1
      assert local_rows(inst, cell) == 1
      assert segments(ctx) == before + 1
    end
  end

  describe "the publication" do
    for mode <- [:idle, :after_publish, :idle_or_write] do
      test "#{mode} removes nothing before it is acknowledged", ctx do
        {gate, ctx} = gated(ctx)
        inst = start(ctx, cleanup: unquote(mode))
        cell = unique_cell()
        GatedStore.hold(gate, ~r{/feed/})
        insert(inst, cell, 1)
        eventually(fn -> GatedStore.held(gate) != [] end)

        quiet()
        assert %{feed_published: 0, feed_cleaned: 0} = state(inst, cell)
        assert local_rows(inst, cell) == 1

        GatedStore.open(gate)
        cleaned(inst, cell)
        eventually(fn -> stored_rows(ctx, cell) == 0 end)
      end
    end

    for mode <- [:idle, :after_publish, :idle_or_write] do
      test "#{mode} removes the published rows and keeps those handed over but not published",
           ctx do
        {gate, ctx} = gated(ctx)
        inst = start(ctx, cleanup: unquote(mode), cleanup_after_ms: 600)
        cell = unique_cell()
        insert(inst, cell, 1)
        published(inst, cell)
        GatedStore.hold(gate, ~r{/feed/})
        insert(inst, cell, 2)
        eventually(fn -> GatedStore.held(gate) != [] end)

        state = cleaned(inst, cell)

        quiet()
        assert state(inst, cell).feed_sent > state.feed_published
        assert stored_rows(ctx, cell) == 1
        assert local_rows(inst, cell) == 1
        GatedStore.open(gate)
      end
    end

    test "after_publish keeps the rows of a batch handed over but not published", ctx do
      {gate, ctx} = gated(ctx)
      inst = start(ctx, cleanup: :after_publish)
      cell = unique_cell()
      GatedStore.hold(gate, ~r{/feed/})
      insert(inst, cell, 1)
      eventually(fn -> GatedStore.held(gate) != [] end)
      [first] = GatedStore.held(gate)
      # The sender has the second batch while it waits for the first.
      insert(inst, cell, 2)
      eventually(fn -> state(inst, cell).feed_sent == state(inst, cell).pipe.acked.txid end)

      # The report of the first batch makes the cell clean.
      GatedStore.release(gate, first)
      eventually(fn -> state(inst, cell).feed_cleaned > 0 end)
      eventually(fn -> length(GatedStore.held(gate)) == 1 end)

      quiet()

      assert %{feed_published: published, feed_cleaned: published, feed_sent: sent} =
               state(inst, cell)

      assert sent > published
      assert stored_rows(ctx, cell) == 1
      assert local_rows(inst, cell) == 1
      GatedStore.open(gate)
    end

    test "a failed publication leaves the rows, a later one clears them", ctx do
      {gate, ctx} = gated(ctx)
      inst = start(ctx, cleanup: :idle, publish_retry_ms: 20, publish_retry_max_ms: 40)
      cell = unique_cell()
      GatedStore.hold(gate, ~r{/feed/})
      insert(inst, cell, 1)

      eventually(fn ->
        case GatedStore.held(gate) do
          [key] -> GatedStore.fail(gate, key) == :ok
          _ -> false
        end
      end)

      quiet()
      assert %{feed_cleaned: 0} = state(inst, cell)
      GatedStore.open(gate)
      cleaned(inst, cell)
    end
  end

  describe "the guards" do
    test "no cleanup while a batch is in flight and the pipeline is full", ctx do
      {gate, ctx} = gated(ctx)
      inst = start(ctx, [cleanup: :after_publish], pipeline_depth: 1)
      cell = unique_cell()
      # The publication waits, then the next batch does.
      GatedStore.hold(gate, ~r{/feed/})
      insert(inst, cell, 1)
      eventually(fn -> GatedStore.held(gate) != [] end)
      [feed_key] = GatedStore.held(gate)
      before = segments(ctx)
      GatedStore.hold(gate, ~r{/0000/})

      write =
        Task.async(fn -> Alambique.execute(inst, cell, "INSERT INTO notes VALUES (2, 'x')") end)

      eventually(fn -> length(GatedStore.held(gate)) == 2 end)
      GatedStore.release(gate, feed_key)
      eventually(fn -> state(inst, cell).feed_published > 0 end)

      quiet()
      assert %{feed_cleaned: 0, pipe: %{queue: [_]}} = state(inst, cell)
      assert segments(ctx) == before

      GatedStore.open(gate)
      assert {:ok, _} = Task.await(write)
      cleaned(inst, cell)
    end

    test "idle waits for the batch in flight, whatever the quiet time", ctx do
      {gate, ctx} = gated(ctx)
      inst = start(ctx, cleanup: :idle, cleanup_after_ms: 300)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)
      before = segments(ctx)
      GatedStore.hold(gate, ~r{/0000/})

      write =
        Task.async(fn -> Alambique.execute(inst, cell, "INSERT INTO notes VALUES (2, 'x')") end)

      eventually(fn -> GatedStore.held(gate) != [] end)

      Process.sleep(3 * 300)
      assert %{feed_cleaned: 0} = state(inst, cell)
      assert segments(ctx) == before

      GatedStore.open(gate)
      assert {:ok, _} = Task.await(write)
      cleaned(inst, cell)
    end

    # `cleanup/1` sends its batch in the callback that committed it, after
    # `cleanup_ready?` saw room (cell.ex), so a full pipe with `unsent` set is
    # not reachable from outside. The nearest state: the cleanup committed in
    # the cell and `unsent` set, the pipe full, then the `:flush` message.
    test "a cleanup committed while the pipe is full goes out once it has room", ctx do
      {gate, ctx} = gated(ctx)
      inst = start(ctx, [cleanup: :in_write], pipeline_depth: 1)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)
      pid = cell_pid(inst, cell)
      before = segments(ctx)
      GatedStore.hold(gate, ~r{/0000/})
      on_exit(fn -> catch_exit(GatedStore.open(gate)) end)

      write =
        Task.async(fn -> Alambique.execute(inst, cell, "INSERT INTO notes VALUES (2, 'x')") end)

      eventually(fn -> GatedStore.held(gate) != [] end)

      :sys.replace_state(pid, fn s ->
        {:ok, s} = Alambique.Feed.Publisher.cleanup(s)
        %{s | unsent: true}
      end)

      send(pid, :flush)
      assert %{unsent: true, pipe: %{flush_wanted: true, queue: [_]}} = state(inst, cell)
      assert %{feed_cleaned: cleaned} = state(inst, cell)
      assert cleaned > 0
      assert segments(ctx) == before

      GatedStore.open(gate)
      assert {:ok, _} = Task.await(write)
      eventually(fn -> (s = state(inst, cell)).unsent == false and s.pipe.queue == [] end)

      # The write's batch and the cleanup's: no third.
      quiet()
      assert segments(ctx) == before + 2
      assert stored_rows(ctx, cell) == 1
      assert state(inst, cell).feed_cleaned == cleaned
    end

    test "no cleanup while the lease is not valid", ctx do
      inst = start(ctx, [cleanup: :idle], lease_ttl_ms: 30_000)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)
      pid = cell_pid(inst, cell)
      lease = :sys.get_state(pid).lease
      :sys.replace_state(pid, &%{&1 | lease: %{&1.lease | expires_at_ms: 0}})

      quiet()
      assert %{feed_cleaned: 0} = :sys.get_state(pid)

      :sys.replace_state(pid, &%{&1 | lease: lease})
      cleaned(inst, cell)
    end

    test "no cleanup while the file is still being filled", ctx do
      seeder =
        start_instance(ctx, migrations: @schema, lazy_restore: false, feed: [cleanup: :in_write])

      cell = unique_cell()

      {:ok, _} =
        Alambique.transaction(seeder, cell, fn tx ->
          for id <- 1..50,
              do:
                {:ok, _} =
                  Alambique.Tx.execute(tx, "INSERT INTO notes VALUES (?, randomblob(900))", [id])

          :ok
        end)

      :ok = Alambique.hibernate(seeder, cell)
      stop_supervised!(seeder)
      gate = :atomics.new(1, [])

      CrashPoints.arm(
        :hydrate_before_fetch,
        fn _, %{hydrator: hydrator} -> hold(gate, hydrator) end,
        times: :infinity,
        when: &match?(%{cell: ^cell}, &1)
      )

      lazy = [
        min_bytes: 0,
        concurrency: 1,
        batch_bytes: 1_024,
        miss_pages: 1,
        verify_writes: false
      ]

      inst = start(ctx, [cleanup: :idle], lazy_restore: lazy)
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      eventually(fn -> state(inst, cell).feed_published > 0 end)

      quiet()
      assert %{feed_cleaned: 0, hydrator: hydrator} = state(inst, cell)
      assert hydrator != nil

      :atomics.put(gate, 1, 1)
      cleaned(inst, cell)
    end

    defp hold(gate, hydrator) do
      if :atomics.get(gate, 1) == 0 and Process.alive?(hydrator) do
        Process.sleep(5)
        hold(gate, hydrator)
      end
    end

    test "a failed cleanup leaves feed_cleaned and is tried again", ctx do
      inst = start(ctx, cleanup: :idle)
      cell = unique_cell()
      insert(inst, cell, 1)
      published(inst, cell)
      CrashPoints.arm(:feed_before_cleanup, :raise, times: 2)

      eventually(fn -> state(inst, cell).feed_cleanup_failures >= 1 end)
      assert Process.alive?(cell_pid(inst, cell))
      assert %{feed_cleaned: 0} = state(inst, cell)

      cleaned(inst, cell)
      assert %{feed_cleanup_failures: 0} = state(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
    end
  end

  describe "the idle cache shrink" do
    for {name, shrink, cleanup} <- [{"before", 100, 500}, {"after", 500, 100}] do
      test "cleans first when the shrink is due #{name} the cleanup", ctx do
        inst =
          start(ctx, [cleanup: :idle, cleanup_after_ms: unquote(cleanup)],
            awake: [cache_kib: 512, idle_cache_after_ms: unquote(shrink), idle_cache_kib: 16]
          )

        cell = unique_cell()
        insert(inst, cell, 1)
        published(inst, cell)
        pid = cell_pid(inst, cell)

        shrunk = eventually(fn -> (s = :sys.get_state(pid)).idle_cache == :shrunk and s end)
        assert shrunk.feed_published > 0
        assert shrunk.feed_cleaned == shrunk.feed_published
      end
    end

    for {name, shrink, cleanup} <- [{"before", 100, 500}, {"after", 500, 100}] do
      test "the collection follows the cleanup when the shrink is due #{name} it", ctx do
        inst =
          start(ctx, [cleanup: :idle, cleanup_after_ms: unquote(cleanup)],
            awake: [
              cache_kib: 512,
              idle_cache_after_ms: unquote(shrink),
              idle_cache_kib: 16,
              idle_gc: true
            ]
          )

        cell = unique_cell()
        insert(inst, cell, 1)
        published(inst, cell)
        pid = cell_pid(inst, cell)
        test = self()

        CrashPoints.arm(:feed_before_cleanup, fn _, _ -> send(test, :cleanup) end,
          when: &match?(%{cell: ^cell}, &1)
        )

        :erlang.trace(pid, true, [:garbage_collection])
        on_exit(fn -> catch_error(:erlang.trace(pid, false, [:garbage_collection])) end)

        # No full collection before the cleanup, one after it.
        assert [] = collections_until(:cleanup)
        assert [_ | _] = collections_until(:shrunk, pid)
      end
    end

    test "waits for a bounded cleanup to finish before it shrinks", ctx do
      inst =
        start(ctx, [cleanup: :idle, cleanup_rows: 1, cleanup_after_ms: 300],
          awake: [cache_kib: 512, idle_cache_after_ms: 100, idle_cache_kib: 16]
        )

      cell = unique_cell()
      for id <- 1..4, do: insert(inst, cell, id)
      published(inst, cell)
      pid = cell_pid(inst, cell)

      shrunk = eventually(fn -> (s = :sys.get_state(pid)).idle_cache == :shrunk and s end)
      assert shrunk.feed_cleaned == shrunk.feed_published
    end
  end

  describe "cleanup_rows" do
    test "removes the rows in chunks of whole batches", ctx do
      inst = start(ctx, cleanup: :idle, cleanup_rows: 2, cleanup_after_ms: 400)
      cell = unique_cell()
      for id <- 1..6, do: insert(inst, cell, id)
      published(inst, cell)
      before = segments(ctx)

      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
      assert segments(ctx) - before == 3
      assert keys(records(ctx, cell)) == [1, 2, 3, 4, 5, 6]
    end

    test "a batch larger than the bound goes whole, in one cleanup", ctx do
      inst = start(ctx, cleanup: :idle, cleanup_rows: 3, cleanup_after_ms: 400)
      cell = unique_cell()

      {:ok, _} =
        Alambique.transaction(inst, cell, fn tx ->
          for id <- 1..10,
              do: {:ok, _} = Alambique.Tx.execute(tx, "INSERT INTO notes VALUES (?, 'x')", [id])

          :ok
        end)

      published(inst, cell)
      before = segments(ctx)
      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
      assert segments(ctx) - before == 1
    end
  end

  describe "a crash" do
    for mode <- [:idle, :after_publish] do
      test "after the cleanup landed loses and repeats nothing (#{mode})", ctx do
        inst = start(ctx, cleanup: unquote(mode))
        cell = unique_cell()
        for id <- 1..3, do: insert(inst, cell, id)
        cleaned(inst, cell)
        eventually(fn -> stored_rows(ctx, cell) == 0 end)
        first = records(ctx, cell)
        assert keys(first) == [1, 2, 3]

        kill(inst, cell)
        assert {:ok, %{rows: [[0]]}} = Alambique.query(inst, cell, @rows)
        insert(inst, cell, 4)
        eventually(fn -> length(records(ctx, cell)) == 4 end)
        assert Enum.take(records(ctx, cell), 3) == first
        assert keys(records(ctx, cell)) == [1, 2, 3, 4]
      end
    end

    test "after the delete committed but before its batch was sent publishes the rows again, under the same ids",
         ctx do
      inst = start(ctx, cleanup: :idle)
      cell = unique_cell()
      for id <- 1..3, do: insert(inst, cell, id)
      published(inst, cell)
      first = records(ctx, cell)
      segments = segments(ctx)

      CrashPoints.arm(:feed_after_cleanup, :kill, when: &match?(%{cell: ^cell}, &1))
      eventually(fn -> Alambique.owner(inst, cell) == :none end)
      # The store never saw the delete.
      assert segments(ctx) == segments
      assert stored_rows(ctx, cell) == 3

      assert {:ok, %{rows: [[3]]}} = Alambique.query(inst, cell, @rows)
      # The new owner publishes the rows again, and only then removes them.
      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)

      all = records(ctx, cell)
      assert all |> Enum.map(& &1.id) |> Enum.uniq() == Enum.map(first, & &1.id)

      for {_id, same} <- Enum.group_by(all, & &1.id),
          do: assert(same |> Enum.map(&Map.delete(&1, :at)) |> Enum.uniq() |> length() == 1)

      assert all |> Enum.uniq_by(& &1.id) |> Enum.map(&Map.delete(&1, :at)) ==
               Enum.map(first, &Map.delete(&1, :at))
    end

    defp kill(inst, cell) do
      pid = cell_pid(inst, cell)
      Process.exit(pid, :kill)
      eventually(fn -> Alambique.owner(inst, cell) == :none end)
    end
  end

  describe "history with a cleanup batch" do
    test "a freeze and an unfreeze keep the data and publish nothing twice", ctx do
      inst = start(ctx, cleanup: :idle)
      cell = unique_cell()
      for id <- 1..3, do: insert(inst, cell, id)
      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
      first = records(ctx, cell)

      assert :ok = Alambique.freeze(inst, cell)
      assert :none = Alambique.owner(inst, cell)
      assert :ok = Alambique.unfreeze(inst, cell)

      assert {:ok, %{rows: [[3]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM notes")
      assert {:ok, %{rows: [[0]]}} = Alambique.query(inst, cell, @rows)
      insert(inst, cell, 4)
      eventually(fn -> length(records(ctx, cell)) == 4 end)
      assert Enum.take(records(ctx, cell), 3) == first
    end

    test "a rewind to before and to after a cleanup batch restores the data", ctx do
      inst = start(ctx, cleanup: :idle)
      cell = unique_cell()
      for id <- 1..2, do: insert(inst, cell, id)
      before = position(inst, cell)
      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
      {:ok, %{txid: after_cleanup}} = Alambique.position(inst, cell)
      assert after_cleanup > before
      for id <- 3..4, do: insert(inst, cell, id)
      published(inst, cell)

      assert {:ok, %{to: ^before}} = Alambique.rewind(inst, cell, {:txid, before})
      assert ids(inst, cell) == [1, 2]
      insert(inst, cell, 5)
      assert ids(inst, cell) == [1, 2, 5]
      cleaned(inst, cell)

      assert {:ok, %{to: ^after_cleanup}} = Alambique.rewind(inst, cell, {:txid, after_cleanup})
      assert ids(inst, cell) == [1, 2]
      insert(inst, cell, 6)
      assert ids(inst, cell) == [1, 2, 6]
      cleaned(inst, cell)
      eventually(fn -> stored_rows(ctx, cell) == 0 end)
    end

    defp position(inst, cell) do
      {:ok, %{txid: txid}} = Alambique.position(inst, cell)
      txid
    end

    defp ids(inst, cell) do
      {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT id FROM notes ORDER BY id")
      List.flatten(rows)
    end
  end
end
