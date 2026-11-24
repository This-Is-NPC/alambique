defmodule Alambique.FeedHandOverTest do
  @moduledoc """
  The cell hands the rows of acknowledged batches to its sender in pieces of
  `feed: [hand_over_rows:]` (design §13): a large batch never holds the
  mailbox of the cell, and the feed it publishes is the one of an unbounded
  hand-over.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Cell.State
  alias Alambique.Feed
  alias Alambique.Feed.Publisher
  alias AlambiqueTestkit.{CrashPoints, GatedStore}

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @cell "piece-cell"

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  defp start(ctx, prefix, feed, extra \\ []),
    do:
      start_instance(
        ctx,
        [migrations: @schema, prefix: prefix, feed: [window_ms: 60_000] ++ feed] ++ extra
      )

  defp insert(inst, cell, ids) do
    {:ok, _} =
      Alambique.transaction(inst, cell, fn tx ->
        for id <- ids,
            do: {:ok, _} = Alambique.Tx.execute(tx, "INSERT INTO notes VALUES (?, 'x')", [id])

        :ok
      end)
  end

  defp feed_ctx(ctx, prefix), do: Feed.ctx(store: ctx.store, prefix: prefix, settle_ms: 0)

  defp records(ctx, prefix) do
    {:ok, records, _} = Feed.read(feed_ctx(ctx, prefix), nil, 1_000)
    Enum.filter(records, &(&1.cell == @cell))
  end

  defp bodies(%{store: {mod, config}}, prefix) do
    {:ok, keys, _} = mod.list(config, prefix <> "/feed/", nil, 10_000)
    for key <- keys, do: elem(mod.get(config, key), 1)
  end

  defp keys(records), do: Enum.map(records, & &1.key["id"])

  # Counts the hand-overs of the cell, which a piece each.
  defp count_pieces(cell) do
    counter = :counters.new(1, [])

    CrashPoints.arm(:feed_before_publish, fn _, _ -> :counters.add(counter, 1, 1) end,
      times: :infinity,
      when: &match?(%{cell: ^cell}, &1)
    )

    counter
  end

  defp assert_pieces(_pieces, _rows, nil), do: :ok

  defp assert_pieces(pieces, rows, bound),
    do: assert(:counters.get(pieces, 1) >= ceil(rows / bound))

  defp cell_pid(inst), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), @cell)

  describe "a batch in pieces" do
    # One batch to an object, and the four batches of the test in one.
    for rows <- [nil, 1, 3, 4], {batches, linger} <- [{1, 0}, {4, 5_000}] do
      test "publishes ids and objects of the whole batch (hand_over_rows: #{inspect(rows)}, #{batches} to an object)",
           ctx do
        feed = [object_batches: unquote(batches), linger_ms: unquote(linger)]
        plain = start(ctx, "plain", feed, name: :plain)
        split = start(ctx, "split", [hand_over_rows: unquote(rows)] ++ feed)
        expected = Enum.to_list(1..22)

        write = fn inst ->
          insert(inst, @cell, [1])
          insert(inst, @cell, 2..11)
          insert(inst, @cell, 12..13)
          insert(inst, @cell, 14..22)
        end

        write.(plain)
        eventually(fn -> keys(records(ctx, "plain")) |> Enum.uniq() == expected end)

        pieces = count_pieces(@cell)
        write.(split)
        eventually(fn -> keys(records(ctx, "split")) |> Enum.uniq() == expected end)

        ids = fn prefix -> prefix |> then(&records(ctx, &1)) |> Enum.map(& &1.id) end
        assert ids.("split") == ids.("plain")
        assert bodies(ctx, "split") == bodies(ctx, "plain")

        positions = for r <- records(ctx, "split"), do: r.position
        assert Enum.uniq(positions) == positions

        # No piece has more rows than the bound.
        assert_pieces(pieces, length(expected), unquote(rows))
      end
    end

    test "a sender that ends in the middle of a batch gets the whole batch again", ctx do
      inst = start(ctx, "kill", [hand_over_rows: 3], name: :kill)
      armed = :counters.new(1, [])

      # The sender ends before the third piece of the large batch.
      CrashPoints.arm(
        :feed_before_publish,
        fn _, %{sender: sender} ->
          :counters.add(armed, 1, 1)
          if :counters.get(armed, 1) == 3, do: Process.exit(sender, :kill)
        end,
        times: :infinity,
        when: &match?(%{cell: @cell}, &1)
      )

      insert(inst, @cell, 1..10)
      eventually(fn -> keys(records(ctx, "kill")) |> Enum.uniq() == Enum.to_list(1..10) end)

      assert :counters.get(armed, 1) > 4
      records = records(ctx, "kill")
      assert keys(records) == Enum.to_list(1..10)

      assert Enum.map(records, & &1.position) ==
               for(n <- 0..9, do: {elem(hd(records).position, 0), n})

      assert %State{feed_part: nil, feed_backlog: 0} = :sys.get_state(cell_pid(inst))
    end

    test "a message of a continuation that no longer exists is ignored", ctx do
      inst = start(ctx, "stale", [hand_over_rows: 3], name: :stale)
      insert(inst, @cell, 1..10)
      eventually(fn -> keys(records(ctx, "stale")) == Enum.to_list(1..10) end)

      state = :sys.get_state(cell_pid(inst))
      tick = make_ref()
      waiting = %{state | feed_tick: tick, feed_part: {state.feed_sent + 1, 4}}

      assert Publisher.continue(waiting, make_ref()) == waiting
      assert %{feed_tick: nil} = Publisher.continue(waiting, tick)

      # Sent to a live cell it changes nothing.
      send(cell_pid(inst), {:feed_part, make_ref()})
      assert %{feed_sent: sent, feed_part: nil} = :sys.get_state(cell_pid(inst))
      assert sent == state.feed_sent
    end

    test "a call in the mailbox is served between two pieces", ctx do
      inst = start(ctx, "fair", [hand_over_rows: 3], name: :fair)
      test = self()
      gates = for n <- [2, 3], into: %{}, do: {n, :atomics.new(1, [])}
      seen = :counters.new(1, [])

      CrashPoints.arm(
        :feed_before_publish,
        fn _, _ ->
          :counters.add(seen, 1, 1)
          n = :counters.get(seen, 1)
          send(test, {:piece, n})

          with {:ok, gate} <- Map.fetch(gates, n),
               do: eventually(fn -> :atomics.get(gate, 1) == 1 end)
        end,
        times: :infinity,
        when: &match?(%{cell: @cell}, &1)
      )

      # The first piece is of the first transaction alone.
      insert(inst, @cell, 1..10)
      assert_receive {:piece, 2}
      pid = cell_pid(inst)

      call = Task.async(fn -> Alambique.query(inst, @cell, "SELECT count(*) FROM notes") end)
      eventually(fn -> Process.info(pid, :message_queue_len) |> elem(1) >= 1 end)
      :atomics.put(gates[2], 1, 1)

      # The call is answered before the third piece is handed over.
      assert {:ok, %{rows: [[10]]}} = Task.await(call, 5_000)
      refute_received {:piece, 3}
      :atomics.put(gates[3], 1, 1)
      eventually(fn -> keys(records(ctx, "fair")) == Enum.to_list(1..10) end)
    end
  end

  describe "the stop" do
    test "hands over everything, in pieces, before the cell sleeps", ctx do
      {:ok, gate} = GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)

      inst =
        start(
          %{ctx | store: GatedStore.wrap(gate, ctx.store)},
          "stop",
          [hand_over_rows: 3, max_backlog_batches: 1, stop_publish_ms: 5_000],
          name: :stop
        )

      pieces = count_pieces(@cell)
      GatedStore.hold(gate, ~r{/feed/})
      insert(inst, @cell, [1])
      eventually(fn -> GatedStore.held(gate) != [] end)
      # The cap keeps the second batch in the file.
      insert(inst, @cell, 2..11)
      assert :counters.get(pieces, 1) == 1
      assert %{feed_capped: true} = :sys.get_state(cell_pid(inst))

      stop = Task.async(fn -> Alambique.hibernate(inst, @cell) end)
      eventually(fn -> :counters.get(pieces, 1) == 5 end)
      GatedStore.open(gate)

      assert :ok = Task.await(stop, 10_000)
      assert Alambique.owner(inst, @cell) == :none
      assert keys(records(%{ctx | store: ctx.store}, "stop")) == Enum.to_list(1..11)
    end

    test "gives up at stop_publish_ms and leaves the rows for the next owner", ctx do
      inst = start(ctx, "late", [hand_over_rows: 1, stop_publish_ms: 60], name: :late)

      # Every piece takes longer than the whole wait.
      CrashPoints.arm(:feed_before_publish, fn _, _ -> Process.sleep(100) end,
        times: :infinity,
        when: &match?(%{cell: @cell}, &1)
      )

      insert(inst, @cell, 1..6)
      started = System.monotonic_time(:millisecond)
      assert :ok = Alambique.hibernate(inst, @cell)
      assert System.monotonic_time(:millisecond) - started < 3_000
      CrashPoints.reset()

      # The next owner publishes what the file still holds, whole.
      assert {:ok, _} = Alambique.query(inst, @cell, "SELECT 1")
      eventually(fn -> keys(records(ctx, "late")) |> Enum.uniq() == Enum.to_list(1..6) end)
    end
  end
end
