defmodule Alambique.FeedNativeTest do
  @moduledoc """
  `feed: [hand_over: :native]` and `feed: [reader_close:]` (design §13): the
  library reads the rows and builds the text of their records, the sender puts
  them, and the feed is the one of `hand_over: :cell`, byte for byte. The
  paths that leave the cell, the sender or the reader in the middle of a
  hand-over give no row lost and none twice, and no file stays open.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Alambique.CellHelpers

  alias Alambique.{Feed, Sqlite}
  alias Alambique.Cell.State
  alias Alambique.Feed.Publisher
  alias AlambiqueTestkit.CrashPoints

  @schema [
    {1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"},
    {2, ~S|CREATE TABLE "we""ird/t" (id INTEGER PRIMARY KEY, b BLOB, n INTEGER)|}
  ]
  @cell "native-cell"

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

  defp native(feed \\ []), do: [hand_over: :native] ++ feed
  defp sender(feed \\ []), do: [hand_over: :sender] ++ feed

  defp insert(inst, cell, ids) do
    {:ok, _} =
      Alambique.transaction(inst, cell, fn tx ->
        for id <- ids,
            do: {:ok, _} = Alambique.Tx.execute(tx, "INSERT INTO notes VALUES (?, 'x')", [id])

        :ok
      end)
  end

  defp records(ctx, prefix) do
    {:ok, records, _} =
      Feed.read(Feed.ctx(store: ctx.store, prefix: prefix, settle_ms: 0), nil, 1_000)

    Enum.filter(records, &(&1.cell == @cell))
  end

  defp bodies(%{store: {mod, config}}, prefix) do
    {:ok, keys, _} = mod.list(config, prefix <> "/feed/", nil, 10_000)
    for key <- keys, do: elem(mod.get(config, key), 1)
  end

  defp keys(records), do: Enum.map(records, & &1.key["id"])
  defp pid(inst), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), @cell)
  defp state(inst), do: :sys.get_state(pid(inst))

  defp files(ctx) do
    Enum.filter(
      open_files(),
      &(String.starts_with?(&1, ctx.dir) and String.ends_with?(&1, [".db", "-wal", "-shm"]))
    )
  end

  # Holds the process that reaches `point` until the test opens the gate.
  defp hold(point, test) do
    gate = :atomics.new(1, [])

    CrashPoints.arm(
      point,
      fn _, meta ->
        send(test, {:held, point, self(), meta})
        eventually(fn -> :atomics.get(gate, 1) == 1 end)
      end,
      when: &match?(%{cell: @cell}, &1)
    )

    on_exit(fn -> :atomics.put(gate, 1, 1) end)
    gate
  end

  defp open(gate), do: :atomics.put(gate, 1, 1)

  defp count_hands(cell) do
    counter = :counters.new(1, [])

    CrashPoints.arm(:feed_before_publish, fn _, _ -> :counters.add(counter, 1, 1) end,
      times: :infinity,
      when: &match?(%{cell: ^cell}, &1)
    )

    counter
  end

  # Rows whose bytes are the ones the JSON of the feed must not touch.
  defp write_all(inst) do
    {:ok, _} =
      Alambique.transaction(inst, @cell, fn tx ->
        for {id, body} <- [{1, "a\xFFb"}, {2, "n\0l"}, {3, ~s(q"uo\\te/\t)}, {4, nil}, {5, ""}],
            do: {:ok, _} = Alambique.Tx.execute(tx, "INSERT INTO notes VALUES (?, ?)", [id, body])

        :ok
      end)

    insert(inst, @cell, 6..15)

    {:ok, _} =
      Alambique.transaction(inst, @cell, fn tx ->
        {:ok, _} = Alambique.Tx.execute(tx, "UPDATE notes SET body = 'u\xC3' WHERE id = 6", [])
        {:ok, _} = Alambique.Tx.execute(tx, "DELETE FROM notes WHERE id = 7", [])
        :ok
      end)

    {:ok, _} =
      Alambique.transaction(inst, @cell, fn tx ->
        {:ok, _} =
          Alambique.Tx.execute(tx, ~S|INSERT INTO "we""ird/t" VALUES (1, ?, 7)|, [
            {:blob, <<0, 255, 1>>}
          ])

        :ok
      end)
  end

  describe "the feed" do
    for rows <- [nil, 1, 3, 4], {batches, linger} <- [{1, 0}, {4, 5_000}] do
      test "is byte for byte that of :cell and :sender (hand_over_rows: #{inspect(rows)}, #{batches} to an object)",
           ctx do
        feed = [object_batches: unquote(batches), linger_ms: unquote(linger)]
        rows = [hand_over_rows: unquote(rows)]
        plain = start(ctx, "plain", feed, name: :plain)
        reader = start(ctx, "reader", sender(rows ++ feed), name: :reader)
        library = start(ctx, "library", native(rows ++ feed))

        for inst <- [plain, reader, library], do: write_all(inst)

        # 15 inserts, an update, a delete and the row of the odd table.
        eventually(fn ->
          Enum.all?(["plain", "reader", "library"], &(length(records(ctx, &1)) == 18))
        end)

        # The stored bytes, not the decoded records.
        assert bodies(ctx, "library") == bodies(ctx, "plain")
        assert bodies(ctx, "reader") == bodies(ctx, "plain")

        ids = fn prefix -> Enum.map(records(ctx, prefix), & &1.id) end
        assert ids.("library") == ids.("plain")
      end
    end

    test "a batch of ten rows in three pieces has the ids of the whole batch", ctx do
      inst = start(ctx, "pieces", native(hand_over_rows: 4), name: :pieces)
      hands = count_hands(@cell)
      insert(inst, @cell, 1..10)
      eventually(fn -> keys(records(ctx, "pieces")) == Enum.to_list(1..10) end)

      records = records(ctx, "pieces")
      txid = elem(hd(records).position, 0)
      assert Enum.map(records, & &1.position) == for(n <- 0..9, do: {txid, n})
      assert :counters.get(hands, 1) >= 3
      assert %State{feed: %{part: nil, backlog: 0}} = state(inst)
    end

    test "three batches acknowledged together with room for one: one is sent", ctx do
      inst = start(ctx, "room", native(max_backlog_batches: 1), name: :room)
      for id <- 1..3, do: insert(inst, @cell, [id])
      eventually(fn -> keys(records(ctx, "room")) == [1, 2, 3] end)

      # The rows are still in the file: hand them over from 0, as after a reset.
      {sent, state} =
        in_cell(pid(inst), fn st ->
          sent = Publisher.after_ack(%{st | feed: %{st.feed | sent: 0, published: 0, backlog: 0}})
          {sent, st}
        end)

      # Only the batch with room was sent: the library does not send the others.
      assert %{feed: %{backlog: 1, capped: true}} = sent
      assert sent.feed.sent < state.pipe.acked.txid
    end

    test "leaves no binary of the rows on the heap of the cell", ctx do
      inst = start(ctx, "heap", native(), name: :heap)

      {:ok, _} =
        Alambique.execute(inst, @cell, "INSERT INTO notes VALUES (1, ?)", [
          String.duplicate("x", 120_000)
        ])

      eventually(fn -> length(records(ctx, "heap")) == 1 end)

      # The rows are handed over again (the file still has them) by the cell
      # itself, and nothing the size of the text stays with it.
      grown =
        in_cell(pid(inst), fn st ->
          sizes = fn ->
            {:binary, list} = Process.info(self(), :binary)
            for {_id, size, _refs} <- list, do: size
          end

          before = sizes.()

          _ =
            Publisher.after_ack(%{
              st
              | feed: %{
                  st.feed
                  | sent: 0,
                    published: 0,
                    backlog: 0,
                    capped: false,
                    part: nil,
                    tick: nil
                }
            })

          sizes.() -- before
        end)

      assert Enum.all?(grown, &(&1 < 60_000)), "binaries on the cell: #{inspect(grown)}"
    end
  end

  describe "the sender" do
    test "ended in the middle of a batch gets the whole batch again", ctx do
      inst = start(ctx, "kill", native(hand_over_rows: 3), name: :kill)
      armed = :counters.new(1, [])

      # The sender ends, and is gone, before the third piece is read.
      CrashPoints.arm(
        :feed_before_publish,
        fn _, %{sender: sender} ->
          :counters.add(armed, 1, 1)

          if :counters.get(armed, 1) == 3 do
            Process.exit(sender, :kill)
            eventually(fn -> not Process.alive?(sender) end)
          end
        end,
        times: :infinity,
        when: &match?(%{cell: @cell}, &1)
      )

      insert(inst, @cell, 1..10)
      eventually(fn -> keys(records(ctx, "kill")) |> Enum.uniq() == Enum.to_list(1..10) end)

      records = records(ctx, "kill")
      assert keys(records) == Enum.to_list(1..10)

      assert Enum.map(records, & &1.position) ==
               for(n <- 0..9, do: {elem(hd(records).position, 0), n})

      assert %State{feed: %{part: nil, backlog: 0}} = state(inst)
    end

    test "gone when the library sends makes the cell start over, as if it ended later", ctx do
      inst = start(ctx, "gone", native(), name: :gone)
      insert(inst, @cell, [1])
      eventually(fn -> keys(records(ctx, "gone")) == [1] end)

      CrashPoints.arm(
        :feed_before_publish,
        fn _, %{sender: sender} ->
          Process.exit(sender, :kill)
          eventually(fn -> not Process.alive?(sender) end)
        end,
        when: &match?(%{cell: @cell}, &1)
      )

      # The rows are still in the file: the cell hands them over from 0.
      result =
        in_cell(pid(inst), fn st ->
          Publisher.after_ack(%{
            st
            | feed: %{
                st.feed
                | sent: 0,
                  published: 0,
                  backlog: 0,
                  capped: false,
                  part: nil,
                  tick: nil
              }
          })
        end)

      assert %State{feed: %{sent: 0, pid: nil, backlog: 0}} = result
      assert result.feed.tick != nil
    end

    test "that never gets the rows of a piece ends and the batches are handed over again",
         ctx do
      inst = start(ctx, "stale", native(), name: :stale)
      insert(inst, @cell, [1])
      eventually(fn -> keys(records(ctx, "stale")) == [1] end)
      %State{feed: %{pid: old}} = state(inst)

      # A piece whose rows were never sent: the sender gives it up.
      meta = %{txid: 99, ts: 1, rows: 1, partial: false, rewind: false}
      send(old, {:piece, make_ref(), [meta]})
      eventually(fn -> (s = state(inst)).feed.pid != old and s end)

      insert(inst, @cell, [2])
      eventually(fn -> keys(records(ctx, "stale")) == [1, 2] end)
    end

    test "the order of the hand-over holds with a piece per row and a flush", ctx do
      plain = start(ctx, "flush-plain", [], name: :flush_plain)
      inst = start(ctx, "flush", native(hand_over_rows: 1), name: :flush)
      insert(plain, @cell, 1..6)
      insert(inst, @cell, 1..6)

      assert :ok = Alambique.hibernate(inst, @cell)
      assert keys(records(ctx, "flush")) == [1, 2, 3, 4, 5, 6]

      eventually(fn -> keys(records(ctx, "flush-plain")) == [1, 2, 3, 4, 5, 6] end)
      assert bodies(ctx, "flush") == bodies(ctx, "flush-plain")
    end
  end

  describe "a rewind" do
    test "is published as the marker of :cell, between the records around it", ctx do
      plain = start(ctx, "rw-plain", [], name: :rw_plain)
      library = start(ctx, "rw-native", native(), name: :rw_native)

      for inst <- [plain, library] do
        insert(inst, @cell, [1])
        {:ok, %{txid: before}} = Alambique.position(inst, @cell)
        insert(inst, @cell, [2])
        {:ok, _} = Alambique.rewind(inst, @cell, {:txid, before})
        insert(inst, @cell, [3])
      end

      # The records, the marker and the record after it; the time of publication aside.
      items = fn prefix ->
        {:ok, items, _} =
          Feed.read(Feed.ctx(store: ctx.store, prefix: prefix, settle_ms: 0), nil, 1_000)

        for item <- items, item.cell == @cell, do: Map.delete(item, :at)
      end

      eventually(fn -> length(items.("rw-plain")) == 4 and length(items.("rw-native")) == 4 end)
      assert items.("rw-native") == items.("rw-plain")
      assert Enum.any?(items.("rw-native"), &match?(%Alambique.Feed.Rewind{}, &1))
    end
  end

  describe "a captured table name that is not UTF-8" do
    for mode <- [:cell, :native] do
      test "is handed over again and again as with hand_over: #{inspect(mode)}", ctx do
        inst = start(ctx, "bad-#{unquote(mode)}", hand_over: unquote(mode))
        # The hand-over loops: the library logs the refusal once, not every turn.
        log =
          capture_log(fn ->
            hands = count_hands(@cell)

            # A row of the feed as a trigger writes it, for a table that has such a name.
            {:ok, _} =
              Alambique.execute(
                inst,
                @cell,
                "INSERT INTO _alambique_feed (txid, ts, tbl, op, key) " <>
                  "VALUES (-2, 0, CAST(x'ff' AS TEXT), 'insert', '{}')"
              )

            eventually(fn -> :counters.get(hands, 1) >= 3 end)

            # The cell serves meanwhile, and nothing was published.
            assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, @cell, "SELECT 1")
            assert records(ctx, "bad-#{unquote(mode)}") == []
          end)

        if unquote(mode) == :native,
          do: assert(length(String.split(log, "that is not UTF-8")) == 2)
      end
    end
  end

  describe "reader_close" do
    test ":after_read hands over one range at a time and closes the reader after each", ctx do
      inst = start(ctx, "after-read", sender(reader_close: :after_read), name: :after_read)
      gate = hold(:feed_after_read, self())
      insert(inst, @cell, [1])
      assert_receive {:held, :feed_after_read, held, _}, 5_000

      # A second write while the first range is read: its range waits.
      insert(inst, @cell, [2])
      s = state(inst)
      assert s.feed.reading and s.feed.reader != nil
      assert s.feed.sent < s.pipe.acked.txid

      open(gate)
      eventually(fn -> keys(records(ctx, "after-read")) == [1, 2] end)

      eventually(fn ->
        (s = state(inst)).feed.reader == nil and not s.feed.reading and s.feed.backlog == 0
      end)

      # The reader was never closed under a read: the sender never ended.
      assert state(inst).feed.pid == held
      assert Process.alive?(held)
    end

    test ":after_burst keeps the reader while a later range was handed over", ctx do
      inst = start(ctx, "after-burst", sender(reader_close: :after_burst), name: :after_burst)
      assert %{reader_close: :after_burst} = Alambique.Config.fetch!(inst).feed
      insert(inst, @cell, [1])
      eventually(fn -> keys(records(ctx, "after-burst")) == [1] end)

      # Read up to 3 of 5 handed over: more is coming, so the reader stays.
      {kept, closed, gone} =
        in_cell(pid(inst), fn st ->
          {:ok, reader} = Alambique.Cell.Db.open_reader(st.path, 64)
          me = self()
          base = %{st | feed: %{st.feed | reader: reader, pid: me, sent: 5}}
          kept = Publisher.read_done(base, me, 3).feed.reader == reader
          closed = Publisher.read_done(base, me, 5).feed.reader == nil
          {kept, closed, Sqlite.run(reader, "SELECT 1", [])}
        end)

      assert kept and closed
      assert {:error, _} = gone
    end

    test "the default is one constant and reaches the instance", ctx do
      inst = start(ctx, "default", sender(), name: :default_close)
      assert %{reader_close: close} = Alambique.Config.fetch!(inst).feed
      assert close in [:after_read, :after_burst]
    end

    test "a reader that cannot be opened is tried again after a pause", ctx do
      inst = start(ctx, "no-reader", sender(), name: :no_reader)
      CrashPoints.arm(:db_open_reader, fn _, _ -> {:error, "injected"} end)

      log =
        capture_log(fn ->
          insert(inst, @cell, [1])
          eventually(fn -> state(inst).feed.tick != nil end)
          eventually(fn -> keys(records(ctx, "no-reader")) == [1] end)
        end)

      assert log =~ "could not open the feed reader"
      eventually(fn -> (s = state(inst)).feed.tick == nil and s.feed.reader == nil end)
    end
  end

  describe "the stop" do
    test "gives up on a sender in the middle of a read and closes the reader", ctx do
      inst = start(ctx, "give-up", sender(stop_publish_ms: 100), name: :give_up)
      insert(inst, @cell, [1])
      eventually(fn -> keys(records(ctx, "give-up")) == [1] end)
      eventually(fn -> state(inst).feed.reader == nil end)
      baseline = files(ctx)

      gate = hold(:feed_after_read, self())
      insert(inst, @cell, [2])
      assert_receive {:held, :feed_after_read, held, _}, 5_000
      assert length(files(ctx)) > length(baseline)

      # The flush waits for the sender that is held, gives up and kills it.
      assert :ok = Alambique.hibernate(inst, @cell)
      ref = Process.monitor(held)
      assert_receive {:DOWN, ^ref, _, _, _}, 5_000
      assert [] = files(ctx)
      open(gate)

      # The next owner publishes each row once.
      assert {:ok, _} = Alambique.query(inst, @cell, "SELECT 1")
      eventually(fn -> keys(records(ctx, "give-up")) |> Enum.uniq() == [1, 2] end)
      ids = Enum.map(records(ctx, "give-up"), & &1.id)
      assert ids == Enum.uniq(ids)
    end
  end

  describe "a WAL that restarts under an open reader" do
    test "does not lose or repeat a row", ctx do
      inst = start(ctx, "wal", sender(hand_over_rows: 1), name: :wal)
      gate = hold(:feed_after_read, self())
      insert(inst, @cell, 1..5)
      assert_receive {:held, :feed_after_read, _, _}, 5_000
      assert state(inst).feed.reader != nil

      [wal] = Path.wildcard(Path.join(ctx.dir, "**/*.db-wal"))
      before = salt(wal)

      # The reader is between two statements, so the checkpoint empties the WAL
      # and the next write restarts it with other salts.
      assert :ok = Alambique.checkpoint(inst, @cell)
      insert(inst, @cell, [6])
      assert salt(wal) != before, "the WAL did not restart"

      open(gate)
      eventually(fn -> keys(records(ctx, "wal")) == [1, 2, 3, 4, 5, 6] end)
    end

    # The two salts of the WAL header.
    defp salt(wal) do
      <<_::binary-size(16), salt::binary-size(8), _::binary>> = File.read!(wal)
      salt
    end
  end
end
