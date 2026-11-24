defmodule Alambique.FeedSenderReadTest do
  @moduledoc """
  `feed: [hand_over: :sender]` (design §13): the cell hands over TXIDs and the
  sender reads the rows through a read-only connection next to the cell's own.
  The feed it publishes is the one of `hand_over: :cell`, and the reader never
  outlives the connection of the cell.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Admission, Feed, Sqlite}
  alias Alambique.Cell.Db
  alias Alambique.Feed.Capture
  alias AlambiqueTestkit.{CrashPoints, GatedStore}

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @cell "reader-cell"

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

  # The database files of the test's cells that the process holds open.
  defp files(ctx) do
    Enum.filter(
      open_files(),
      &(String.starts_with?(&1, ctx.dir) and String.ends_with?(&1, [".db", "-wal", "-shm"]))
    )
  end

  defp connections, do: Alambique.Replication.Native.stats().connections_open

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

  describe "a second connection" do
    setup do
      dir = Alambique.TestDirs.new("reader")
      path = Path.join(dir, "c.db")
      {:ok, writer} = Db.open(path)
      :ok = Db.writable_script(writer, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
      {:ok, :ok} = Db.transaction(writer, 1, &Capture.install(&1.conn, Capture.everything()))
      {:ok, reader} = Db.open_reader(path, 64)

      on_exit(fn ->
        Sqlite.close(reader)
        Sqlite.close(writer)
      end)

      %{writer: writer, reader: reader}
    end

    defp count(conn), do: Sqlite.run(conn, "SELECT count(*) FROM t", [])

    test "sees what the writer committed and nothing it did not", %{writer: w, reader: r} do
      :ok = Db.writable_script(w, "INSERT INTO t VALUES (1, 'a')")
      assert {:ok, %{rows: [[1]]}} = count(r)

      :ok =
        Sqlite.execute(
          w,
          "PRAGMA query_only = OFF; BEGIN IMMEDIATE; INSERT INTO t VALUES (2, 'b')"
        )

      assert {:ok, %{rows: [[1]]}} = count(r)
      :ok = Sqlite.execute(w, "COMMIT; PRAGMA query_only = ON")
      assert {:ok, %{rows: [[2]]}} = count(r)
    end

    test "cannot write", %{reader: r} do
      assert {:error, "attempt to write a readonly database"} =
               Sqlite.run(r, "INSERT INTO t VALUES (9, 'z')", [])
    end

    test "reads rows only once the batch stamped them", %{writer: w, reader: r} do
      :ok = Db.writable_script(w, "INSERT INTO t VALUES (1, 'a'); INSERT INTO t VALUES (2, 'b')")
      assert {:ok, []} = Capture.rows_from(r, {1, -1}, 1_000, nil)

      :ok = Capture.stamp(w, 3, 1_000)

      assert {:ok, [[3, 1_000, "t", "insert" | _], [3, 1_000, "t", "insert" | _]]} =
               Capture.rows_from(r, {1, -1}, 1_000, nil)

      # A later batch is not read past `upto`.
      :ok = Db.writable_script(w, "INSERT INTO t VALUES (3, 'c')")
      :ok = Capture.stamp(w, 4, 1_001)
      assert {:ok, [_, _]} = Capture.rows_from(r, {1, -1}, 3, nil)
    end

    test "does not keep the checkpoint from finishing between statements", %{writer: w, reader: r} do
      :ok = Db.writable_script(w, "INSERT INTO t VALUES (1, 'a')")
      assert {:ok, _} = count(r)
      assert :ok = Sqlite.checkpoint(w)
    end
  end

  describe "the feed" do
    for rows <- [nil, 3], {batches, linger} <- [{1, 0}, {4, 5_000}] do
      test "is that of hand_over: :cell (hand_over_rows: #{inspect(rows)}, #{batches} to an object)",
           ctx do
        feed = [object_batches: unquote(batches), linger_ms: unquote(linger)]
        plain = start(ctx, "plain", feed, name: :plain)
        read = start(ctx, "read", sender([hand_over_rows: unquote(rows)] ++ feed))

        for inst <- [plain, read] do
          insert(inst, @cell, [1])
          insert(inst, @cell, 2..11)
          insert(inst, @cell, 12..13)
          insert(inst, @cell, 14..22)
        end

        expected = Enum.to_list(1..22)
        eventually(fn -> keys(records(ctx, "plain")) |> Enum.uniq() == expected end)
        eventually(fn -> keys(records(ctx, "read")) |> Enum.uniq() == expected end)

        assert Enum.map(records(ctx, "read"), & &1.id) == Enum.map(records(ctx, "plain"), & &1.id)
        assert bodies(ctx, "read") == bodies(ctx, "plain")
      end
    end

    test "counts TXIDs in the backlog, empty ones too, and goes on after them", ctx do
      # A cleanup of the cell is a batch without rows after the one it removes.
      inst = start(ctx, "tail", sender(cleanup: :after_publish), name: :tail)
      insert(inst, @cell, [1])

      s =
        eventually(fn ->
          s = state(inst)

          s.feed_cleaned > 0 and s.feed_backlog == 0 and s.feed_published == s.feed_sent and
            s.feed_sent == s.pipe.acked.txid and s
        end)

      # Batch 1 had the rows; the cleanup's batch had none and was reported all the same.
      assert s.pipe.acked.txid > 1
      assert s.feed_reader == nil
      insert(inst, @cell, [2])
      eventually(fn -> keys(records(ctx, "tail")) |> Enum.uniq() == [1, 2] end)
    end

    test "max_backlog_batches counts TXIDs", ctx do
      {:ok, gate} = GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)

      inst =
        start(
          %{ctx | store: GatedStore.wrap(gate, ctx.store)},
          "cap",
          sender(max_backlog_batches: 2),
          name: :cap
        )

      GatedStore.hold(gate, ~r{/feed/})
      for id <- 1..5, do: insert(inst, @cell, [id])

      assert %{feed_backlog: 2, feed_capped: true} = state(inst)
      GatedStore.open(gate)

      eventually(fn ->
        keys(records(%{ctx | store: ctx.store}, "cap")) |> Enum.uniq() == [1, 2, 3, 4, 5]
      end)

      eventually(fn -> state(inst).feed_backlog == 0 end)
    end

    test "the reader is open while the sender reads and closed after", ctx do
      inst = start(ctx, "life", sender(), name: :life)
      gate = hold(:feed_after_read, self())
      insert(inst, @cell, [1])
      assert_receive {:held, :feed_after_read, _sender, _}, 5_000
      assert %{feed_reader: reader} = state(inst)
      assert reader != nil
      open(gate)
      eventually(fn -> state(inst).feed_reader == nil end)
    end
  end

  describe "the stop" do
    test "closes the reader of a sender in the middle of a read, when no flush runs", ctx do
      inst = start(ctx, "stop", sender(stop_publish_ms: 0), name: :stop)
      insert(inst, @cell, [1])
      eventually(fn -> keys(records(ctx, "stop")) == [1] end)
      eventually(fn -> state(inst).feed_reader == nil end)
      baseline = files(ctx)

      gate = hold(:feed_after_read, self())
      insert(inst, @cell, [2])
      assert_receive {:held, :feed_after_read, held, _}, 5_000
      assert state(inst).feed_reader != nil
      assert length(files(ctx)) > length(baseline)

      assert :ok = Alambique.hibernate(inst, @cell)
      cell_files = Path.join(ctx.dir, "**/*")
      assert [] = files(ctx), "files left open of #{cell_files}: #{inspect(files(ctx))}"
      assert Path.wildcard(Path.join(ctx.dir, "**/*-wal")) == []

      open(gate)
      ref = Process.monitor(held)
      assert_receive {:DOWN, ^ref, _, _, _}, 5_000
      assert [] = files(ctx)
    end

    test "killing the sender or the cell leaves no file open", ctx do
      inst = start(ctx, "kills", sender(), name: :kills)
      gate = hold(:feed_after_read, self())
      insert(inst, @cell, [1])
      assert_receive {:held, :feed_after_read, held, _}, 5_000
      assert state(inst).feed_reader != nil

      Process.exit(held, :kill)
      eventually(fn -> (s = state(inst)).feed_reader == nil and s end)
      open(gate)
      eventually(fn -> keys(records(ctx, "kills")) == [1] end)

      gate = hold(:feed_after_read, self())
      insert(inst, @cell, [2])
      assert_receive {:held, :feed_after_read, held, _}, 5_000
      Process.exit(pid(inst), :kill)
      ref = Process.monitor(held)
      assert_receive {:DOWN, ^ref, _, _, _}, 5_000
      open(gate)
      eventually(fn -> files(ctx) == [] end)
    end

    test "hands over what is left and publishes it before the cell sleeps", ctx do
      inst = start(ctx, "flush", sender(), name: :flush)
      insert(inst, @cell, 1..5)
      assert :ok = Alambique.hibernate(inst, @cell)
      assert keys(records(ctx, "flush")) == [1, 2, 3, 4, 5]
      assert [] = files(ctx)
    end
  end

  describe "a checkpoint" do
    test "is not kept busy by a reader that is between two statements", ctx do
      inst = start(ctx, "idle-reader", sender(), name: :idle_reader)
      gate = hold(:feed_after_read, self())
      insert(inst, @cell, 1..3)
      assert_receive {:held, :feed_after_read, _, _}, 5_000
      assert state(inst).feed_reader != nil

      assert :ok = Alambique.checkpoint(inst, @cell)
      open(gate)
    end

    test "kept busy by a read does not make the store differ from the file", ctx do
      inst = start(ctx, "busy", sender(), name: :busy)
      test = self()
      release = :atomics.new(1, [])

      # The reader holds a read transaction, as a statement that never ends would.
      CrashPoints.arm(
        :feed_after_read,
        fn _, %{reader: reader} ->
          :ok = Sqlite.execute(reader, "BEGIN")
          {:ok, _} = Sqlite.run(reader, "SELECT count(*) FROM notes", [])
          send(test, :reading)
          eventually(fn -> :atomics.get(release, 1) == 1 end)
          Sqlite.execute(reader, "ROLLBACK")
        end,
        when: &match?(%{cell: @cell}, &1)
      )

      on_exit(fn -> :atomics.put(release, 1, 1) end)
      insert(inst, @cell, 1..3)
      assert_receive :reading, 5_000
      assert {:error, :busy} = Alambique.checkpoint(inst, @cell)

      :atomics.put(release, 1, 1)
      eventually(fn -> state(inst).feed_reader == nil end)

      # The WAL may restart under the position the replicator kept.
      for id <- 4..6, do: insert(inst, @cell, [id])
      expected = rows(inst)
      assert expected == Enum.to_list(1..6)
      assert :ok = Alambique.hibernate(inst, @cell)

      assert ctx
             |> durable_rows(@cell, "SELECT id FROM notes ORDER BY id", "busy")
             |> List.flatten() == expected

      assert rows(inst) == expected
    end

    defp rows(inst) do
      {:ok, %{rows: rows}} = Alambique.query(inst, @cell, "SELECT id FROM notes ORDER BY id")
      List.flatten(rows)
    end
  end

  describe "the memory budget" do
    test "includes the reader while it is open", ctx do
      inst =
        start(ctx, "mem", sender(reader_cache_kib: 64),
          awake: [max_bytes: 1_000_000_000],
          name: :mem
        )

      config = Alambique.Config.fetch!(inst)
      insert(inst, @cell, 1..5)
      eventually(fn -> state(inst).feed_reader == nil end)
      {:ok, _} = Alambique.query(inst, @cell, "SELECT 1")
      without = Admission.bytes(config)

      gate = hold(:feed_after_read, self())
      insert(inst, @cell, [6])
      assert_receive {:held, :feed_after_read, _, _}, 5_000
      s = state(inst)
      {:ok, _} = Alambique.query(inst, @cell, "SELECT 1")

      {:ok, m} = Sqlite.memory(s.feed_reader)
      reader = m.cache_used + m.schema_used + m.stmt_used
      assert reader > 0
      assert Admission.bytes(config) >= without + reader
      open(gate)
      eventually(fn -> state(inst).feed_reader == nil end)
      {:ok, _} = Alambique.query(inst, @cell, "SELECT 1")
      assert Admission.bytes(config) < without + reader
    end
  end

  describe "a wake" do
    # `wake.ex` closes the connection and restores again when a stale owner
    # took the TXID of the fence. The sender starts after that, so no reader
    # exists at that close.
    test "that retries after :txid_taken has no reader, only the connection of the cell", ctx do
      seeder =
        start_instance(ctx, migrations: @schema, prefix: "taken", feed: [window_ms: 60_000])

      insert(seeder, @cell, 1..3)
      :ok = Alambique.hibernate(seeder, @cell)
      stop_supervised!(seeder)

      inst = start(ctx, "taken", sender(), name: :taken)
      repl = repl_ctx(inst, @cell)
      test = self()
      calls = :counters.new(1, [])

      CrashPoints.arm(
        :repl_after_encode,
        fn _, %{cell: cell} ->
          :counters.add(calls, 1, 1)
          n = :counters.get(calls, 1)
          send(test, {:fence, n, files(ctx), self()})

          # A stale owner takes the TXID of the first fence.
          if n == 1 do
            dir = Alambique.TestDirs.new("taken")

            {:ok, result} =
              Alambique.Replication.restore(repl, cell, Path.join(dir, "r"), :latest)

            stale = Alambique.Replication.Replicator.from_restore(result, result.epoch + 1)
            {:ok, _} = Alambique.Replication.Replicator.fence(stale, repl)
          end
        end,
        times: :infinity,
        when: &match?(%{cell: @cell}, &1)
      )

      assert {:ok, _} = Alambique.query(inst, @cell, "SELECT count(*) FROM notes")
      assert_receive {:fence, 1, first, _}
      assert_receive {:fence, 2, retry, _}
      assert first == retry
      assert length(first) > 0
      assert length(Enum.filter(first, &String.ends_with?(&1, ".db"))) == 1

      eventually(fn -> keys(records(ctx, "taken")) |> Enum.uniq() == [1, 2, 3] end)
      eventually(fn -> state(inst).feed_reader == nil end)
    end
  end

  describe "a lazy restore" do
    test "stops with a reader blocked on a page that is not there yet", ctx do
      seeder =
        start_instance(ctx,
          migrations: @schema,
          prefix: "lazy",
          lazy_restore: false,
          feed: [cleanup: :in_write, window_ms: 60_000]
        )

      {:ok, _} =
        Alambique.transaction(seeder, @cell, fn tx ->
          for id <- 1..50,
              do:
                {:ok, _} =
                  Alambique.Tx.execute(tx, "INSERT INTO notes VALUES (?, randomblob(9000))", [id])

          :ok
        end)

      :ok = Alambique.hibernate(seeder, @cell)
      stop_supervised!(seeder)

      release = :atomics.new(1, [])
      on_exit(fn -> :atomics.put(release, 1, 1) end)
      test = self()

      # What the wake reads itself is fetched; from the hand-over on, the sender's
      # reads wait for their pages.
      armed = :atomics.new(1, [])

      CrashPoints.arm(:feed_before_publish, fn _, _ -> :atomics.put(armed, 1, 1) end,
        when: &match?(%{cell: @cell}, &1)
      )

      CrashPoints.arm(
        :hydrate_before_fetch,
        fn _, _ -> eventually(fn -> :atomics.get(release, 1) == 1 end, 30_000) end,
        times: :infinity,
        when: &match?(%{cell: @cell}, &1)
      )

      CrashPoints.arm(
        :hydrate_before_miss,
        fn _, _ ->
          if :atomics.get(armed, 1) == 1 do
            send(test, :blocked)
            eventually(fn -> :atomics.get(release, 1) == 1 end, 30_000)
          end
        end,
        times: :infinity,
        when: &match?(%{cell: @cell}, &1)
      )

      lazy = [
        min_bytes: 0,
        concurrency: 1,
        batch_bytes: 1_024,
        miss_pages: 1,
        verify_writes: false
      ]

      before = connections()

      inst =
        start(ctx, "lazy", sender(stop_publish_ms: 100), name: :lazy, lazy_restore: lazy)

      assert {:ok, _} = Alambique.query(inst, @cell, "SELECT 1")
      # The sender reads rows that are not on the disk yet.
      assert_receive :blocked, 10_000
      assert %{feed_reader: reader, hydrator: hydrator} = state(inst)
      assert reader != nil and hydrator != nil
      assert connections() == before + 2

      # The sender gives up and the cell closes the reader, which was waiting
      # for its page, before it closes its own connection; the discard of the
      # half-filled file then waits for the held fetch.
      stop = Task.async(fn -> Alambique.hibernate(inst, @cell) end)
      eventually(fn -> connections() == before end, 15_000)
      :atomics.put(release, 1, 1)
      assert :ok = Task.await(stop, 15_000)
      assert Alambique.owner(inst, @cell) == :none
    end
  end
end
