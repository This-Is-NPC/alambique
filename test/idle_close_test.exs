defmodule Alambique.IdleCloseTest do
  @moduledoc """
  An awake cell that stays idle for `awake.idle_close_after_ms` closes its
  SQLite connection and keeps everything else (design §7): the lease, the
  timers, the hold on the file. The next request that runs SQL opens the
  database again, after a check of what a foreign writer did meanwhile.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Admission, Config, Foreign, Sqlite}
  alias Alambique.Cell
  alias Alambique.Cell.{Db, Local, State}
  alias AlambiqueTestkit.{CrashPoints, GatedStore}

  @moduletag :capture_log

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @close 100

  setup do
    on_exit(&CrashPoints.reset/0)

    capture([
      [:alambique, :cell, :idle_close],
      [:alambique, :cell, :reopen],
      [:alambique, :cell, :hibernate],
      [:alambique, :cell, :wake]
    ])

    setup_store()
  end

  defp start(ctx, opts \\ []) do
    {awake, opts} = Keyword.pop(opts, :awake, [])

    start_instance(
      ctx,
      [
        migrations: @schema,
        feed: false,
        compaction: false,
        awake: [idle_close_after_ms: @close] ++ awake
      ] ++ opts
    )
  end

  defp insert(inst, cell, n), do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (#{n})")

  defp rows(inst, cell) do
    {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end

  defp pid(inst, cell), do: Cell.whereis(Config.fetch!(inst), cell)
  defp state(inst, cell), do: in_cell(pid(inst, cell), & &1)
  defp path(inst, cell), do: state(inst, cell).path
  defp closed?(inst, cell), do: state(inst, cell).conn == nil
  defp await_closed(inst, cell), do: eventually(fn -> closed?(inst, cell) end)

  defp wal(file) do
    case File.stat(file <> "-wal") do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end

  defp stored(ctx, cell),
    do: ctx |> durable_rows(cell, "SELECT n FROM t ORDER BY n") |> List.flatten()

  defp asleep?(inst, cell), do: eventually(fn -> Alambique.owner(inst, cell) == :none end)

  defp db_files(file),
    do: Enum.filter(open_files(), &(&1 in [file, file <> "-wal", file <> "-shm"]))

  describe "the close" do
    test "closes the connection, keeps the lease and shows it in the facts", ctx do
      inst = start(ctx, awake: [max_bytes: 1_000_000_000])
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      file = path(inst, cell)
      assert db_files(file) != []

      await_closed(inst, cell)
      s = state(inst, cell)
      assert s.mode == :serving
      assert State.lease_valid?(s)
      assert is_reference(s.kept)
      assert s.idle_close == nil
      assert db_files(file) == []
      assert wal(file) == 0

      assert {:ok, %{cache: :closed, cache_used: 0, schema_used: 0, stmt_used: 0}} =
               Alambique.facts(inst, cell)

      assert Alambique.owner(inst, cell) == {:ok, node()}
    end

    test "a call after the close opens the database again and sees the rows", ctx do
      inst = start(ctx)
      cell = unique_cell()
      for n <- 1..3, do: assert({:ok, _} = insert(inst, cell, n))
      await_closed(inst, cell)

      assert rows(inst, cell) == [1, 2, 3]
      s = state(inst, cell)
      assert s.conn != nil
      assert s.kept == nil
      assert is_reference(s.idle_close)
      assert {:ok, %{cache: :full}} = Alambique.facts(inst, cell)

      assert {_, %{result: :ok, wal_dirty?: false}} =
               await_event([:alambique, :cell, :reopen], cell)

      assert {:ok, _} = insert(inst, cell, 4)
      assert rows(inst, cell) == [1, 2, 3, 4]
      assert stored(ctx, cell) == [1, 2, 3, 4]
    end

    test "a request that needs no SQL leaves the database closed", ctx do
      inst = start(ctx)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      await_closed(inst, cell)

      assert {:ok, %{awake: true}} = Alambique.position(inst, cell)
      assert :ok = Alambique.checkpoint(inst, cell)
      assert closed?(inst, cell)
    end

    test "a cell with unreplicated frames in the WAL does not close", ctx do
      inst = start(ctx)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      assert 0 = Foreign.exec(path(inst, cell), "INSERT INTO t VALUES (9)")

      {m, _} =
        await_event([:alambique, :cell, :idle_close], cell, 5_000, &(&1.result == :unreplicated))

      assert m.bytes > 0
      refute closed?(inst, cell)
    end

    test "a cell with a call in flight does not close", ctx do
      {:ok, gate} = GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
      inst = start(%{ctx | store: GatedStore.wrap(gate, ctx.store)})
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)

      GatedStore.hold(gate, ~r{/0000/})
      write = Task.async(fn -> insert(inst, cell, 2) end)
      eventually(fn -> GatedStore.held(gate) != [] end)

      # The timer runs at least three times while the batch is in the air.
      for _ <- 1..3, do: assert(rearm?(inst, cell))
      refute closed?(inst, cell)

      GatedStore.open(gate)
      assert {:ok, _} = Task.await(write)
      await_closed(inst, cell)
      assert rows(inst, cell) == [1, 2]
    end

    # The close timer fires again after a call that did not close.
    defp rearm?(inst, cell) do
      before = state(inst, cell).idle_close
      eventually(fn -> state(inst, cell).idle_close != before end)
      true
    end

    test "a hibernation, a drain and a shutdown leave a valid marker and no WAL", ctx do
      inst = start(ctx)
      cell = unique_cell()
      for n <- 1..3, do: assert({:ok, _} = insert(inst, cell, n))
      file = path(inst, cell)
      await_closed(inst, cell)

      assert :ok = Alambique.hibernate(inst, cell)
      assert asleep?(inst, cell)
      assert %{txid: txid} = Local.read(file)
      assert txid > 0
      refute File.exists?(file <> "-wal")
      refute File.exists?(file <> "-shm")
      assert stored(ctx, cell) == [1, 2, 3]

      # The file is reused: the next wake is local and has the same rows.
      assert rows(inst, cell) == [1, 2, 3]
      assert {_, %{source: :local}} = await_wake_after_hibernate(cell)
    end

    defp await_wake_after_hibernate(cell) do
      drain([:alambique, :cell, :wake], cell) |> List.last()
    end

    test "the idle timeout hibernates a closed cell", ctx do
      inst = start(ctx, idle_timeout_ms: 1_500)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      file = path(inst, cell)
      await_closed(inst, cell)

      {_, %{reason: :idle}} = await_event([:alambique, :cell, :hibernate], cell)
      assert %{txid: _} = Local.read(file)
      refute File.exists?(file <> "-wal")
      assert rows(inst, cell) == [1]
    end
  end

  describe "the hold on the file" do
    test "stays while the database is closed, and goes with the cell", ctx do
      inst = start(ctx)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      file = path(inst, cell)
      await_closed(inst, cell)

      assert Db.open(file) == {:error, :held_by_cell}
      assert Sqlite.open(file) == {:error, :held_by_cell}
      assert Sqlite.open(file, vfs: :alambique) == {:error, :held_by_cell}

      # Reading is not refused, and the cell opens it again past its own hold.
      assert {:ok, ro} = Sqlite.open(file, readonly: true)
      Sqlite.close(ro)
      assert rows(inst, cell) == [1]
      assert Db.open(file) == {:error, :held_by_cell}

      await_closed(inst, cell)
      assert :ok = Alambique.hibernate(inst, cell)
      assert asleep?(inst, cell)
      assert {:ok, conn} = Db.open(file)
      Db.close(conn)
    end
  end

  describe "a foreign writer while the database is closed" do
    defp boot(ctx, mode) do
      inst = start(ctx, sql: [orphan_frames: mode])
      cell = unique_cell()
      for n <- [0, 1], do: assert({:ok, _} = insert(inst, cell, n))
      await_closed(inst, cell)
      %{inst: inst, cell: cell, file: path(inst, cell)}
    end

    test "frames are refused under :stop and the next wake has what the store held", ctx do
      c = boot(ctx, :stop)
      assert 0 = Foreign.exec(c.file, "INSERT INTO t VALUES (50)")
      assert wal(c.file) > 0

      assert Alambique.query(c.inst, c.cell, "SELECT n FROM t") == {:error, :orphan_frames}
      assert asleep?(c.inst, c.cell)
      assert stored(ctx, c.cell) == [0, 1]
      assert rows(c.inst, c.cell) == [0, 1]

      assert {_, %{result: :error, wal_dirty?: true}} =
               await_event([:alambique, :cell, :reopen], c.cell)
    end

    test "frames are replicated under :replicate", ctx do
      c = boot(ctx, :replicate)
      assert 0 = Foreign.exec(c.file, "INSERT INTO t VALUES (50)")

      assert rows(c.inst, c.cell) == [0, 1, 50]
      assert stored(ctx, c.cell) == [0, 1, 50]

      assert {_, %{result: :ok, wal_dirty?: true}} =
               await_event([:alambique, :cell, :reopen], c.cell)

      assert {:ok, _} = insert(c.inst, c.cell, 51)
      assert stored(ctx, c.cell) == [0, 1, 50, 51]
    end

    test "a rewritten database file stops the cell whatever orphan_frames says", ctx do
      c = boot(ctx, :replicate)
      File.write!(c.file, File.read!(c.file))
      File.touch!(c.file, {{2000, 1, 1}, {0, 0, 0}})

      assert Alambique.query(c.inst, c.cell, "SELECT n FROM t") == {:error, :orphan_frames}
      assert asleep?(c.inst, c.cell)
      assert rows(c.inst, c.cell) == [0, 1]
    end

    test "frames in the WAL with a changed database file are lost, not frames", ctx do
      c = boot(ctx, :replicate)
      assert 0 = Foreign.exec(c.file, "INSERT INTO t VALUES (50)")
      File.touch!(c.file, {{2000, 1, 1}, {0, 0, 0}})

      assert Alambique.query(c.inst, c.cell, "SELECT n FROM t") == {:error, :orphan_frames}
      assert asleep?(c.inst, c.cell)
      assert stored(ctx, c.cell) == [0, 1]
      assert rows(c.inst, c.cell) == [0, 1]
    end

    test "a rollback journal is lost", ctx do
      c = boot(ctx, :replicate)
      File.write!(c.file <> "-journal", "x")

      assert Alambique.query(c.inst, c.cell, "SELECT n FROM t") == {:error, :orphan_frames}
      assert asleep?(c.inst, c.cell)
    end

    test "frames found at a hibernation are judged first", ctx do
      c = boot(ctx, :replicate)
      assert 0 = Foreign.exec(c.file, "INSERT INTO t VALUES (50)")

      assert :ok = Alambique.hibernate(c.inst, c.cell)
      assert stored(ctx, c.cell) == [0, 1, 50]
      refute File.exists?(c.file <> "-wal")
      assert rows(c.inst, c.cell) == [0, 1, 50]
    end
  end

  describe "a failed reopen" do
    test "answers the caller, stops the cell and frees the hold", ctx do
      inst = start(ctx)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      file = path(inst, cell)
      await_closed(inst, cell)

      CrashPoints.arm(:cell_before_reopen, fn _, _ -> {:error, :boom} end)

      assert Alambique.query(inst, cell, "SELECT n FROM t") == {:error, {:reopen, :boom}}
      assert asleep?(inst, cell)
      assert {:ok, conn} = Db.open(file)
      Db.close(conn)
      assert rows(inst, cell) == [1]
    end
  end

  describe "the memory the admission budget counts" do
    test "is what a reopen rebuilds while closed, and the measured memory after", ctx do
      inst = start(ctx, awake: [max_bytes: 1_000_000_000])
      config = Config.fetch!(inst)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      assert rows(inst, cell) == [1]
      open = Admission.bytes(config)
      assert open > 0

      await_closed(inst, cell)
      floor = state(inst, cell).reopen_floor
      assert floor > 0
      assert Admission.bytes(config) == floor
      assert floor < open

      assert rows(inst, cell) == [1]
      assert state(inst, cell).reopen_floor == 0
      assert Admission.bytes(config) > floor
    end

    test "a cell that does not track its memory still closes and reopens", ctx do
      inst = start(ctx, awake: [track_bytes: false])
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      await_closed(inst, cell)
      assert {:ok, %{cache: :closed}} = Alambique.facts(inst, cell)
      assert rows(inst, cell) == [1]
    end
  end

  describe "the change feed" do
    @feed_schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

    defp feed_start(ctx, feed, close \\ @close) do
      start_instance(ctx,
        migrations: @feed_schema,
        compaction: false,
        feed: Keyword.merge([window_ms: 60_000, cleanup: :idle, cleanup_after_ms: 200], feed),
        awake: [idle_close_after_ms: close]
      )
    end

    defp feed_keys(ctx, cell) do
      ctx = Alambique.Feed.ctx(store: ctx.store, prefix: "cells-test", settle_ms: 0)
      {:ok, records, _} = Alambique.Feed.read(ctx, nil, 1_000)
      for r <- records, r.cell == cell, do: r.key["n"]
    end

    for mode <- [:cell, :sender] do
      test "hands over the rows of a write after a reopen (hand_over: #{mode})", ctx do
        inst = feed_start(ctx, hand_over: unquote(mode))
        cell = unique_cell()
        assert {:ok, _} = insert(inst, cell, 1)
        await_closed(inst, cell)
        assert state(inst, cell).feed_reader == nil
        assert eventually(fn -> feed_keys(ctx, cell) == [1] end)

        assert {:ok, _} = insert(inst, cell, 2)
        assert eventually(fn -> feed_keys(ctx, cell) == [1, 2] end)
        await_closed(inst, cell)
        assert state(inst, cell).feed_reader == nil
        assert rows(inst, cell) == [1, 2]
      end
    end

    test "a sender that dies while the cell is closed leaves the connection closed", ctx do
      inst = feed_start(ctx, hand_over: :cell)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      assert eventually(fn -> feed_keys(ctx, cell) == [1] end)
      await_closed(inst, cell)

      sender = state(inst, cell).feed_pid
      assert is_pid(sender)
      Process.exit(sender, :kill)
      eventually(fn -> state(inst, cell).feed_pid == nil end)
      assert closed?(inst, cell)

      assert {:ok, _} = insert(inst, cell, 2)
      assert eventually(fn -> feed_keys(ctx, cell) == [1, 2] end)
      assert rows(inst, cell) == [1, 2]
    end

    test "does not close while acknowledged rows were not handed over (hand_over: :cell)", ctx do
      inst = feed_start(ctx, [cleanup: :in_write, hand_over: :cell], 800)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      eventually(fn -> (s = state(inst, cell)).feed_published > 0 and s.pipe.queue == [] end)

      # As after an acknowledgement with no valid lease: the batch is
      # acknowledged and its rows are in the file, but were never handed over.
      :sys.replace_state(pid(inst, cell), fn s ->
        %{s | feed_sent: 0, feed_published: 0, feed_backlog: 0}
      end)

      for _ <- 1..3, do: assert(rearm?(inst, cell))
      refute closed?(inst, cell)
    end

    test "closes after a cleanup batch that has no rows (hand_over: :cell)", ctx do
      inst = feed_start(ctx, hand_over: :cell)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      await_closed(inst, cell)

      # The cleanup batch is acknowledged and carries no feed rows, so the
      # newest TXID published stays behind it.
      s = state(inst, cell)
      assert s.pipe.acked.txid > s.feed_published
      assert s.feed_cleaned == s.feed_published
    end

    test "reads the feed rows with the rules of a gated connection", ctx do
      inst = feed_start(ctx, [cleanup: :in_write, hand_over: :cell], 30_000)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      eventually(fn -> state(inst, cell).feed_published > 0 end)

      # A plain read, allowed unarmed, by the process that runs the cell's
      # statements; any other process is refused.
      query = fn s -> Alambique.Feed.Capture.rows_from(s.conn, {1, -1}, 100, 10) end
      assert {:ok, [[_txid | _] | _]} = in_cell(pid(inst, cell), query)
      conn = state(inst, cell).conn
      assert Alambique.Feed.Capture.rows_from(conn, {1, -1}, 100, 10) == {:error, :not_owner}
    end

    test "waits for the rows of a published batch to be cleaned, in a batch that lands first",
         ctx do
      {:ok, gate} = GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
      inst = feed_start(%{ctx | store: GatedStore.wrap(gate, ctx.store)}, [], 800)
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      eventually(fn -> (s = state(inst, cell)).feed_published > 0 and s.pipe.queue == [] end)

      # The cleanup batch is the next segment: held, it is in flight when the
      # timer looks again.
      GatedStore.hold(gate, ~r{/0000/})
      eventually(fn -> GatedStore.held(gate) != [] end)
      for _ <- 1..3, do: assert(rearm?(inst, cell))
      s = state(inst, cell)
      assert s.conn != nil
      assert s.pipe.queue != []

      GatedStore.open(gate)
      await_closed(inst, cell)
      s = state(inst, cell)
      assert s.feed_cleaned == s.feed_published
      assert rows(inst, cell) == [1]
    end

    test "does not close while published rows wait in the backlog, whoever kills the sender",
         ctx do
      {:ok, gate} = GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)

      inst = feed_start(%{ctx | store: GatedStore.wrap(gate, ctx.store)}, hand_over: :cell)

      cell = unique_cell()
      GatedStore.hold(gate, ~r{/feed/})
      assert {:ok, _} = insert(inst, cell, 1)
      eventually(fn -> GatedStore.held(gate) != [] end)
      eventually(fn -> (s = state(inst, cell)).pipe.queue == [] and s.feed_backlog > 0 end)

      sender = state(inst, cell).feed_pid
      Process.exit(sender, :kill)
      for _ <- 1..3, do: assert(rearm?(inst, cell))
      refute closed?(inst, cell)

      GatedStore.open(gate)
      assert eventually(fn -> feed_keys(ctx, cell) == [1] end)
      await_closed(inst, cell)
      assert feed_keys(ctx, cell) == [1]
    end
  end
end
