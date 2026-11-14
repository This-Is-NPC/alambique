defmodule Alambique.CompactorTest do
  @moduledoc "The background compactor of an instance (design §9) and the freeze hooks around it."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Compaction.Hold
  alias Alambique.CountingStore
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.CrashPoints

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @fast [
    interval_ms: 20,
    retry_ms: 50,
    fanout: 2,
    max_level: 2,
    snapshot_interval_txids: 1_000
  ]

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp level_files(ctx, cell, level) do
    prefix = Keys.files_prefix("cells-test", cell) <> "000#{level}/"
    {:ok, keys, nil} = AlambiqueTestkit.MemoryStore.list(ctx.store_config, prefix, nil, 1000)
    length(keys)
  end

  defp snapshot_count(ctx, cell) do
    {:ok, keys, nil} =
      AlambiqueTestkit.MemoryStore.list(
        ctx.store_config,
        Keys.files_prefix("cells-test", cell) <> "0009/",
        nil,
        1000
      )

    length(keys)
  end

  describe "background passes" do
    test "a written cell is compacted without being asked", ctx do
      inst = start_instance(ctx, migrations: @schema, compaction: @fast, retention: false)
      cell = unique_cell()
      for n <- 1..12, do: assert({:ok, _} = put(inst, cell, n))

      eventually(fn -> level_files(ctx, cell, 1) >= 6 and level_files(ctx, cell, 2) >= 3 end)
      assert :ok = Alambique.hibernate(inst, cell)

      rows = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")
      assert rows == Enum.map(1..12, &[&1])
    end

    test "a snapshot is taken and retention deletes what it covers", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.put(@fast, :snapshot_interval_txids, 6),
          retention: [pitr_window_ms: 0, grace_ms: 0],
          deletion_grace_ms: 0
        )

      cell = unique_cell()
      for n <- 1..12, do: assert({:ok, _} = put(inst, cell, n))

      eventually(fn -> snapshot_count(ctx, cell) >= 1 and level_files(ctx, cell, 0) < 8 end)
      assert :ok = Alambique.hibernate(inst, cell)
      eventually(fn -> snapshot_count(ctx, cell) >= 1 end)

      rows = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")
      assert rows == Enum.map(1..12, &[&1])
    end

    test "no more passes run at once than the concurrency", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          retention: false,
          compaction: [interval_ms: 600_000, max_age_ms: 1, fanout: 2, concurrency: 1]
        )

      test = self()

      CrashPoints.arm(
        :compact_between_merges,
        fn _point, %{cell: cell} ->
          unless Process.put(:gated, true) do
            send(test, {:gated, cell, self()})
            receive do: (:release -> :ok)
          end
        end,
        times: :infinity
      )

      cells = for i <- 1..3, do: "slot-#{i}"
      for cell <- cells, n <- 1..2, do: assert({:ok, _} = put(inst, cell, n))
      send(:ets.info(Alambique.Config.fetch!(inst).compactor, :owner), :tick)

      for _ <- cells do
        assert_receive {:gated, _cell, pid}, 5_000
        refute_receive {:gated, _, _}, 200
        send(pid, :release)
      end
    end

    test "every ready cell is compacted although commits never stop", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          retention: false,
          compaction: [
            interval_ms: 600_000,
            max_age_ms: 600_000,
            fanout: 2,
            concurrency: 2
          ]
        )

      config = Alambique.Config.fetch!(inst)
      cells = for _ <- 1..12, do: unique_cell()
      for cell <- cells, n <- 1..2, do: assert({:ok, _} = put(inst, cell, n))

      # Every pass is followed by new commits of its cell, so more cells are
      # ready than a tick takes, tick after tick, until `total` passes.
      total = 48
      passes = :ets.new(:passes, [:public, :set])
      me = self()
      id = {__MODULE__, make_ref()}

      :telemetry.attach(
        id,
        [:alambique, :compaction, :pass],
        fn _event, _measurements, %{cell: cell}, _ ->
          count = :ets.update_counter(passes, cell, 1, {cell, 0})

          if :ets.foldl(fn {_, n}, acc -> acc + n end, 0, passes) < total do
            Alambique.Compaction.Compactor.notify(config, cell, 1_000 * count + 1)
            Alambique.Compaction.Compactor.notify(config, cell, 1_000 * count + 2)
          else
            send(me, :done)
          end
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(id) end)

      send(:ets.info(config.compactor, :owner), :tick)
      assert_receive :done, 30_000
      # The stop of the instance compacts the cells it releases: not counted.
      :telemetry.detach(id)
      assert for(cell <- cells, :ets.lookup(passes, cell) == [], do: cell) == []
    end

    test "cells nobody writes cost nothing", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.merge(@fast, max_age_ms: 600_000, fanout: 8)
        )

      config = Alambique.Config.fetch!(inst)
      table = config.compactor
      processes = length(Process.list())

      # One write each: far below the fanout, so rows wait for max_age_ms.
      cells = for _ <- 1..5, do: unique_cell()
      for cell <- cells, do: assert({:ok, _} = put(inst, cell, 1))
      for cell <- cells, do: Alambique.hibernate(inst, cell)

      assert :ets.info(table, :size) == 5
      Process.sleep(100)
      assert :ets.info(table, :size) == 5
      assert length(Process.list()) <= processes
      assert Process.whereis(table) == nil
    end

    test "a row that waited long enough is looked at and leaves the table", ctx do
      inst =
        start_instance(ctx, migrations: @schema, compaction: Keyword.put(@fast, :max_age_ms, 30))

      table = Alambique.Config.fetch!(inst).compactor
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      eventually(fn -> :ets.info(table, :size) == 0 end)
      assert :ok = Alambique.hibernate(inst, cell)
    end

    test "the table is bounded", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.merge(@fast, queue_limit: 3, interval_ms: 600_000)
        )

      table = Alambique.Config.fetch!(inst).compactor
      for _ <- 1..10, do: assert({:ok, _} = put(inst, unique_cell(), 1))
      assert :ets.info(table, :size) == 3
    end

    test "compaction can be turned off", ctx do
      inst = start_instance(ctx, migrations: @schema, compaction: false)
      assert {:ok, _} = put(inst, unique_cell(), 1)
      assert Alambique.Config.fetch!(inst).compaction == nil
      assert :ets.whereis(Alambique.Config.fetch!(inst).compactor) == :undefined
    end

    @tag :capture_log
    test "a crashing pass does not stop the compactor", ctx do
      inst = start_instance(ctx, migrations: @schema, compaction: @fast, retention: false)
      cell = unique_cell()
      CrashPoints.arm(:compact_before_put, :raise, times: 1)
      for n <- 1..6, do: assert({:ok, _} = put(inst, cell, n))

      eventually(fn -> level_files(ctx, cell, 1) >= 2 end, 10_000)

      assert [{_, pid, _, _}] =
               inst
               |> Supervisor.which_children()
               |> Enum.filter(&match?({Compactor, _, _, _}, &1))

      assert Process.alive?(pid)
    end

    @tag :capture_log
    test "a pass that is killed is compacted again and holds no slot", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.put(@fast, :concurrency, 1),
          retention: false
        )

      cell = unique_cell()
      CrashPoints.arm(:compact_between_merges, :kill, times: 1)
      for n <- 1..6, do: assert({:ok, _} = put(inst, cell, n))

      # The slot of the killed pass is free again: the retry (and a pass of
      # another cell) runs, though `concurrency` is 1.
      eventually(fn -> level_files(ctx, cell, 1) >= 2 end, 10_000)
      other = unique_cell()
      for n <- 1..4, do: assert({:ok, _} = put(inst, other, n))
      eventually(fn -> level_files(ctx, other, 1) >= 1 end, 10_000)
    end

    @tag :capture_log
    test "a cleanup that fails is logged and asked again later", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.put(@fast, :retry_ms, 600_000)
        )

      config = Alambique.Config.fetch!(inst)
      cell = unique_cell()
      job = {cell, "nowhere"}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          Alambique.Compaction.Compactor.cleanup(config, cell, "nowhere", 0)
          [{_, asked_for}] = :ets.lookup(config.cleanups, job)
          # Taken, failed, and put back for the retry delay.
          eventually(fn ->
            match?([{_, due}] when due > asked_for + 300_000, :ets.lookup(config.cleanups, job))
          end)
        end)

      assert log =~ "cleanup of #{cell} at nowhere failed"
      assert log =~ "unknown_location"
    end

    @tag :capture_log
    test "a failed pass keeps the count of the TXIDs it did not compact", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.put(@fast, :retry_ms, 600_000),
          retention: false
        )

      table = Alambique.Config.fetch!(inst).compactor
      cell = unique_cell()
      CrashPoints.arm(:compact_before_put, :raise, times: 1)
      for n <- 1..6, do: assert({:ok, _} = put(inst, cell, n))

      # The row comes back after `retry_ms`, still counting from the first TXID.
      later = now_ms() + 300_000

      eventually(fn ->
        match?([{_, _, _, due}] when due > later, :ets.lookup(table, cell))
      end)

      assert [{_, 1, _last, _}] = :ets.lookup(table, cell)
    end

    test "a message it does not know changes nothing", ctx do
      inst = start_instance(ctx, migrations: @schema, compaction: @fast, retention: false)
      config = Alambique.Config.fetch!(inst)
      pid = :ets.info(config.compactor, :owner)
      send(pid, :stray)
      cell = unique_cell()
      for n <- 1..6, do: assert({:ok, _} = put(inst, cell, n))

      eventually(fn -> level_files(ctx, cell, 1) >= 2 end, 10_000)
      assert Process.alive?(pid)
    end

    test "a compactor that is not running is not an error for the cells" do
      config = %{
        compactor: :no_such_table,
        cleanups: :no_such_table,
        compaction: %{queue_limit: 10, max_age_ms: 1}
      }

      assert :ok = Alambique.Compaction.Compactor.notify(config, "c", 1)
      assert :ok = Alambique.Compaction.Compactor.cleanup(config, "c", "loc", 0)
    end

    test "emits a tick event when the passes of a busy period end", ctx do
      capture([[:alambique, :compactor, :tick]])
      inst = start_instance(ctx, migrations: @schema, compaction: @fast)
      for n <- 1..4, do: assert({:ok, _} = put(inst, unique_cell(), n))

      assert_receive {:telemetry, [:alambique, :compactor, :tick], %{duration: _, cells: cells},
                      %{instance: ^inst}},
                     5_000

      assert cells >= 1
    end
  end

  describe "two instances" do
    test "compactors of the old and the new owner keep one consistent chain", ctx do
      # A last commit left alone waits `max_age_ms` for its pass.
      compaction = Keyword.put(@fast, :max_age_ms, 100)
      opts = [migrations: @schema, compaction: compaction, retention: false]
      a = start_instance(ctx, [{:name, :compact_a} | opts])
      b = start_instance(ctx, [{:name, :compact_b} | opts])
      cell = unique_cell()

      # The cell moves between the instances while both compactors run.
      for round <- 0..7, {inst, offset} <- [{a, 0}, {b, 100}] do
        for n <- 1..4, do: assert({:ok, _} = put(inst, cell, offset + round * 10 + n))
        assert :ok = Alambique.hibernate(inst, cell)
      end

      expected = for round <- 0..7, offset <- [0, 100], n <- 1..4, do: [offset + round * 10 + n]

      for inst <- [a, b] do
        table = Alambique.Config.fetch!(inst).compactor
        eventually(fn -> :ets.info(table, :size) == 0 end, 10_000)
      end

      assert durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n") == Enum.sort(expected)

      # No two files of one level overlap, whoever wrote them.
      for level <- 1..2 do
        prefix = Keys.files_prefix("cells-test", cell) <> "000#{level}/"
        {:ok, keys, nil} = AlambiqueTestkit.MemoryStore.list(ctx.store_config, prefix, nil, 1000)

        ranges =
          for key <- keys do
            <<min::binary-size(16), "-", max::binary-size(16), ".ltx">> = Path.basename(key)
            {String.to_integer(min, 16), String.to_integer(max, 16)}
          end

        assert ranges == Enum.uniq(ranges)

        assert Enum.all?(ranges, fn {min, max} -> Enum.count(ranges, &(&1 == {min, max})) == 1 end)

        assert Enum.all?(ranges, fn {min, max} ->
                 Enum.all?(ranges, fn {min2, max2} ->
                   {min, max} == {min2, max2} or max < min2 or max2 < min
                 end)
               end)
      end
    end
  end

  describe "a zombie past the chain" do
    @tag :capture_log
    test "is ignored by the wake and overwritten by the new owner", ctx do
      inst = start_instance(ctx, migrations: @schema, compaction: @fast)

      cell = unique_cell()
      for n <- 1..3, do: assert({:ok, _} = put(inst, cell, n))
      assert :ok = Alambique.hibernate(inst, cell)

      # A write of epoch 0 right after the head: it can never be applied.
      config = Alambique.Config.fetch!(inst)
      {:ok, location} = Alambique.Placement.locate(config, cell)

      {:ok, %{txid: head}} =
        Alambique.Replication.head(Alambique.Config.repl_ctx(config, location), cell)

      key = Keys.segment("cells-test", cell, 0, head + 1, head + 1)

      {:ok, _} =
        AlambiqueTestkit.MemoryStore.put(ctx.store_config, key, "x", :if_none_match, %{
          "epoch" => "0"
        })

      assert {:ok, _} = put(inst, cell, 4)

      assert {:ok, %{rows: [[1], [2], [3], [4]]}} =
               Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")

      # Its TXID now holds the new owner's fence.
      assert {:ok, body, _, %{"epoch" => epoch}} =
               AlambiqueTestkit.MemoryStore.get(ctx.store_config, key)

      assert epoch != "0"
      assert body != "x"
    end
  end

  describe "held bytes" do
    @manual [
      interval_ms: 600_000,
      fanout: 2,
      max_level: 2,
      max_merges: 1_000,
      snapshot_interval_txids: 1_000
    ]

    # Writes `n` commits while the compactor sleeps, then runs one tick.
    # Returns the cell, the counts of the store requests of the pass and the
    # instance.
    defp hot_cell(ctx, compaction, n) do
      {_, counts} = counting = CountingStore.wrap(ctx.store)

      inst =
        start_instance(%{ctx | store: counting},
          migrations: @schema,
          compaction: Keyword.merge(@manual, compaction),
          retention: false
        )

      cell = unique_cell()
      for i <- 1..n, do: assert({:ok, _} = put(inst, cell, i))
      CountingStore.reset(counts)

      config = Alambique.Config.fetch!(inst)
      send(:ets.info(config.compactor, :owner), :tick)
      eventually(fn -> level_files(ctx, cell, 2) >= div(n, 4) end)
      {inst, cell, counts}
    end

    defp stored(ctx, cell, {level, min, max}) do
      key = Keys.segment("cells-test", cell, level, min, max)
      {:ok, body, _, _} = AlambiqueTestkit.MemoryStore.get(ctx.store_config, key)
      body
    end

    defp compaction_requests(counts, op),
      do: Enum.filter(CountingStore.keys(counts, op), &String.ends_with?(&1, ".ltx"))

    test "a hot cell compacts without reading its files", ctx do
      {inst, cell, counts} = hot_cell(ctx, [], 64)

      assert compaction_requests(counts, :get) == []
      assert compaction_requests(counts, :head) == []
      # One listing for the pass; the scan for pending cleanups (design §18) lists apart.
      assert Enum.reject(CountingStore.listed(counts), &String.ends_with?(&1, "/moved/"))
             |> length() == 1

      # What the pass wrote equals what a merge of the stored inputs makes.
      merges = [
        {{1, 1, 2}, [{0, 1, 1}, {0, 2, 2}]},
        {{1, 3, 4}, [{0, 3, 3}, {0, 4, 4}]},
        {{2, 1, 4}, [{1, 1, 2}, {1, 3, 4}]}
      ]

      for {output, inputs} <- merges do
        bodies = Enum.map(inputs, &stored(ctx, cell, &1))
        assert {:ok, merged} = Alambique.Replication.Native.merge_ltx(bodies)
        assert stored(ctx, cell, output) == merged
      end

      assert :ok = Alambique.hibernate(inst, cell)
      rows = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")
      assert rows == Enum.map(1..64, &[&1])
    end

    test "without held bytes the pass reads every input", ctx do
      {_inst, _cell, counts} = hot_cell(ctx, [hold_bytes: 0], 16)

      # 8 level 1 files from 16 level 0 ones, 4 level 2 from the 8.
      assert length(compaction_requests(counts, :get)) >= 16 + 8
    end

    test "a stopped cell leaves nothing held", ctx do
      {inst, cell, _counts} = hot_cell(ctx, [], 16)
      config = Alambique.Config.fetch!(inst)
      assert Hold.total(config) > 0

      assert :ok = Alambique.hibernate(inst, cell)
      eventually(fn -> Hold.total(config) == 0 end)

      assert :ets.select_count(config.holds, [{{{:_, :_, :_, :_}, :_, :_, :_, :_}, [], [true]}]) ==
               0
    end

    test "the budget bounds what is held, oldest first", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.merge(@manual, hold_bytes: 9_000),
          retention: false
        )

      config = Alambique.Config.fetch!(inst)
      cell = unique_cell()
      for i <- 1..24, do: assert({:ok, _} = put(inst, cell, i))

      assert Hold.total(config) in 1..9_000

      # A suffix of the commits: the oldest rows went first.
      held = :ets.select(config.holds, [{{{cell, 0, :"$1", :"$1"}, :_, :_, :_, :_}, [], [:"$1"]}])
      newest = Enum.max(held)
      assert Enum.sort(held) == Enum.to_list((newest - length(held) + 1)..newest)
      assert newest - length(held) >= 1
    end

    test "a segment over the object limit is not held", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.merge(@manual, hold_object_bytes: 1),
          retention: false
        )

      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      assert Hold.total(Alambique.Config.fetch!(inst)) == 0
    end

    @tag :capture_log
    test "a pass that crashes between merges is finished by the next one", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          compaction: Keyword.merge(@manual, retry_ms: 20),
          retention: false
        )

      cell = unique_cell()
      for i <- 1..16, do: assert({:ok, _} = put(inst, cell, i))
      test = self()

      CrashPoints.arm(:compact_between_merges, fn point, _ ->
        send(test, {:fired, point})
        raise CrashPoints.Crash, point: point
      end)

      owner = :ets.info(Alambique.Config.fetch!(inst).compactor, :owner)
      send(owner, :tick)
      assert_receive {:fired, :compact_between_merges}

      eventually(fn ->
        send(owner, :tick)
        level_files(ctx, cell, 2) >= 4
      end)

      assert :ok = Alambique.hibernate(inst, cell)
      rows = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")
      assert rows == Enum.map(1..16, &[&1])
    end

    @tag :capture_log
    test "a crash between the hold and the notification loses only the optimisation", ctx do
      {inst, cell, _counts} = hot_cell(ctx, [], 4)
      test = self()

      CrashPoints.arm(:hold_after_ack, fn point, _ ->
        send(test, {:fired, point})
        raise CrashPoints.Crash, point: point
      end)

      put(inst, cell, 100)
      assert_receive {:fired, :hold_after_ack}

      for n <- 5..12, do: eventually(fn -> match?({:ok, _}, put(inst, cell, n)) end)
      config = Alambique.Config.fetch!(inst)
      send(:ets.info(config.compactor, :owner), :tick)
      eventually(fn -> level_files(ctx, cell, 2) >= 2 end)

      assert :ok = Alambique.hibernate(inst, cell)
      rows = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")
      assert Enum.map(1..12, &[&1]) -- rows == []
    end
  end

  describe "options" do
    test "are validated" do
      base = [store: {AlambiqueTestkit.MemoryStore, %{server: nil}}, data_dir: "/tmp/x"]

      for bad <- [
            [compaction: [bogus: 1]],
            [compaction: [fanout: 1]],
            [compaction: [concurrency: 0]],
            [compaction: [hold_bytes: -1]],
            [compaction: [hold_object_bytes: 1.5]],
            [compaction: :yes],
            [retention: [pitr_window_ms: -1]],
            [retention: [bogus: 1]]
          ] do
        assert_raise ArgumentError, fn -> Alambique.Config.build(base ++ bad) end
      end

      assert %{compaction: nil, retention: nil} =
               Alambique.Config.build(base ++ [compaction: false, retention: false])

      assert %{compaction: %{hold_bytes: 0}} =
               Alambique.Config.build(base ++ [compaction: [hold_bytes: 0]])

      assert %{
               compaction: %{
                 fanout: 8,
                 concurrency: 2,
                 hold_bytes: 33_554_432,
                 hold_object_bytes: 1_048_576
               },
               retention: retention
             } =
               Alambique.Config.build(base)

      assert retention[:pitr_window_ms] == 7 * 86_400_000
    end
  end
end
