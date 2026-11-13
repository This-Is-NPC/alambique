defmodule Alambique.ReplicationTest do
  @moduledoc "Replicate and restore (design §6): latest, by TXID, by timestamp, checkpoints, snapshots."
  use ExUnit.Case, async: false

  import Alambique.ReplicationHelpers
  import Alambique.CellHelpers, only: [eventually: 1]

  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Native, Replicator}
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  setup do
    {pid, store} = start_store()
    dir = tmp_dir()
    on_exit(fn -> File.rm_rf(dir) end)
    on_exit(&CrashPoints.reset/0)
    world = new_world()
    {:ok, store: store, pid: pid, dir: dir, world: world, ctx: ctx(store, world)}
  end

  defp commit_and_sync(conn, state, ctx, ops) do
    Enum.each(ops, &safe_op(conn, &1))
    assert {:ok, state} = Replicator.sync(state, ctx)
    state
  end

  # A replicated commit, then one that is not, on a connection nothing keeps
  # afterwards. Returns the state and the open connections before the drop.
  defp commit_then_drop(dir, ctx, name) do
    path = Path.join(dir, name <> ".db")
    conn = open_db(path)
    state = commit_and_sync(conn, Replicator.new("cell", path, 4096, 1), ctx, [{:put, 1, 4000}])
    open = Alambique.NativeStats.read().connections_open
    safe_op(conn, {:put, 2, 4000})
    {state, open}
  end

  defp files(state), do: Enum.map(["", "-wal"], &File.read!(state.db_path <> &1))

  # Waits until a connection counted in `open` has been closed by its destructor.
  defp collect(open) do
    eventually(fn ->
      :erlang.garbage_collect()
      Alambique.NativeStats.read().connections_open < open
    end)
  end

  defp restore!(ctx, dir, target, name \\ "restored") do
    path = Path.join(dir, name)
    assert {:ok, result} = Replication.restore(ctx, "cell", path, target)
    {path, result}
  end

  test "an empty store restores to an empty database at TXID 0", %{ctx: ctx, dir: dir} do
    {path, result} = restore!(ctx, dir, :latest)
    assert result.txid == 0
    assert File.read!(path) == ""

    assert {:error, {:target_not_reached, 0}} =
             Replication.restore(ctx, "cell", Path.join(dir, "x"), {:txid, 3})
  end

  test "head reports the newest file of the chain, or nil for an empty store", %{
    ctx: ctx,
    dir: dir
  } do
    assert {:ok, nil} = Replication.head(ctx, "cell")

    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])
    state = commit_and_sync(conn, state, ctx, [{:put, 2, 100}])

    assert {:ok, head} = Replication.head(ctx, "cell")

    assert head == %{
             txid: 2,
             checksum: state.checksum,
             epoch: 1,
             data: 2,
             page_size: 4096,
             commit: 2
           }
  end

  test "the first sync uploads the whole database as TXID 1", %{
    ctx: ctx,
    dir: dir,
    store: {mod, cfg}
  } do
    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 100}, {:put, 2, 5000}])
    assert state.txid == 1
    key = Keys.segment(ctx.prefix, "cell", 0, 1, 1)
    assert {:ok, body, _, %{"epoch" => "1", "node" => "node-a"}} = mod.get(cfg, key)
    assert {:ok, {1, 1, _, _, 0, 4096}, _} = Native.ltx_info(body, true)
  end

  test "a sync with nothing new does nothing", %{ctx: ctx, dir: dir, store: store} do
    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 10}])
    assert {:ok, ^state} = Replicator.sync(state, ctx)
    assert MemoryStore.count(elem(store, 1)) == 1
  end

  test "restores the latest state after many batches", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db")

    state =
      Enum.reduce(1..12, state, fn i, st ->
        commit_and_sync(conn, st, ctx, [
          {:put, i, i * 400},
          {:txn, [{:put, 100 + i, 9000}, {:del, i - 1}]}
        ])
      end)

    {path, result} = restore!(ctx, dir, :latest)
    assert result.txid == state.txid
    assert result.checksum == state.checksum
    assert result.epoch == 1
    assert dump_file(path) == dump(conn)
  end

  test "restored file is byte-identical to a checkpointed source", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 3000}, {:put, 2, 3000}, {:fill, 1}])
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 10}, {:del, 2}])
    assert {:ok, _state} = Replicator.checkpoint(state, conn)
    {path, _} = restore!(ctx, dir, :latest)
    assert File.read!(path) == File.read!(Path.join(dir, "src.db"))
  end

  test "restore by TXID returns every intermediate state", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db")

    {_, dumps} =
      Enum.reduce(1..8, {state, %{}}, fn i, {st, dumps} ->
        st =
          commit_and_sync(conn, st, ctx, [
            {:put, i, 200 * i},
            {:txn, [{:del, i - 2}, {:put, 50, 700}]}
          ])

        {st, Map.put(dumps, st.txid, dump(conn))}
      end)

    for {txid, expected} <- dumps do
      {path, result} = restore!(ctx, dir, {:txid, txid}, "at-#{txid}")
      assert result.txid == txid
      assert dump_file(path) == expected
    end

    assert {:error, {:target_not_reached, 8}} =
             Replication.restore(ctx, "cell", Path.join(dir, "late"), {:txid, 9})

    refute File.exists?(Path.join(dir, "late"))
  end

  test "restore by timestamp stops at the last batch at or before it", %{
    ctx: ctx,
    dir: dir,
    world: world
  } do
    {conn, state} = new_replicator(dir, "src.db")

    {_, points} =
      Enum.reduce(1..6, {state, []}, fn i, {st, acc} ->
        ManualClock.advance(world, 1000)
        st = commit_and_sync(conn, st, ctx, [{:put, i, 1500}])
        {st, [{ManualClock.world_ms(world), st.txid, dump(conn)} | acc]}
      end)

    for {ts, txid, expected} <- points do
      for target <- [ts, ts + 999] do
        {path, result} = restore!(ctx, dir, {:timestamp, target}, "ts-#{target}")
        assert result.txid == txid
        assert result.timestamp == ts
        assert dump_file(path) == expected
      end
    end

    {first_ts, _, _} = List.last(points)
    # Before the first batch the database did not exist yet: empty, TXID 0.
    assert {:ok, %{txid: 0}} =
             Replication.restore(ctx, "cell", Path.join(dir, "early"), {:timestamp, first_ts - 1})

    {last_ts, last_txid, _} = hd(points)

    assert {:ok, %{txid: ^last_txid}} =
             Replication.restore(
               ctx,
               "cell",
               Path.join(dir, "far"),
               {:timestamp, last_ts + 10_000_000}
             )
  end

  test "checkpoints restart the WAL and replication keeps working", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 5000}, {:put, 2, 100}])

    state =
      Enum.reduce(1..6, state, fn i, st ->
        st = commit_and_sync(conn, st, ctx, [{:put, i, 300 * i}, {:put, 9, 8000}])
        assert {:ok, st} = Replicator.checkpoint(st, conn)
        assert st.wal_pos == {0, 0, 0, 0, 0}
        commit_and_sync(conn, st, ctx, [{:del, i}, {:put, 20 + i, 1000}])
      end)

    {path, result} = restore!(ctx, dir, :latest)
    assert result.txid == state.txid
    assert dump_file(path) == dump(conn)
  end

  test "checkpoint refuses while committed frames are not replicated", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])
    safe_op(conn, {:put, 2, 100})
    assert {:error, :unreplicated_frames} = Replicator.checkpoint(state, conn)
    state = commit_and_sync(conn, state, ctx, [])
    assert {:ok, _} = Replicator.checkpoint(state, conn)
  end

  test "shrinking the database by VACUUM is replicated", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, Enum.map(1..40, &{:put, &1, 4000}))
    big = state.commit
    state = commit_and_sync(conn, state, ctx, Enum.map(1..39, &{:del, &1}) ++ [:vacuum])
    assert state.commit < big
    {path, _} = restore!(ctx, dir, :latest)
    assert dump_file(path) == dump(conn)
  end

  test "other page sizes work", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db", 1, 1024)
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 3000}, {:put, 2, 10}])
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])
    {path, result} = restore!(ctx, dir, :latest)
    assert result.page_size == 1024
    assert dump_file(path) == dump(conn)
    assert result.txid == state.txid
  end

  test "an existing database with data starts with a full snapshot", %{ctx: ctx, dir: dir} do
    path = Path.join(dir, "old.db")
    conn = open_db(path)
    run_ops(conn, [{:put, 1, 100}, {:put, 2, 9000}])
    assert :ok = Alambique.Sqlite.checkpoint(conn)
    run_ops(conn, [{:put, 3, 100}])
    state = Replicator.new("cell", path, 4096, 1)
    assert {:ok, state} = Replicator.sync(state, ctx)
    assert state.txid == 1
    {restored, _} = restore!(ctx, dir, :latest)
    assert dump_file(restored) == dump(conn)
  end

  describe "snapshots" do
    test "a snapshot shortens the chain and old targets still work", %{
      ctx: ctx,
      dir: dir,
      store: {mod, cfg}
    } do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}, {:put, 2, 4000}])
      state = commit_and_sync(conn, state, ctx, [{:put, 3, 4000}])
      d2 = dump(conn)
      state = commit_and_sync(conn, state, ctx, [{:del, 1}])
      d3 = dump(conn)
      assert {:ok, key} = Replicator.snapshot(state, ctx)
      assert key == Keys.snapshot(ctx.prefix, "cell", 3)
      assert {:ok, _, _, %{"epoch" => "1"}} = mod.get(cfg, key)
      state = commit_and_sync(conn, state, ctx, [{:put, 4, 100}])

      {path, result} = restore!(ctx, dir, :latest)
      assert result.txid == state.txid and result.segments == 2
      assert dump_file(path) == dump(conn)

      {path, result} = restore!(ctx, dir, {:txid, 1}, "old")
      assert result.txid == 1
      assert dump_file(path) |> elem(1) |> Map.get("kv") |> length() == 2

      {path, _} = restore!(ctx, dir, {:txid, 3}, "at3")
      assert dump_file(path) == d3

      {path, _} = restore!(ctx, dir, {:txid, 2}, "at2")
      assert dump_file(path) == d2
    end

    test "a snapshot sees replicated WAL frames that were not checkpointed", %{
      ctx: ctx,
      dir: dir,
      store: {mod, cfg}
    } do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}])
      expected = dump(conn)
      # Unreplicated commit after the state: must not leak into the snapshot.
      safe_op(conn, {:put, 2, 4000})
      assert {:ok, key} = Replicator.snapshot(state, ctx)
      assert {:ok, body, _, _} = mod.get(cfg, key)
      path = Path.join(dir, "from-snapshot")
      {:ok, _, post} = Native.apply_ltx(path, body, 0, 0)
      assert post == state.checksum
      assert dump_file(path) == expected
    end

    test "a snapshot refuses a WAL checkpointed outside checkpoint/2", %{ctx: ctx, dir: dir} do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}])
      safe_op(conn, {:put, 2, 4000})
      # Moves the unreplicated commit into the database file.
      assert :ok = Alambique.Sqlite.checkpoint(conn)
      assert {:error, :snapshot_checksum_mismatch} = Replicator.snapshot(state, ctx)
    end

    test "a connection closed by garbage collection or a crash leaves the files as they were",
         %{ctx: ctx, dir: dir} do
      ctx_a = %{ctx | prefix: "collected"}
      {state, open} = commit_then_drop(dir, ctx_a, "collected")
      before = files(state)
      collect(open)
      assert files(state) == before
      assert {:ok, _key} = Replicator.snapshot(state, ctx_a)

      ctx_b = %{ctx | prefix: "crashed"}
      test = self()

      owner =
        spawn(fn ->
          send(test, commit_then_drop(dir, ctx_b, "crashed"))
          Process.sleep(:infinity)
        end)

      assert_receive {state, open}, 5_000
      before = files(state)
      Process.exit(owner, :kill)
      collect(open)
      assert files(state) == before
      assert {:ok, _key} = Replicator.snapshot(state, ctx_b)
    end

    test "restore by timestamp does not use a snapshot newer than the target", %{
      ctx: ctx,
      dir: dir,
      world: world
    } do
      {conn, state} = new_replicator(dir, "src.db")
      ManualClock.advance(world, 1000)
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])
      ts1 = ManualClock.world_ms(world)
      d1 = dump(conn)
      ManualClock.advance(world, 1000)
      state = commit_and_sync(conn, state, ctx, [{:put, 2, 100}])
      ManualClock.advance(world, 1000)
      assert {:ok, _} = Replicator.snapshot(state, ctx)
      {path, result} = restore!(ctx, dir, {:timestamp, ts1})
      assert result.txid == 1
      assert dump_file(path) == d1
    end

    test "snapshot of a state with nothing replicated is refused", %{ctx: ctx, dir: dir} do
      {_conn, state} = new_replicator(dir, "src.db")
      assert {:error, :nothing_replicated} = Replicator.snapshot(state, ctx)
    end
  end

  describe "merged segments (compaction input, design §9)" do
    test "a merged level-1 segment restores the same state", %{
      ctx: ctx,
      dir: dir,
      store: {mod, cfg}
    } do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}, {:put, 2, 100}])

      state =
        Enum.reduce(2..5, state, fn i, st ->
          commit_and_sync(conn, st, ctx, [{:put, i, 700 * i}, {:del, i - 1}])
        end)

      bodies = for t <- 1..5, do: elem(mod.get(cfg, Keys.segment(ctx.prefix, "cell", 0, t, t)), 1)
      {:ok, merged} = Native.merge_ltx(Enum.slice(bodies, 1, 3))
      {:ok, {2, 4, _, _, _, _}, _} = Native.ltx_info(merged, true)

      for t <- 2..4, do: mod.delete(cfg, Keys.segment(ctx.prefix, "cell", 0, t, t))

      {:ok, _} =
        mod.put(cfg, Keys.segment(ctx.prefix, "cell", 1, 2, 4), merged, :if_none_match, %{
          "epoch" => "1"
        })

      {path, result} = restore!(ctx, dir, :latest)
      assert result.txid == state.txid
      assert result.segments == 3
      assert dump_file(path) == dump(conn)

      # A target inside the merged range cannot be reached from level 1, only from level 0.
      assert {:error, {:target_not_reached, 1}} =
               Replication.restore(ctx, "cell", Path.join(dir, "mid"), {:txid, 3})
    end
  end

  describe "restore is atomic" do
    test "a crash mid-restore leaves the destination untouched", %{ctx: ctx, dir: dir} do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])
      _ = commit_and_sync(conn, state, ctx, [{:put, 2, 100}])
      dest = Path.join(dir, "dest.db")
      File.write!(dest, "old contents")
      File.write!(dest <> "-wal", "old wal")

      CrashPoints.arm(:repl_mid_restore, :raise)
      assert_raise CrashPoints.Crash, fn -> Replication.restore(ctx, "cell", dest, :latest) end
      assert File.read!(dest) == "old contents"
      assert File.exists?(dest <> "-wal")

      assert {:ok, %{txid: 2}} = Replication.restore(ctx, "cell", dest, :latest)
      refute File.exists?(dest <> "-wal")
      assert dump_file(dest) == dump(conn)
    end

    test "a failing store gives an error and leaves the destination", %{
      ctx: ctx,
      dir: dir,
      store: store
    } do
      {conn, state} = new_replicator(dir, "src.db")
      _ = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])
      MemoryStore.inject(elem(store, 1), kind: :error, ops: [:get, :get_range])
      dest = Path.join(dir, "dest.db")
      assert {:error, :injected} = Replication.restore(ctx, "cell", dest, :latest)
      refute File.exists?(dest)
      assert Path.wildcard(dest <> "*") == []
    end
  end

  test "continuing from a restore with a new epoch", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db")
    state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}])
    _ = commit_and_sync(conn, state, ctx, [{:put, 2, 100}])
    Alambique.Sqlite.close(conn)

    path = Path.join(dir, "node-b.db")
    assert {:ok, result} = Replication.restore(ctx, "cell", path, :latest)
    state = Replicator.from_restore(result, 2)
    assert state.txid == 2 and state.checksum == result.checksum
    assert {:ok, state} = Replicator.fence(state, ctx)
    assert state.txid == 3

    conn = open_db(path)
    state = commit_and_sync(conn, state, ctx, [{:put, 3, 6000}, {:del, 1}])
    assert {:ok, state} = Replicator.checkpoint(state, conn)
    state = commit_and_sync(conn, state, ctx, [{:put, 4, 10}])
    assert state.txid == 5

    {restored, result} = restore!(ctx, dir, :latest)
    assert result.epoch == 2
    assert dump_file(restored) == dump(conn)
  end

  describe "an upload that fails" do
    test "a sync reports it and the same state can sync again", %{ctx: ctx, dir: dir} = t do
      {conn, state} = new_replicator(dir, "src.db")
      Enum.each([{:put, 1, 4000}], &safe_op(conn, &1))
      {mod, cfg} = t.store

      fault = MemoryStore.inject(cfg, kind: :error, ops: [:put], key: ~r/\.ltx$/)
      assert {:error, :injected} = Replicator.sync(state, ctx)
      assert {:ok, nil} = Replication.head(ctx, "cell")

      MemoryStore.remove(cfg, fault)
      assert {:ok, next} = Replicator.sync(state, ctx)
      assert next.txid == 1
      assert {:ok, _, _, _} = mod.get(cfg, Keys.segment(ctx.prefix, "cell", 0, 1, 1))
      {path, _} = restore!(ctx, dir, :latest)
      assert dump_file(path) == dump(conn)
    end

    test "a snapshot reports it", %{ctx: ctx, dir: dir, store: {_, cfg}} do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}])

      fault = MemoryStore.inject(cfg, kind: :error, ops: [:put], key: ~r/\.ltx$/)
      assert {:error, :injected} = Replicator.snapshot(state, ctx)
      MemoryStore.remove(cfg, fault)
      assert {:ok, _key} = Replicator.snapshot(state, ctx)
    end

    test "a snapshot of a state that does not match its file is refused", %{ctx: ctx, dir: dir} do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}])

      assert {:error, :snapshot_checksum_mismatch} =
               Replicator.snapshot(%{state | checksum: state.checksum + 1}, ctx)

      assert {:error, :nothing_replicated} = Replicator.snapshot(%{state | txid: 0}, ctx)
    end
  end

  describe "replace" do
    setup %{ctx: ctx, dir: dir} do
      {conn, state} = new_replicator(dir, "src.db")
      state = commit_and_sync(conn, state, ctx, [{:put, 1, 4000}])
      {:ok, head} = Replication.head(ctx, "cell")
      {:ok, conn: conn, state: state, head: head}
    end

    test "a file with another page count is refused before anything is sent", t do
      %{ctx: ctx, state: state, head: head, store: {_, cfg}} = t
      count = MemoryStore.count(cfg)

      assert {:error, {:capture, {:page_count, found, 0}}} =
               Replicator.replace(ctx, "cell", state.db_path, 4096, head, 1, pages: 0)

      assert found > 0
      assert MemoryStore.count(cfg) == count
    end

    test "a chain that moved is reported as taken, not overwritten", t do
      %{ctx: ctx, conn: conn, state: state, head: head, store: {mod, cfg}} = t
      commit_and_sync(conn, state, ctx, [{:put, 2, 100}])
      {:ok, found, _, _} = mod.get(cfg, Keys.segment(ctx.prefix, "cell", 0, 2, 2))
      {:ok, now} = Replication.head(ctx, "cell")

      # `head` is the chain as it was read: TXID 2 was written since.
      assert {:error, :txid_taken} =
               Replicator.replace(ctx, "cell", state.db_path, 4096, head, 1, pages: now.commit)

      assert {:ok, ^found, _, _} = mod.get(cfg, Keys.segment(ctx.prefix, "cell", 0, 2, 2))
    end

    test "an upload that fails leaves the outcome to the caller", t do
      %{ctx: ctx, state: state, head: head, store: {_, cfg}} = t
      MemoryStore.inject(cfg, kind: :error, ops: [:put], key: ~r/\.ltx$/)

      assert {:error, :injected} =
               Replicator.replace(ctx, "cell", state.db_path, 4096, head, 1, pages: pages(head))
    end

    defp pages(head), do: head.commit
  end

  test "from_restore refuses an older epoch", %{ctx: ctx, dir: dir} do
    {conn, state} = new_replicator(dir, "src.db", 5)
    _ = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])
    {_, result} = restore!(ctx, dir, :latest)
    assert_raise ArgumentError, fn -> Replicator.from_restore(result, 4) end
  end

  test "telemetry reports syncs and restores", %{ctx: ctx, dir: dir} do
    id = "repl-telemetry-#{System.unique_integer()}"
    test = self()

    :telemetry.attach_many(
      id,
      [[:alambique, :replication, :sync], [:alambique, :replication, :restore]],
      &__MODULE__.forward/4,
      test
    )

    on_exit(fn -> :telemetry.detach(id) end)
    {conn, state} = new_replicator(dir, "src.db")
    _ = commit_and_sync(conn, state, ctx, [{:put, 1, 100}])

    assert_receive {[:alambique, :replication, :sync], %{bytes: bytes, duration: _},
                    %{cell: "cell", txid: 1, epoch: 1, kind: :batch, result: :ok}}

    assert bytes > 100
    restore!(ctx, dir, :latest)

    assert_receive {[:alambique, :replication, :restore], %{duration: _},
                    %{cell: "cell", result: :ok, txid: 1, segments: 1}}
  end

  def forward(event, measurements, meta, test), do: send(test, {event, measurements, meta})
end
