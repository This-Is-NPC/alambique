defmodule Alambique.CompactionTest do
  @moduledoc "Compaction passes (design §9): merging, snapshots, epochs, faults."
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.Compaction
  alias Alambique.Replication.{Keys, Replicator}
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    on_exit(&CrashPoints.reset/0)
    env
  end

  defp compact(env, opts \\ []),
    do: Compaction.run(env.ctx, cell(), Keyword.merge([fanout: 2, max_level: 3], opts))

  defp manifests(env) do
    {:ok, keys, nil} = env.mod.list(env.cfg, Alambique.Trash.prefix(env.ctx.prefix), nil, 10_000)
    keys
  end

  defp meta(env, key) do
    {:ok, _etag, meta} =
      env.mod.head(env.cfg, Keys.cell_prefix(env.ctx.prefix, cell()) <> "/" <> key)

    meta
  end

  describe "merging" do
    test "merges aligned blocks level by level and restores the same state", env do
      owner = history(env, new_owner(env, "a.db"), 20)
      {:ok, plain, _} = restore(env, :latest)
      assert plain.segments == 20

      assert {:ok, %{merges: 17, snapshot: nil, more: false, head: 20}} = compact(env)

      assert Enum.sort(segments(env, 1)) ==
               Enum.map(1..10, &{2 * &1 - 1, 2 * &1})

      assert Enum.sort(segments(env, 2)) == [{1, 4}, {5, 8}, {9, 12}, {13, 16}, {17, 20}]
      assert Enum.sort(segments(env, 3)) == [{1, 8}, {9, 16}]
      assert length(segments(env, 0)) == 20

      assert {:ok, result, dump} = restore(env, :latest)
      assert dump == owner.dumps[20]
      assert result.segments == 3
      assert result.txid == 20
    end

    test "files that the next level replaces are deleted, level 0 and the top level stay", env do
      owner = history(env, new_owner(env, "a.db"), 20)

      assert {:ok, %{merges: 17}} = compact(env, deletion_grace_ms: 0)

      # Level 1 is replaced by level 2 everywhere; level 2 by level 3 up to 16.
      assert segments(env, 1) == []
      assert segments(env, 2) == [{17, 20}]
      assert Enum.sort(segments(env, 3)) == [{1, 8}, {9, 16}]
      assert length(segments(env, 0)) == 20

      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[20]

      for txid <- [1, 5, 13, 19] do
        assert {:ok, %{txid: ^txid}, dump} = restore(env, {:txid, txid})
        assert dump == owner.dumps[txid]
      end
    end

    test "superseded files wait for the deletion grace in a manifest", env do
      history(env, new_owner(env, "a.db"), 8)
      assert {:ok, _} = compact(env, deletion_grace_ms: 86_400_000)
      assert length(segments(env, 1)) == 4
      assert length(segments(env, 2)) == 2
      assert [_manifest] = manifests(env)

      ManualClock.advance(env.world, 86_400_001)
      Alambique.Trash.reap_ctx(env.ctx, Map.new(Alambique.Trash.defaults()))
      assert segments(env, 1) == []
      assert manifests(env) == []
    end

    test "delete_superseded: false keeps every merged file", env do
      history(env, new_owner(env, "a.db"), 20)
      assert {:ok, _} = compact(env, deletion_grace_ms: 0, delete_superseded: false)
      assert length(segments(env, 1)) == 10
      assert length(segments(env, 2)) == 5
    end

    test "a file another compactor already wrote is not marked again", env do
      history(env, new_owner(env, "a.db"), 4)
      assert {:ok, %{merges: 3}} = compact(env, deletion_grace_ms: 86_400_000)
      before = manifests(env)
      assert {:ok, %{merges: 0}} = compact(env, deletion_grace_ms: 86_400_000)
      assert manifests(env) == before
    end

    test "a second pass finds nothing to do", env do
      history(env, new_owner(env, "a.db"), 9)
      assert {:ok, %{merges: merges}} = compact(env)
      assert merges > 0
      before = Enum.sort(files(env))
      assert {:ok, %{merges: 0, bytes: 0}} = compact(env)
      assert Enum.sort(files(env)) == before
    end

    test "leaves the tail that does not fill a block", env do
      history(env, new_owner(env, "a.db"), 7)
      assert {:ok, %{head: 7}} = compact(env, fanout: 4, max_level: 1)
      assert segments(env, 1) == [{1, 4}]
    end

    test "merged files carry the highest epoch of their inputs (R2)", env do
      a = history(env, new_owner(env, "a.db"), 3)
      b = take_over(env, "b.db", 2, a.dumps)
      b = history(env, b, 4)
      assert b.state.txid == 8

      assert {:ok, _} = compact(env, max_level: 1)
      # {1,2} is epoch 1, {3,4} spans the takeover fence at 4 and is epoch 2.
      assert meta(env, "0001/#{Keys.hex(1)}-#{Keys.hex(2)}.ltx")["epoch"] == "1"
      assert meta(env, "0001/#{Keys.hex(3)}-#{Keys.hex(4)}.ltx")["epoch"] == "2"
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == b.dumps[8]
    end

    test "max_merges bounds a pass and reports that more is left", env do
      owner = history(env, new_owner(env, "a.db"), 16)
      assert {:ok, %{merges: 2, more: true}} = compact(env, max_merges: 2, max_level: 1)
      assert {:ok, %{merges: 2, more: true}} = compact(env, max_merges: 2, max_level: 1)

      assert {:ok, %{merges: 4, more: false}} = compact(env, max_merges: 4, max_level: 1)
      assert length(segments(env, 1)) == 8
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[16]
    end

    test "emits merge and pass events", env do
      history(env, new_owner(env, "a.db"), 4)
      id = {__MODULE__, make_ref()}

      :telemetry.attach_many(
        id,
        [[:alambique, :compaction, :merge], [:alambique, :compaction, :pass]],
        &Alambique.CellHelpers.relay/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(id) end)
      {:ok, _} = compact(env, max_level: 1)

      assert_received {:telemetry, [:alambique, :compaction, :merge], %{bytes: bytes, inputs: 2},
                       %{cell: "cell", level: 1, min: 1, max: 2, result: :ok}}

      assert bytes > 0

      assert_received {:telemetry, [:alambique, :compaction, :pass], %{merges: 2, duration: _},
                       %{cell: "cell", result: :ok, head: 4}}
    end
  end

  describe "snapshots by bytes" do
    @never [snapshot_interval_txids: 1_000, snapshot_interval_ms: 1_000_000_000_000]

    test "a snapshot is due when the bytes since the last one reach the ratio", env do
      owner = history(env, new_owner(env, "a.db"), 6)

      assert {:ok, %{snapshot: nil}} = compact(env, @never ++ [snapshot_ratio: 1_000_000])
      assert {:ok, %{snapshot: 6}} = compact(env, @never ++ [snapshot_ratio: 0.0001])
      assert snapshots(env) == [6]

      # Nothing was written since, so the same ratio is not due again.
      owner = history(env, owner, 4)
      assert {:ok, %{snapshot: nil}} = compact(env, @never ++ [snapshot_ratio: 1_000_000])
      assert {:ok, %{snapshot: 10}} = compact(env, @never ++ [snapshot_ratio: 0.0001])
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[10]
    end

    test "the minimum size applies to a cell without a snapshot", env do
      history(env, new_owner(env, "a.db"), 6)

      assert {:ok, %{snapshot: nil}} =
               compact(env, @never ++ [snapshot_ratio: 1, snapshot_min_bytes: 1_000_000_000])
    end

    test "a snapshot without a count waits for the limits of age and TXIDs", env do
      owner = history(env, new_owner(env, "a.db"), 6)
      assert {:ok, %{snapshot: 6}} = compact(env, @never ++ [snapshot_ratio: 0.0001])

      # What an older release leaves: a snapshot and no count.
      env.mod.delete(env.cfg, Keys.chain(env.ctx.prefix, cell()))
      history(env, owner, 4)
      assert {:ok, %{snapshot: nil}} = compact(env, @never ++ [snapshot_ratio: 0.0001])
      assert {:ok, %{snapshot: 10}} = compact(env, snapshot_interval_txids: 4)
    end

    test "the count belongs to the newest snapshot", env do
      history(env, new_owner(env, "a.db"), 6)
      assert {:ok, %{snapshot: 6}} = compact(env, @never ++ [snapshot_ratio: 0.0001])
      key = Keys.chain(env.ctx.prefix, cell())
      {:ok, body, _, _} = env.mod.get(env.cfg, key)
      assert [<<"6">>, size, "0"] = String.split(body, ":")
      assert String.to_integer(size) > 0
    end
  end

  describe "thinning snapshots" do
    defp days(env, opts) do
      Enum.reduce(1..3, new_owner(env, "a.db"), fn _day, owner ->
        owner = history(env, owner, 5, 1)
        {:ok, _} = compact(env, [snapshot_interval_txids: 5, deletion_grace_ms: 0] ++ opts)
        ManualClock.advance(env.world, 86_400_000)
        owner
      end)
    end

    test "a snapshot close to the one before it goes when the next is taken", env do
      owner = days(env, [])
      assert snapshots(env) == [5, 15]

      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]

      for txid <- [3, 8, 12] do
        assert {:ok, %{txid: ^txid}, dump} = restore(env, {:txid, txid})
        assert dump == owner.dumps[txid]
      end
    end

    test "snapshot_keep_ms: 0 keeps every snapshot", env do
      days(env, snapshot_keep_ms: 0)
      assert snapshots(env) == [5, 10, 15]
    end

    test "a snapshot the anchor of retention may be is kept", env do
      days(env, pitr_window_ms: 86_400_000)
      assert snapshots(env) == [5, 10, 15]
    end
  end

  describe "snapshots" do
    test "one is taken after enough TXIDs", env do
      owner = history(env, new_owner(env, "a.db"), 10)

      assert {:ok, %{snapshot: nil}} = compact(env, snapshot_interval_txids: 11)
      assert {:ok, %{snapshot: 10}} = compact(env, snapshot_interval_txids: 10)
      assert snapshots(env) == [10]

      meta = meta(env, "0009/#{Keys.hex(1)}-#{Keys.hex(10)}.ltx")
      assert meta["epoch"] == "1"
      assert String.to_integer(meta["ts"]) == String.to_integer(meta["created"])

      assert {:ok, result, dump} = restore(env, :latest)
      assert dump == owner.dumps[10]
      assert result.segments == 1
    end

    test "the next one builds on the previous snapshot", env do
      owner = history(env, new_owner(env, "a.db"), 6)
      {:ok, _} = compact(env, snapshot_interval_txids: 6)
      owner = history(env, owner, 6)
      assert {:ok, %{snapshot: 12}} = compact(env, snapshot_interval_txids: 6)
      assert snapshots(env) == [6, 12]
      assert {:ok, %{segments: 1}, dump} = restore(env, :latest)
      assert dump == owner.dumps[12]
      # Restoring a TXID in between starts from the older snapshot.
      assert {:ok, %{txid: 8}, dump} = restore(env, {:txid, 8})
      assert dump == owner.dumps[8]
    end

    test "one is due when the newest state is old enough", env do
      owner = history(env, new_owner(env, "a.db"), 3)
      assert {:ok, %{snapshot: nil}} = compact(env, snapshot_interval_ms: 1_000)
      ManualClock.advance(env.world, 1_000)
      assert {:ok, %{snapshot: 3}} = compact(env, snapshot_interval_ms: 1_000)
      assert {:ok, %{snapshot: nil}} = compact(env, snapshot_interval_ms: 1_000)
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[3]
    end

    test "no snapshot for an empty store or one already covered", env do
      assert {:ok, %{snapshot: nil, head: 0, merges: 0}} = compact(env)
      history(env, new_owner(env, "a.db"), 2)
      {:ok, _} = compact(env, snapshot_interval_txids: 2)
      assert {:ok, %{snapshot: nil}} = compact(env, snapshot_interval_txids: 1)
    end
  end

  describe "epochs (R2)" do
    # A stale owner (epoch 1) writes TXID 5 after the new owner (epoch 2)
    # fenced TXID 4: nothing may merge that write into a higher epoch.
    defp stale_write_after_takeover(env) do
      a = history(env, new_owner(env, "a.db"), 3)
      b = take_over(env, "b.db", 2, a.dumps)
      stale = %{a.state | txid: b.state.txid}
      Alambique.ReplicationHelpers.safe_op(a.conn, {:put, 9, 100})
      assert {:ok, stale} = Replicator.sync(stale, env.ctx)
      assert stale.txid == 5
      {a, b}
    end

    test "a stale write after the new fence is never merged", env do
      {_a, b} = stale_write_after_takeover(env)
      assert {:ok, %{skipped: []}} = compact(env, max_level: 3)

      # The orphan at 5 pairs with 6 only if 6 exists; nothing covers 5.
      refute Enum.any?(files(env), fn
               {level, min, max} when level > 0 -> min <= 5 and 5 <= max
               _ -> false
             end)

      assert {:ok, %{txid: 4}, dump} = restore(env, {:txid, 4})
      assert dump == b.dumps[4]
    end

    test "a group whose epochs decrease is skipped", env do
      a = history(env, new_owner(env, "a.db"), 3)
      b = take_over(env, "b.db", 2, a.dumps)
      b = history(env, b, 3)
      assert b.state.txid == 7

      # An epoch-1 file in the middle of an epoch-2 block, as a fenced writer
      # that landed late would leave it.
      key = Keys.segment(env.ctx.prefix, cell(), 0, 6, 6)
      {:ok, body, _, meta} = env.mod.get(env.cfg, key)
      env.mod.delete(env.cfg, key)
      {:ok, _} = env.mod.put(env.cfg, key, body, :if_none_match, Map.put(meta, "epoch", "1"))

      assert {:ok, %{skipped: skipped}} = compact(env, max_level: 1)
      assert {:epoch_regression, {1, 5, 6}} in skipped
      refute {1, 5, 6} in Enum.map(files(env), & &1)
    end
  end

  describe "faults" do
    test "a failed upload fails the pass and the next pass repairs it", env do
      owner = history(env, new_owner(env, "a.db"), 8)

      MemoryStore.inject(env.cfg,
        kind: :error,
        ops: [:put],
        key: ~r{/000[1-9]/},
        times: :infinity
      )

      assert {:error, _} = compact(env, timeout_ms: 50)
      assert segments(env, 1) == []
      clear_faults(env)

      assert {:ok, %{merges: merges}} = compact(env)
      assert merges > 0
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[8]
    end

    test "a lost acknowledgement leaves a file the next pass accepts", env do
      owner = history(env, new_owner(env, "a.db"), 4)
      MemoryStore.inject(env.cfg, kind: :lost_ack, ops: [:put], key: ~r{/0001/}, times: 1)

      assert {:ok, %{merges: merges}} = compact(env, max_level: 1)
      # The retry saw the file already there.
      assert merges == 1 or merges == 2
      assert Enum.sort(segments(env, 1)) == [{1, 2}, {3, 4}]
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[4]
    end

    test "unreadable inputs fail the pass without changing the store", env do
      history(env, new_owner(env, "a.db"), 4)
      before = Enum.sort(files(env))
      MemoryStore.inject(env.cfg, kind: :error, ops: [:get], times: :infinity)
      assert {:error, _} = compact(env, timeout_ms: 50)
      assert Enum.sort(files(env)) == before
    end

    for point <- [
          :compact_before_put,
          :compact_after_put,
          :compact_before_snapshot,
          :compact_after_snapshot
        ] do
      test "a crash at #{point} leaves a store that restores and compacts", env do
        owner = history(env, new_owner(env, "a.db"), 6)
        CrashPoints.arm(unquote(point), :raise)

        assert_raise CrashPoints.Crash, fn ->
          compact(env, max_level: 1, snapshot_interval_txids: 6)
        end

        CrashPoints.reset()
        assert {:ok, _, dump} = restore(env, :latest)
        assert dump == owner.dumps[6]

        assert {:ok, _} = compact(env, max_level: 1, snapshot_interval_txids: 6)
        assert Enum.sort(segments(env, 1)) == [{1, 2}, {3, 4}, {5, 6}]
        assert snapshots(env) == [6]
        assert {:ok, %{segments: 1}, dump} = restore(env, :latest)
        assert dump == owner.dumps[6]
      end
    end
  end

  describe "concurrent compactors" do
    test "two compactors racing on one cell produce one consistent set of files", env do
      owner = history(env, new_owner(env, "a.db"), 40)

      other = %{env.ctx | node: "node-b"}

      run = fn ctx ->
        Compaction.run(ctx, cell(), fanout: 2, max_level: 3, snapshot_interval_txids: 20)
      end

      results =
        [env.ctx, other, env.ctx, other]
        |> Enum.map(&Task.async(fn -> run.(&1) end))
        |> Task.await_many(30_000)

      assert Enum.all?(results, &match?({:ok, _}, &1))

      # Aligned blocks mean the same keys no matter who wrote them.
      assert {:ok, %{merges: 0}} = run.(env.ctx)
      assert snapshots(env) == [40]
      assert {:ok, %{segments: 1}, dump} = restore(env, :latest)
      assert dump == owner.dumps[40]

      for txid <- [3, 17, 25, 40] do
        assert {:ok, _, dump} = restore(env, {:txid, txid})
        assert dump == owner.dumps[txid]
      end
    end

    test "a compactor racing a writer never breaks the chain", env do
      owner = new_owner(env, "a.db")

      compactor =
        Task.async(fn ->
          for _ <- 1..30 do
            {:ok, _} = Compaction.run(env.ctx, cell(), fanout: 2, max_level: 2)
            Process.sleep(1)
          end
        end)

      owner = history(env, owner, 40, 0)
      Task.await(compactor, 30_000)
      {:ok, _} = Compaction.run(env.ctx, cell(), fanout: 2, max_level: 2)

      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[40]
    end
  end
end
