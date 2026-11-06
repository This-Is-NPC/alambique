defmodule Alambique.RetentionTest do
  @moduledoc "Retention (design §9): the anchor, the grace period and what it leaves alone."
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.Retention
  alias Alambique.Replication.{Keys, Replicator}
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @day 86_400_000

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    on_exit(&CrashPoints.reset/0)
    env
  end

  defp retain(env, opts \\ []),
    do:
      Retention.run(
        env.ctx,
        cell(),
        Keyword.merge([pitr_window_ms: @day, grace_ms: 0, deletion_grace_ms: 0], opts)
      )

  describe "covered files" do
    test "nothing goes while no snapshot is older than the window", env do
      owner = history(env, new_owner(env, "a.db"), 6)
      {:ok, _} = compact_pass(env, snapshot_interval_txids: 6)
      before = Enum.sort(files(env))

      assert {:ok, %{anchor: nil, segments: 0, snapshots: 0}} = retain(env)
      assert Enum.sort(files(env)) == before
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[6]
    end

    test "files an old enough snapshot covers are deleted and the rest stays", env do
      owner = daily_history(env)
      assert snapshots(env) == [5, 10, 15]

      # Now is three days after the first writes: the snapshots at 5, 10, 15 are
      # 3, 2 and 1 days old. A one day window anchors at the snapshot of 15.
      assert {:ok, %{anchor: 15, snapshots: 2, segments: segments}} = retain(env)
      assert segments > 0
      assert snapshots(env) == [15]

      assert for(level <- 0..2, do: segments(env, level)) == [[], [], []]

      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]
      # Out of the window: refused, never wrong.
      assert {:error, _} = restore(env, {:txid, 7})
    end

    test "a two day window keeps what a day does not", env do
      owner = daily_history(env)
      assert {:ok, %{anchor: 10}} = retain(env, pitr_window_ms: 2 * @day)
      assert snapshots(env) == [10, 15]

      for txid <- 10..15 do
        assert {:ok, %{txid: ^txid}, dump} = restore(env, {:txid, txid})
        assert dump == owner.dumps[txid]
      end

      assert {:error, _} = restore(env, {:txid, 9})
    end

    test "every point of the window can be restored by TXID and by time", env do
      owner = daily_history(env)
      now = ManualClock.world_ms(env.world)
      {:ok, %{anchor: anchor}} = retain(env, pitr_window_ms: @day + 3)
      assert anchor == 10

      for txid <- anchor..15 do
        assert {:ok, _, dump} = restore(env, {:txid, txid})
        assert dump == owner.dumps[txid]
      end

      assert {:ok, %{txid: 12}, dump} = restore(env, {:timestamp, stamp(env, 12)})
      assert dump == owner.dumps[12]
      assert {:ok, %{txid: 15}, _} = restore(env, {:timestamp, now})
    end

    test "the newest snapshot stays even when the window is zero", env do
      owner = daily_history(env)
      assert {:ok, %{anchor: 15}} = retain(env, pitr_window_ms: 0)
      assert snapshots(env) == [15]
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]
    end

    test "waits for the grace period after the anchor was created", env do
      owner = daily_history(env)
      before = Enum.sort(files(env))

      assert {:ok, %{anchor: 15, waiting: true, segments: 0, snapshots: 0}} =
               retain(env, grace_ms: 10 * @day)

      assert Enum.sort(files(env)) == before
      ManualClock.advance(env.world, 10 * @day)

      assert {:ok, %{anchor: 15, waiting: false, snapshots: 2}} =
               retain(env, grace_ms: 10 * @day)

      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]
    end

    test "does not trust an anchor without a creation time", env do
      daily_history(env)
      key = Keys.snapshot(env.ctx.prefix, cell(), 15)
      {:ok, body, _, meta} = env.mod.get(env.cfg, key)
      env.mod.delete(env.cfg, key)
      {:ok, _} = env.mod.put(env.cfg, key, body, :if_none_match, Map.delete(meta, "created"))

      assert {:ok, %{anchor: 15, waiting: true, snapshots: 0}} = retain(env)
    end

    test "a snapshot without a stamp is read for it", env do
      owner = daily_history(env)
      key = Keys.snapshot(env.ctx.prefix, cell(), 15)
      {:ok, body, _, meta} = env.mod.get(env.cfg, key)
      env.mod.delete(env.cfg, key)
      stripped = meta |> Map.delete("ts") |> Map.put("created", "1")
      {:ok, _} = env.mod.put(env.cfg, key, body, :if_none_match, stripped)

      assert {:ok, %{anchor: 15}} = retain(env)
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]
    end

    test "a snapshot made by the replicator is a valid anchor", env do
      owner = history(env, new_owner(env, "a.db"), 4)
      assert {:ok, _} = Replicator.snapshot(owner.state, env.ctx)
      owner = history(env, owner, 3)
      ManualClock.advance(env.world, 2 * @day)

      assert {:ok, %{anchor: 4, snapshots: 0, segments: 4}} = retain(env)
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[7]
    end

    test "emits a run event with what it deleted", env do
      daily_history(env)
      id = {__MODULE__, make_ref()}

      :telemetry.attach(
        id,
        [:alambique, :retention, :run],
        &Alambique.CellHelpers.relay/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(id) end)
      {:ok, _} = retain(env)

      assert_received {:telemetry, [:alambique, :retention, :run],
                       %{duration: _, snapshots: 2, segments: segments},
                       %{cell: "cell", result: :ok, anchor: 15}}

      assert segments > 0
    end
  end

  describe "a tail past the chain" do
    test "is not deleted and does not stop the restore (design §6, R3)", env do
      {_a, b} = orphaned(env)
      before = Enum.sort(files(env))

      assert {:ok, %{anchor: nil}} = retain(env)
      assert Enum.sort(files(env)) == before
      assert {:ok, %{txid: 4}, dump} = restore(env, :latest)
      assert dump == b.dumps[4]
    end
  end

  describe "faults" do
    test "a failed deletion fails the run and a later run completes it", env do
      owner = daily_history(env)
      MemoryStore.inject(env.cfg, kind: :error, ops: [:delete], times: :infinity)
      assert {:error, _} = retain(env, timeout_ms: 50)

      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]

      clear_faults(env)
      assert {:ok, %{anchor: 15}} = retain(env)
      assert snapshots(env) == [15]
    end

    test "deletions that were applied but not acknowledged are fine", env do
      owner = daily_history(env)
      MemoryStore.inject(env.cfg, kind: :lost_ack, ops: [:delete], times: 3)
      assert {:ok, %{anchor: 15}} = retain(env)
      assert snapshots(env) == [15]
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]
    end

    test "an unreadable listing or snapshot header fails the run and deletes nothing", env do
      daily_history(env)
      before = Enum.sort(files(env))

      fault = MemoryStore.inject(env.cfg, kind: :error, ops: [:list], reason: :down)
      assert {:error, :down} = retain(env, timeout_ms: 50)
      MemoryStore.remove(env.cfg, fault)

      MemoryStore.inject(env.cfg, kind: :error, ops: [:head], reason: :down)
      assert {:error, :down} = retain(env, timeout_ms: 50)
      assert Enum.sort(files(env)) == before
    end

    test "a snapshot without a stamp that cannot be read fails the run", env do
      daily_history(env)
      before = Enum.sort(files(env))
      key = Keys.snapshot(env.ctx.prefix, cell(), 15)
      {:ok, _} = env.mod.put(env.cfg, key, "not a snapshot", :none, %{})

      assert {:error, {:snapshot, ^key, _reason}} = retain(env)
      assert length(files(env)) == length(before)
    end

    test "the marker of the covered files decides what is marked again", env do
      daily_history(env)
      marker = Keys.trashed(env.ctx.prefix, cell())
      opts = [deletion_grace_ms: @day]

      assert {:ok, %{anchor: 15, snapshots: 2}} = retain(env, opts)
      assert {:ok, "15", _, _} = env.mod.get(env.cfg, marker)
      # Recorded, so there is nothing new to mark.
      assert {:ok, %{anchor: 15, snapshots: 0, segments: 0}} = retain(env, opts)

      # A marker nobody can read counts as none: the files are marked again.
      {:ok, _} = env.mod.put(env.cfg, marker, "garbage", :none, %{})
      assert {:ok, %{anchor: 15, snapshots: 2}} = retain(env, opts)
      assert {:ok, "15", _, _} = env.mod.get(env.cfg, marker)
    end

    test "a marker that cannot be read or written fails the run", env do
      daily_history(env)
      opts = [deletion_grace_ms: @day, timeout_ms: 50]

      fault = MemoryStore.inject(env.cfg, kind: :error, ops: [:put], key: ~r{/trashed$})
      assert {:error, _} = retain(env, opts)
      MemoryStore.remove(env.cfg, fault)
      assert {:ok, %{anchor: 15}} = retain(env, opts)

      MemoryStore.inject(env.cfg, kind: :error, ops: [:get], key: ~r{/trashed$}, reason: :down)
      assert {:error, :down} = retain(env, opts)
    end

    for point <- [:trash_before_delete, :trash_after_delete] do
      @tag :capture_log
      test "a crash at #{point} leaves every restore point valid", env do
        owner = daily_history(env)
        CrashPoints.arm(unquote(point), :raise, times: 1)

        # Deletions run in linked tasks: the crash takes the run down.
        Process.flag(:trap_exit, true)
        assert {:error, {:task_exit, _}} = retain(env)
        CrashPoints.reset()

        assert {:ok, _, dump} = restore(env, :latest)
        assert dump == owner.dumps[15]

        for txid <- 15..15 do
          assert {:ok, _, dump} = restore(env, {:txid, txid})
          assert dump == owner.dumps[txid]
        end

        assert {:ok, %{anchor: 15}} = retain(env)
        assert snapshots(env) == [15]
      end
    end
  end

  # The timestamp at which TXID `txid` was stamped.
  defp stamp(env, txid) do
    key = Keys.segment(env.ctx.prefix, cell(), 0, txid, txid)
    {:ok, _, _, meta} = env.mod.get(env.cfg, key)
    String.to_integer(meta["ts"])
  end
end
