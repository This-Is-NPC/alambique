defmodule Alambique.CompactionPropertiesTest do
  @moduledoc """
  Compaction and retention never change what a restore returns (design §9):
  workloads interleaved with compaction passes, retention runs, reaper rounds,
  store faults, crash points and owner changes. The model is the database dump
  after each acknowledged TXID, which is what an uncompacted restore returns.
  With a deletion grace (design §9.1) the store is audited too: no object is
  deleted before the manifest that names it is due, every manifest waits the
  grace, and once the reaper ran everything retention marked is gone.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CompactionHelpers
  import Alambique.ReplicationHelpers, only: [safe_op: 2]

  alias Alambique.{AuditStore, Compaction, Retention, Trash}
  alias Alambique.Compaction.Hold
  alias Alambique.Replication.Replicator
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @compaction_points [
    :compact_before_put,
    :compact_after_put,
    :compact_before_snapshot,
    :compact_after_snapshot,
    :compact_between_merges
  ]
  @retention_points [
    :trash_before_mark,
    :trash_after_mark,
    :retain_before_marker,
    :trash_before_delete,
    :trash_after_delete,
    :trash_before_entry_delete
  ]

  setup do
    on_exit(&CrashPoints.reset/0)
    :ok
  end

  # Generators

  defp op_gen do
    one_of([
      gen all(k <- integer(1..12), n <- member_of([10, 200, 1500, 5000])) do
        {:put, k, n}
      end,
      map(integer(1..12), &{:del, &1}),
      map(integer(1..2), &{:fill, &1}),
      constant(:vacuum)
    ])
  end

  defp command_gen do
    frequency([
      {8, map(list_of(op_gen(), min_length: 1, max_length: 3), &{:write, &1})},
      {3, map(member_of([5, 40, 400, 6000]), &{:advance, &1})},
      {3, constant(:reap)},
      {5, map(integer(1..3), &{:compact, &1})},
      {4, constant(:retain)},
      {1, constant({:takeover, :local})},
      {1, constant({:takeover, :remote})},
      {1, constant(:zombie)},
      {2, map(member_of([:all, :some]), &{:evict, &1})},
      {2,
       gen all(kind <- member_of([:error, :timeout, :lost_ack]), times <- integer(1..8)) do
         {:fault, kind, times}
       end},
      {2, map(member_of(@compaction_points ++ @retention_points), &{:crash, &1})},
      {1, constant(:clear)}
    ])
  end

  # Faults hit everything compaction and retention do, and spare the level-0
  # puts of the writer, which replication's own properties cover.
  defp inject(env, kind, times) do
    MemoryStore.inject(env.cfg,
      kind: kind,
      ops: [:get, :head, :list, :delete],
      times: times,
      ms: 1
    )

    MemoryStore.inject(env.cfg,
      kind: kind,
      ops: [:put],
      key: ~r{/000[1-9]/},
      times: times,
      ms: 1
    )
  end

  # Model

  defp run(commands, opts) do
    # The owner's writes and fences keep the helper's long deadline: no fault
    # targets level-0 puts, so a timeout there would be load, not a fault (an
    # unknown outcome the model cannot follow). Each pass sets its own 40 ms.
    env = audited(setup_world())
    Process.flag(:trap_exit, true)
    owner = new_owner(env, "w0.db")
    hold = hold_config(opts)

    model = %{
      owner: owner,
      hold: hold,
      feeds: true,
      epoch: 1,
      stamps: %{},
      newest_snapshot: 0,
      opts: opts,
      names: 0
    }

    try do
      model = Enum.reduce(commands, model, &step(&1, &2, env))
      verify(model, env)
    after
      CrashPoints.reset()
      if hold, do: :ets.delete(hold.holds)
      if conn = Process.delete(:conn), do: Alambique.Sqlite.close(conn)
      File.rm_rf(env.dir)
    end
  end

  # The hold table of the node that runs the compactor, as an instance makes
  # it; nil when holding is off.
  defp hold_config(%{hold_bytes: 0}), do: nil

  defp hold_config(opts) do
    config = %{
      holds: :"hold_#{System.unique_integer([:positive])}",
      compaction: %{hold_bytes: opts.hold_bytes, hold_object_bytes: opts.hold_object_bytes}
    }

    Hold.create(config)
    config
  end

  # What the cell of this node does for each file that landed.
  defp feed(%{hold: hold, feeds: true}, env) when hold != nil,
    do: [landed: &Hold.put_upload(hold, env.ctx, &1)]

  defp feed(_model, _env), do: []

  # Every request of the context goes through the audit log.
  defp audited(env) do
    audit =
      AuditStore.wrap({env.mod, env.cfg}, fn -> ManualClock.world_ms(env.world) end)

    env |> Map.put(:audit, elem(audit, 1)) |> Map.update!(:ctx, &%{&1 | store: audit})
  end

  defp stamp(model, env, owner) do
    Process.put(:conn, owner.conn)
    ts = ManualClock.world_ms(env.world)

    Enum.reduce(Map.keys(owner.dumps), model.stamps, fn txid, acc ->
      Map.put_new(acc, txid, ts)
    end)
  end

  defp step({:write, ops}, model, env) do
    {:ok, owner} = write(env, model.owner, ops, 1, feed(model, env))
    %{model | owner: owner, stamps: stamp(model, env, owner)}
  end

  defp step({:advance, ms}, model, env) do
    ManualClock.advance(env.world, ms)
    model
  end

  defp step({:compact, levels}, model, env) do
    opts = [
      fanout: model.opts.fanout,
      max_level: levels,
      snapshot_interval_txids: 5,
      snapshot_interval_ms: 300,
      pitr_window_ms: model.opts.window,
      timeout_ms: 40,
      deletion_grace_ms: model.opts.deletion,
      batch_keys: 4,
      hold: model.hold
    ]

    safely(fn -> Compaction.run(env.ctx, cell(), opts) end)
    note_snapshots(model, env)
  end

  defp step(:retain, model, env) do
    opts = [
      pitr_window_ms: model.opts.window,
      grace_ms: model.opts.grace,
      deletion_grace_ms: model.opts.deletion,
      batch_keys: 4,
      timeout_ms: 40
    ]

    safely(fn -> Retention.run(env.ctx, cell(), opts) end)
    note_snapshots(model, env)
  end

  defp step(:reap, model, env) do
    safely(fn -> reap(env) end)
    note_snapshots(model, env)
  end

  # The cell restarts on the node of the compactor (it drops its rows) or
  # moves to another node (its rows stay, as after a crash, and nobody feeds
  # new ones).
  defp step({:takeover, where}, model, env) do
    clear_faults(env)
    CrashPoints.reset()
    Alambique.Sqlite.close(model.owner.conn)
    if model.hold && where == :local, do: Hold.drop_cell(model.hold, cell())
    model = %{model | feeds: where == :local}
    epoch = model.epoch + 1
    name = "w#{model.names + 1}.db"
    owner = take_over(env, name, epoch, model.owner.dumps, feed(model, env))
    model = %{model | owner: owner, epoch: epoch, names: model.names + 1}
    %{model | stamps: stamp(model, env, owner)}
  end

  # The old owner is not stopped: it writes the next TXID after the new owner
  # fenced the one before (its file lands, held by this node when the old
  # owner lived here), and the new owner, elsewhere, then overwrites it.
  defp step(:zombie, %{owner: %{state: %{txid: 0}}} = model, _env), do: model

  defp step(:zombie, model, env) do
    clear_faults(env)
    CrashPoints.reset()
    zombie = model.owner
    epoch = model.epoch + 1
    owner = take_over(env, "w#{model.names + 1}.db", epoch, zombie.dumps)
    safe_op(zombie.conn, {:put, 9, 100})
    Replicator.sync(%{zombie.state | txid: owner.state.txid}, env.ctx, feed(model, env))
    Alambique.Sqlite.close(zombie.conn)
    model = %{model | owner: owner, feeds: false, epoch: epoch, names: model.names + 1}
    %{model | stamps: stamp(model, env, owner)}
  end

  defp step({:evict, _}, %{hold: nil} = model, _env), do: model

  defp step({:evict, :all}, model, _env) do
    Hold.drop_cell(model.hold, cell())
    model
  end

  defp step({:evict, :some}, model, _env) do
    spec = [
      {{{cell(), :"$1", :"$2", :"$3"}, :_, :_, :_, :_}, [], [{{cell(), :"$1", :"$2", :"$3"}}]}
    ]

    Hold.drop(model.hold, model.hold.holds |> :ets.select(spec) |> Enum.take_every(2))
    model
  end

  defp step({:fault, kind, times}, model, env) do
    inject(env, kind, times)
    model
  end

  defp step({:crash, point}, model, _env) do
    CrashPoints.arm(point, :raise)
    model
  end

  defp step(:clear, model, env) do
    clear_faults(env)
    model
  end

  # A crash point or an exit inside a pass is a compactor dying: the next
  # command goes on from whatever it left in the store.
  defp safely(fun) do
    fun.()
  rescue
    CrashPoints.Crash -> :crashed
  catch
    :exit, _ -> :crashed
  end

  defp note_snapshots(model, env) do
    case clear_and_list(env) do
      [] -> model
      txids -> %{model | newest_snapshot: max(model.newest_snapshot, Enum.max(txids))}
    end
  end

  # A listing can fail while faults are active: that tells nothing new.
  defp clear_and_list(env) do
    snapshots(env)
  rescue
    MatchError -> []
  end

  defp reap(env),
    do: Trash.reap_ctx(env.ctx, %{page_size: 3, concurrency: 2, rate: 10_000, timeout_ms: 40})

  # Checks

  defp verify(model, env) do
    clear_faults(env)
    CrashPoints.reset()
    check(model, env, "after the workload")

    # Whatever the passes left half done, fault-free passes finish the job.
    for _ <- 1..3 do
      {:ok, _} = step_ok(model, env)
    end

    check(model, env, "after fault-free passes")
    assert {:ok, %{merges: 0}} = compact_once(model, env)
    reaped(model, env)
    audit(model, env)
  end

  # Retention marks what the newest anchor covers; once the grace passed and
  # the reaper ran, none of it is left and no manifest waits.
  defp reaped(model, env) do
    ManualClock.advance(env.world, model.opts.grace + 1)
    {:ok, %{anchor: anchor}} = retention(model, env)
    ManualClock.advance(env.world, model.opts.deletion + 1)
    reap(env)
    assert %{errors: 0} = reap(env)
    check(model, env, "after the reaper")

    {:ok, keys, nil} = env.mod.list(env.cfg, Trash.prefix(env.ctx.prefix), nil, 10_000)
    assert keys == [], "manifests left"

    if anchor do
      assert Enum.all?(snapshots(env), &(&1 >= anchor)), "a covered snapshot is left"

      assert Enum.all?(for(l <- 0..8, {_, max} <- segments(env, l), do: max), &(&1 > anchor)),
             "a covered segment is left"
    end
  end

  defp audit(%{opts: %{deletion: 0}}, _env), do: :ok

  defp audit(model, env) do
    manifests = AuditStore.manifests(env.audit)

    for {at, key} <- AuditStore.deletes(env.audit), String.ends_with?(key, ".ltx") do
      assert Enum.any?(manifests, fn {due, put_ms, keys} ->
               key in keys and due <= at and due - put_ms == model.opts.deletion
             end),
             "#{key} was deleted at #{at} without a manifest that was due"
    end
  end

  defp step_ok(model, env) do
    with {:ok, _} <- compact_once(model, env), do: retention(model, env)
  end

  defp retention(model, env) do
    Retention.run(env.ctx, cell(),
      pitr_window_ms: model.opts.window,
      grace_ms: model.opts.grace,
      deletion_grace_ms: model.opts.deletion,
      batch_keys: 4,
      timeout_ms: 40
    )
  end

  defp compact_once(model, env) do
    Compaction.run(env.ctx, cell(),
      fanout: model.opts.fanout,
      max_level: 3,
      snapshot_interval_txids: 5,
      snapshot_interval_ms: 300,
      pitr_window_ms: model.opts.window,
      timeout_ms: 40,
      deletion_grace_ms: model.opts.deletion,
      batch_keys: 4,
      hold: model.hold
    )
  end

  defp check(model, env, when_) do
    dumps = model.owner.dumps
    last = model.owner.state.txid
    now = ManualClock.world_ms(env.world)

    # The latest restore is the last acknowledged batch.
    assert {:ok, %{txid: ^last}, dump} = restore(env, :latest), "latest #{when_}"
    assert dump == Map.get(dumps, last, {[], %{}}), "latest state #{when_}"

    # Every point is either restored exactly or refused; every point of the
    # window is restored.
    for txid <- 1..last//1, Map.has_key?(dumps, txid) do
      in_window? = model.stamps[txid] > now - model.opts.window

      case restore(env, {:txid, txid}) do
        {:ok, %{txid: ^txid}, dump} ->
          assert dump == dumps[txid], "state at TXID #{txid} #{when_}"

        {:ok, other, _} ->
          flunk("TXID #{txid} restored as #{other.txid} #{when_}")

        {:error, reason} ->
          refute in_window?, "TXID #{txid} is in the window but #{inspect(reason)} #{when_}"
      end
    end

    # Restoring by time stops at the last TXID stamped at or before it.
    model.stamps
    |> Map.values()
    |> Enum.uniq()
    |> Enum.take_random(3)
    |> Enum.each(fn ts ->
      expected =
        model.stamps
        |> Enum.filter(fn {_, s} -> s <= ts end)
        |> Enum.map(&elem(&1, 0))
        |> Enum.max()

      case restore(env, {:timestamp, ts}) do
        {:ok, %{txid: ^expected}, dump} ->
          assert dump == dumps[expected], "state at time #{ts} #{when_}"

        {:ok, other, _} ->
          flunk("time #{ts} restored TXID #{other.txid}, expected #{expected} #{when_}")

        {:error, reason} ->
          refute ts > now - model.opts.window,
                 "time #{ts} is in the window but #{inspect(reason)} #{when_}"
      end
    end)

    # Retention never removes the newest snapshot: a newer one may exist now.
    if model.newest_snapshot > 0 do
      assert Enum.max(snapshots(env), fn -> 0 end) >= model.newest_snapshot,
             "newest snapshot kept #{when_}"
    end
  end

  # Found by the property: compaction that does not know the window thins the
  # snapshot that retention takes as its anchor.
  @tag :capture_log
  test "compaction keeps the snapshot a window anchors on" do
    run(
      [
        {:write, [{:put, 12, 5000}]},
        {:write, [{:put, 10, 10}, :vacuum, {:fill, 1}]},
        :zombie,
        {:takeover, :remote},
        {:write, [:vacuum]},
        {:compact, 1},
        {:write, [{:del, 1}, :vacuum]},
        {:advance, 6000},
        {:compact, 3},
        {:write, [{:del, 7}, :vacuum]},
        {:write, [{:fill, 2}, {:put, 9, 200}, {:del, 1}]}
      ],
      %{fanout: 2, window: 500, grace: 0, deletion: 300, hold_bytes: 0, hold_object_bytes: 300}
    )
  end

  @tag :capture_log
  property "restore is unchanged by compaction, retention, faults and crashes" do
    check all(
            commands <- list_of(command_gen(), min_length: 4, max_length: 35),
            fanout <- member_of([2, 3]),
            window <- member_of([0, 30, 500, 100_000]),
            grace <- member_of([0, 0, 200]),
            deletion <- member_of([0, 300, 5000]),
            hold_bytes <- member_of([0, 3_000, 40_000, 10_000_000]),
            hold_object_bytes <- member_of([300, 6_000, 1_000_000]),
            max_runs: 120
          ) do
      run(commands, %{
        fanout: fanout,
        window: window,
        grace: grace,
        deletion: deletion,
        hold_bytes: hold_bytes,
        hold_object_bytes: hold_object_bytes
      })
    end
  end
end
