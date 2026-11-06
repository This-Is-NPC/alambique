defmodule Alambique.SnapshotPropertiesTest do
  @moduledoc """
  Invariant S1 (design §6): a snapshot built from the database file and the
  replicated WAL frames has the checksum of the replicated state and restores
  to its content, whatever runs beside it: other replicators writing,
  syncing, checkpointing and taking snapshots on the dirty schedulers,
  compaction and retention on the same store, store faults, and SQLite work
  on other connections.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.ReplicationHelpers

  alias Alambique.{Compaction, Retention}
  alias Alambique.Replication.{Native, Replicator}
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @moduletag timeout: 600_000
  @workers 8

  # Generators

  defp simple_op_gen do
    one_of([
      gen all(k <- integer(1..30), n <- member_of([10, 200, 1500, 5000, 9000])) do
        {:put, k, n}
      end,
      map(integer(1..30), &{:del, &1}),
      map(integer(1..2), &{:fill, &1}),
      map(integer(1..2), &{:drop, &1})
    ])
  end

  defp op_gen do
    frequency([
      {6, simple_op_gen()},
      {2, map(list_of(simple_op_gen(), min_length: 1, max_length: 4), &{:txn, &1})},
      {1, map(list_of(simple_op_gen(), min_length: 1, max_length: 3), &{:rollback, &1})},
      {1, constant(:vacuum)}
    ])
  end

  defp command_gen do
    frequency([
      {5, map(list_of(op_gen(), min_length: 1, max_length: 3), &{:write, &1})},
      {5, constant(:sync)},
      {4, constant(:snapshot)},
      {2, constant(:checkpoint)},
      {1,
       gen all(
             kind <- member_of([:error, :timeout, :lost_ack, :latency]),
             op <- member_of([:put, :get, :list]),
             times <- integer(1..6)
           ) do
         {:fault, kind, op, times}
       end}
    ])
  end

  property "S1: a snapshot has the checksum and content of the replicated state" do
    check all(
            plans <-
              list_of(list_of(command_gen(), min_length: 5, max_length: 30), length: @workers),
            max_runs: 25
          ) do
      run(plans)
    end
  end

  # Model

  defp run(plans) do
    {store_pid, {_, cfg} = store} = start_store()
    world = new_world()
    root = tmp_dir()

    workers =
      for i <- 1..@workers do
        dir = Path.join(root, "w#{i}")
        File.mkdir_p!(dir)
        %{dir: dir, ctx: ctx(store, world, prefix: "w#{i}", timeout_ms: 25)}
      end

    background = [
      spawn_link(fn -> maintain(Enum.map(workers, & &1.ctx)) end)
      | for(i <- 1..4, do: spawn_link(fn -> churn(Path.join(root, "noise#{i}.db")) end))
    ]

    try do
      failures =
        workers
        |> Enum.zip(plans)
        |> Enum.map(fn {worker, plan} ->
          Task.async(fn -> replicate(worker, plan, cfg, world) end)
        end)
        |> Task.await_many(:infinity)
        |> List.flatten()

      assert failures == [], Enum.map_join(failures, "\n", &inspect(&1, limit: :infinity))
    after
      Enum.each(background, &stop/1)
      GenServer.stop(store_pid)
      File.rm_rf(root)
    end
  end

  defp stop(pid) do
    Process.unlink(pid)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    receive do: ({:DOWN, ^ref, _, _, _} -> :ok)
  end

  # Compaction and retention of every worker's cell, in a loop, as the
  # compactor of an instance does.
  defp maintain(ctxs) do
    for ctx <- ctxs do
      Compaction.run(ctx, "cell", fanout: 2, max_level: 3, snapshot_interval_txids: 4)
      Retention.run(ctx, "cell", pitr_window_ms: 1, grace_ms: 0, deletion_grace_ms: 0)
    end

    maintain(ctxs)
  end

  # SQLite work on other connections, so that the allocations of the dirty
  # schedulers interleave with the replicators'.
  defp churn(path) do
    conn = open_db(path)

    for k <- 1..50 do
      safe_op(conn, {:put, k, 2000})
      rows(conn, "SELECT count(*), sum(length(v)) FROM kv")
    end

    Alambique.Sqlite.close(conn)
    File.rm(path)
    churn(path)
  end

  defp replicate(worker, plan, cfg, world) do
    {conn, state} = new_replicator(worker.dir, "src.db")
    model = %{state: state, dumps: %{}, failures: []}

    try do
      Enum.reduce(plan, model, &step(&1, &2, conn, worker, cfg, world)).failures
    after
      Alambique.Sqlite.close(conn)
    end
  end

  defp step({:write, ops}, model, conn, _worker, _cfg, world) do
    ManualClock.advance(world, 1)
    Enum.each(ops, &safe_op(conn, &1))
    model
  end

  defp step({:fault, kind, op, times}, model, _conn, _worker, cfg, _world) do
    MemoryStore.inject(cfg, kind: kind, ops: [op], times: times, ms: 2)
    model
  end

  defp step(:sync, model, conn, worker, _cfg, _world) do
    case Replicator.sync(model.state, worker.ctx) do
      {:ok, %{txid: txid} = next} when txid != model.state.txid ->
        %{model | state: next, dumps: Map.put(model.dumps, txid, dump(conn))}

      {:ok, next} ->
        %{model | state: next}

      {:error, _} ->
        model
    end
  end

  defp step(:checkpoint, model, conn, _worker, _cfg, _world) do
    case Replicator.checkpoint(model.state, conn) do
      {:ok, next} -> %{model | state: next}
      {:error, _} -> model
    end
  end

  defp step(:snapshot, %{state: state} = model, _conn, worker, cfg, _world) do
    case Replicator.snapshot(state, worker.ctx) do
      {:ok, key} ->
        check_snapshot(model, worker, cfg, key)

      {:error, :snapshot_checksum_mismatch} ->
        fail(model, {:snapshot_checksum_mismatch, diagnose(state)})

      {:error, :nothing_replicated} when state.txid == 0 ->
        model

      # Store faults: the snapshot did not land, or landed unacknowledged.
      {:error, _} ->
        model
    end
  end

  # The object restores to the content recorded when its TXID was acknowledged.
  defp check_snapshot(model, worker, cfg, key) do
    case MemoryStore.get(cfg, key) do
      {:ok, body, _etag, _meta} ->
        path = Path.join(worker.dir, "snap-#{System.unique_integer([:positive])}.db")
        {:ok, _header, post} = Native.apply_ltx(path, body, 0, 0)
        restored = dump_file(path)
        File.rm(path)

        cond do
          post != model.state.checksum ->
            fail(model, {:snapshot_object_checksum, key, post, model.state.checksum})

          restored != model.dumps[model.state.txid] ->
            fail(model, {:snapshot_content, key, model.state.txid})

          true ->
            model
        end

      # Removed by retention after a newer snapshot, or a read fault.
      {:error, _} ->
        model
    end
  end

  defp fail(model, failure), do: %{model | failures: [failure | model.failures]}

  # What differs between the replicated state and the file plus WAL now.
  defp diagnose(state) do
    wal = state.db_path <> "-wal"
    header = with {:ok, bin} <- File.read(wal), do: binary_part(bin, 0, min(32, byte_size(bin)))

    again =
      Native.capture_full(state.db_path, wal, state.page_size, state.txid, 0, state.wal_pos)

    pages =
      case again do
        {:ok, _segment, _pos, _sums, checksum, commit} ->
          %{again_matches: checksum == state.checksum, commit: {commit, state.commit}}

        other ->
          %{again: other}
      end

    Map.merge(pages, %{
      txid: state.txid,
      wal_pos: state.wal_pos,
      wal_header: header,
      db_size: File.stat(state.db_path)
    })
  end
end
