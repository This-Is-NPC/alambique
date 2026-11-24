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
  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Native, Replicator}
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
      {1, map(integer(0..62), &{:disagree, &1})},
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

  # What the store holds of the snapshot of the last TXID when it is restored.
  defp corruption_gen do
    one_of([
      constant(:intact),
      map(integer(0..99), &{:flip, &1}),
      map(integer(0..99), &{:truncate, &1}),
      constant(:empty),
      constant(:neighbour)
    ])
  end

  property "S1: a snapshot has the checksum and content of the replicated state" do
    check all(
            plans <-
              list_of(
                tuple({list_of(command_gen(), min_length: 5, max_length: 30), corruption_gen()}),
                length: @workers
              ),
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
      parent = self()

      tasks =
        workers
        |> Enum.zip(plans)
        |> Enum.map(fn {worker, {plan, _}} ->
          Task.async(fn ->
            {conn, model} = replicate(worker, plan, cfg, world)
            send(parent, {:replicated, self()})
            receive do: (:quiet -> :ok)
            {model, final_write(conn, model, worker)}
          end)
        end)

      for %{pid: pid} <- tasks, do: receive(do: ({:replicated, ^pid} -> :ok))
      Enum.each(background, &stop/1)
      MemoryStore.clear(cfg)
      Enum.each(tasks, &send(&1.pid, :quiet))
      runs = Task.await_many(tasks, :infinity)

      failures = Enum.flat_map(runs, fn {model, _} -> model.failures end)
      assert failures == [], Enum.map_join(failures, "\n", &inspect(&1, limit: :infinity))

      for {{worker, {_, corruption}}, {_, final}} <- Enum.zip(Enum.zip(workers, plans), runs) do
        settle(worker, final, cfg, corruption)
      end
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

    {conn, Enum.reduce(plan, model, &step(&1, &2, conn, worker, cfg, world))}
  end

  # The context of the quiet phase: the store answers, so a deadline is not a fault.
  defp calm(worker), do: %{worker.ctx | timeout_ms: 10_000}

  # One more committed write, replicated, and the content it leaves. A worker
  # whose last upload failed may have landed it: it restores and fences, as
  # a new owner does, and goes on from the restored file.
  defp final_write(conn, model, worker) do
    safe_op(conn, {:put, 1, 100})

    {conn, state} =
      case Replicator.sync(model.state, calm(worker)) do
        {:ok, state} -> {conn, state}
        {:error, _} -> recover(conn, worker)
      end

    expected = dump(conn)
    Alambique.Sqlite.close(conn)
    {state, expected}
  end

  defp recover(conn, worker) do
    Alambique.Sqlite.close(conn)
    dest = Path.join(worker.dir, "recovered.db")
    {:ok, result} = Replication.restore(calm(worker), "cell", dest, :latest)
    {:ok, state} = Replicator.fence(Replicator.from_restore(result, 2), calm(worker))
    conn = open_db(dest)
    safe_op(conn, {:put, 1, 100})
    {:ok, state} = Replicator.sync(state, calm(worker))
    {conn, state}
  end

  # With the store quiet, the worker takes one more snapshot, the only object
  # left in the store is that snapshot, and the store holds it as `corruption`
  # says. A restore must then give the replicated state, refuse the object by
  # name, or fall back to the snapshot beside it; it never gives another state.
  defp settle(worker, {state, expected}, cfg, corruption) do
    ctx = calm(worker)
    assert state.txid > 0

    assert {:ok, key} = Replicator.snapshot(state, ctx)
    assert key == Keys.snapshot(ctx.prefix, "cell", state.txid)

    assert {:ok, whole} = restore(worker, ctx)
    assert {whole.txid, whole.checksum} == {state.txid, state.checksum}

    {:ok, keys, nil} = MemoryStore.list(cfg, Keys.files_prefix(ctx.prefix, "cell"), nil, 100_000)
    for other <- keys, other != key, do: :ok = MemoryStore.delete(cfg, other)

    {:ok, body, _etag, meta} = MemoryStore.get(cfg, key)

    case corruption do
      :intact ->
        assert {:ok, snap} = restore(worker, ctx)
        assert {snap.txid, snap.checksum, snap.segments} == {state.txid, state.checksum, 1}
        assert dump_file(snap.path) == expected

      :neighbour ->
        next = Keys.snapshot(ctx.prefix, "cell", state.txid + 1)
        {:ok, _} = MemoryStore.put(cfg, next, body, :if_none_match, meta)
        # A snapshot ahead of a chain that ends at the snapshot below it is a
        # sign of missing files, and the refusal of the first is the answer.
        assert restore(worker, ctx) == {:error, {:bad_snapshot, next}}

      other ->
        {:ok, _} = MemoryStore.put(cfg, key, corrupt(body, other), :none, meta)

        case restore(worker, ctx) do
          {:error, {:snapshot, ^key, _}} ->
            :ok

          {:error, {:bad_snapshot, ^key}} ->
            :ok

          {:ok, snap} ->
            flunk("restored #{inspect(snap)} from a corrupt snapshot (#{inspect(other)})")

          {:error, error} ->
            flunk("the corrupt snapshot was not named: #{inspect(error)}")
        end
    end
  end

  defp restore(worker, ctx) do
    dest = Path.join(worker.dir, "restored-#{System.unique_integer([:positive])}.db")
    Replication.restore(ctx, "cell", dest, :latest)
  end

  defp corrupt(_body, :empty), do: ""

  defp corrupt(body, {:truncate, n}),
    do: binary_part(body, 0, div(byte_size(body) * n, 100))

  defp corrupt(body, {:flip, n}) do
    at = div((byte_size(body) - 1) * n, 99)
    <<head::binary-size(^at), byte, tail::binary>> = body
    <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>
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

  # A state whose checksum disagrees with the file and the replicated frames
  # gets no snapshot.
  defp step({:disagree, bit}, %{state: state} = model, _conn, worker, _cfg, _world)
       when state.txid > 0 do
    wrong = %{state | checksum: Bitwise.bxor(state.checksum, Bitwise.bsl(1, bit))}

    case Replicator.snapshot(wrong, worker.ctx) do
      {:error, :snapshot_checksum_mismatch} -> model
      other -> fail(model, {:snapshot_of_a_wrong_checksum, bit, other})
    end
  end

  defp step({:disagree, _bit}, model, _conn, _worker, _cfg, _world), do: model

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
        if key == Keys.snapshot(worker.ctx.prefix, "cell", state.txid),
          do: check_snapshot(model, worker, cfg, key),
          else: fail(model, {:snapshot_key, key, state.txid})

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

      # A snapshot of the last TXID is not removed by retention.
      {:error, :not_found} ->
        fail(model, {:snapshot_missing, key})

      # A read fault.
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
