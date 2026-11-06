defmodule Alambique.ReplicationPropertiesTest do
  @moduledoc """
  Invariant R1 (design §6): after any crash, restore yields the state of the
  last acknowledged batch, or of a later batch whose segment landed, and
  never a mix.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.ReplicationHelpers

  alias Alambique.Replication
  alias Alambique.Replication.{Native, Replicator}
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @crash_points [
    :repl_before_capture,
    :repl_after_encode,
    :repl_after_put,
    :repl_before_checkpoint
  ]

  setup do
    on_exit(&CrashPoints.reset/0)
  end

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
      {2, constant(:checkpoint)},
      {2,
       gen all(kind <- member_of([:error, :timeout, :lost_ack]), times <- integer(1..12)) do
         {:fault, kind, times}
       end},
      {1, map(member_of(@crash_points), &{:crash, &1})}
    ])
  end

  # Model

  defp run(commands, restore_crash?) do
    {_pid, {_, cfg} = store} = start_store()
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 25)
    {conn, state} = new_replicator(dir, "src.db")

    model = %{state: state, acked: 0, attempted: %{}, stopped: false}

    try do
      model = Enum.reduce_while(commands, model, &step(&1, &2, conn, ctx, cfg))
      verify(model, ctx, cfg, dir, restore_crash?)
    after
      CrashPoints.reset()
      Alambique.Sqlite.close(conn)
      File.rm_rf(dir)
    end
  end

  defp step({:write, ops}, model, conn, _ctx, _cfg) do
    Enum.each(ops, &safe_op(conn, &1))
    {:cont, model}
  end

  defp step({:fault, kind, times}, model, _conn, _ctx, cfg) do
    MemoryStore.inject(cfg, kind: kind, ops: [:put], times: times, ms: 1)
    {:cont, model}
  end

  defp step({:crash, point}, model, _conn, _ctx, _cfg) do
    CrashPoints.arm(point, :raise)
    {:cont, model}
  end

  defp step(:sync, %{state: state} = model, conn, ctx, _cfg) do
    txid = state.txid + 1
    seen = [dump(conn) | Map.get(model.attempted, txid, [])]
    model = %{model | attempted: Map.put(model.attempted, txid, seen)}

    try do
      case Replicator.sync(state, ctx) do
        {:ok, next} -> {:cont, %{model | state: next, acked: max(model.acked, next.txid)}}
        {:error, :fenced} -> {:halt, model}
        {:error, _} -> {:cont, model}
      end
    rescue
      CrashPoints.Crash -> {:halt, model}
    end
  end

  defp step(:checkpoint, %{state: state} = model, conn, _ctx, _cfg) do
    case Replicator.checkpoint(state, conn) do
      {:ok, next} -> {:cont, %{model | state: next}}
      {:error, _} -> {:cont, model}
    end
  rescue
    CrashPoints.Crash -> {:halt, model}
  end

  defp verify(model, ctx, cfg, dir, restore_crash?) do
    MemoryStore.clear(cfg)
    CrashPoints.reset()
    dest = Path.join(dir, "restored.db")

    if restore_crash? do
      CrashPoints.arm(:repl_mid_restore, :raise)

      crashed? =
        try do
          Replication.restore(ctx, "cell", dest, :latest)
          false
        rescue
          CrashPoints.Crash -> true
        end

      CrashPoints.reset()

      if crashed?,
        do: refute(File.exists?(dest), "a crashed restore must not leave a destination file")

      File.rm(dest)
    end

    assert {:ok, result} = Replication.restore(ctx, "cell", dest, :latest)

    assert result.txid >= model.acked,
           "restored TXID #{result.txid} is older than the last acknowledged #{model.acked}"

    expected =
      if result.txid == 0, do: [{[], %{}}], else: Map.get(model.attempted, result.txid, [])

    restored = dump_file(dest)

    assert restored in expected,
           "restored state at TXID #{result.txid} matches no batch that was ever attempted"

    {:ok, _sums, checksum, _pages} = Native.file_state(dest, result.page_size || 4096)
    assert checksum == result.checksum

    if result.txid == model.state.txid, do: assert(result.checksum == model.state.checksum)

    # The restored state can be continued by a new owner.
    next = Replicator.from_restore(result, 2)
    assert {:ok, fenced} = Replicator.fence(next, ctx)
    assert fenced.txid == result.txid + 1
  end

  property "R1: restore equals the last acknowledged batch or a later one that landed" do
    check all(
            commands <- list_of(command_gen(), min_length: 1, max_length: 30),
            restore_crash? <- boolean(),
            max_runs: 150
          ) do
      run(commands, restore_crash?)
    end
  end
end
