defmodule Alambique.ReplicationPropertiesTest do
  @moduledoc """
  Invariant R1 (design §6): after any crash, restore yields the state of the
  last acknowledged batch, or of a later batch whose segment landed, and
  never a mix.

  Invariant R2: an object written by an owner of an older epoch is never part
  of the restored chain. Restore ignores it when it is a tail, names it when
  it is not, and a store that fails requests gives an error, never another
  state.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CompactionHelpers,
    only: [setup_world: 0, new_owner: 2, take_over: 4, write: 3, history: 3, restore: 2, cell: 0]

  import Alambique.ReplicationHelpers

  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Native, Replicator}
  alias AlambiqueTestkit.{CrashPoints, HostileStore, MemoryStore}

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

  # R2

  defp rival_command_gen do
    tuple({member_of([:owner, :stale]), integer(1..30), member_of([10, 200, 1500, 5000])})
  end

  property "R2: an object of an older epoch is not part of the restored chain" do
    check all(
            steps <- integer(1..5),
            commands <- list_of(rival_command_gen(), min_length: 1, max_length: 8),
            stale_last? <- boolean(),
            reach <- member_of([nil, 16, 17, 18]),
            ops <-
              uniq_list_of(member_of([:get, :head, :list]),
                min_length: 1,
                max_length: 3,
                max_tries: 100
              ),
            times <- integer(1..4),
            max_runs: 30
          ) do
      commands = if stale_last?, do: commands ++ [{:stale, 1, 100}], else: commands
      rivals(steps, commands, reach, ops, times)
    end
  end

  # The owner of epoch 1 writes `steps` TXIDs and is taken over by an owner of
  # epoch 2. Then the new owner writes and the old one keeps uploading, in any
  # order. An old object stays at the TXID after the head when the new owner
  # has not reached it. A further object at `reach` TXIDs past the head is
  # still a tail up to 16 and makes the old object an error from 17.
  defp rivals(steps, commands, reach, ops, times) do
    env = setup_world()

    try do
      old = history(env, new_owner(env, "a.db"), steps)
      new = take_over(env, "b.db", 2, old.dumps)

      {new, old} =
        Enum.reduce(commands, {new, old}, fn command, {new, old} ->
          rival_step(command, env, new, old)
        end)

      head = new.state.txid
      lingering? = old.state.txid > head
      if lingering? and reach, do: put_stale(env, head + reach)

      expected =
        if lingering? and reach in [17, 18] do
          object = Keys.segment(env.ctx.prefix, cell(), 0, head + 1, head + 1)
          {:error, {:epoch_regression, object, 1, 2}}
        else
          {:ok, head, new.dumps[head]}
        end

      hostile = HostileStore.config(env.cfg)
      faulty = %{env | ctx: %{env.ctx | store: {HostileStore, hostile}}}
      HostileStore.fail(hostile, ops, times)
      answer = restore(faulty, :latest)
      assert_rival_answer(answer, expected, HostileStore.delivered(hostile) > 0)

      HostileStore.heal(hostile)
      assert_rival_answer(restore(faulty, :latest), expected, false)
      assert_rival_answer(restore(env, :latest), expected, false)

      if match?({:ok, _, _}, expected),
        do: assert({:ok, %{txid: ^head, epoch: 2}} = Replication.head(env.ctx, cell()))

      Alambique.Sqlite.close(old.conn)
      Alambique.Sqlite.close(new.conn)
    after
      GenServer.stop(env.pid)
      File.rm_rf(env.dir)
    end
  end

  defp rival_step({:owner, k, n}, env, new, old) do
    {:ok, new} = write(env, new, [{:put, k, n}])
    {new, old}
  end

  # The old owner commits and uploads at the TXID after the highest it knows
  # of, or after the new owner's head when that is further.
  defp rival_step({:stale, k, n}, env, new, old) do
    safe_op(old.conn, {:put, k, n})
    state = %{old.state | txid: max(old.state.txid, new.state.txid)}

    case Replicator.sync(state, env.ctx) do
      {:ok, next} -> {new, %{old | state: next}}
      {:error, _} -> {new, old}
    end
  end

  defp put_stale(env, txid) do
    key = Keys.segment(env.ctx.prefix, cell(), 0, txid, txid)
    {:ok, _} = env.mod.put(env.cfg, key, "x", :none, %{"epoch" => "1"})
  end

  # A failed request may fail the restore; otherwise the answer is the one the
  # chain of the new owner gives.
  defp assert_rival_answer({:ok, result, dump}, {:ok, head, expected}, _faulted?) do
    assert result.txid == head
    assert result.epoch == 2
    assert dump == expected
  end

  defp assert_rival_answer(answer, answer, _faulted?), do: :ok
  defp assert_rival_answer({:error, _}, _expected, true), do: :ok

  defp assert_rival_answer(answer, expected, faulted?) do
    flunk(
      "restore answered #{inspect(answer)}, expected #{inspect(expected)} " <>
        "(a request failed: #{faulted?})"
    )
  end
end
