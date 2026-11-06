defmodule Alambique.ReplicationFencingTest do
  @moduledoc "Fencing and invariant R2 (design §6), and upload retries."
  use ExUnit.Case, async: false

  import Alambique.ReplicationHelpers

  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Native, Replicator}
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  setup do
    {pid, {mod, cfg} = store} = start_store()
    dir = tmp_dir()
    on_exit(fn -> File.rm_rf(dir) end)
    on_exit(&CrashPoints.reset/0)
    world = new_world()
    {:ok, store: store, mod: mod, cfg: cfg, pid: pid, dir: dir, ctx: ctx(store, world)}
  end

  defp write(conn, state, ctx, ops) do
    Enum.each(ops, &safe_op(conn, &1))
    Replicator.sync(state, ctx)
  end

  defp write!(conn, state, ctx, ops) do
    assert {:ok, state} = write(conn, state, ctx, ops)
    state
  end

  # Wakes a new owner: restore, then fence at the next TXID.
  defp wake(ctx, dir, name, epoch) do
    path = Path.join(dir, name)
    {:ok, result} = Replication.restore(ctx, "cell", path, :latest)
    state = Replicator.from_restore(result, epoch)
    {path, state}
  end

  defp epochs(mod, cfg, ctx, txids) do
    for t <- txids do
      {:ok, _, _, meta} = mod.get(cfg, Keys.segment(ctx.prefix, "cell", 0, t, t))
      String.to_integer(meta["epoch"])
    end
  end

  describe "fencing" do
    test "after the new owner's fence, the old owner's next sync is fenced", %{ctx: ctx, dir: dir} do
      {conn_a, a} = new_replicator(dir, "a.db", 1)
      a = write!(conn_a, a, ctx, [{:put, 1, 100}])
      a = write!(conn_a, a, ctx, [{:put, 2, 100}])

      {_path, b} = wake(ctx, dir, "b.db", 2)
      assert {:ok, b} = Replicator.fence(b, ctx)
      assert b.txid == 3

      id = "fenced-#{System.unique_integer()}"
      test = self()
      :telemetry.attach(id, [:alambique, :replication, :fenced], &__MODULE__.forward/4, test)
      on_exit(fn -> :telemetry.detach(id) end)

      assert {:error, :fenced} = write(conn_a, a, ctx, [{:put, 3, 100}])

      assert_receive {[:alambique, :replication, :fenced], %{},
                      %{cell: "cell", txid: 3, epoch: 1}}

      # Still fenced when asked again, and nothing of A's write is in the store.
      assert {:error, :fenced} = Replicator.sync(a, ctx)
    end

    test "a stale write that won a TXID before the fence is adopted on re-restore", %{
      ctx: ctx,
      dir: dir
    } do
      {conn_a, a} = new_replicator(dir, "a.db", 1)
      a = write!(conn_a, a, ctx, [{:put, 1, 100}])
      {_path_b, b} = wake(ctx, dir, "b.db", 2)

      # The old owner, unaware, lands TXID 2 first.
      a = write!(conn_a, a, ctx, [{:put, 2, 100}])
      assert a.txid == 2
      assert {:error, :txid_taken} = Replicator.fence(b, ctx)

      {path_b2, b} = wake(ctx, dir, "b2.db", 2)
      assert b.txid == 2
      assert dump_file(path_b2) == dump(conn_a)
      assert {:ok, b} = Replicator.fence(b, ctx)
      assert b.txid == 3

      assert {:error, :fenced} = write(conn_a, a, ctx, [{:put, 3, 100}])
    end

    test "the new owner's writes continue the old chain without a gap", %{
      ctx: ctx,
      dir: dir,
      mod: mod,
      cfg: cfg
    } do
      {conn_a, a} = new_replicator(dir, "a.db", 1)
      _ = write!(conn_a, a, ctx, [{:put, 1, 5000}])
      {path, b} = wake(ctx, dir, "b.db", 2)
      {:ok, b} = Replicator.fence(b, ctx)
      conn_b = open_db(path)
      b = write!(conn_b, b, ctx, [{:put, 2, 5000}])
      _ = write!(conn_b, b, ctx, [{:del, 1}])
      assert epochs(mod, cfg, ctx, 1..4) == [1, 2, 2, 2]
      {restored, result} = restore_to(ctx, dir)
      assert %{epoch: 2, txid: 4} = result
      assert dump_file(restored) == dump(conn_b)
    end

    test "the fence is a no-op for the data", %{ctx: ctx, dir: dir} do
      {conn_a, a} = new_replicator(dir, "a.db", 1)
      a = write!(conn_a, a, ctx, [{:put, 1, 100}])
      {_, b} = wake(ctx, dir, "b.db", 2)
      assert {:ok, fenced} = Replicator.fence(b, ctx)
      assert fenced.checksum == a.checksum
      assert fenced.commit == a.commit
    end

    test "the fence on an empty store claims TXID 1", %{ctx: ctx, dir: dir} do
      path = Path.join(dir, "n.db")
      conn = open_db(path)
      state = Replicator.new("cell", path, 4096, 1)
      assert {:ok, state} = Replicator.fence(state, ctx)
      assert state.txid == 1
      assert {:error, :txid_taken} = Replicator.fence(Replicator.new("cell", path, 4096, 2), ctx)
      state = write!(conn, state, ctx, [{:put, 1, 100}])
      assert state.txid == 2
      {restored, _} = restore_to(ctx, dir)
      assert dump_file(restored) == dump(conn)
    end
  end

  describe "R2: epochs never decrease along the restored chain" do
    # A stale owner (epoch 1) that believes it is at TXID 3 although the new
    # owner (epoch 2) fenced TXID 3.
    defp stale_write_after_fence(ctx, dir) do
      {conn_a, a} = new_replicator(dir, "a.db", 1)
      a = write!(conn_a, a, ctx, [{:put, 1, 100}])
      {_, b} = wake(ctx, dir, "b.db", 2)
      {:ok, b} = Replicator.fence(b, ctx)
      stale = %{a | txid: b.txid}
      assert {:ok, stale} = write(conn_a, stale, ctx, [{:put, 2, 100}])
      assert stale.txid == 3
      b
    end

    test "a lower epoch after a higher one is a tail, not part of the chain", %{
      ctx: ctx,
      dir: dir
    } do
      b = stale_write_after_fence(ctx, dir)
      assert b.txid == 2

      assert {:ok, %{txid: 2, epoch: 2}} =
               Replication.restore(ctx, "cell", Path.join(dir, "r1"), :latest)

      assert {:ok, %{txid: 2, epoch: 2}} = Replication.head(ctx, "cell")
    end

    test "the state before the offending segment is still restorable", %{ctx: ctx, dir: dir} do
      stale_write_after_fence(ctx, dir)

      assert {:ok, %{txid: 2, epoch: 2}} =
               Replication.restore(ctx, "cell", Path.join(dir, "r2"), {:txid, 2})
    end

    test "a lower-epoch alternative is skipped in favour of a valid one", %{
      ctx: ctx,
      dir: dir,
      mod: mod,
      cfg: cfg
    } do
      {conn_a, a} = new_replicator(dir, "a.db", 1)
      _ = write!(conn_a, a, ctx, [{:put, 1, 2000}])
      {path, b} = wake(ctx, dir, "b.db", 2)
      {:ok, b} = Replicator.fence(b, ctx)
      conn_b = open_db(path)
      b = write!(conn_b, b, ctx, [{:put, 2, 2000}])
      b = write!(conn_b, b, ctx, [{:put, 3, 2000}])
      assert b.txid == 4

      bodies = for t <- 3..4, do: elem(mod.get(cfg, Keys.segment(ctx.prefix, "cell", 0, t, t)), 1)
      {:ok, merged} = Native.merge_ltx(bodies)
      merged_key = Keys.segment(ctx.prefix, "cell", 1, 3, 4)
      {:ok, _} = mod.put(cfg, merged_key, merged, :if_none_match, %{"epoch" => "1"})

      # The level-1 file claims epoch 1 after epoch 2: level 0 is used instead.
      assert {:ok, %{txid: 4, epoch: 2, segments: 4}} =
               Replication.restore(ctx, "cell", Path.join(dir, "ok"), :latest)

      # With the level-0 files gone the only way forward is the regression.
      for t <- 3..4, do: mod.delete(cfg, Keys.segment(ctx.prefix, "cell", 0, t, t))

      assert {:error, {:epoch_regression, ^merged_key, 1, 2}} =
               Replication.restore(ctx, "cell", Path.join(dir, "bad"), :latest)
    end

    test "a snapshot's epoch counts as the chain's epoch", %{
      ctx: ctx,
      dir: dir,
      mod: mod,
      cfg: cfg
    } do
      {conn_a, a} = new_replicator(dir, "a.db", 1)
      a = write!(conn_a, a, ctx, [{:put, 1, 100}])
      {:ok, key} = Replicator.snapshot(a, ctx)
      {:ok, body, _, _} = mod.get(cfg, key)
      mod.delete(cfg, key)
      {:ok, _} = mod.put(cfg, key, body, :none, %{"epoch" => "5"})
      a = write!(conn_a, a, ctx, [{:put, 2, 100}])
      _ = a

      assert {:ok, %{txid: 1, epoch: 5}} =
               Replication.restore(ctx, "cell", Path.join(dir, "r"), :latest)
    end
  end

  describe "upload retries" do
    test "transient errors are retried with the same bytes", %{
      ctx: ctx,
      dir: dir,
      mod: mod,
      cfg: cfg
    } do
      {conn, state} = new_replicator(dir, "a.db")
      MemoryStore.inject(cfg, kind: :error, ops: [:put], times: 2)
      state = write!(conn, state, ctx, [{:put, 1, 100}])
      assert state.txid == 1

      assert {:ok, _etag, %{"epoch" => "1"}} =
               mod.head(cfg, Keys.segment(ctx.prefix, "cell", 0, 1, 1))
    end

    test "a lost acknowledgement is recognised as our own write", %{ctx: ctx, dir: dir, cfg: cfg} do
      {conn, state} = new_replicator(dir, "a.db")
      MemoryStore.inject(cfg, kind: :lost_ack, ops: [:put], times: 1)
      state = write!(conn, state, ctx, [{:put, 1, 100}])
      assert state.txid == 1
      state = write!(conn, state, ctx, [{:put, 2, 100}])
      assert state.txid == 2
    end

    test "giving up returns the error with the state usable for a retry", %{
      ctx: ctx,
      dir: dir,
      mod: mod,
      cfg: cfg
    } do
      {conn, state} = new_replicator(dir, "a.db")
      id = MemoryStore.inject(cfg, kind: :error, ops: [:put], reason: :down)
      assert {:error, :down} = write(conn, state, ctx, [{:put, 1, 100}])
      assert {:error, :not_found} = mod.get(cfg, Keys.segment(ctx.prefix, "cell", 0, 1, 1))
      MemoryStore.remove(cfg, id)
      assert {:ok, %{txid: 1}} = Replicator.sync(state, ctx)
    end

    test "a write that landed but was never acknowledged is adopted by a later attempt", %{
      ctx: ctx,
      dir: dir,
      cfg: cfg
    } do
      {conn, state} = new_replicator(dir, "a.db")
      id = MemoryStore.inject(cfg, kind: :lost_ack, ops: [:put], reason: :down)
      assert {:error, :down} = write(conn, state, ctx, [{:put, 1, 100}])
      MemoryStore.remove(cfg, id)
      # Same bytes as before because nothing was written in between.
      assert {:ok, %{txid: 1}} = Replicator.sync(state, ctx)
    end

    test "a different batch at a TXID that landed unacknowledged counts as fenced", %{
      ctx: ctx,
      dir: dir,
      cfg: cfg
    } do
      {conn, state} = new_replicator(dir, "a.db")
      state = write!(conn, state, ctx, [{:put, 1, 100}])
      id = MemoryStore.inject(cfg, kind: :lost_ack, ops: [:put], reason: :down)
      assert {:error, :down} = write(conn, state, ctx, [{:put, 2, 100}])
      MemoryStore.remove(cfg, id)
      assert {:error, :fenced} = write(conn, state, ctx, [{:put, 3, 100}])
    end

    test "the deadline bounds the retries", %{ctx: ctx, dir: dir, cfg: cfg} do
      {conn, state} = new_replicator(dir, "a.db")
      MemoryStore.inject(cfg, kind: :error, ops: [:put])
      safe_op(conn, {:put, 1, 100})

      {us, result} =
        :timer.tc(fn ->
          Replicator.sync(state, ctx, deadline: System.monotonic_time(:millisecond) + 30)
        end)

      assert {:error, :injected} = result
      assert us < 500_000
    end

    test "a crash after the put leaves the segment in place", %{
      ctx: ctx,
      dir: dir,
      mod: mod,
      cfg: cfg
    } do
      {conn, state} = new_replicator(dir, "a.db")
      CrashPoints.arm(:repl_after_put, :raise)
      assert_raise CrashPoints.Crash, fn -> write(conn, state, ctx, [{:put, 1, 100}]) end
      assert {:ok, _, _} = mod.head(cfg, Keys.segment(ctx.prefix, "cell", 0, 1, 1))
    end
  end

  def forward(event, measurements, meta, test), do: send(test, {event, measurements, meta})

  defp restore_to(ctx, dir) do
    path = Path.join(dir, "restored-#{System.unique_integer([:positive])}")
    {:ok, result} = Replication.restore(ctx, "cell", path, :latest)
    {path, result}
  end
end
