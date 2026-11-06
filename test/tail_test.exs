defmodule Alambique.TailTest do
  @moduledoc """
  A tail past a gap is ignored, and only a lower-epoch object is overwritten
  (design §6, R3). Tails are written straight into the store; the gated store
  decides in which order uploads land.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [setup_store: 0, start_instance: 1]
  import Alambique.CompactionHelpers
  import Alambique.ReplicationHelpers, only: [safe_op: 2]

  alias Alambique.{Compaction, Replication}
  alias Alambique.Replication.{Chain, Keys, Replicator}
  alias AlambiqueTestkit.{GatedStore, MemoryStore}

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  defp key(env, txid, level \\ 0), do: Keys.segment(env.ctx.prefix, cell(), level, txid, txid)
  defp delete_segment(env, txid), do: env.mod.delete(env.cfg, key(env, txid))

  # `ctx` with every put of a level-0 file held until the test says so.
  defp gated(env) do
    gate = start_supervised!(GatedStore)
    GatedStore.hold(gate, ~r{/0000/})
    {gate, %{env.ctx | store: GatedStore.wrap(gate, {env.mod, env.cfg})}}
  end

  describe "a tail past a gap" do
    test "restore ends at the head and Head agrees", env do
      owner = history(env, new_owner(env, "a.db"), 5)
      delete_segment(env, 3)

      assert {:ok, %{txid: 2, checksum: checksum, epoch: 1}, dump} = restore(env, :latest)
      assert dump == owner.dumps[2]
      assert {:ok, %{txid: 2, checksum: ^checksum, epoch: 1}} = Replication.head(env.ctx, cell())

      # Targets are served from the chain; one inside the tail is not reached.
      assert {:ok, %{txid: 2}, _} = restore(env, {:txid, 2})
      assert {:error, {:target_not_reached, 2}} = restore(env, {:txid, 5})
    end

    test "the window is inclusive", env do
      # The hole at 3 leaves files up to 18 = head (2) + window.
      history(env, new_owner(env, "a.db"), 18)
      delete_segment(env, 3)
      assert {:ok, %{txid: 2}, _} = restore(env, :latest)
      assert {:ok, %{txid: 2}} = Replication.head(env.ctx, cell())
    end

    test "a zombie right after the fence is ignored", env do
      {_a, b} = orphaned(env)

      assert {:ok, %{txid: 4}, dump} = restore(env, :latest)
      assert dump == b.dumps[4]
      assert {:ok, %{txid: 4, epoch: 2}} = Replication.head(env.ctx, cell())
    end

    test "a zombie after a snapshot is not the head", env do
      a = history(env, new_owner(env, "a.db"), 3)
      {:ok, _} = Replicator.snapshot(a.state, env.ctx)
      b = take_over(env, "b.db", 2, a.dumps)
      safe_op(a.conn, {:put, 9, 100})
      {:ok, _} = Replicator.sync(%{a.state | txid: b.state.txid}, env.ctx)

      assert {:ok, %{txid: 4, epoch: 2}} = Replication.head(env.ctx, cell())
    end

    test "the chain is the same before and after a late upload lands", env do
      a = history(env, new_owner(env, "a.db"), 3)
      b = take_over(env, "b.db", 2, a.dumps)
      safe_op(a.conn, {:put, 9, 100})
      {gate, late_ctx} = gated(env)
      zombie = key(env, b.state.txid + 1)

      task = Task.async(fn -> Replicator.sync(%{a.state | txid: b.state.txid}, late_ctx) end)
      :ok = GatedStore.await(gate, zombie)
      assert {:ok, %{txid: 4}, _} = restore(env, :latest)
      assert {:error, :not_found} = env.mod.get(env.cfg, zombie)

      :ok = GatedStore.release(gate, zombie)
      assert {:ok, _} = Task.await(task)
      assert {:ok, _, _, %{"epoch" => "1"}} = env.mod.get(env.cfg, zombie)
      assert {:ok, %{txid: 4}, dump} = restore(env, :latest)
      assert dump == b.dumps[4]
      assert {:ok, %{txid: 4}} = Replication.head(env.ctx, cell())
    end
  end

  describe "what stays loud" do
    test "a file past the window", env do
      history(env, new_owner(env, "a.db"), 19)
      delete_segment(env, 3)
      assert {:error, {:gap, 3}} = restore(env, :latest)
    end

    test "a merged file past a gap", env do
      history(env, new_owner(env, "a.db"), 6)
      {:ok, _} = Compaction.run(env.ctx, cell(), fanout: 2, max_level: 1)
      delete_segment(env, 3)
      env.mod.delete(env.cfg, Keys.segment(env.ctx.prefix, cell(), 1, 3, 4))

      assert {:error, {:gap, 3}} = restore(env, :latest)
    end

    test "a lower epoch with a file past the window", env do
      {_a, _b} = orphaned(env)
      # The zombie is at 5. A file at 4 + window is still part of a tail.
      {:ok, _} = env.mod.put(env.cfg, key(env, 4 + 16), "x", :none, %{"epoch" => "1"})
      assert {:ok, %{txid: 4}, _} = restore(env, :latest)

      {:ok, _} =
        env.mod.put(env.cfg, key(env, 5 + 16), "x", :none, %{"epoch" => "1"})

      assert {:error, {:epoch_regression, zombie, 1, 2}} = restore(env, :latest)
      assert zombie == key(env, 5)
    end
  end

  describe "the put rule" do
    test "the next write overwrites a zombie", env do
      {_a, b} = orphaned(env)
      zombie = key(env, 5)
      {:ok, old, _, %{"epoch" => "1"}} = env.mod.get(env.cfg, zombie)

      b = write!(env, b, [{:put, 1, 500}])

      assert b.state.txid == 5
      assert {:ok, new, _, %{"epoch" => "2"}} = env.mod.get(env.cfg, zombie)
      assert new != old
      assert {:ok, %{txid: 5}, dump} = restore(env, :latest)
      assert dump == b.dumps[5]
    end

    test "the wake fence overwrites a zombie at its TXID", env do
      a = history(env, new_owner(env, "a.db"), 2)
      {:ok, _} = env.mod.put(env.cfg, key(env, 3), "x", :if_none_match, %{"epoch" => "0"})

      b = take_over(env, "b.db", 2, a.dumps)

      assert b.state.txid == 3
      assert {:ok, body, _, %{"epoch" => "2"}} = env.mod.get(env.cfg, key(env, 3))
      assert body != "x"
    end

    test "an object of the same or a newer epoch is never overwritten", env do
      {_a, b} = orphaned(env)

      for epoch <- ["2", "3"] do
        env.mod.delete(env.cfg, key(env, 5))
        {:ok, _} = env.mod.put(env.cfg, key(env, 5), "x", :if_none_match, %{"epoch" => epoch})

        safe_op(b.conn, {:put, 1, 500})
        assert {:error, :fenced} = Replicator.sync(b.state, env.ctx)
        assert {:ok, "x", _, %{"epoch" => ^epoch}} = env.mod.get(env.cfg, key(env, 5))
      end
    end

    test "the overwrite is conditional on the object it read", env do
      {_a, b} = orphaned(env)
      safe_op(b.conn, {:put, 1, 500})
      {gate, late_ctx} = gated(env)
      target = key(env, 5)

      task = Task.async(fn -> Replicator.sync(b.state, late_ctx) end)
      # The create is rejected; the overwrite is held.
      :ok = GatedStore.release(gate, target)
      :ok = GatedStore.await(gate, target)

      # A newer owner takes the TXID meanwhile: the overwrite must not land.
      {:ok, _} = env.mod.put(env.cfg, target, "newer", :none, %{"epoch" => "3"})
      :ok = GatedStore.release(gate, target)
      :ok = GatedStore.release(gate, target)

      assert {:error, :fenced} = Task.await(task)
      assert {:ok, "newer", _, %{"epoch" => "3"}} = env.mod.get(env.cfg, target)
    end
  end

  describe "Chain" do
    test "the window is 16, the cap of the pipeline depth" do
      assert Chain.window() == 16
    end

    test "tail?/3" do
      single = &%{level: 0, min: &1, max: &1}
      assert Chain.tail?([], [], 5)
      assert Chain.tail?([single.(7), single.(9)], [], 5)
      assert Chain.tail?([single.(5 + 16)], [], 5)
      refute Chain.tail?([single.(6 + 16)], [], 5)
      refute Chain.tail?([%{level: 1, min: 7, max: 7}], [], 5)
      refute Chain.tail?([%{level: 0, min: 7, max: 8}], [], 5)
      refute Chain.tail?([single.(7)], [%{txid: 7}], 5)
    end

    test "live/1 stops before the first lower epoch" do
      assert Chain.live([]) == []
      assert Chain.live([{:a, 1}, {:b, 1}, {:c, 2}, {:d, 2}]) == [:a, :b, :c, :d]
      assert Chain.live([{:a, 1}, {:b, 2}, {:c, 1}, {:d, 3}]) == [:a, :b]
    end
  end

  test "the gated store passes other requests and orders the held puts" do
    {:ok, pid} = MemoryStore.start_link()
    inner = {MemoryStore, MemoryStore.config(pid)}
    gate = start_supervised!(GatedStore)
    {mod, config} = GatedStore.wrap(gate, inner)
    GatedStore.hold(gate, ~r{/held/})

    assert {:ok, _} = mod.put(config, "free", "x", :none, %{})
    one = Task.async(fn -> mod.put(config, "k/held/1", "1", :none, %{}) end)
    two = Task.async(fn -> mod.put(config, "k/held/2", "2", :none, %{}) end)
    :ok = GatedStore.await(gate, "k/held/1")
    :ok = GatedStore.await(gate, "k/held/2")
    assert {:ok, [], nil} = mod.list(config, "k/", nil, 10)

    :ok = GatedStore.fail(gate, "k/held/1")
    :ok = GatedStore.lose_ack(gate, "k/held/2")
    assert {:error, :gated} = Task.await(one)
    assert {:error, :lost_ack} = Task.await(two)
    assert {:ok, ["k/held/2"], nil} = mod.list(config, "k/", nil, 10)
  end
end
