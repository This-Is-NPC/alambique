defmodule Alambique.LeaseFaultsTest do
  use ExUnit.Case, async: false

  import Alambique.LeaseHelpers

  alias Alambique.Lease
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @start 1_000_000_000

  setup do
    on_exit(&CrashPoints.reset/0)
    {:ok, pid} = start_supervised(MemoryStore)
    config = MemoryStore.config(pid)
    world = ManualClock.new(@start)
    prefix = unique_prefix("faults")
    ctx = node_ctx({MemoryStore, config}, world, prefix)
    {:ok, store: config, world: world, ctx: ctx}
  end

  describe "acquire" do
    test "a lost acknowledgement is confirmed by re-reading", %{store: store, ctx: ctx} do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)
      assert {:ok, lease} = Lease.acquire(ctx, "c", "a")
      assert {:ok, ^lease} = Lease.read(ctx, "c")
      assert lease.etag
    end

    test "a failed put that was not applied is an error", %{store: store, ctx: ctx} do
      MemoryStore.inject(store, kind: :error, ops: [:put], times: 1)
      assert {:error, :injected} = Lease.acquire(ctx, "c", "a")
      assert :none = Lease.read(ctx, "c")
      assert {:ok, %Lease{epoch: 1}} = Lease.acquire(ctx, "c", "a")
    end

    test "a timeout on put is an error", %{store: store, ctx: ctx} do
      MemoryStore.inject(store, kind: :timeout, ops: [:put], ms: 1, times: 1)
      assert {:error, :timeout} = Lease.acquire(ctx, "c", "a")
    end

    test "an unreadable lease is an error", %{store: store, ctx: ctx} do
      MemoryStore.inject(store, kind: :error, ops: [:get], times: 1)
      assert {:error, :injected} = Lease.acquire(ctx, "c", "a")
    end

    test "a lost acknowledgement whose confirming read fails is an error", %{
      store: store,
      ctx: ctx
    } do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)

      # Fail the read that confirms the write, not the one that precedes it.
      CrashPoints.arm(:lease_before_put, fn _, _ ->
        MemoryStore.inject(store, kind: :error, ops: [:get], times: 1)
      end)

      assert {:error, :lost_ack} = Lease.acquire(ctx, "c", "a")
    end

    test "losing the race between read and write reports the winner", %{
      ctx: ctx,
      world: world,
      store: store
    } do
      {:ok, _} = Lease.acquire(ctx, "c", "old")
      ManualClock.advance(world, ttl_ms() + skew_ms() + 1)
      rival = node_ctx({MemoryStore, store}, world, ctx.prefix)
      test_pid = self()

      CrashPoints.arm(:lease_before_put, fn _, %{op: :acquire} ->
        # Runs inside our acquire, after its read and before its write.
        send(test_pid, {:rival, Lease.acquire(rival, "c", "rival")})
      end)

      assert {:error, {:owned, "rival", _}} = Lease.acquire(ctx, "c", "a")
      assert_received {:rival, {:ok, %Lease{epoch: 2}}}
    end

    test "a crash right after the write leaves the lease held", %{ctx: ctx, world: world} do
      CrashPoints.arm(:lease_after_put, :raise)
      assert_raise CrashPoints.Crash, fn -> Lease.acquire(ctx, "c", "a") end
      CrashPoints.reset()

      assert {:error, {:owned, "a", _}} = Lease.acquire(ctx, "c", "b")
      ManualClock.advance(world, ttl_ms() + skew_ms() + 1)
      assert {:ok, %Lease{epoch: 2}} = Lease.acquire(ctx, "c", "b")
    end
  end

  describe "take_over" do
    test "losing the race between read and write is contended", %{
      ctx: ctx,
      store: store,
      world: world
    } do
      {:ok, _} = Lease.acquire(ctx, "c", "old")
      rival = node_ctx({MemoryStore, store}, world, ctx.prefix)
      test_pid = self()

      CrashPoints.arm(:lease_before_put, fn _, %{op: :take_over} ->
        # Runs inside our take_over, after its read and before its write.
        send(test_pid, {:rival, Lease.take_over(rival, "c", "rival")})
      end)

      assert {:error, {:contended, "rival", 2}} = Lease.take_over(ctx, "c", "a")
      assert_received {:rival, {:ok, %Lease{epoch: 2}, %{owner: "old"}}}
      assert {:ok, %Lease{epoch: 2, owner: "rival"}} = Lease.read(ctx, "c")
    end

    test "a lost acknowledgement is confirmed by re-reading", %{store: store, ctx: ctx} do
      {:ok, _} = Lease.acquire(ctx, "c", "old")
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)

      assert {:ok, lease, %{owner: "old"}} = Lease.take_over(ctx, "c", "a")
      assert lease.epoch == 2 and lease.etag
      assert {:ok, ^lease} = Lease.read(ctx, "c")
    end

    test "a failed put changes nothing and can be retried", %{store: store, ctx: ctx} do
      {:ok, _} = Lease.acquire(ctx, "c", "old")
      MemoryStore.inject(store, kind: :error, ops: [:put], times: 1)

      assert {:error, :injected} = Lease.take_over(ctx, "c", "a")
      assert {:ok, %Lease{epoch: 1, owner: "old"}} = Lease.read(ctx, "c")
      assert {:ok, %Lease{epoch: 2}, _} = Lease.take_over(ctx, "c", "a")
    end

    test "an unreadable lease is an error", %{store: store, ctx: ctx} do
      MemoryStore.inject(store, kind: :error, ops: [:get], times: 1)
      assert {:error, :injected} = Lease.take_over(ctx, "c", "a")
    end

    test "verify reports an unreadable lease as an error, not lost", %{store: store, ctx: ctx} do
      {:ok, lease} = Lease.acquire(ctx, "c", "a")
      MemoryStore.inject(store, kind: :error, ops: [:get], times: 1)
      assert {:error, :injected} = Lease.verify(ctx, lease)
      assert :ok = Lease.verify(ctx, lease)
    end
  end

  describe "renew" do
    setup %{ctx: ctx} do
      {:ok, lease} = Lease.acquire(ctx, "c", "a")
      {:ok, lease: lease}
    end

    test "a lost acknowledgement adopts the stored renewal", %{
      store: store,
      ctx: ctx,
      lease: lease
    } do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)
      assert {:ok, renewed} = Lease.renew(ctx, lease)
      assert renewed.seq == lease.seq + 1
      assert {:ok, ^renewed} = Lease.read(ctx, "c")
      assert {:ok, %Lease{seq: 3}} = Lease.renew(ctx, renewed)
    end

    test "a failed put leaves the old lease usable", %{store: store, ctx: ctx, lease: lease} do
      MemoryStore.inject(store, kind: :error, ops: [:put], times: 1)
      assert {:error, :injected} = Lease.renew(ctx, lease)
      assert {:ok, %Lease{seq: 2}} = Lease.renew(ctx, lease)
    end

    test "a failed put followed by an unreadable store is an error, not lost", %{
      store: store,
      ctx: ctx,
      lease: lease
    } do
      MemoryStore.inject(store, kind: :error, ops: [:put, :get], times: 2)
      assert {:error, _} = Lease.renew(ctx, lease)
      assert {:ok, _} = Lease.renew(ctx, lease)
    end

    test "retrying after an unconfirmed renewal adopts it", %{
      store: store,
      ctx: ctx,
      lease: lease
    } do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)
      MemoryStore.inject(store, kind: :error, ops: [:get], times: 1)
      assert {:error, :injected} = Lease.renew(ctx, lease)

      assert {:ok, adopted} = Lease.renew(ctx, lease)
      assert adopted.seq == lease.seq + 1
    end

    test "a lost acknowledgement when another node took over is lost", %{
      store: store,
      ctx: ctx,
      world: world,
      lease: lease
    } do
      ManualClock.advance(world, ttl_ms() + skew_ms() + 1)
      {:ok, _} = Lease.acquire(node_ctx({MemoryStore, store}, world, ctx.prefix), "c", "b")

      MemoryStore.inject(store, kind: :error, ops: [:put], times: 1)
      assert {:error, :lost} = Lease.renew(ctx, lease)
    end
  end

  describe "release" do
    setup %{ctx: ctx} do
      {:ok, lease} = Lease.acquire(ctx, "c", "a")
      {:ok, lease: lease}
    end

    test "a lost acknowledgement is confirmed", %{store: store, ctx: ctx, lease: lease} do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)
      assert :ok = Lease.release(ctx, lease)
      assert {:ok, %Lease{released: true}} = Lease.read(ctx, "c")
    end

    test "a failed put can be retried", %{store: store, ctx: ctx, lease: lease} do
      MemoryStore.inject(store, kind: :error, ops: [:put], times: 1)
      assert {:error, :injected} = Lease.release(ctx, lease)
      assert {:ok, %Lease{released: false}} = Lease.read(ctx, "c")
      assert :ok = Lease.release(ctx, lease)
    end

    test "releases on top of a renewal that landed unacknowledged", %{
      store: store,
      ctx: ctx,
      lease: lease
    } do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)
      MemoryStore.inject(store, kind: :error, ops: [:get], times: 1)
      assert {:error, :injected} = Lease.renew(ctx, lease)

      assert :ok = Lease.release(ctx, lease)
      assert {:ok, %Lease{released: true, seq: 3}} = Lease.read(ctx, "c")
    end
  end

  def forward(name, measurements, metadata, pid),
    do: send(pid, {:event, name, measurements, metadata})

  describe "telemetry" do
    setup do
      id = make_ref()
      test_pid = self()

      events =
        for op <- [:acquire, :take_over, :renew, :release, :lost], do: [:alambique, :lease, op]

      :telemetry.attach_many(
        id,
        events,
        &__MODULE__.forward/4,
        test_pid
      )

      on_exit(fn -> :telemetry.detach(id) end)
    end

    test "emits take_over with the replaced lease", %{ctx: ctx} do
      {:ok, _} = Lease.acquire(ctx, "c", "a")
      assert {:ok, _, _} = Lease.take_over(ctx, "c", "b", cause: {:suspect, "a"})

      assert_received {:event, [:alambique, :lease, :take_over], %{duration: _},
                       %{
                         cell: "c",
                         owner: "b",
                         cause: {:suspect, "a"},
                         previous_owner: "a",
                         previous_epoch: 1,
                         result: :ok,
                         epoch: 2
                       }}

      assert {:error, {:epoch_changed, 2}} = Lease.take_over(ctx, "c", "z", epoch: 1)

      assert_received {:event, [:alambique, :lease, :take_over], _,
                       %{result: :epoch_changed, current_epoch: 2, previous_epoch: 2}}
    end

    test "emits acquire, renew, release and lost", %{ctx: ctx, world: world, store: store} do
      {:ok, lease} = Lease.acquire(ctx, "c", "a")

      assert_received {:event, [:alambique, :lease, :acquire], %{duration: d},
                       %{cell: "c", owner: "a", result: :ok, epoch: 1}}

      assert is_integer(d)
      assert {:error, {:owned, "a", _}} = Lease.acquire(ctx, "c", "b")

      assert_received {:event, [:alambique, :lease, :acquire], _,
                       %{owner: "b", result: :owned, current_owner: "a"}}

      {:ok, renewed} = Lease.renew(ctx, lease)
      assert_received {:event, [:alambique, :lease, :renew], _, %{result: :ok, epoch: 1}}

      MemoryStore.inject(store, kind: :error, ops: [:put], times: 1)
      Lease.renew(ctx, renewed)

      assert_received {:event, [:alambique, :lease, :renew], _,
                       %{result: :error, reason: :injected}}

      :ok = Lease.release(ctx, renewed)
      assert_received {:event, [:alambique, :lease, :release], _, %{result: :ok}}

      ManualClock.advance(world, 1)
      {:ok, _} = Lease.acquire(ctx, "c", "b")
      assert {:error, :lost} = Lease.renew(ctx, renewed)

      assert_received {:event, [:alambique, :lease, :lost], %{},
                       %{op: :renew, cell: "c", owner: "a", epoch: 1}}

      assert_received {:event, [:alambique, :lease, :renew], _, %{result: :lost}}
    end
  end
end
