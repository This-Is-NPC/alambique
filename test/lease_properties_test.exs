defmodule Alambique.LeasePropertiesTest do
  @moduledoc "Invariant L1 (design §5) and the fencing and clock-skew guarantees."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.LeaseHelpers

  alias Alambique.Lease
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @start 1_000_000_000
  @ttl 10_000
  @margin 1_000

  setup do
    {:ok, pid} = start_supervised(MemoryStore, restart: :temporary)
    on_exit(&AlambiqueTestkit.CrashPoints.reset/0)
    {:ok, store: {MemoryStore, MemoryStore.config(pid)}, pid: pid}
  end

  defp ctx(store, world, prefix, offset),
    do: node_ctx(store, world, prefix, offset: offset, ttl_ms: @ttl, skew_margin_ms: @margin)

  # Generators

  defp op_gen do
    one_of([
      constant(:acquire),
      constant(:take_over),
      constant(:renew),
      constant(:release),
      map(integer(0..(2 * @ttl)), &{:advance, &1})
    ])
  end

  defp fault_gen do
    gen all(
          kind <- member_of([:error, :timeout, :lost_ack, :latency]),
          ops <- member_of([[:put], [:get], [:put, :get]]),
          times <- integer(1..4),
          ms <- integer(0..3)
        ) do
      [kind: kind, ops: ops, times: times, ms: ms]
    end
  end

  defp schedule_gen do
    gen all(
          nodes <- integer(2..5),
          offsets <- list_of(integer((-3 * @margin)..(3 * @margin)), length: nodes),
          scripts <- list_of(list_of(op_gen(), min_length: 3, max_length: 12), length: nodes),
          faults <- list_of(fault_gen(), max_length: 6)
        ) do
      %{offsets: offsets, scripts: scripts, faults: faults}
    end
  end

  property "L1: epochs are unique per owner and increase across random concurrent schedules, also with take_over",
           %{
             store: {_, store_config} = store
           } do
    check all(schedule <- schedule_gen(), max_runs: 100) do
      MemoryStore.clear(store_config)
      world = ManualClock.new(@start)
      prefix = unique_prefix("l1")
      log = :ets.new(:grants, [:public, :duplicate_bag])
      Alambique.Fault.install(fn _, _ -> :erlang.yield() end)
      for fault <- schedule.faults, do: MemoryStore.inject(store_config, fault)

      schedule.scripts
      |> Enum.zip(schedule.offsets)
      |> Enum.with_index()
      |> Enum.map(fn {{script, offset}, i} ->
        Task.async(fn ->
          run_script(script, ctx(store, world, prefix, offset), "node#{i}", log)
        end)
      end)
      |> Task.await_many(30_000)

      events = :ets.tab2list(log)
      :ets.delete(log)
      MemoryStore.clear(store_config)

      assert_l1(events, Lease.read(ctx(store, world, prefix, 0), "c"))
    end
  end

  defp run_script(script, ctx, owner, log) do
    Enum.reduce(script, nil, fn op, lease -> step(op, lease, ctx, owner, log) end)
  end

  defp step({:advance, ms}, lease, ctx, _owner, _log) do
    {ManualClock, %{world: world}} = ctx.clock
    ManualClock.advance(world, ms)
    lease
  end

  defp step(:acquire, lease, ctx, owner, log) do
    case Lease.acquire(ctx, "c", owner) do
      {:ok, new} ->
        :ets.insert(log, {:acquire, new})
        new

      _ ->
        lease
    end
  end

  defp step(:take_over, lease, ctx, owner, log) do
    case Lease.take_over(ctx, "c", owner) do
      {:ok, new, _previous} ->
        :ets.insert(log, {:acquire, new})
        new

      _ ->
        lease
    end
  end

  defp step(:renew, nil, _ctx, _owner, _log), do: nil

  defp step(:renew, lease, ctx, _owner, log) do
    case Lease.renew(ctx, lease) do
      {:ok, new} ->
        :ets.insert(log, {:renew, lease, new})
        new

      {:error, :lost} ->
        nil

      {:error, _} ->
        lease
    end
  end

  defp step(:release, nil, _ctx, _owner, _log), do: nil

  defp step(:release, lease, ctx, _owner, log) do
    case Lease.release(ctx, lease) do
      :ok ->
        :ets.insert(log, {:release, lease})
        nil

      {:error, :lost} ->
        nil

      {:error, _} ->
        lease
    end
  end

  defp assert_l1(events, final) do
    acquires = for {:acquire, l} <- events, do: l

    # No two different tokens were ever granted the same (cell, epoch).
    for {_epoch, group} <- Enum.group_by(acquires, & &1.epoch) do
      assert group |> Enum.map(& &1.token) |> Enum.uniq() |> length() == 1
    end

    # Writes form one chain, so seq orders the acquisitions in time; the
    # epoch must strictly increase along it.
    by_seq = Enum.sort_by(acquires, & &1.seq)
    assert Enum.uniq_by(by_seq, & &1.seq) == by_seq
    epochs = Enum.map(by_seq, & &1.epoch)
    assert epochs == Enum.uniq(epochs)
    assert epochs == Enum.sort(epochs)

    # Renew never changes the epoch or the owner token.
    for {:renew, old, new} <- events do
      assert {new.epoch, new.token} == {old.epoch, old.token}
      assert new.seq > old.seq
    end

    # Fencing: nothing is confirmed for an epoch after a higher epoch landed.
    for {kind, old, new} <- Enum.map(events, &normalize/1),
        %{epoch: higher, seq: higher_seq} <- acquires,
        higher > old.epoch do
      assert max(old.seq, new.seq) < higher_seq, "#{kind} confirmed after epoch #{higher}"
    end

    case final do
      {:ok, stored} -> assert stored.epoch >= Enum.max([0 | epochs])
      :none -> assert acquires == []
    end
  end

  defp normalize({:acquire, l}), do: {:acquire, l, l}
  defp normalize({:renew, old, new}), do: {:renew, old, new}
  defp normalize({:release, l}), do: {:release, l, l}

  property "fencing: a stale owner can never renew or release after a higher epoch", %{
    store: store
  } do
    check all(
            offset_a <- integer(-5_000..5_000),
            extra <- integer(2..(3 * @margin)),
            y <- integer(1..(extra - 1)),
            release_first? <- boolean(),
            fail_a_put? <- boolean(),
            max_runs: 200
          ) do
      {_, store_config} = store
      MemoryStore.clear(store_config)
      world = ManualClock.new(@start)
      prefix = unique_prefix("fence")

      # B's clock runs ahead of A's by more than twice the margin.
      gap = 2 * @margin + extra
      a = ctx(store, world, prefix, offset_a)
      b = ctx(store, world, prefix, offset_a + gap)

      {:ok, stale} = Lease.acquire(a, "c", "a")
      ManualClock.advance(world, @ttl - @margin - extra + y)

      assert Lease.valid?(a, stale), "A still believes its lease is valid"
      assert {:ok, %Lease{epoch: 2} = current} = Lease.acquire(b, "c", "b")

      current = if release_first?, do: release_and_keep(b, current), else: current
      if fail_a_put?, do: MemoryStore.inject(store_config, kind: :lost_ack, ops: [:put], times: 1)

      assert {:error, :lost} = Lease.renew(a, stale)
      assert {:error, :lost} = Lease.release(a, stale)
      assert {:ok, %Lease{epoch: 2, owner: "b"}} = Lease.read(b, "c")
      assert current.token != stale.token
    end
  end

  defp release_and_keep(ctx, lease) do
    :ok = Lease.release(ctx, lease)
    lease
  end

  property "skew within the margin: owner validity ends before others see expiry", %{store: store} do
    check all(
            offset_a <- integer(-@margin..@margin),
            offset_b <- integer(-@margin..@margin),
            renew_after <- integer(0..(@ttl - @margin - 1)),
            elapsed <- integer(0..(@ttl + 3 * @margin)),
            max_runs: 300
          ) do
      {_, store_config} = store
      MemoryStore.clear(store_config)
      world = ManualClock.new(@start)
      prefix = unique_prefix("skew")
      a = ctx(store, world, prefix, offset_a)
      b = ctx(store, world, prefix, offset_b)

      {:ok, lease} = Lease.acquire(a, "c", "a")
      ManualClock.advance(world, renew_after)
      {:ok, lease} = Lease.renew(a, lease)
      ManualClock.advance(world, elapsed)

      if Lease.valid?(a, lease) do
        assert {:error, {:owned, "a", _}} = Lease.acquire(b, "c", "b")
      end
    end
  end

  property "skew beyond the margin is what lets two nodes overlap", %{store: store} do
    # Sanity check of the generator above: with a gap over 2 * margin there are
    # times when the owner is valid and another node acquires.
    {_, store_config} = store
    world = ManualClock.new(@start)
    prefix = unique_prefix("overlap")
    a = ctx(store, world, prefix, 0)
    b = ctx(store, world, prefix, 2 * @margin + 100)

    {:ok, lease} = Lease.acquire(a, "c", "a")
    ManualClock.advance(world, @ttl - @margin - 50)
    assert Lease.valid?(a, lease)
    assert {:ok, %Lease{epoch: 2}} = Lease.acquire(b, "c", "b")
    MemoryStore.clear(store_config)
  end
end
