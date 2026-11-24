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

  defmodule FixedClock do
    @moduledoc false
    @behaviour Alambique.Clock

    @impl true
    def now_ms(ms), do: ms
  end

  setup do
    {:ok, pid} = start_supervised(MemoryStore, restart: :temporary)
    on_exit(&AlambiqueTestkit.CrashPoints.reset/0)
    {:ok, store: {MemoryStore, MemoryStore.config(pid)}, pid: pid}
  end

  defp ctx(store, world, prefix, offset),
    do: node_ctx(store, world, prefix, offset: offset, ttl_ms: @ttl, skew_margin_ms: @margin)

  # The context of `ctx` with a clock that stands still at `ms`.
  defp frozen(ctx, ms), do: %{ctx | clock: {FixedClock, ms}}

  # Distances from the expiry of a lease: one below, at and one above each of
  # its three edges, `expiry - margin`, `expiry` and `expiry + margin`.
  defp edge_deltas, do: for(edge <- [-@margin, 0, @margin], step <- [-1, 0, 1], do: edge + step)

  # Generators

  defp delta_gen, do: one_of([member_of(edge_deltas()), integer((-2 * @margin)..(2 * @margin))])

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

  defp ahead_gen(left) do
    threshold = 2 * @margin + left

    one_of([
      integer(0..(6 * @margin)),
      map(member_of([-1, 0, 1]), &max(0, threshold + &1))
    ])
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

      finals =
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
      reader = ctx(store, world, prefix, 0)

      assert_l1(events, Lease.read(reader, "c"))
      assert_foreign_refused(reader, events)

      for {lease, offset} <- Enum.zip(finals, schedule.offsets), lease != nil do
        assert_validity_edges(ctx(store, world, prefix, offset), lease)
      end
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
        :ets.insert(log, [{:acquire, new}, {:plain, new}])
        new

      _ ->
        lease
    end
  end

  defp step(:take_over, lease, ctx, owner, log) do
    case Lease.take_over(ctx, "c", owner) do
      {:ok, new, _previous} ->
        :ets.insert(log, [{:acquire, new}, {:taken, new}])
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

  defp assert_l1(all, final) do
    {marks, events} = Enum.split_with(all, &(elem(&1, 0) in [:plain, :taken]))
    assert_successors(events, marks)

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

  # Every lease a node got back from the store, by the writes that made it.
  defp written(events) do
    Enum.flat_map(events, fn
      {:acquire, lease} -> [lease]
      {:renew, _old, new} -> [new]
      {:release, _lease} -> []
      {kind, _lease} when kind in [:plain, :taken] -> []
    end)
  end

  # An acquisition that follows a lease we saw written took its epoch + 1. An
  # acquire (not a take-over) did so at its own clock past the expiry of that
  # lease plus the margin: the clock of the attempt is `expires_at_ms - ttl`.
  defp assert_successors(events, marks) do
    written = events |> written() |> Map.new(&{&1.seq, &1})

    for {kind, lease} <- marks, {:ok, prev} <- [Map.fetch(written, lease.seq - 1)] do
      assert lease.epoch == prev.epoch + 1,
             "#{kind}: epoch #{lease.epoch} after epoch #{prev.epoch}"

      if kind == :plain do
        attempted = lease.expires_at_ms - @ttl

        assert attempted > prev.expires_at_ms + @margin,
               "acquired at #{attempted}, before the expiry #{prev.expires_at_ms} plus the margin"
      end
    end
  end

  # A holder of an older acquisition can neither renew nor release the stored
  # lease, and the attempt leaves it as it was.
  defp assert_foreign_refused(ctx, events) do
    with {:ok, stored} <- Lease.read(ctx, "c") do
      stale = for lease <- written(events), lease.token != stored.token, do: lease

      for lease <- Enum.take(stale, 6) do
        assert {:error, :lost} = Lease.renew(ctx, lease)
        assert {:ok, ^stored} = Lease.read(ctx, "c")
        assert {:error, :lost} = Lease.release(ctx, lease)
        assert {:ok, ^stored} = Lease.read(ctx, "c")
      end
    end
  end

  # The owner may use a lease before `expires_at_ms - skew_margin_ms` only.
  defp assert_validity_edges(ctx, lease) do
    for delta <- edge_deltas() do
      at = frozen(ctx, lease.expires_at_ms + delta)

      assert Lease.valid?(at, lease) == delta < -@margin, "valid? at #{delta} ms from the expiry"
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

  property "an acquirer takes the cell exactly past expiry plus the margin", %{store: store} do
    # The owner is still valid (`left` ms before its validity ends) while the
    # acquirer's clock reads `ahead` ms more than the owner's. The acquirer is
    # past the expiry plus the margin when `ahead > 2 * margin + left`: only
    # then do two nodes overlap.
    check all(left <- integer(1..(3 * @margin)), ahead <- ahead_gen(left), max_runs: 300) do
      {_, store_config} = store
      MemoryStore.clear(store_config)
      world = ManualClock.new(@start)
      prefix = unique_prefix("overlap")
      a = ctx(store, world, prefix, 0)
      b = ctx(store, world, prefix, ahead)

      {:ok, lease} = Lease.acquire(a, "c", "a")
      ManualClock.advance(world, @ttl - @margin - left)
      assert Lease.valid?(a, lease)

      answer = Lease.acquire(b, "c", "b")

      if ahead > 2 * @margin + left do
        assert {:ok, %Lease{epoch: 2, owner: "b"}} = answer
      else
        assert {:error, {:owned, "a", expires}} = answer
        assert expires == lease.expires_at_ms
      end
    end
  end

  property "validity and acquisition meet at the edges of the expiry", %{store: store} do
    check all(offset <- integer(-5_000..5_000), delta <- delta_gen(), max_runs: 300) do
      {_, store_config} = store
      MemoryStore.clear(store_config)
      world = ManualClock.new(@start)
      prefix = unique_prefix("edges")
      a = ctx(store, world, prefix, offset)
      b = ctx(store, world, prefix, offset)

      {:ok, lease} = Lease.acquire(a, "c", "a")
      # Both clocks read `delta` ms from the expiry of the lease.
      ManualClock.advance(world, @ttl + delta)

      owner_valid? = Lease.valid?(a, lease)
      assert owner_valid? == delta < -@margin, "owner validity #{delta} ms from the expiry"

      answer = Lease.acquire(b, "c", "b")
      {:ok, stored} = Lease.read(b, "c")

      if delta > @margin do
        assert {:ok, %Lease{epoch: epoch, owner: "b"}} = answer
        assert epoch == lease.epoch + 1
      else
        assert {:error, {:owned, "a", expires}} = answer
        assert expires == lease.expires_at_ms
        assert {stored.token, stored.epoch, stored.seq} == {lease.token, lease.epoch, lease.seq}
      end

      # Never both at once.
      refute owner_valid? and match?({:ok, _}, answer)

      # An early take-over ignores the clock and advances the epoch by one.
      assert {:ok, taken, previous} = Lease.take_over(b, "c", "b")
      assert previous.epoch == stored.epoch
      assert taken.epoch == stored.epoch + 1
    end
  end
end
