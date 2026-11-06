defmodule Alambique.WakeOverlapTest do
  @moduledoc "A wake does not wait for requests one at a time that do not depend on each other (design §7)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, CountingStore, Lease, Placement, TracingStore}
  alias Alambique.Replication
  alias Alambique.Replication.Objects
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @latency 40
  @commits 14

  setup do
    capture([[:alambique, :cell, :wake]])
    setup_store()
  end

  # A cell with one small segment per commit, hibernated, in the store.
  defp seed(ctx) do
    seeder = start_instance(ctx, migrations: @schema, compaction: false, lazy_restore: false)
    cell = unique_cell()

    for n <- 1..@commits,
        do: {:ok, _} = Alambique.execute(seeder, cell, "INSERT INTO t VALUES (?, 'x')", [n])

    :ok = Alambique.hibernate(seeder, cell)
    drain([:alambique, :cell, :wake], cell)
    {seeder, cell}
  end

  # Wakes `cell` on an instance with no local file, with `@latency` ms on every request.
  defp cold_wake(ctx, cell, opts) do
    {TracingStore, tracing} = traced = TracingStore.wrap(ctx.store)
    reader = start_instance(ctx, [migrations: @schema, compaction: false, store: traced] ++ opts)
    MemoryStore.inject(ctx.store_config, kind: :latency, ms: @latency)
    assert {:ok, _node} = Alambique.touch(reader, cell)
    MemoryStore.clear(ctx.store_config)
    {reader, await_event([:alambique, :cell, :wake], cell), TracingStore.trace(tracing)}
  end

  defp request(trace, op, pattern) do
    case Enum.filter(trace, fn {o, key, _, _} -> o == op and key =~ pattern end) do
      [one] -> one
      other -> flunk("expected one #{op} of #{inspect(pattern)}, got #{length(other)}")
    end
  end

  test "the lease, the frozen check and the listing run together, then the files", ctx do
    {_seeder, cell} = seed(ctx)

    {reader, {measurements, meta}, trace} = cold_wake(ctx, cell, lazy_restore: false)

    assert meta.source == :restore
    lease = request(trace, :put, ~r|/lease$|)
    frozen = request(trace, :head, ~r|/frozen$|)
    listing = request(trace, :list, ~r|/cells/|)
    assert TracingStore.overlap?(lease, frozen)
    assert TracingStore.overlap?(lease, listing)

    # The files of the chain are read at the same time as well, and only the
    # fence waits for all of it.
    reads = Enum.filter(trace, fn {op, key, _, _} -> op == :get and key =~ "/0000/" end)
    assert Enum.count(reads, &TracingStore.overlap?(&1, hd(reads))) > @commits / 2

    {:put, _, fence_from, _} = request(trace, :put, ~r|/0000/|)

    assert Enum.all?(reads, fn {_, _, _, to} -> to <= fence_from end)
    assert elem(lease, 3) <= fence_from

    # Before: one request after the other, about 22 of them (880 ms).
    assert System.convert_time_unit(measurements.duration, :native, :millisecond) < 12 * @latency

    assert {:ok, %{rows: [[@commits]]}} = Alambique.query(reader, cell, "SELECT count(*) FROM t")
  end

  test "a wake reads the lease once and reports its requests", ctx do
    {_seeder, cell} = seed(ctx)

    {_reader, {measurements, meta}, _trace} = cold_wake(ctx, cell, lazy_restore: false)

    # The router's read is the wake's: a second read is not made.
    assert meta.requests[{:get, :lease}] == 1
    assert meta.requests[{:put, :lease}] == 1
    assert meta.requests[{:head, :frozen}] == 1
    assert meta.requests[{:list, :listing}] == 1
    assert meta.requests[{:put, :segment}] == 1
    assert meta.requests[{:get, :segment}] >= @commits

    assert measurements.trips == meta.requests |> Map.values() |> Enum.sum()

    assert measurements.gets ==
             for({{:get, _}, n} <- meta.requests, reduce: 0, do: (acc -> acc + n))

    assert measurements.lists == 1
  end

  test "a reused file reads the epoch of the last segment from the segment itself", ctx do
    instance = start_instance(ctx, migrations: @schema, compaction: false)
    cell = unique_cell()

    for n <- 1..5,
        do: {:ok, _} = Alambique.execute(instance, cell, "INSERT INTO t VALUES (?, 'x')", [n])

    :ok = Alambique.hibernate(instance, cell)
    drain([:alambique, :cell, :wake], cell)

    {:ok, location} = Placement.locate(Config.fetch!(instance), cell)
    ctx_r = Config.repl_ctx(Config.fetch!(instance), location)

    {:ok, {segments, snapshots}} =
      Objects.list_cell(ctx_r, cell, System.monotonic_time(:millisecond) + 5_000)

    files = length(segments) + length(snapshots)

    assert {:ok, _node} = Alambique.touch(instance, cell)
    {_, meta} = await_event([:alambique, :cell, :wake], cell, 5_000, &(&1.source == :local))

    # One HEAD for each file but the last, which one ranged read describes.
    heads =
      for({{:head, kind}, n} <- meta.requests, kind in [:segment, :snapshot], do: n) |> Enum.sum()

    assert heads == min(files, 17) - 1
    assert meta.requests[{:get, :segment}] == 1
  end

  test "a lease read before is not read again, and a stale one is not trusted", ctx do
    instance = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    config = Config.fetch!(instance)
    {:ok, location} = Placement.locate(config, cell)
    lease_ctx = Config.lease_ctx(config, location)
    counting = CountingStore.wrap(lease_ctx.store)
    counted = %{lease_ctx | store: counting}
    {CountingStore, counts} = counting

    {:ok, a} = Lease.acquire(counted, cell, "a")
    :ok = Lease.release(counted, a)
    seen = Lease.read(counted, cell)
    assert {:ok, %{released: true}} = seen

    CountingStore.reset(counts)
    {:ok, b} = Lease.acquire(counted, cell, "b", seen: seen)
    assert b.epoch == a.epoch + 1
    assert CountingStore.count(counts, :get) == 0

    # `seen` is stale now: "b" holds the lease. "c" must not take it.
    assert {:error, {:owned, "b", _}} = Lease.acquire(counted, cell, "c", seen: seen)
    assert {:ok, %{owner: "b", epoch: epoch}} = Lease.read(counted, cell)
    assert epoch == b.epoch
  end

  test "a restore that is too small for a lazy one reads the listing once", ctx do
    {seeder, cell} = seed(ctx)
    counting = CountingStore.wrap(ctx.store)
    {CountingStore, counts} = counting
    repl_ctx = %{repl_ctx(seeder, cell) | store: {Alambique.Store.Guard, counting}}
    dest = Path.join(ctx.dir, "small.db")

    assert {:ok, result} =
             Replication.restore(repl_ctx, cell, dest, :latest,
               lazy: %{min_bytes: 100_000_000, wait_ms: 1_000}
             )

    refute Map.has_key?(result, :lazy)
    assert length(CountingStore.listed(counts)) == 1
  end

  test "a restore holds back what it reads ahead when told to", ctx do
    {_seeder, cell} = seed(ctx)

    {reader, _event, _trace} =
      cold_wake(ctx, cell,
        lazy_restore: false,
        restore: [fetch_concurrency: 1, prefetch_bytes: 1]
      )

    assert {:ok, %{rows: [[@commits]]}} = Alambique.query(reader, cell, "SELECT count(*) FROM t")
  end
end
