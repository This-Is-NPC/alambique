defmodule Alambique.RestoreMemoryCapTest do
  @moduledoc """
  A restore holds at most `max_inflight_bytes` of what it reads from the store
  (design §6.3), or one object when that is larger, plus the fixed overhead of
  its parallel first reads.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.ReplicationHelpers

  alias Alambique.Replication
  alias Alambique.Replication.{Hydrator, Lazy}
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @edge 4096
  @concurrency 2

  defmodule PeakStore do
    @moduledoc """
    Counts the bytes of whole objects the store has returned and the restore
    has not yet applied, and keeps the largest count. A ranged read that
    returns the whole object counts too; a head of a larger one does not,
    because the restore drops it.
    """
    @behaviour Alambique.Store

    def new(inner) do
      %{
        inner: inner,
        fetched: :atomics.new(1, []),
        applied: :atomics.new(1, []),
        peak: :atomics.new(1, [])
      }
    end

    def peak(%{peak: peak}), do: :atomics.get(peak, 1)
    def applied(%{applied: applied}, bytes), do: :atomics.put(applied, 1, bytes)

    defp note(%{fetched: fetched, applied: applied, peak: peak}, bytes) do
      held = :atomics.add_get(fetched, 1, bytes) - :atomics.get(applied, 1)
      if held > :atomics.get(peak, 1), do: :atomics.put(peak, 1, held)
    end

    @impl true
    def get(%{inner: {m, c}} = cfg, key) do
      with {:ok, body, _, _} = result <- m.get(c, key) do
        note(cfg, byte_size(body))
        result
      end
    end

    @impl true
    def get_range(%{inner: {m, c}} = cfg, key, offset, length) do
      with {:ok, body, total, _} = result <- m.get_range(c, key, offset, length) do
        if offset == 0 and byte_size(body) == total, do: note(cfg, total)
        result
      end
    end

    @impl true
    def put(%{inner: {m, c}}, key, body, condition, meta),
      do: m.put(c, key, body, condition, meta)

    @impl true
    def head(%{inner: {m, c}}, key), do: m.head(c, key)
    @impl true
    def list(%{inner: {m, c}}, prefix, after_key, limit), do: m.list(c, prefix, after_key, limit)
    @impl true
    def delete(%{inner: {m, c}}, key), do: m.delete(c, key)
  end

  defmodule FlightStore do
    @moduledoc """
    Slows the ranged reads and keeps the largest number of bytes asked for by
    the reads that were under way at once.
    """
    @behaviour Alambique.Store

    def new(inner), do: %{inner: inner, now: :atomics.new(1, []), peak: :atomics.new(1, [])}
    def peak(%{peak: peak}), do: :atomics.get(peak, 1)

    @impl true
    def get_range(%{inner: {m, c}, now: now, peak: peak}, key, offset, length) do
      held = :atomics.add_get(now, 1, length)
      if held > :atomics.get(peak, 1), do: :atomics.put(peak, 1, held)
      Process.sleep(5)
      m.get_range(c, key, offset, length)
    after
      :atomics.sub(now, 1, length)
    end

    @impl true
    def get(%{inner: {m, c}}, key), do: m.get(c, key)
    @impl true
    def put(%{inner: {m, c}}, key, body, condition, meta),
      do: m.put(c, key, body, condition, meta)

    @impl true
    def head(%{inner: {m, c}}, key), do: m.head(c, key)
    @impl true
    def list(%{inner: {m, c}}, prefix, after_key, limit), do: m.list(c, prefix, after_key, limit)
    @impl true
    def delete(%{inner: {m, c}}, key), do: m.delete(c, key)
  end

  # Commits one blob of each size, so that the cell has one segment per size.
  defp seed(sizes) do
    {_pid, {_, cfg} = store} = start_store()
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 10_000)
    {conn, state} = new_replicator(dir, "src.db")

    Enum.reduce(Enum.with_index(sizes, 1), state, fn {n, k}, state ->
      run_op(conn, {:put, k, n})
      {:ok, state} = Replication.Replicator.sync(state, ctx)
      state
    end)

    {conn, dir, ctx, cfg}
  end

  # The segments with their sizes, to know what a restore has applied.
  defp segment_sizes(ctx, cfg) do
    {:ok, {segments, _}} =
      Replication.Objects.list_cell(ctx, "cell", System.monotonic_time(:millisecond) + 5_000)

    for seg <- segments do
      {:ok, body, _, _} = MemoryStore.get(cfg, seg.key)
      {seg.max, byte_size(body)}
    end
  end

  defp restore_with_cap(ctx, inner, sizes, cap, dest) do
    store = PeakStore.new(inner)

    CrashPoints.arm(
      :repl_mid_restore,
      fn _name, %{txid: txid} ->
        PeakStore.applied(
          store,
          for({max, bytes} <- sizes, max <= txid, reduce: 0, do: (acc -> acc + bytes))
        )
      end,
      times: :infinity
    )

    ctx =
      Map.merge(ctx, %{
        store: {PeakStore, store},
        max_inflight_bytes: cap,
        edge_bytes: @edge,
        fetch_concurrency: @concurrency
      })

    result = Replication.restore(ctx, "cell", dest, :latest)
    CrashPoints.reset()
    {result, PeakStore.peak(store)}
  end

  setup do
    on_exit(&CrashPoints.reset/0)
  end

  test "a restore does not read ahead more than the cap" do
    {conn, dir, ctx, cfg} = seed(List.duplicate(300_000, 6))
    sizes = segment_sizes(ctx, cfg)
    cap = 700_000

    {result, peak} =
      restore_with_cap(ctx, {MemoryStore, cfg}, sizes, cap, Path.join(dir, "dest.db"))

    assert {:ok, %{txid: 6}} = result
    assert dump_file(Path.join(dir, "dest.db")) == dump(conn)
    assert peak <= cap + @concurrency * @edge
  end

  property "the bytes a restore holds never pass the cap or its largest object" do
    check all(
            sizes <-
              list_of(member_of([100, 5_000, 40_000, 200_000, 400_000]),
                min_length: 1,
                max_length: 8
              ),
            cap <- member_of([1, 50_000, 300_000, 1_000_000, 50_000_000]),
            max_runs: 12
          ) do
      {conn, dir, ctx, cfg} = seed(sizes)
      dest = Path.join(dir, "dest.db")

      try do
        segments = segment_sizes(ctx, cfg)
        {result, peak} = restore_with_cap(ctx, {MemoryStore, cfg}, segments, cap, dest)
        largest = segments |> Enum.map(&elem(&1, 1)) |> Enum.max()

        assert {:ok, %{txid: txid, checksum: checksum}} = result
        assert txid == length(sizes)
        assert peak <= max(cap, largest) + @concurrency * @edge

        # The same database as a restore with no cap in the way.
        assert dump_file(dest) == dump(conn)
        unbounded = Path.join(dir, "unbounded.db")

        assert {:ok, %{checksum: ^checksum}} =
                 Replication.restore(ctx, "cell", unbounded, :latest)

        assert dump_file(unbounded) == dump(conn)
      after
        Alambique.Sqlite.close(conn)
        File.rm_rf(dir)
      end
    end
  end

  test "the reads of a hydration under way stay under the cap" do
    {conn, dir, ctx, cfg} = seed(List.duplicate(300_000, 6))
    flights = FlightStore.new({MemoryStore, cfg})
    cap = 300_000
    ctx = Map.merge(ctx, %{store: {FlightStore, flights}, max_inflight_bytes: cap})
    dest = Path.join(dir, "lazy.db")

    assert {:ok, result} =
             Replication.restore(ctx, "cell", dest, :latest,
               lazy: %{min_bytes: 0, wait_ms: 5_000}
             )

    # Reads of the restore itself are not the hydration's.
    flights = %{flights | peak: :atomics.new(1, [])}
    ctx = %{ctx | store: {FlightStore, flights}}
    lazy = Lazy.new(result, ctx, miss_pages: 4, batch_bytes: 200_000)
    {:ok, hydrator} = Hydrator.start_link(lazy, concurrency: 4, fetch_ms: 5_000, poll_ms: 10)

    assert_receive {:hydrated, ^hydrator}, 10_000
    assert FlightStore.peak(flights) <= cap
    assert dump_file(dest) == dump(conn)
  end

  test "a lazy restore under a cap too small for its edges still builds the same database" do
    {conn, dir, ctx, _cfg} = seed(List.duplicate(5_000, 6))
    dest = Path.join(dir, "lazy.db")
    ctx = Map.merge(ctx, %{max_inflight_bytes: 3 * @edge, edge_bytes: @edge})

    assert {:ok, result} =
             Replication.restore(ctx, "cell", dest, :latest,
               lazy: %{min_bytes: 0, wait_ms: 5_000}
             )

    lazy = Lazy.new(result, ctx, miss_pages: 4, batch_bytes: 200_000)
    {:ok, hydrator} = Hydrator.start_link(lazy, concurrency: 4, fetch_ms: 5_000, poll_ms: 10)

    assert_receive {:hydrated, ^hydrator}, 10_000
    assert dump_file(dest) == dump(conn)
  end
end
