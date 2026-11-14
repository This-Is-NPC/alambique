# Wake time of a long chain by `fetch_concurrency` and `max_inflight_bytes`
# (design §6.3).
#
#   mise run bench:restore-concurrency
#   mise run bench:restore-concurrency -- 400    # segments in the chain
#
# One cell with a chain of 800 segments (no compaction, one commit each) is
# restored in full, with the in-memory store adding a fixed delay to every
# request. Each combination runs three times, interleaved; the table shows the
# median wall time of `Replication.restore/4`, the most requests in flight at
# once and the growth of the binary memory of the VM at its peak, sampled every
# 2 ms (an estimate of what the restore holds).
#
#   ALAMBIQUE_BENCH_LATENCY      added latency per request in ms      (default "25")
#   ALAMBIQUE_BENCH_CONCURRENCY  values of fetch_concurrency          (default "1 2 4 8 16")
#   ALAMBIQUE_BENCH_BUDGETS      values of max_inflight_bytes         (default "67108864 2516582")
#   ALAMBIQUE_BENCH_ROW          bytes of random data per commit      (default "1000")
#
# With ALAMBIQUE_BENCH_CELLS set, the bench restores several cells at once
# instead (the wake of a node, design §6.3):
#
#   ALAMBIQUE_BENCH_CELLS    segments of each cell, cycled over the restores
#                            (default "112 164 300 500 830")
#   ALAMBIQUE_BENCH_RESTORES number of restores at once, one run per value
#                            (default "1 4 12 14")
#
# It prints the wall time of each run, the median and longest restore, and the
# share of scheduler time in use. Pin the VM to one CPU as a small node is:
#
#   ELIXIR_ERL_OPTIONS="+S 1:1 +SDcpu 1:1 +SDio 1" taskset -c 0 \
#     mise run bench:restore-concurrency

defmodule Slow do
  @moduledoc false
  @behaviour Alambique.Store

  def new(inner, ms) do
    %{inner: inner, ms: ms, now: :counters.new(1, []), peak: :atomics.new(1, [])}
  end

  def peak(%{peak: peak}), do: :atomics.get(peak, 1)

  defp slow(%{ms: ms, now: now, peak: peak}, fun) do
    :counters.add(now, 1, 1)
    held = :counters.get(now, 1)
    if held > :atomics.get(peak, 1), do: :atomics.put(peak, 1, held)
    Process.sleep(ms)
    fun.()
  after
    :counters.sub(now, 1, 1)
  end

  @impl true
  def get(%{inner: {m, c}} = s, key), do: slow(s, fn -> m.get(c, key) end)
  @impl true
  def get_range(%{inner: {m, c}} = s, key, o, l), do: slow(s, fn -> m.get_range(c, key, o, l) end)
  @impl true
  def put(%{inner: {m, c}} = s, k, b, cond, meta),
    do: slow(s, fn -> m.put(c, k, b, cond, meta) end)

  @impl true
  def head(%{inner: {m, c}} = s, key), do: slow(s, fn -> m.head(c, key) end)
  @impl true
  def list(%{inner: {m, c}} = s, p, a, l), do: slow(s, fn -> m.list(c, p, a, l) end)
  @impl true
  def delete(%{inner: {m, c}} = s, key), do: slow(s, fn -> m.delete(c, key) end)
end

defmodule Bench do
  import Alambique.ReplicationHelpers
  alias Alambique.Replication
  alias AlambiqueTestkit.MemoryStore

  @reps 3

  def env(name, default),
    do: name |> System.get_env(default) |> String.split() |> Enum.map(&String.to_integer/1)

  defp sample(base, peak) do
    receive do
      {:stop, from} -> send(from, {:peak, peak})
    after
      2 -> sample(base, max(peak, :erlang.memory(:binary) - base))
    end
  end

  def run_many(cells) do
    [ms] = env("ALAMBIQUE_BENCH_LATENCY", "25")
    [row] = env("ALAMBIQUE_BENCH_ROW", "1000")
    [c] = env("ALAMBIQUE_BENCH_CONCURRENCY", "16")
    [b] = env("ALAMBIQUE_BENCH_BUDGETS", "2516582")
    counts = env("ALAMBIQUE_BENCH_RESTORES", "1 4 12 14")

    {_pid, {_, cfg} = store} = start_store()
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 600_000)

    for {segments, i} <- Enum.with_index(cells) do
      path = Path.join(dir, "src-#{i}.db")
      conn = open_db(path)
      state = Replication.Replicator.new("cell#{i}", path, 4096, 1)

      Enum.reduce(1..segments, state, fn k, state ->
        run_op(conn, {:put, k, row})
        {:ok, state} = Replication.Replicator.sync(state, ctx)
        state
      end)

      Alambique.Sqlite.close(conn)
    end

    IO.puts("cells: #{inspect(cells)}, latency #{ms} ms, concurrency #{c}, budget #{b}\n")
    IO.puts("restores  total s  median s  max s  scheduler use")

    for n <- counts, rep <- 1..@reps do
      slow = Slow.new({MemoryStore, cfg}, ms)
      ctx = Map.merge(ctx, %{store: {Slow, slow}, fetch_concurrency: c, max_inflight_bytes: b})
      :erlang.system_flag(:scheduler_wall_time, true)
      before = :erlang.statistics(:scheduler_wall_time_all) |> Enum.sort()

      {total, times} =
        :timer.tc(fn ->
          for i <- 0..(n - 1) do
            Task.async(fn ->
              dest = Path.join(dir, "dest-#{n}-#{rep}-#{i}.db")
              idx = rem(i, length(cells))

              {us, {:ok, _}} =
                :timer.tc(fn -> Replication.restore(ctx, "cell#{idx}", dest, :latest) end)

              File.rm(dest)
              us
            end)
          end
          |> Task.await_many(:infinity)
        end)

      after_ = :erlang.statistics(:scheduler_wall_time_all) |> Enum.sort()

      {active, all} =
        Enum.zip(before, after_)
        |> Enum.reduce({0, 0}, fn {{_, a0, t0}, {_, a1, t1}}, {a, t} ->
          {a + a1 - a0, t + t1 - t0}
        end)

      sorted = Enum.sort(times)

      IO.puts(
        :io_lib.format("~8w  ~7.2f  ~8.2f  ~5.2f  ~11.1f%", [
          n,
          total / 1_000_000,
          Enum.at(sorted, div(n, 2)) / 1_000_000,
          List.last(sorted) / 1_000_000,
          100 * active / max(all, 1)
        ])
      )
    end
  end

  def run(segments) do
    [ms] = env("ALAMBIQUE_BENCH_LATENCY", "25")
    [row] = env("ALAMBIQUE_BENCH_ROW", "1000")
    concurrencies = env("ALAMBIQUE_BENCH_CONCURRENCY", "1 2 4 8 16")
    budgets = env("ALAMBIQUE_BENCH_BUDGETS", "67108864 2516582")

    {_pid, {_, cfg} = store} = start_store()
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 600_000)
    {conn, state} = new_replicator(dir, "src.db")

    Enum.reduce(1..segments, state, fn k, state ->
      run_op(conn, {:put, k, row})
      {:ok, state} = Replication.Replicator.sync(state, ctx)
      state
    end)

    {:ok, {segs, _}} =
      Replication.Objects.list_cell(ctx, "cell", System.monotonic_time(:millisecond) + 60_000)

    bytes =
      Enum.sum(for s <- segs, {:ok, b, _, _} = MemoryStore.get(cfg, s.key), do: byte_size(b))

    IO.puts("segments: #{length(segs)}, #{bytes} bytes, latency #{ms} ms per request\n")

    cases = for b <- budgets, c <- concurrencies, do: {b, c}

    results =
      for rep <- 1..@reps, {b, c} <- cases do
        slow = Slow.new({MemoryStore, cfg}, ms)
        dest = Path.join(dir, "dest-#{rep}.db")
        ctx = Map.merge(ctx, %{store: {Slow, slow}, fetch_concurrency: c, max_inflight_bytes: b})
        :erlang.garbage_collect()
        base = :erlang.memory(:binary)
        sampler = spawn_link(fn -> sample(base, 0) end)

        {us, {:ok, %{txid: txid}}} =
          :timer.tc(fn -> Replication.restore(ctx, "cell", dest, :latest) end)

        send(sampler, {:stop, self()})
        mem = receive do: ({:peak, p} -> p)
        ^txid = segments
        File.rm(dest)
        {{b, c}, us, Slow.peak(slow), mem}
      end

    IO.puts("budget (bytes)  concurrency  median s  requests at once  peak binary KiB")

    for {b, c} = key <- cases do
      runs = for {^key, us, peak, mem} <- results, do: {us, peak, mem}
      {us, _, _} = runs |> Enum.sort() |> Enum.at(1)
      peak = runs |> Enum.map(&elem(&1, 1)) |> Enum.max()
      mem = runs |> Enum.map(&elem(&1, 2)) |> Enum.max()

      IO.puts(
        :io_lib.format("~14w  ~11w  ~8.2f  ~16w  ~15w", [
          b,
          c,
          us / 1_000_000,
          peak,
          div(mem, 1024)
        ])
      )
    end

    Alambique.Sqlite.close(conn)
  end
end

case System.get_env("ALAMBIQUE_BENCH_CELLS") do
  nil ->
    case System.argv() do
      [n | _] -> String.to_integer(n)
      _ -> 800
    end
    |> Bench.run()

  cells ->
    cells |> String.split() |> Enum.map(&String.to_integer/1) |> Bench.run_many()
end
