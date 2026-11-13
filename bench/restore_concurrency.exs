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

segments =
  case System.argv() do
    [n | _] -> String.to_integer(n)
    _ -> 800
  end

Bench.run(segments)
