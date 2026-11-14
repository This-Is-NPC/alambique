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
#   ALAMBIQUE_BENCH_STORE    "memory" (default) or "seaweedfs": the container of
#                            `mise run seaweedfs:up`, read through a proxy that
#                            adds the latency to every request
#   ALAMBIQUE_BENCH_POOL_SIZE `pool_size` of the S3 store (default: its own)
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

  # The store the restores read from, and the one the cells are written to. The
  # in-memory store gets the delay in `Slow`; SeaweedFS (`mise run
  # seaweedfs:up`) is read through a proxy that delays every request.
  defp backend("memory", ms) do
    {_pid, {_, cfg} = store} = start_store()
    {store, fn -> {Slow, Slow.new({MemoryStore, cfg}, ms)} end}
  end

  defp backend("seaweedfs", ms) do
    config = %{
      endpoint: "http://127.0.0.1:18333",
      bucket: "bench-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower),
      access_key_id: "alambique",
      secret_access_key: "alambique-secret",
      path_style: true,
      receive_timeout: 30_000
    }

    {:ok, %{status: status}} =
      Req.put(
        url: "#{config.endpoint}/#{config.bucket}",
        aws_sigv4: [
          access_key_id: config.access_key_id,
          secret_access_key: config.secret_access_key,
          service: "s3",
          region: "us-east-1"
        ],
        retry: false
      )

    true = status in [200, 409]
    port = start_proxy(18333, ms)
    via = Map.merge(config, %{endpoint: "http://127.0.0.1:#{port}"})

    via =
      case env("ALAMBIQUE_BENCH_POOL_SIZE", "") do
        [size] -> Map.put(via, :pool_size, size)
        [] -> via
      end

    {{Alambique.Store.S3, config}, fn -> {Alambique.Store.S3, via} end}
  end

  # The proxy is a process of its own (`bench/delay_proxy.exs`), on another
  # CPU, so that its work does not count in the one the bench pins the VM to.
  # It stops when its stdin closes, which happens when this VM exits.
  defp start_proxy(target, ms) do
    [cpu] = env("ALAMBIQUE_BENCH_PROXY_CPU", "5")

    port =
      Port.open({:spawn_executable, System.find_executable("taskset")}, [
        :binary,
        :exit_status,
        args: [
          "-c",
          "#{cpu}",
          System.find_executable("elixir"),
          "bench/delay_proxy.exs",
          "#{target}",
          "#{ms}"
        ]
      ])

    receive do
      {^port, {:data, line}} -> String.trim(line) |> String.to_integer()
    after
      20_000 -> raise "the delay proxy did not start"
    end
  end

  def run_many(cells) do
    [ms] = env("ALAMBIQUE_BENCH_LATENCY", "25")
    [row] = env("ALAMBIQUE_BENCH_ROW", "1000")
    cs = env("ALAMBIQUE_BENCH_CONCURRENCY", "16")
    [b] = env("ALAMBIQUE_BENCH_BUDGETS", "2516582")
    counts = env("ALAMBIQUE_BENCH_RESTORES", "1 4 12 14")

    {store, restore_store} =
      backend(System.get_env("ALAMBIQUE_BENCH_STORE", "memory"), ms)

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

    IO.puts("cells: #{inspect(cells)}, latency #{ms} ms, budget #{b}\n")
    IO.puts("concurrency  restores  total s  median s  max s  failed  scheduler use")

    for c <- cs, n <- counts, rep <- 1..@reps do
      ctx =
        Map.merge(ctx, %{store: restore_store.(), fetch_concurrency: c, max_inflight_bytes: b})

      :erlang.system_flag(:scheduler_wall_time, true)
      before = :erlang.statistics(:scheduler_wall_time_all) |> Enum.sort()

      {total, results} =
        :timer.tc(fn ->
          for i <- 0..(n - 1) do
            Task.async(fn ->
              dest = Path.join(dir, "dest-#{c}-#{n}-#{rep}-#{i}.db")
              idx = rem(i, length(cells))

              {us, result} =
                :timer.tc(fn -> Replication.restore(ctx, "cell#{idx}", dest, :latest) end)

              File.rm(dest)
              {us, result}
            end)
          end
          |> Task.await_many(:infinity)
        end)

      times = for {us, _} <- results, do: us
      failed = Enum.count(results, &(not match?({_, {:ok, _}}, &1)))

      for {_, {:error, reason}} <-
            Enum.take(Enum.filter(results, &match?({_, {:error, _}}, &1)), 1),
          do: IO.puts("  first failure: #{inspect(reason, limit: 8)}")

      after_ = :erlang.statistics(:scheduler_wall_time_all) |> Enum.sort()

      {active, all} =
        Enum.zip(before, after_)
        |> Enum.reduce({0, 0}, fn {{_, a0, t0}, {_, a1, t1}}, {a, t} ->
          {a + a1 - a0, t + t1 - t0}
        end)

      sorted = Enum.sort(times)

      IO.puts(
        :io_lib.format("~11w  ~8w  ~7.2f  ~8.2f  ~5.2f  ~6w  ~11.1f%", [
          c,
          n,
          total / 1_000_000,
          Enum.at(sorted, div(n, 2)) / 1_000_000,
          List.last(sorted) / 1_000_000,
          failed,
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
