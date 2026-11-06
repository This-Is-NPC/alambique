# Commit latency and throughput of one cell (design §15).
#
#   mise run bench:commit             # 3 s per case
#   mise run bench:commit -- 10       # seconds per case
#
# Closed loop: N writers hammer one cell with single-row inserts; each call
# returns after its batch is durable. Open loop: single-row inserts arrive at
# a fixed mean rate (Poisson), one process per arrival, whatever the cell
# does. Cases: the in-memory store with added latency on every segment upload
# (local disk, R2, cross-region S3) and the disposable SeaweedFS from
# `mise run seaweedfs:up` (skipped when it is not running).
#
# The matrix comes from the environment (space-separated lists):
#
#   ALAMBIQUE_BENCH_DEPTHS     pipeline_depth values            (default "1 2 4 8")
#   ALAMBIQUE_BENCH_LATENCIES  added upload latency in ms       (default "0 20 60 100")
#   ALAMBIQUE_BENCH_WRITERS    closed-loop writers              (default "1 8 64 256")
#   ALAMBIQUE_BENCH_RATES      open-loop arrivals per second    (default "100 500")
#   ALAMBIQUE_BENCH_BYTES      payload bytes per row            (default "16")
#   ALAMBIQUE_BENCH_FEED_LATENCIES  added latency in ms on the put of every
#                              feed object (the feed is always on and the
#                              bench table is captured)         (default "0")
#   ALAMBIQUE_BENCH_SEAWEEDFS  "0" skips the SeaweedFS cases    (default "1")
#   ALAMBIQUE_BENCH_READ_RATIO fraction of calls that are `Alambique.query`
#                              reads of the same cell           (default "0")

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  def run(seconds) do
    machine()
    IO.puts("duration:  #{seconds} s per case\n")

    IO.puts(
      "store                 depth  mode    load   bytes  done/s  p50 ms  p95 ms  p99 ms  puts/s  writes/put  inflight"
    )

    for ms <- list("LATENCIES", "0 20 60 100"), feed_ms <- list("FEED_LATENCIES", "0") do
      memory_case(ms, feed_ms, seconds)
    end

    if System.get_env("ALAMBIQUE_BENCH_SEAWEEDFS", "1") != "0", do: seaweedfs_case(seconds)
  end

  defp list(name, default) do
    ("ALAMBIQUE_BENCH_" <> name)
    |> System.get_env(default)
    |> String.split()
    |> Enum.map(&String.to_integer/1)
  end

  defp read_ratio do
    {ratio, _} = "ALAMBIQUE_BENCH_READ_RATIO" |> System.get_env("0") |> Float.parse()
    ratio
  end

  defp memory_case(ms, feed_ms, seconds) do
    {:ok, store} = MemoryStore.start_link()
    config = MemoryStore.config(store)

    if ms > 0 do
      MemoryStore.inject(config, kind: :latency, ms: ms, ops: [:put], key: ~r{/0000/})
    end

    if feed_ms > 0 do
      MemoryStore.inject(config, kind: :latency, ms: feed_ms, ops: [:put], key: ~r{/feed/})
    end

    label = if feed_ms > 0, do: "memory +#{ms} feed +#{feed_ms}", else: "memory +#{ms} ms"
    cases(label, {MemoryStore, config}, seconds)
    GenServer.stop(store)
  end

  defp seaweedfs_case(seconds) do
    config = %{
      endpoint: "http://127.0.0.1:18333",
      bucket: "bench-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower),
      access_key_id: "alambique",
      secret_access_key: "alambique-secret",
      path_style: true,
      receive_timeout: 30_000
    }

    created =
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

    case created do
      {:ok, %{status: status}} when status in [200, 409] ->
        cases("SeaweedFS (local)", {Alambique.Store.S3, config}, seconds)

      _ ->
        IO.puts("SeaweedFS (local)     skipped: not running (mise run seaweedfs:up)")
    end
  end

  defp cases(label, store, seconds) do
    loads =
      for(w <- list("WRITERS", "1 8 64 256"), do: {:closed, w}) ++
        for r <- list("RATES", "100 500"), r > 0, do: {:open, r}

    for depth <- list("DEPTHS", "1 2 4 8"),
        bytes <- list("BYTES", "16"),
        {mode, load} <- loads do
      with_instance(store, depth, fn inst ->
        measure(label, inst, depth, mode, load, bytes, seconds)
      end)
    end
  end

  defp with_instance(store, depth, fun) do
    dir = Alambique.TestDirs.new("bench")
    name = :"bench_commit_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: name,
           store: store,
           prefix: "bench-" <> Integer.to_string(System.unique_integer([:positive])),
           data_dir: dir,
           migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}],
           idle_timeout_ms: 600_000,
           lease_ttl_ms: 60_000,
           pipeline_depth: depth,
           compaction: false,
           retention: false}
        ],
        strategy: :one_for_one
      )

    try do
      fun.(name)
    after
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  defp measure(label, inst, depth, mode, load, bytes, seconds) do
    cell = "bench"
    payload = String.duplicate("x", bytes)
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (v) VALUES ('warm')")

    counter = :counters.new(3, [])
    ref = make_ref()

    :telemetry.attach(
      {__MODULE__, ref},
      [:alambique, :cell, :commit],
      &__MODULE__.count/4,
      counter
    )

    deadline = System.monotonic_time(:millisecond) + seconds * 1000
    ratio = read_ratio()

    write = fn ->
      if ratio > 0 and :rand.uniform() < ratio do
        Alambique.query(inst, cell, "SELECT count(*) FROM t")
      else
        Alambique.execute(inst, cell, "INSERT INTO t (v) VALUES (?)", [payload])
      end
    end

    {wall_us, latencies} = :timer.tc(fn -> drive(mode, load, write, deadline) end)

    :telemetry.detach({__MODULE__, ref})
    sorted = Enum.sort(latencies)
    n = length(sorted)
    secs = wall_us / 1_000_000
    batches = :counters.get(counter, 1)

    IO.puts(
      :io_lib.format(
        "~ts ~5B  ~-6s ~5B  ~6B  ~6B  ~6.1f  ~6.1f  ~6.1f  ~6.1f  ~10.1f  ~8.2f",
        [
          String.pad_trailing(if(ratio > 0, do: "#{label} r#{ratio}", else: label), 21),
          depth,
          mode,
          load,
          bytes,
          round(n / secs),
          pct(sorted, n, 0.50),
          pct(sorted, n, 0.95),
          pct(sorted, n, 0.99),
          batches / secs,
          :counters.get(counter, 2) / max(batches, 1),
          :counters.get(counter, 3) / max(batches, 1)
        ]
      )
    )
  end

  def count(_event, %{transactions: t, inflight: inflight}, %{result: :ok}, counter) do
    :counters.add(counter, 1, 1)
    :counters.add(counter, 2, t)
    :counters.add(counter, 3, inflight)
  end

  def count(_event, _measurements, _meta, _counter), do: :ok

  defp drive(:closed, writers, write, deadline) do
    1..writers
    |> Task.async_stream(fn _ -> loop(write, deadline, []) end,
      max_concurrency: writers,
      timeout: :infinity
    )
    |> Enum.flat_map(fn {:ok, l} -> l end)
  end

  # Arrivals are scheduled on absolute times, so a slow cell cannot slow them.
  defp drive(:open, rate, write, deadline) do
    parent = self()

    arrivals =
      arrive(parent, write, System.monotonic_time(:microsecond), rate, deadline * 1000, 0)

    collect(arrivals, [])
  end

  defp arrive(parent, write, next, rate, deadline_us, count) do
    now = System.monotonic_time(:microsecond)

    cond do
      now >= deadline_us ->
        count

      next > now ->
        Process.sleep(div(next - now, 1000))
        arrive(parent, write, next, rate, deadline_us, count)

      true ->
        spawn(fn ->
          {us, {:ok, _}} = :timer.tc(write)
          send(parent, {:latency, us})
        end)

        gap = trunc(-:math.log(1 - :rand.uniform()) / rate * 1_000_000)
        arrive(parent, write, next + gap, rate, deadline_us, count + 1)
    end
  end

  defp collect(0, acc), do: acc

  defp collect(pending, acc) do
    receive do
      {:latency, us} -> collect(pending - 1, [us | acc])
    after
      30_000 -> acc
    end
  end

  defp loop(write, deadline, acc) do
    if System.monotonic_time(:millisecond) >= deadline do
      acc
    else
      {us, {:ok, _}} = :timer.tc(write)
      loop(write, deadline, [us | acc])
    end
  end

  defp pct(sorted, n, p), do: Enum.at(sorted, min(n - 1, trunc(n * p))) / 1000

  defp machine do
    cpu =
      "/proc/cpuinfo"
      |> File.read!()
      |> then(&Regex.run(~r/model name\s*:\s*(.+)/, &1))
      |> then(fn [_, name] -> name end)

    IO.puts("""
    machine:   #{cpu}, #{System.schedulers_online()} schedulers
    runtime:   Erlang/OTP #{:erlang.system_info(:otp_release)}, Elixir #{System.version()}
    """)
  end
end

seconds =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 3
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(seconds)
after
  Alambique.TestDirs.clean()
end
