# Store round trips of a job in one awake cell (design §9, §13).
#
#   mise run bench:trips                 # 100 jobs per case
#   mise run bench:trips -- 200          # jobs per case
#
# Closed loop: `concurrency` workers each run jobs back to back against one
# awake cell. A job is two `Alambique.execute` calls (two commits with row
# changes, in separate calls), the shape of a job handler that writes twice.
# The store is the in-memory store with a uniform added latency on every
# operation. Every request is counted by kind (a wrapper store, by key). The
# default compaction and retention settings are kept; after the jobs the bench
# waits until the feed and the compactor are quiet, so the passes that the
# jobs made due are counted too. Rows that did not fill a block yet stay
# uncompacted, as they do in service.
#
# The matrix comes from the environment (space-separated lists):
#
#   ALAMBIQUE_BENCH_LATENCIES   added latency in ms, every operation  (default "25 100")
#   ALAMBIQUE_BENCH_CONCURRENCY workers                               (default "1 4")
#   ALAMBIQUE_BENCH_QUIET_MS    quiet time that ends a case           (default "12000")
#   ALAMBIQUE_BENCH_FEED_OBJECT_BATCHES  feed `object_batches`        (default "1")
#   ALAMBIQUE_BENCH_FEED_LINGER_MS       feed `linger_ms`             (default "0")

defmodule Counted do
  @moduledoc false
  # Wraps a store and counts requests by kind, from the key.
  @behaviour Alambique.Store

  @kinds [
    :feed_put,
    :segment_put,
    :compaction_put,
    :other_put,
    :compaction_get,
    :compaction_head,
    :compaction_list,
    :other_get,
    :other_head,
    :other_list,
    :other_delete
  ]

  def kinds, do: @kinds

  def wrap({_m, _c} = inner),
    do: {__MODULE__, %{inner: inner, counters: :counters.new(length(@kinds), [:atomics])}}

  def read(%{counters: c}),
    do: Map.new(Enum.with_index(@kinds), fn {k, i} -> {k, :counters.get(c, i + 1)} end)

  def reset(%{counters: c}), do: for(i <- 1..length(@kinds), do: :counters.put(c, i, 0))

  # Requests of the feed and the compactor, to see when both are quiet.
  def activity(%{counters: c}) do
    for k <- [:feed_put, :compaction_put, :compaction_get, :compaction_head, :compaction_list],
        reduce: 0 do
      acc -> acc + :counters.get(c, Enum.find_index(@kinds, &(&1 == k)) + 1)
    end
  end

  defp bump(%{counters: c}, kind),
    do: :counters.add(c, Enum.find_index(@kinds, &(&1 == kind)) + 1, 1)

  defp segment?(key),
    do: Regex.match?(~r|/cells/[^/]+/000[0-9]/[0-9a-f]{16}-[0-9a-f]{16}\.ltx\z|, key)

  defp level0?(key), do: String.contains?(key, "/0000/")

  defp kind(:put, key) do
    cond do
      String.contains?(key, "/feed/") -> :feed_put
      segment?(key) and level0?(key) -> :segment_put
      segment?(key) -> :compaction_put
      true -> :other_put
    end
  end

  defp kind(op, key) when op in [:get, :get_range] do
    if segment?(key), do: :compaction_get, else: :other_get
  end

  defp kind(:head, key), do: if(segment?(key), do: :compaction_head, else: :other_head)

  defp kind(:list, prefix),
    do: if(String.contains?(prefix, "/cells/"), do: :compaction_list, else: :other_list)

  defp kind(:delete, _), do: :other_delete

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}} = s, key, body, cond, meta) do
    bump(s, kind(:put, key))
    m.put(c, key, body, cond, meta)
  end

  @impl true
  def get(%{inner: {m, c}} = s, key) do
    bump(s, kind(:get, key))
    m.get(c, key)
  end

  @impl true
  def get_range(%{inner: {m, c}} = s, key, offset, length) do
    bump(s, kind(:get_range, key))
    m.get_range(c, key, offset, length)
  end

  @impl true
  def head(%{inner: {m, c}} = s, key) do
    bump(s, kind(:head, key))
    m.head(c, key)
  end

  @impl true
  def list(%{inner: {m, c}} = s, prefix, start_after, limit) do
    bump(s, kind(:list, prefix))
    m.list(c, prefix, start_after, limit)
  end

  @impl true
  def delete(%{inner: {m, c}} = s, key) do
    bump(s, kind(:delete, key))
    m.delete(c, key)
  end
end

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  def run(jobs) do
    IO.puts("jobs per case: #{jobs} (2 commits with changes each, separate calls)")
    IO.puts("compaction and retention: defaults; quiet time #{quiet_ms()} ms")
    IO.puts("feed: #{inspect(feed_opts())}\n")

    for ms <- list("LATENCIES", "25 100"), workers <- list("CONCURRENCY", "1 4") do
      one_case(ms, workers, jobs)
    end

    :ok
  end

  defp list(name, default) do
    ("ALAMBIQUE_BENCH_" <> name)
    |> System.get_env(default)
    |> String.split()
    |> Enum.map(&String.to_integer/1)
  end

  defp feed_opts do
    [
      object_batches: env_int("FEED_OBJECT_BATCHES", "1"),
      linger_ms: env_int("FEED_LINGER_MS", "0")
    ]
  end

  defp env_int(name, default),
    do: ("ALAMBIQUE_BENCH_" <> name) |> System.get_env(default) |> String.to_integer()

  defp quiet_ms, do: "ALAMBIQUE_BENCH_QUIET_MS" |> System.get_env("12000") |> String.to_integer()

  defp one_case(ms, workers, jobs) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, mem} = MemoryStore.start_link()
    config = MemoryStore.config(mem)
    {Counted, counted} = store = Counted.wrap({MemoryStore, config})
    name = :"bench_trips_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: name,
           store: store,
           prefix: "bench",
           data_dir: dir,
           migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}],
           feed: feed_opts(),
           idle_timeout_ms: 600_000,
           lease_ttl_ms: 60_000}
        ],
        strategy: :one_for_one
      )

    try do
      cell = "bench"
      {:ok, _} = Alambique.execute(name, cell, "INSERT INTO t (v) VALUES ('warm')")
      wait_quiet(counted, 2_000)
      MemoryStore.inject(config, kind: :latency, ms: ms)
      Counted.reset(counted)

      job = fn ->
        {:ok, _} = Alambique.execute(name, cell, "INSERT INTO t (v) VALUES ('a')")
        {:ok, _} = Alambique.execute(name, cell, "INSERT INTO t (v) VALUES ('b')")
      end

      {us, _} = :timer.tc(fn -> drive(workers, jobs, job) end)
      during = Counted.read(counted)
      wait_quiet(counted, quiet_ms())
      report(ms, workers, jobs, us, during, Counted.read(counted))
    after
      Supervisor.stop(sup)
      GenServer.stop(mem)
      File.rm_rf(dir)
    end
  end

  defp drive(workers, jobs, job) do
    left = :atomics.new(1, [])
    :atomics.put(left, 1, jobs)

    1..workers
    |> Task.async_stream(fn _ -> work(left, job) end,
      max_concurrency: workers,
      timeout: :infinity
    )
    |> Stream.run()
  end

  defp work(left, job) do
    if :atomics.sub_get(left, 1, 1) >= 0 do
      job.()
      work(left, job)
    end
  end

  # Returns when the feed and the compactor made no request for `quiet` ms.
  defp wait_quiet(counted, quiet) do
    poll(counted, Counted.activity(counted), System.monotonic_time(:millisecond), quiet)
  end

  defp poll(counted, last, since, quiet) do
    Process.sleep(250)
    now = System.monotonic_time(:millisecond)
    activity = Counted.activity(counted)

    cond do
      activity != last -> poll(counted, activity, now, quiet)
      now - since >= quiet -> :ok
      true -> poll(counted, last, since, quiet)
    end
  end

  defp report(ms, workers, jobs, us, during, total) do
    secs = us / 1_000_000
    per = fn k -> Map.fetch!(total, k) / jobs end
    sum = fn keys -> Enum.sum(for k <- keys, do: Map.fetch!(total, k)) end
    all = sum.(Counted.kinds())
    reads = sum.([:compaction_get, :compaction_head, :compaction_list])

    IO.puts("== store +#{ms} ms, concurrency #{workers}")

    IO.puts(
      :io_lib.format("  jobs/s (load phase):  ~.2f  (~B jobs in ~.1f s)", [
        jobs / secs,
        jobs,
        secs
      ])
    )

    IO.puts(
      "  requests per job (after the compactor is quiet; in the load phase alone: " <>
        "#{Float.round(Enum.sum(Map.values(during)) / jobs, 2)} in all)"
    )

    for {label, key} <- [
          {"feed object puts", :feed_put},
          {"segment uploads (level 0)", :segment_put},
          {"compaction puts (merged, snapshot)", :compaction_put},
          {"compaction gets", :compaction_get},
          {"compaction heads", :compaction_head},
          {"compaction lists", :compaction_list},
          {"other puts", :other_put},
          {"other gets", :other_get},
          {"other heads", :other_head},
          {"other lists", :other_list},
          {"other deletes", :other_delete}
        ] do
      IO.puts(:io_lib.format("    ~-36s ~6.2f", [label, per.(key)]))
    end

    IO.puts(:io_lib.format("    ~-36s ~6.2f", ["compaction reads (get+head+list)", reads / jobs]))
    IO.puts(:io_lib.format("    ~-36s ~6.2f", ["TOTAL round trips", all / jobs]))
    IO.puts("")
  end
end

jobs =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 100
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(jobs)
after
  Alambique.TestDirs.clean()
end
