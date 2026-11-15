# The store breaker under local overload (design §3): a node on one CPU takes
# commits at a fixed rate above what it can serve while the store is healthy
# (SeaweedFS behind the delay proxy, 25 ms per request). Before the breaker
# counted only what the store did, the overload made requests give up, the
# breaker opened for its cool-down, the node idled, the queue grew and the
# timeouts reopened it: throughput stayed well below capacity.
#
#   mise run seaweedfs:up
#   mise run bench:breaker                    # RATE commits per second for SECONDS
#
# It prints the commits that completed in every second, the breaker's opens
# with their reasons, the store failures by reason and whether they counted, and the
# crashes with their kind and detail.
#
#   ALAMBIQUE_BENCH_LATENCY          added latency per request in ms        (default "25")
#   ALAMBIQUE_BENCH_BREAKER_CELLS    cells that take commits                 (default "12")
#   ALAMBIQUE_BENCH_BREAKER_RATE     commits per second, all cells together  (default "150")
#   ALAMBIQUE_BENCH_BREAKER_SECONDS  length of the run                       (default "60")
#   ALAMBIQUE_BENCH_BREAKER_BURN     busy loops that take the CPU like an app (default "1")
#   ALAMBIQUE_BENCH_BREAKER_OPTS     extra Alambique options, an Elixir keyword list
#   ALAMBIQUE_BENCH_BREAKER_POOL     connections of the store's pool; with it the wait for one
#                                    gives up after 200 ms, a node queue that the store did not cause (default: the store's own)
#   ALAMBIQUE_BENCH_BREAKER_CPU      CPU of the node                         (default "0")
#   ALAMBIQUE_BENCH_PROXY_CPU        CPU of the proxy                        (default "5")

Code.require_file("seaweedfs_store.exs", __DIR__)

defmodule BreakerLoad do
  @moduledoc false

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}]
  @events [[:alambique, :store, :breaker, :open], [:alambique, :store, :failure]]

  defp int(name, default), do: name |> System.get_env(default) |> String.to_integer()

  def relay(event, _measurements, meta, table),
    do: :ets.insert(table, {System.monotonic_time(:millisecond), event, meta})

  defp burn, do: burn()

  def run do
    seconds = int("ALAMBIQUE_BENCH_BREAKER_SECONDS", "60")
    rate = int("ALAMBIQUE_BENCH_BREAKER_RATE", "150")
    cells = for i <- 1..int("ALAMBIQUE_BENCH_BREAKER_CELLS", "12"), do: "room-#{i}"
    # A commit every `interval` ms in each cell, whatever the earlier ones did.
    interval = max(round(1000 * length(cells) / rate), 1)

    bucket = "breaker-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
    s3 = BenchSeaweedfs.bucket(bucket)
    port = BenchSeaweedfs.start_proxy(int("ALAMBIQUE_BENCH_LATENCY", "25"))

    s3 =
      case System.get_env("ALAMBIQUE_BENCH_BREAKER_POOL") do
        nil ->
          s3

        size ->
          Map.merge(s3, %{
            pool_size: String.to_integer(size),
            req_options: [finch: [pool_timeout: 200]]
          })
      end

    extra =
      case System.get_env("ALAMBIQUE_BENCH_BREAKER_OPTS") do
        nil -> []
        code -> code |> Code.eval_string() |> elem(0)
      end

    dir = Alambique.TestDirs.new("bench")

    opts =
      Keyword.merge(
        [
          name: :bench_breaker,
          store: {Alambique.Store.S3, Map.put(s3, :endpoint, "http://127.0.0.1:#{port}")},
          prefix: "breaker",
          data_dir: dir,
          migrations: @schema,
          lease_ttl_ms: 30_000
        ],
        extra
      )

    {:ok, sup} = Supervisor.start_link([{Alambique, opts}], strategy: :one_for_one)

    table = :ets.new(:events, [:public, :bag])
    :telemetry.attach_many({__MODULE__, :events}, @events, &__MODULE__.relay/4, table)

    # Wake the cells before the clock starts.
    for cell <- cells, do: {:ok, _} = Alambique.touch(:bench_breaker, cell, timeout: 60_000)

    lags = :ets.new(:lags, [:public, :bag])
    sampler = spawn(fn -> sample(lags) end)
    burners = for _ <- 1..int("ALAMBIQUE_BENCH_BREAKER_BURN", "1")//1, do: spawn(&burn/0)
    done = :ets.new(:done, [:public, :bag])
    t0 = System.monotonic_time(:millisecond)
    stop = t0 + seconds * 1000
    row = String.duplicate("x", 100)

    tasks =
      for {cell, i} <- Enum.with_index(cells) do
        Task.async(fn ->
          Process.sleep(div(interval * i, length(cells)))
          open(cell, row, stop, interval, done, System.monotonic_time(:millisecond), [])
        end)
      end

    # The commits still running 20 s after the load stopped are given up.
    pending = tasks |> Task.await_many(:infinity) |> List.flatten()
    give_up = System.monotonic_time(:millisecond) + 20_000

    wait_for(pending, give_up)
    for pid <- pending, do: Process.exit(pid, :kill)
    for pid <- [sampler | burners], do: Process.exit(pid, :kill)
    report(t0, seconds, done, table, lags)
    :telemetry.detach({__MODULE__, :events})
    Supervisor.stop(sup)
    File.rm_rf(dir)
  end

  # How late a process that waits 1 ms is back, and the run queue, every 100 ms.
  defp sample(table) do
    started = System.monotonic_time(:millisecond)

    receive do
    after
      1 -> :ok
    end

    now = System.monotonic_time(:millisecond)
    :ets.insert(table, {now, now - started - 1, :erlang.statistics(:run_queue)})

    receive do
    after
      99 -> sample(table)
    end
  end

  defp wait_for(pids, give_up) do
    if Enum.any?(pids, &Process.alive?/1) and System.monotonic_time(:millisecond) < give_up do
      Process.sleep(100)
      wait_for(pids, give_up)
    end
  end

  defp open(cell, row, stop, interval, done, due, tasks) do
    if due >= stop do
      tasks
    else
      pid =
        spawn(fn ->
          started = System.monotonic_time(:millisecond)
          result = Alambique.execute(:bench_breaker, cell, "INSERT INTO t (v) VALUES (?)", [row])
          now = System.monotonic_time(:millisecond)
          :ets.insert(done, {now, now - started, match?({:ok, _}, result), result})
        end)

      wait = due + interval - System.monotonic_time(:millisecond)
      if wait > 0, do: Process.sleep(wait)
      open(cell, row, stop, interval, done, due + interval, [pid | tasks])
    end
  end

  defp report(t0, seconds, done, table, lags) do
    all = :ets.tab2list(done)
    ok = for {at, ms, true, _} <- all, do: {at, ms}
    failed = for {_, _, false, result} <- all, do: result

    per_second =
      for s <- 0..(seconds - 1) do
        Enum.count(ok, fn {at, _} -> div(at - t0, 1000) == s end)
      end

    IO.puts("commits completed per second:\n" <> Enum.join(per_second, " "))
    third = max(div(seconds, 3), 1)
    mean = fn list -> Float.round(Enum.sum(list) / max(length(list), 1), 1) end

    IO.puts(
      "mean per second: first #{third} s #{mean.(Enum.take(per_second, third))}, " <>
        "last #{third} s #{mean.(Enum.take(per_second, -third))}, " <>
        "whole run #{mean.(per_second)}"
    )

    ms = ok |> Enum.map(&elem(&1, 1)) |> Enum.sort()
    pick = fn q -> Enum.at(ms, min(round(q * length(ms)), length(ms) - 1), 0) end

    samples = :ets.tab2list(lags)

    per_second_lag =
      for s <- 0..(seconds - 1) do
        Enum.max(for({at, lag, _} <- samples, div(at - t0, 1000) == s, do: lag), fn -> 0 end)
      end

    IO.puts("scheduler lag, max of each second, ms:\n" <> Enum.join(per_second_lag, " "))
    IO.puts("run queue max: #{Enum.max(for({_, _, q} <- samples, do: q), fn -> 0 end)}")

    IO.puts(
      "completed #{length(ok)}, failed #{length(failed)}, latency ms p50 #{pick.(0.5)} p99 #{pick.(0.99)}"
    )

    IO.puts("failures: " <> inspect(failed |> Enum.frequencies() |> Enum.take(5)))

    events = :ets.tab2list(table)

    opens =
      for {at, [:alambique, :store, :breaker, :open], meta} <- events, do: {at - t0, meta.reason}

    IO.puts("breaker opens: #{length(opens)} at ms #{inspect(opens)}")

    by_reason =
      for {_, [:alambique, :store, :failure], m} <- events, reduce: %{} do
        acc -> Map.update(acc, {m.operation, m.reason, m.counted}, 1, &(&1 + 1))
      end

    IO.puts("store failures {operation, reason, counted}: #{inspect(by_reason)}")

    crashes =
      for {_, [:alambique, :store, :failure], %{kind: kind} = m} when kind != nil <- events,
          reduce: %{} do
        acc -> Map.update(acc, {m.reason, m.counted, kind, m.detail}, 1, &(&1 + 1))
      end

    IO.puts("store crashes {reason, counted, kind, detail}: #{inspect(crashes, limit: :infinity)}")

    times = for {at, [:alambique, :store, :failure], _} <- events, do: at - t0
    IO.puts("store failures at ms (first 10): #{inspect(times |> Enum.sort() |> Enum.take(10))}")
  end
end

BreakerLoad.run()
