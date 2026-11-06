# WAL growth under steady writes (design §15).
#
#   mise run bench:wal            # 60 s
#   mise run bench:wal -- 300     # another duration
#
# Keeps one cell awake with 8 closed-loop writers of single-row inserts against
# the in-memory store with no added latency, so only local cost shows. Every
# 2 s it reports commits/s and p99 of the interval, the size of the cell's
# `-wal` file and the BEAM memory. A WAL that is never checkpointed grows
# with every commit; the columns show whether the cost per commit follows.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @interval_ms 2_000
  @writers 8

  def run(seconds) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()
    name = :bench_wal

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: name,
           store: {MemoryStore, MemoryStore.config(store)},
           prefix: "bench-wal",
           data_dir: dir,
           migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}],
           idle_timeout_ms: 600_000,
           lease_ttl_ms: 60_000,
           compaction: false,
           retention: false}
        ],
        strategy: :one_for_one
      )

    try do
      IO.puts("duration:  #{seconds} s, #{@writers} writers, interval #{@interval_ms} ms\n")
      IO.puts("   t s  commits/s  p99 ms  wal MB  beam MB")
      {:ok, _} = Alambique.execute(name, "wal", "INSERT INTO t (v) VALUES ('warm')")
      deadline = System.monotonic_time(:millisecond) + seconds * 1000
      collector = self()

      for _ <- 1..@writers do
        spawn_link(fn -> write(name, deadline, collector, System.monotonic_time(:millisecond), []) end)
      end

      t0 = System.monotonic_time(:millisecond)
      report(dir, deadline, t0, t0, @writers)
    after
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  # Writers send their latencies (µs) to the collector every interval.
  defp write(name, deadline, collector, flushed, acc) do
    now = System.monotonic_time(:millisecond)

    cond do
      now >= deadline ->
        send(collector, {:lat, acc})
        send(collector, :done)

      now - flushed >= @interval_ms ->
        send(collector, {:lat, acc})
        write(name, deadline, collector, now, [])

      true ->
        {us, {:ok, _}} =
          :timer.tc(fn -> Alambique.execute(name, "wal", "INSERT INTO t (v) VALUES ('x')") end)

        write(name, deadline, collector, flushed, [us | acc])
    end
  end

  defp report(dir, deadline, t0, last, writers) do
    {acc, writers} = gather([], writers, last + @interval_ms)
    now = System.monotonic_time(:millisecond)

    if acc != [] do
      sorted = Enum.sort(acc)
      n = length(sorted)
      p99 = Enum.at(sorted, min(n - 1, trunc(n * 0.99))) / 1000
      wal = dir |> Path.join("**/*-wal") |> Path.wildcard() |> Enum.map(&File.stat!(&1).size)

      IO.puts(
        :io_lib.format("~5.1f  ~9B  ~6.1f  ~6.1f  ~7.1f", [
          (now - t0) / 1000,
          round(n / ((now - last) / 1000)),
          p99,
          Enum.sum(wal) / 1_048_576,
          :erlang.memory(:total) / 1_048_576
        ])
      )
    end

    if writers > 0 and now < deadline + 5_000, do: report(dir, deadline, t0, now, writers)
  end

  # Collect latency batches until `next` or until the writers are done.
  defp gather(acc, writers, next) do
    wait = max(next - System.monotonic_time(:millisecond), 0)

    receive do
      {:lat, l} -> gather(l ++ acc, writers, next)
      :done -> gather(acc, writers - 1, next)
    after
      wait -> {acc, writers}
    end
  end
end

seconds =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 60
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(seconds)
after
  Alambique.TestDirs.clean()
end
