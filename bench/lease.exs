# Cost of `Alambique.lease/3`, which a caller may run on every refused call to
# turn the expiry into a Retry-After.
#
#   mise run bench:lease
#   mise run bench:lease -- 2000    # calls per case
#
# One cell held by a holder that is not a member (the case of a swap), read
# from a second instance over the same store. The store is the in-memory store
# with a uniform added latency on every request, `ALAMBIQUE_BENCH_LATENCIES`
# in ms (default "0 25"). Prints the median and the 99th percentile of one
# call, and the median of the same call with the lease read in the store alone
# (`Alambique.Lease.read/2`), to show what `lease/3` adds to one GET.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore
  alias Alambique.{Config, Lease, Placement}

  def run(calls, latencies) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()
    config = MemoryStore.config(store)

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: :bench_lease,
           store: {MemoryStore, config},
           prefix: "bench-lease",
           data_dir: dir,
           migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY)"}],
           compaction: false,
           retention: false}
        ],
        strategy: :one_for_one
      )

    try do
      instance = Config.fetch!(:bench_lease)
      {:ok, location} = Placement.locate(instance, "held")
      ctx = Config.lease_ctx(instance, location)
      {:ok, _} = Lease.acquire(ctx, "held", "other-node")

      IO.puts("calls per case: #{calls}\n")

      IO.puts(
        :io_lib.format("~-10ts ~-22ts ~12ts ~12ts", [
          "latency",
          "operation",
          "median µs",
          "p99 µs"
        ])
      )

      for ms <- latencies do
        fault = if ms > 0, do: MemoryStore.inject(config, kind: :latency, ms: ms)

        row(ms, "Alambique.lease/3", calls, fn ->
          {:ok, %{owner_id: "other-node"}} = Alambique.lease(:bench_lease, "held")
        end)

        row(ms, "Lease.read/2", calls, fn -> {:ok, _} = Lease.read(ctx, "held") end)
        if fault, do: MemoryStore.remove(config, fault)
      end
    after
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  defp row(ms, label, calls, fun) do
    times = for _ <- 1..calls, do: elem(:timer.tc(fun), 0)
    sorted = Enum.sort(times)
    median = Enum.at(sorted, div(calls, 2))
    p99 = Enum.at(sorted, min(calls - 1, trunc(calls * 0.99)))
    IO.puts(:io_lib.format("~-10s ~-22s ~12B ~12B", ["#{ms} ms", label, median, p99]))
  end
end

calls =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 500
  end

latencies =
  "ALAMBIQUE_BENCH_LATENCIES"
  |> System.get_env("0 25")
  |> String.split()
  |> Enum.map(&String.to_integer/1)

Alambique.TestDirs.sweep()

try do
  Bench.run(calls, latencies)
after
  Alambique.TestDirs.clean()
end
