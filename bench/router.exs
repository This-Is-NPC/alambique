# Cost of the routing path of one local call (design §15).
#
#   mise run bench:router
#   mise run bench:router -- 200000    # calls per repetition
#
# Wakes one cell on a single node against the in-memory store, then times
# `Alambique.query` on it from one process (the whole call), `Cell.load/2`
# (the check of `awake.max_pending` before each call) and `Cell.whereis/2`
# alone. Three repetitions; the median µs per operation is reported.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @reps 3

  def run(calls) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()
    name = :bench_router

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: name,
           store: {MemoryStore, MemoryStore.config(store)},
           prefix: "bench-router",
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
      {:ok, _} = Alambique.execute(name, "r", "INSERT INTO t (v) VALUES ('x')")
      config = Alambique.Config.fetch!(name)

      IO.puts("calls per repetition: #{calls}, #{@reps} repetitions, median µs per operation\n")

      row("Alambique.query", calls, fn -> {:ok, _} = Alambique.query(name, "r", "SELECT 1") end)
      row("Cell.load", calls, fn -> {:ok, _} = Alambique.Cell.load(config, "r") end)
      row("Cell.whereis", calls, fn -> true = is_pid(Alambique.Cell.whereis(config, "r")) end)
    after
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  defp row(label, calls, fun) do
    times =
      for _ <- 1..@reps do
        {us, _} = :timer.tc(fn -> for _ <- 1..calls, do: fun.() end)
        us / calls
      end

    median = times |> Enum.sort() |> Enum.at(1)
    IO.puts(:io_lib.format("~-18s ~8.3f µs", [label, median]))
  end
end

calls =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 100_000
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(calls)
after
  Alambique.TestDirs.clean()
end
