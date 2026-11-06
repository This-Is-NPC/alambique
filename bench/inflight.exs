# Cost of the in-flight marker per write (design §10, §15).
#
#   mise run bench:inflight
#   mise run bench:inflight -- 5000    # writes per repetition
#
# One closed-loop writer on one cell against the in-memory store, with the
# marker on (the default `quarantine: []`) and off (`quarantine: false`).
# Three repetitions per mode, interleaved; reports the median µs per write.
# Then times 100 000 `Inflight.busy/2` calls alone.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @reps 3

  def run(writes) do
    IO.puts("writes per repetition: #{writes}, #{@reps} repetitions, median µs per write\n")

    results =
      for rep <- 1..@reps, {label, quarantine} <- [{"marker on", []}, {"marker off", false}] do
        {label, once(writes, quarantine, rep)}
      end

    for label <- ["marker on", "marker off"] do
      times = for {^label, t} <- results, do: t
      IO.puts(:io_lib.format("~-12s ~8.1f µs", [label, times |> Enum.sort() |> Enum.at(1)]))
    end

    pair()
  end

  defp once(writes, quarantine, rep) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()
    name = :"bench_inflight_#{rep}_#{inspect(quarantine)}"

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: name,
           store: {MemoryStore, MemoryStore.config(store)},
           prefix: "bench-inflight",
           data_dir: dir,
           migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}],
           idle_timeout_ms: 600_000,
           lease_ttl_ms: 60_000,
           quarantine: quarantine,
           compaction: false,
           retention: false}
        ],
        strategy: :one_for_one
      )

    try do
      {:ok, _} = Alambique.execute(name, "i", "INSERT INTO t (v) VALUES ('warm')")

      {us, _} =
        :timer.tc(fn ->
          for _ <- 1..writes,
              do: {:ok, _} = Alambique.execute(name, "i", "INSERT INTO t (v) VALUES ('x')")
        end)

      us / writes
    after
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  defp pair do
    dir = Alambique.TestDirs.new("bench")

    marker =
      Alambique.Inflight.open(
        %{quarantine: %{}, data_dir: dir, inflight: :no_tracker},
        "x",
        :cell
      )

    {us, _} =
      :timer.tc(fn -> for _ <- 1..100_000, do: Alambique.Inflight.busy(marker, fn -> :ok end) end)

    IO.puts(:io_lib.format("\nInflight.busy/2 alone: ~.2f µs per call", [us / 100_000]))
    File.rm_rf(dir)
  end
end

writes =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 3_000
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(writes)
after
  Alambique.TestDirs.clean()
end
