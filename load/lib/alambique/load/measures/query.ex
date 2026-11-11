defmodule Alambique.Load.Measures.Query do
  @moduledoc false
  # Cross-cell queries per second: each query is `Alambique.Query.all/5` over
  # K cells of a population of P that sleep. One pass over the whole population
  # before the first step fills the replica cache, so the steps measure warm
  # queries (a freshness check per cell, not a restore). The dirty IO pool use
  # comes from the service's probe.

  alias Alambique.Load.{Env, Measure}

  def unit, do: "queries/s"
  def latency, do: {:p99, Env.float("QUERY_P99_MS", 1_000)}
  def mem_fraction, do: 1.0
  def probe?, do: true
  def ramp_opts, do: [refine: Env.int("REFINE", 2)]
  def levels, do: "LEVELS" |> Env.numbers([1, 2, 4, 8, 16, 32, 64]) |> Enum.map(&round/1)

  defp population, do: Env.int("QUERY_POPULATION", 200)
  defp per_query, do: Env.int("QUERY_CELLS", 20)

  def setup(ctx) do
    Measure.populate(ctx, "q", population())
    :ok = Measure.call(ctx, :await_asleep, [120_000], 150_000)
    all = for i <- 1..population(), do: Measure.cell("q", i)
    :ok = Measure.call(ctx, :cross_query, [all], 600_000)
  end

  def prepare(_ctx, level), do: Measure.offsets(level, Measure.step_s(), level)

  def run(ctx, _level, offsets) do
    p = population()
    k = per_query()

    op =
      Measure.remote(ctx, :cross_query, fn i ->
        [for(j <- 0..(k - 1), do: Measure.cell("q", rem(i * k + j, p) + 1))]
      end)

    result = Measure.open_loop(ctx, offsets, op)
    Measure.put_extra(result, %{cells_per_query: k, population: p})
  end
end
