defmodule Alambique.Load.Measures.Wakes do
  @moduledoc false
  # Wakes per second: a population of cells with data is put to sleep, then
  # distinct sleeping cells are woken, one read each, at the target rate. In
  # `cold` mode (the default) their local files are removed first, so a wake
  # restores from the store, as after a move or a restart; `local` keeps them.
  # The measured window ends when the woken cells are asleep again, so the
  # trips and the CPU per wake include putting the cell back to sleep.

  alias Alambique.Load.{Env, Measure}

  def unit, do: "wakes/s"
  def latency, do: {:p95, Env.float("WAKES_P95_MS", 500)}
  def mem_fraction, do: 1.0
  def probe?, do: false
  def ramp_opts, do: [refine: Env.int("REFINE", 2)]
  def levels, do: "LEVELS" |> Env.numbers([5, 10, 20, 40, 80, 160, 320]) |> Enum.map(&round/1)

  defp mode, do: Env.get("WAKE_MODE", "cold")

  def setup(_ctx), do: :ok

  def prepare(ctx, level) do
    offsets = Measure.offsets(level, Measure.step_s(), level)
    needed = length(offsets)
    Measure.populate(ctx, "w", needed)
    :ok = Measure.call(ctx, :await_asleep, [120_000], 150_000)

    if mode() == "cold" do
      cells = for i <- 1..needed, do: Measure.cell("w", i)

      for chunk <- Enum.chunk_every(cells, 1_000),
          do: :ok = Measure.call(ctx, :drop_local, [chunk])
    end

    offsets
  end

  def run(ctx, _level, offsets) do
    result =
      Measure.open_loop(ctx, offsets, Measure.remote(ctx, :wake, &[Measure.cell("w", &1 + 1)]))

    asleep = Measure.call(ctx, :await_asleep, [Env.int("TAIL_S", 60) * 1000], 120_000)
    Measure.put_extra(result, %{mode: mode(), asleep_again: asleep == :ok})
  end
end
