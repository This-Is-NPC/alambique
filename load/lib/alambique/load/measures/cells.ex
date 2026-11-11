defmodule Alambique.Load.Measures.Cells do
  @moduledoc false
  # Awake cells per node: each step wakes the cells it adds to the ones already
  # awake (the idle timeout of the service is longer than the ramp), at a fixed
  # rate, then holds so that memory settles. The step ends early when process
  # memory reaches its criterion. Level = cells awake at the end of the step.
  # Memory per cell is the slope of process memory against cells awake across
  # the steps (see Alambique.Load.Summary); there is no refinement or
  # confirmation, because the cells stay awake.

  alias Alambique.Load.{Env, Measure}

  def unit, do: "awake cells"
  def latency, do: nil
  def mem_fraction, do: Env.float("CELLS_MEM_FRACTION", 0.9)
  def probe?, do: false
  # The awake cells are the state of the ramp: memory stays high by design.
  def holds_memory?, do: true
  def ramp_opts, do: [refine: 0, confirm: false]

  def levels,
    do:
      "LEVELS"
      |> Env.numbers([500, 1_000, 2_000, 4_000, 8_000, 16_000, 32_000])
      |> Enum.map(&round/1)

  def setup(_ctx), do: :ok

  def prepare(_ctx, level) do
    rate = Env.int("CELLS_FILL_RATE", 100)
    added = max(level - Alambique.Load.State.get(:awake, 0), 0)
    {:ok, for(i <- 0..(added - 1)//1, do: div(i * 1_000_000, rate))}
  end

  @doc """
  The cells a disturbed attempt woke, `a-(before + 1)` to `a-now`: put to sleep
  before a repeat, so that the repeat wakes (and measures) them again.
  """
  def added_cells(before, now), do: for(i <- (before + 1)..now//1, do: Measure.cell("a", i))

  # Called after a step that ran and is to be repeated.
  def rewind(ctx, _level) do
    before = Alambique.Load.State.get(:awake_before, 0)
    now = Alambique.Load.State.get(:awake, 0)
    _failed = Measure.call(ctx, :sleep_cells, [added_cells(before, now), 64], 600_000)
    Alambique.Load.State.put(:awake, before)
  end

  def run(ctx, level, offsets) do
    have = Alambique.Load.State.get(:awake, 0)
    Alambique.Load.State.put(:awake_before, have)

    result =
      Measure.open_loop(
        ctx,
        offsets,
        Measure.remote(ctx, :touch, &[Measure.cell("a", have + &1 + 1)])
      )

    Alambique.Load.State.put(:awake, have + result.ok)

    result = if result.aborted, do: result, else: hold(ctx, result, Env.int("HOLD_S", 60))
    awake = Measure.call(ctx, :awake)
    Measure.put_extra(result, %{awake: awake, target: level, added: result.ok})
  end

  # Waits for memory to settle, watching the same stop conditions.
  defp hold(ctx, result, seconds) do
    Enum.reduce_while(1..max(seconds, 1), result, fn _, acc ->
      case ctx.abort.() do
        nil ->
          Process.sleep(1_000)
          {:cont, acc}

        reason ->
          {:halt, %{acc | aborted: reason}}
      end
    end)
  end
end
