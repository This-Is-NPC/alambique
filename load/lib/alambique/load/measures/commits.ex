defmodule Alambique.Load.Measures.Commits do
  @moduledoc false
  # Commits per second on one node: N cells written round-robin, each commit
  # one acknowledged write. Variant `small` writes a 120-byte row, `large` a
  # 75 KB blob. Ack latency is measured at the caller from the scheduled time.

  alias Alambique.Load.{Env, Measure}

  def unit, do: "commits/s"
  def latency, do: {:p99, Env.float("COMMITS_P99_MS", 250)}
  def mem_fraction, do: 1.0
  def probe?, do: false
  def holds_memory?, do: false
  def ramp_opts, do: [refine: Env.int("REFINE", 2)]

  def levels do
    default =
      if variant() == :large,
        do: [5, 10, 20, 40, 80, 160, 320],
        else: [50, 100, 200, 400, 800, 1600, 3200]

    "LEVELS" |> Env.numbers(default) |> Enum.map(&round/1)
  end

  defp variant, do: if(Env.get("VARIANT") == "large", do: :large, else: :small)
  defp cells, do: Env.int("COMMIT_CELLS", 64)
  defp rows, do: Env.int("COMMIT_ROWS", 8)

  def setup(ctx) do
    # Wake every cell before the first step, so that no step pays for it.
    ids = for i <- 1..cells(), do: Measure.cell("c", i)

    ids
    |> Task.async_stream(&Measure.call(ctx, :touch, [&1]), max_concurrency: 32, timeout: 120_000)
    |> Enum.each(fn {:ok, :ok} -> :ok end)

    :ok
  end

  def prepare(_ctx, level), do: {:ok, Measure.offsets(level, Measure.step_s(), level)}

  # A repeat writes everything again; nothing to undo.
  def rewind(_ctx, _level), do: :ok

  def run(ctx, _level, offsets) do
    kind = variant()
    n = cells()
    rows = rows()
    # Each step writes new values: the sequence continues where it stopped.
    base = Alambique.Load.State.get(:seq, 0)
    Alambique.Load.State.put(:seq, base + length(offsets))

    op =
      Measure.remote(ctx, :commit, fn i ->
        [Measure.cell("c", rem(i, n) + 1), kind, base + i, rows]
      end)

    result = Measure.open_loop(ctx, offsets, op)
    Measure.put_extra(result, %{kind: kind, cells: n})
  end
end
