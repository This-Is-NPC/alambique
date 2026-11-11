defmodule Alambique.Load.Measure do
  @moduledoc false
  # What the four measures share: calls on the service node, the open-loop
  # run of a schedule against it, and the population of cells they use.
  #
  # A measure module defines `unit/0`, `levels/0`, `ramp_opts/0`, `latency/0`
  # (`{percentile, limit_ms}` or nil), `mem_fraction/0` (the share of the memory
  # limit that stops the ramp), `probe?/0`, `setup/1` (once), `prepare/2` (before
  # each step, outside its measurement, returns what `run/3` needs) and
  # `run/3`, which returns the map of `Alambique.Load.OpenLoop.run/3` plus
  # `:extra`.

  alias Alambique.Load.{Env, OpenLoop, Schedule}

  @ops Alambique.Load.Service.Ops

  @doc "A call on the service node."
  def call(ctx, fun, args \\ [], timeout \\ 60_000),
    do: :erpc.call(ctx.node, @ops, fun, args, timeout)

  @doc "Settings common to the open-loop steps."
  def step_s, do: Env.int("STEP_S", 180)
  def arrivals, do: if(Env.get("ARRIVALS", "poisson") == "fixed", do: :fixed, else: :poisson)

  def offsets(rate, step_s \\ step_s(), seed \\ 1),
    do: Schedule.offsets(rate, step_s, arrivals(), seed)

  @doc "Runs `op.(index)` on the schedule, stopping early when the host or the service says so."
  def open_loop(ctx, offsets, op) do
    OpenLoop.run(offsets, op,
      max_outstanding: Env.int("MAX_OUTSTANDING", 5_000),
      drain_ms: Env.int("DRAIN_S", 20) * 1000,
      abort: ctx.abort
    )
  end

  @doc "The op of a rate measure: one call on the service with a timeout."
  def remote(ctx, fun, args_fun) do
    timeout = Env.int("OP_TIMEOUT_MS", 15_000)
    fn index -> :erpc.call(ctx.node, @ops, fun, args_fun.(index), timeout) end
  end

  @doc """
  Makes sure cells `1..count` of `prefix` exist with data and sleep. Existing
  ones (`State` key `{:populated, prefix}`) are kept.
  """
  def populate(ctx, prefix, count) do
    key = {:populated, prefix}
    have = Alambique.Load.State.get(key, 0)

    if count > have do
      cells = for i <- (have + 1)..count, do: "#{prefix}-#{i}"
      rows = Env.int("CELL_ROWS", 20)
      bytes = Env.int("ROW_BYTES", 1_500)
      conc = Env.int("POPULATE_CONCURRENCY", 64)

      for chunk <- Enum.chunk_every(cells, 500) do
        [] = call(ctx, :populate, [chunk, rows, bytes, conc], 600_000)
      end

      Alambique.Load.State.put(key, count)
    end

    :ok
  end

  def cell(prefix, i), do: "#{prefix}-#{i}"

  def put_extra(result, extra), do: Map.update(result, :extra, extra, &Map.merge(&1, extra))
end
