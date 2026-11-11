defmodule Alambique.Load.Measures.Wakes do
  @moduledoc false
  # Wakes per second: a population of cells with data is put to sleep, then
  # distinct sleeping cells are woken, one read each, at the target rate. Each
  # step wakes cells no step woke before, so the population grows with the
  # level and the step length (`ALAMBIQUE_LOAD_WAKES_STEP_S`, default the
  # global step) and sits on the service's data tmpfs, asleep.
  #
  # In `cold` mode (the default) the local file of every cell of the step is
  # removed first (`Ops.drop_local/1`), so a wake restores the cell from the
  # store, as after a move or a restart; `local` keeps the file, and a wake
  # reopens it and only checks the store. The measured window ends when the
  # woken cells are asleep again, so the trips and the CPU per wake include
  # putting the cell back to sleep.
  #
  # The population has to fit the data tmpfs: before preparing a level its data
  # is estimated (fresh cells times the bytes one asleep cell took, measured
  # after the previous level, or `ALAMBIQUE_LOAD_CELL_BYTES` before any), and a
  # level that would take more than `ALAMBIQUE_LOAD_DATA_ESTIMATE_FRACTION`
  # (0.9) of `ALAMBIQUE_LOAD_DATA_MB` is invalid `data_full` without running.

  alias Alambique.Load.{Capacity, Env, Measure, Sampler, State}

  @mib 1_048_576

  def unit, do: "wakes/s"
  def latency, do: {:p95, Env.float("WAKES_P95_MS", 500)}
  def mem_fraction, do: 1.0
  def probe?, do: false
  def holds_memory?, do: false
  def ramp_opts, do: [refine: Env.int("REFINE", 2)]
  def levels, do: "LEVELS" |> Env.numbers([5, 10, 20, 40, 80, 160, 320]) |> Enum.map(&round/1)

  defp mode, do: Env.get("WAKE_MODE", "cold")
  def step_s, do: Env.int("WAKES_STEP_S", Measure.step_s())

  def setup(_ctx), do: :ok

  # A repeat prepares again (the population is kept, the local files dropped).
  def rewind(_ctx, _level), do: :ok

  def prepare(ctx, level) do
    offsets = Measure.offsets(level, step_s(), level)
    needed = length(offsets)

    with {:ok, shmem_mb} <- data_used_mb(),
         :ok <- fits(needed, shmem_mb),
         :ok <- populate(ctx, needed),
         :ok <- asleep(ctx) do
      remember_cell_bytes()
      if mode() == "cold", do: drop_local(ctx, needed)
      {:ok, offsets}
    end
  end

  defp data_used_mb do
    case Sampler.cgroup() do
      {:ok, reading} -> {:ok, reading.shmem_mb}
      :error -> {:invalid, :service_down, %{}}
    end
  end

  defp fits(needed, shmem_mb) do
    State.get(:data_base_mb) || State.put(:data_base_mb, shmem_mb)
    fresh = max(needed - State.get({:populated, "w"}, 0), 0)
    per_cell = State.get(:cell_bytes, Env.int("CELL_BYTES", 160_000))
    data_mb = Env.int("DATA_MB", 1024)

    if Capacity.data_exceeds?(shmem_mb, fresh, per_cell, data_mb, estimate_fraction()) do
      estimate_mb = shmem_mb + fresh * per_cell / @mib

      {:invalid, :data_full,
       %{
         data_estimate_mb: Float.round(estimate_mb, 1),
         data_used_mb: Float.round(shmem_mb, 1),
         data_mb: data_mb,
         fresh_cells: fresh,
         cell_bytes: round(per_cell)
       }}
    else
      :ok
    end
  end

  defp populate(ctx, needed) do
    case Measure.populate(ctx, "w", needed) do
      :ok -> :ok
      {:error, failed, reason} -> failure(%{prepare_error: reason, failed_cells: failed})
    end
  end

  defp asleep(ctx) do
    case Measure.call(ctx, :await_asleep, [120_000], 150_000) do
      :ok -> :ok
      error -> failure(%{prepare_error: inspect(error, limit: 10)})
    end
  end

  # A failed preparation is `data_full` when the data tmpfs is at its limit,
  # else `prepare_failed`; the first error is kept.
  defp failure(extra) do
    shmem_mb = with {:ok, shmem_mb} <- data_used_mb(), do: shmem_mb

    reason =
      if is_number(shmem_mb),
        do: Capacity.prepare_failure(shmem_mb, Env.int("DATA_MB", 1024), full_fraction()),
        else: :prepare_failed

    {:invalid, reason, extra}
  end

  # Bytes one asleep cell takes, from the data tmpfs now.
  defp remember_cell_bytes do
    with {:ok, shmem_mb} <- data_used_mb() do
      populated = State.get({:populated, "w"}, 0)
      base_mb = State.get(:data_base_mb, 0)
      State.put(:cell_bytes, Capacity.cell_bytes(shmem_mb, base_mb, populated, 0))
    end
  end

  defp drop_local(ctx, needed) do
    cells = for i <- 1..needed, do: Measure.cell("w", i)

    for chunk <- Enum.chunk_every(cells, 1_000),
        do: :ok = Measure.call(ctx, :drop_local, [chunk])
  end

  defp estimate_fraction, do: Env.float("DATA_ESTIMATE_FRACTION", 0.9)
  defp full_fraction, do: Env.float("DATA_FULL_FRACTION", 0.95)

  def run(ctx, _level, offsets) do
    result =
      Measure.open_loop(ctx, offsets, Measure.remote(ctx, :wake, &[Measure.cell("w", &1 + 1)]))

    asleep = Measure.call(ctx, :await_asleep, [Env.int("TAIL_S", 60) * 1000], 120_000)
    Measure.put_extra(result, %{mode: mode(), asleep_again: asleep == :ok, step_s: step_s()})
  end
end
