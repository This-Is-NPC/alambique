defmodule Alambique.Load.Config do
  @moduledoc false
  # The settings of one case: the `case.json` that scripts/load_config.py wrote
  # (see docs/how-to-load-test.md). The driver reads it once at start and keeps
  # it for the measures. Keys are strings, as in the file.

  @key {__MODULE__, :case}
  @never 1.0e12

  @doc "Reads a case file."
  @spec read!(Path.t()) :: map
  def read!(path), do: path |> File.read!() |> Jason.decode!()

  @doc "Makes `cfg` the settings the rest of the harness reads."
  @spec put(map) :: :ok
  def put(cfg), do: :persistent_term.put(@key, cfg)

  @doc "The value at `path` (a list of keys) of the settings."
  @spec get([String.t()]) :: term
  def get(path), do: get_in(:persistent_term.get(@key), path)

  @doc "The workload setting `key` of the case's measure."
  @spec workload(String.t()) :: term
  def workload(key), do: get(["workload", key])

  @doc """
  The limits the verdicts and the guards use, from the criteria of the case.
  A criterion that is off gets a limit nothing reaches, so that the checks of
  `Alambique.Load.Verdict` and `Alambique.Load.Capacity` stay as they are.
  `holds_memory?` is true for a measure whose state keeps memory between steps.
  """
  @spec limits(map, boolean) :: map
  def limits(cfg, holds_memory?) do
    j = cfg["judging"]

    %{
      delivery_min: limit(j["delivery"], "min_ratio", 0.0),
      max_net_load: limit(j["host_load"], "max", @never),
      max_driver_swap_mb: limit(j["driver_swap"], "max_mb", @never),
      max_store_swap_mb: limit(j["store_swap"], "max_growth_mb", @never),
      max_host_swap_mb: limit(j["host_swap"], "max_mb", @never),
      max_error_ratio: limit(j["errors"], "max_pct", 100.0) / 100,
      max_busy_ratio: limit(j["busy"], "max_pct", 100.0) / 100,
      latency: latency(j["latency"]),
      mem_limit_mb: cfg["memory_mb"],
      mem_fraction: limit(j["memory"], "max_pct", @never) / 100,
      cooldown?: j["cooldown"]["enabled"],
      cooldown_mem_fraction: j["cooldown"]["max_mem_pct"] / 100,
      cooldown_max_s: j["cooldown"]["max_wait_s"],
      cooldown_memory?: not holds_memory?,
      store_mb: cfg["store"]["tmpfs_mb"],
      store_full_fraction: limit(j["store_full"], "max_pct", @never) / 100,
      store_error_fraction: limit(j["store_full"], "error_pct", @never) / 100,
      store_min_free_mb: limit(j["store_full"], "min_free_mb", 0.0),
      suspect_throttle_fraction: limit(j["suspect_throttle"], "max_pct", @never) / 100,
      fit_min_r2: cfg["report"]["fit_min_r2"],
      fit_min_span: cfg["report"]["fit_min_span"],
      min_available_gib: limit(j["host_memory"], "min_available_gib", 0.0),
      settle_s: j["host_load"]["quiet_window_s"],
      wait_quiet?: j["host_load"]["enabled"],
      wait_quiet_step_s: j["host_load"]["quiet_max_wait_s"],
      data_estimate_fraction: limit(j["data_full"], "estimate_pct", @never) / 100,
      data_full_fraction: limit(j["data_full"], "full_pct", @never) / 100,
      attempts: cfg["attempts"]
    }
  end

  defp limit(%{"enabled" => true} = criterion, key, _off), do: criterion[key] * 1.0
  defp limit(_criterion, _key, off), do: off * 1.0

  defp latency(%{"enabled" => true, "percentile" => p, "max_ms" => ms}),
    do: {String.to_existing_atom(p), ms * 1.0}

  defp latency(_), do: nil
end
