defmodule Alambique.Load.Verdict do
  @moduledoc false
  # Decides what one step means. `decide/2` first reads why the step ended,
  # then `judge/2` takes the measurements in this order:
  #
  #   1. host net load above the limit        -> repeat the step
  #   2. swap: the driver's swap, or the
  #      host's, above its limit, or the
  #      store's swap grown by more than its
  #      limit during the step                -> repeat the step
  #      the service has any swap             -> invalid (`:swap`)
  #   3. process memory at its criterion      -> bad
  #   4. nothing was planned                  -> invalid (`:no_operations`; a
  #                                              step without operations is
  #                                              never good)
  #   5. errors that say the store is full    -> invalid (`:store_full`; a store
  #                                              5xx counts once the store is
  #                                              past `store_error_fraction`)
  #   6. fewer than 95% of the planned
  #      operations completed                 -> invalid (not a failure: the
  #                                              harness or the service did not
  #                                              carry the load it was asked)
  #   7. errors above the allowed share       -> bad (`:errors`)
  #   8. `{:error, :busy}` refusals above
  #      their share of the completed ones    -> bad (`:busy`)
  #   9. the latency percentile of the
  #      measure above its limit              -> bad
  #   otherwise good.
  #
  # `stats` holds `planned`, `completed`, `ok`, `errors` (without the busy
  # refusals), `busy`, `latency_ms` (percentile name to value), `net_load_max`,
  # `swap` (`%{driver_max_mb, host_max_mb, store_growth_mb, service_mb}`, nil
  # values when unreadable), `mem_mb_max` (nil without a reading), `aborted`
  # (why the step stopped early, or nil) and `service_gone?` (the container
  # could not be read after the step). `limits` holds `delivery_min`,
  # `max_net_load`, `max_driver_swap_mb`, `max_host_swap_mb`,
  # `max_store_swap_mb`, `max_error_ratio`, `max_busy_ratio`, `latency`
  # (`{percentile, limit_ms}` or nil), `mem_limit_mb` and `mem_fraction` (the
  # share of the limit that is the criterion).

  @type result :: :good | {:bad, term} | {:invalid, term} | {:repeat, term}

  @spec delivery_ratio(map) :: float
  def delivery_ratio(%{planned: 0}), do: 1.0
  def delivery_ratio(%{planned: planned, completed: completed}), do: completed / planned

  @doc """
  The verdict of a finished step. The reason the step ended comes first: the
  harness's own stops are invalid, and a stop on memory is `bad` even when it
  came before any reading (`:service_down` is only a container that is gone).
  """
  @spec decide(map, map) :: result
  def decide(stats, limits) do
    cond do
      stats.aborted in [:host_low_memory, :service_down, :store_full] ->
        {:invalid, stats.aborted}

      stats.service_gone? ->
        {:invalid, :service_down}

      stats.mem_mb_max == nil and stats.aborted != :memory ->
        {:invalid, :no_samples}

      true ->
        judge(stats, limits)
    end
  end

  @spec judge(map, map) :: result
  def judge(stats, limits) do
    cond do
      stats.net_load_max > limits.max_net_load ->
        {:repeat, {:host_busy, stats.net_load_max}}

      swap = swap_problem(stats[:swap], limits) ->
        swap

      stats[:aborted] == :memory or stats.mem_mb_max >= limits.mem_limit_mb * limits.mem_fraction ->
        {:bad, :memory}

      stats.planned == 0 ->
        {:invalid, :no_operations}

      stats.errors > 0 and stats[:store_full] ->
        {:invalid, :store_full}

      delivery_ratio(stats) < limits.delivery_min ->
        {:invalid, {:delivery, delivery_ratio(stats)}}

      stats.errors > limits.max_error_ratio * stats.completed ->
        {:bad, :errors}

      stats.busy > limits.max_busy_ratio * stats.completed ->
        {:bad, :busy}

      latency_bad?(stats, limits.latency) ->
        {percentile, limit} = limits.latency
        {:bad, {:latency, percentile, limit}}

      true ->
        :good
    end
  end

  defp swap_problem(nil, _), do: nil

  defp swap_problem(swap, limits) do
    cond do
      (swap.service_mb || 0) > 0 ->
        {:invalid, {:swap, :service, swap.service_mb}}

      (swap.driver_max_mb || 0) > limits.max_driver_swap_mb ->
        {:repeat, {:swap, :driver, swap.driver_max_mb}}

      (swap.store_growth_mb || 0) > limits.max_store_swap_mb ->
        {:repeat, {:swap, :store, swap.store_growth_mb}}

      (swap.host_max_mb || 0) > limits.max_host_swap_mb ->
        {:repeat, {:swap, :host, swap.host_max_mb}}

      true ->
        nil
    end
  end

  @full ~r/no space|nospace|enospc|disk full|volume (is )?full|quota/i

  @server_error ~r/\{:http, 5\d\d/

  @doc """
  Whether the error samples of a step (`%{error: text, count: n}`) say the store
  is full: a "no space" text, or a store 5xx while the store was `strained`
  (near its limit, see `Capacity.strained?/2`; a store that full answers 500
  without saying why).
  """
  @spec store_full?([map], boolean) :: boolean
  def store_full?(error_samples, strained?) do
    Enum.any?(error_samples, &(&1.error =~ @full or (strained? and &1.error =~ @server_error)))
  end

  defp latency_bad?(_stats, nil), do: false

  defp latency_bad?(stats, {percentile, limit_ms}) do
    case stats.latency_ms[percentile] do
      nil -> false
      value -> value > limit_ms
    end
  end

  @doc "Whether the service's CPU was throttled for more than `fraction` of the step."
  @spec suspect?(number, number, number) :: boolean
  def suspect?(throttled_ms, duration_s, fraction),
    do: duration_s > 0 and throttled_ms / 1000 > fraction * duration_s

  @doc "The verdict as the ramp sees it."
  @spec ramp_verdict(result) :: Alambique.Load.Ramp.verdict()
  def ramp_verdict(:good), do: :good
  def ramp_verdict({:bad, _}), do: :bad
  def ramp_verdict({:invalid, _}), do: :invalid
end
