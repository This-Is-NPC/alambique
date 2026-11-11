defmodule Alambique.Load.Verdict do
  @moduledoc false
  # Decides what one step means, in this order:
  #
  #   1. host net load above the limit        -> repeat the step
  #   2. swap: the driver's swap, or the
  #      host's, above its limit, or the
  #      store's swap grown by more than its
  #      limit during the step                -> repeat the step
  #      the service has any swap             -> invalid (`:swap`)
  #   3. process memory at its criterion      -> bad
  #   4. fewer than 95% of the planned
  #      operations completed                 -> invalid (not a failure: the
  #                                              harness or the service did not
  #                                              carry the load it was asked)
  #   5. errors above the allowed share       -> bad (`:store_full` when
  #                                              the errors say the store is
  #                                              out of space, else `:errors`)
  #   6. the latency percentile of the
  #      measure above its limit              -> bad
  #   otherwise good.
  #
  # `stats` holds `planned`, `completed`, `errors`, `latency_ms` (percentile
  # name to value), `net_load_max`, `swap` (`%{driver_max_mb, host_max_mb, store_growth_mb, service_mb}`,
  # nil values when unreadable), `mem_mb_max` and `aborted` (`:memory` when
  # the step stopped early because memory reached its criterion). `limits`
  # holds `delivery_min`, `max_net_load`, `max_driver_swap_mb`, `max_host_swap_mb`, `max_store_swap_mb`, `max_error_ratio`, `latency`
  # (`{percentile, limit_ms}` or nil), `mem_limit_mb` and `mem_fraction` (the
  # share of the limit that is the criterion).

  @type result :: :good | {:bad, term} | {:invalid, term} | {:repeat, term}

  @spec delivery_ratio(map) :: float
  def delivery_ratio(%{planned: 0}), do: 1.0
  def delivery_ratio(%{planned: planned, completed: completed}), do: completed / planned

  @spec judge(map, map) :: result
  def judge(stats, limits) do
    cond do
      stats.net_load_max > limits.max_net_load ->
        {:repeat, {:host_busy, stats.net_load_max}}

      swap = swap_problem(stats[:swap], limits) ->
        swap

      stats[:aborted] == :memory or stats.mem_mb_max >= limits.mem_limit_mb * limits.mem_fraction ->
        {:bad, :memory}

      delivery_ratio(stats) < limits.delivery_min ->
        {:invalid, {:delivery, delivery_ratio(stats)}}

      stats.errors > limits.max_error_ratio * stats.completed ->
        {:bad, if(stats[:store_full], do: :store_full, else: :errors)}

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

  @doc "Whether the error samples of a step (`%{error: text, count: n}`) say the store is full."
  @spec store_full?([map]) :: boolean
  def store_full?(error_samples), do: Enum.any?(error_samples, &(&1.error =~ @full))

  defp latency_bad?(_stats, nil), do: false

  defp latency_bad?(stats, {percentile, limit_ms}) do
    case stats.latency_ms[percentile] do
      nil -> false
      value -> value > limit_ms
    end
  end

  @doc "The verdict as the ramp sees it."
  @spec ramp_verdict(result) :: Alambique.Load.Ramp.verdict()
  def ramp_verdict(:good), do: :good
  def ramp_verdict({:bad, _}), do: :bad
  def ramp_verdict({:invalid, _}), do: :invalid
end
