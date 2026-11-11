defmodule Alambique.Load.Verdict do
  @moduledoc false
  # Decides what one step means, in this order:
  #
  #   1. host net load above the limit        -> repeat the step
  #   2. process memory at its criterion      -> bad
  #   3. fewer than 95% of the planned
  #      operations completed                 -> invalid (not a failure: the
  #                                              harness or the service did not
  #                                              carry the load it was asked)
  #   4. errors above the allowed share       -> bad
  #   5. the latency percentile of the
  #      measure above its limit              -> bad
  #   otherwise good.
  #
  # `stats` holds `planned`, `completed`, `errors`, `latency_ms` (percentile
  # name to value), `net_load_max`, `mem_mb_max` and `aborted` (`:memory` when
  # the step stopped early because memory reached its criterion). `limits`
  # holds `delivery_min`, `max_net_load`, `max_error_ratio`, `latency`
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

      stats[:aborted] == :memory or stats.mem_mb_max >= limits.mem_limit_mb * limits.mem_fraction ->
        {:bad, :memory}

      delivery_ratio(stats) < limits.delivery_min ->
        {:invalid, {:delivery, delivery_ratio(stats)}}

      stats.errors > limits.max_error_ratio * stats.completed ->
        {:bad, :errors}

      latency_bad?(stats, limits.latency) ->
        {percentile, limit} = limits.latency
        {:bad, {:latency, percentile, limit}}

      true ->
        :good
    end
  end

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
