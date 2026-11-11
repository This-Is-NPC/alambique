defmodule Alambique.Load.Stats do
  @moduledoc false
  # Percentiles and the least-squares line of the step summaries.

  @doc "Nearest-rank percentile of a sorted list; `nil` for an empty list."
  @spec percentile([number], number) :: number | nil
  def percentile([], _p), do: nil

  def percentile(sorted, p) do
    n = length(sorted)
    Enum.at(sorted, min(n - 1, max(ceil(p / 100 * n) - 1, 0)))
  end

  @spec mean([number]) :: float | nil
  def mean([]), do: nil
  def mean(values), do: Enum.sum(values) / length(values)

  @doc """
  The line `y = intercept + slope * x` through `points`, or `nil` when there
  are fewer than two distinct `x`. `r2` is the share of the variance of `y`
  the line explains (1.0 when `y` does not vary).
  """
  @spec linear_fit([{number, number}]) :: %{slope: float, intercept: float, r2: float} | nil
  def linear_fit(points) do
    mx = mean(Enum.map(points, &elem(&1, 0)))
    my = mean(Enum.map(points, &elem(&1, 1)))
    sxx = Enum.sum(for {x, _} <- points, do: (x - mx) * (x - mx))

    if length(points) < 2 or sxx == 0 do
      nil
    else
      sxy = Enum.sum(for {x, y} <- points, do: (x - mx) * (y - my))
      slope = sxy / sxx
      syy = Enum.sum(for {_, y} <- points, do: (y - my) * (y - my))

      %{
        slope: slope,
        intercept: my - slope * mx,
        r2: if(syy == 0, do: 1.0, else: sxy * sxy / (sxx * syy))
      }
    end
  end
end
