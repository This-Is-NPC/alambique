defmodule Alambique.Load.Schedule do
  @moduledoc false
  # Arrival times of an open-loop step: the offsets, in microseconds from the
  # start of the step, at which operations are due. They do not depend on how
  # fast the service answers.

  @doc """
  Offsets of `rate` operations per second over `duration_s` seconds, ascending.
  `:fixed` spaces them evenly; `:poisson` draws exponential gaps from `seed`.
  """
  @spec offsets(number, number, :fixed | :poisson, integer) :: [non_neg_integer]
  def offsets(rate, duration_s, kind, seed \\ 1)

  def offsets(rate, duration_s, :fixed, _seed) do
    count = trunc(rate * duration_s)
    for i <- 0..(count - 1)//1, do: trunc(i * 1_000_000 / rate)
  end

  def offsets(rate, duration_s, :poisson, seed) do
    state = :rand.seed_s(:exsss, {seed, 7, 11})
    draw(state, rate, 0.0, duration_s * 1_000_000, [])
  end

  defp draw(state, rate, at, limit, acc) do
    {u, state} = :rand.uniform_s(state)
    next = at + -:math.log(1.0 - u) / rate * 1_000_000

    if next >= limit,
      do: Enum.reverse(acc),
      else: draw(state, rate, next, limit, [trunc(next) | acc])
  end
end
