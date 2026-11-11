defmodule Alambique.Load.Ramp do
  @moduledoc false
  # Which level to run next. A ramp climbs a list of levels until one is bad;
  # it then refines by bisection between the last good level and the first bad
  # one, and confirms the last good level once more. An invalid step (the
  # harness could not deliver the load) ends the ramp without a verdict.

  defstruct phase: :ramp,
            levels: [],
            refine_left: 0,
            confirm?: true,
            lo: nil,
            hi: nil,
            confirmation: nil,
            stopped: nil

  @type verdict :: :good | :bad | :invalid
  @type t :: %__MODULE__{}

  @doc """
  A ramp over ascending integer `levels`. Options: `:refine` (bisection steps,
  default 2) and `:confirm` (repeat the last good level, default true).
  """
  @spec new([integer], keyword) :: t
  def new(levels, opts \\ []) do
    %__MODULE__{
      levels: levels,
      refine_left: Keyword.get(opts, :refine, 2),
      confirm?: Keyword.get(opts, :confirm, true)
    }
  end

  @doc "The next step, `{:run, level, phase}`, or `:done`."
  @spec next(t) :: {:run, integer, :ramp | :refine | :confirm} | :done
  def next(%{phase: :done}), do: :done
  def next(%{phase: :ramp, levels: [level | _]}), do: {:run, level, :ramp}
  def next(%{phase: :refine, lo: lo, hi: hi}), do: {:run, div(lo + hi, 2), :refine}
  def next(%{phase: :confirm, lo: lo}), do: {:run, lo, :confirm}

  @doc "Records the verdict of the step `next/1` returned."
  @spec record(t, verdict) :: t
  def record(%{phase: :done} = ramp, _verdict), do: ramp

  def record(%{phase: :confirm} = ramp, verdict),
    do: %{ramp | phase: :done, confirmation: verdict}

  def record(ramp, :invalid), do: %{ramp | phase: :done, stopped: :invalid}

  def record(%{phase: :ramp, levels: [level | rest]} = ramp, :good) do
    ramp = %{ramp | levels: rest, lo: level}
    if rest == [], do: to_confirm(ramp, :exhausted), else: ramp
  end

  def record(%{phase: :ramp, levels: [level | _], lo: nil} = ramp, :bad),
    do: %{ramp | phase: :done, hi: level, stopped: :bad}

  def record(%{phase: :ramp, levels: [level | _]} = ramp, :bad), do: refine(%{ramp | hi: level})

  def record(%{phase: :refine, lo: lo, hi: hi} = ramp, verdict) do
    mid = div(lo + hi, 2)
    ramp = if verdict == :good, do: %{ramp | lo: mid}, else: %{ramp | hi: mid}
    refine(%{ramp | refine_left: ramp.refine_left - 1})
  end

  defp refine(%{refine_left: left, lo: lo, hi: hi} = ramp) when left > 0 and hi - lo > 1,
    do: %{ramp | phase: :refine}

  defp refine(ramp), do: to_confirm(ramp, :bad)

  defp to_confirm(%{confirm?: true} = ramp, stopped),
    do: %{ramp | phase: :confirm, stopped: stopped}

  defp to_confirm(ramp, stopped), do: %{ramp | phase: :done, stopped: stopped}

  @doc "What the ramp found: last good level, first bad level, why it stopped and the confirmation."
  @spec outcome(t) :: map
  def outcome(ramp) do
    %{
      last_good: ramp.lo,
      first_bad: ramp.hi,
      stopped: ramp.stopped,
      confirmation: ramp.confirmation
    }
  end
end
