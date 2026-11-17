defmodule Alambique.Load.RampTest do
  @moduledoc """
  The levels a ramp runs, against a service that is good up to a threshold:
  the ramp climbs, refines strictly between the last good and the first bad
  level, confirms the last good one, and an invalid step ends it without a
  verdict.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Load.Ramp

  # Runs the ramp to its end, `verdict.(level)` deciding each step; returns the
  # steps as `{level, phase, verdict}` and the outcome.
  defp drive(ramp, verdict, steps \\ []) do
    case Ramp.next(ramp) do
      :done ->
        {Enum.reverse(steps), Ramp.outcome(ramp)}

      {:run, level, phase} ->
        v = verdict.(level)
        drive(Ramp.record(ramp, v), verdict, [{level, phase, v} | steps])
    end
  end

  property "the last good level is below the threshold and the first bad one above it" do
    check all(
            levels <- uniq_list_of(integer(1..2_000), min_length: 1, max_length: 12),
            threshold <- integer(0..2_100),
            refine <- integer(0..5)
          ) do
      levels = Enum.sort(levels)
      verdict = fn level -> if level <= threshold, do: :good, else: :bad end
      {steps, outcome} = drive(Ramp.new(levels, refine: refine), verdict)

      ran = for {level, _, _} <- steps, do: level
      good = for {level, _, :good} <- steps, do: level
      bad = for {level, _, :bad} <- steps, do: level

      # The ramp phase runs a prefix of the levels, in order.
      ramped = for {level, :ramp, _} <- steps, do: level
      assert ramped == Enum.take(levels, length(ramped))

      # Every refinement is strictly between the last good and the first bad
      # level known when it runs, so the bracket only narrows.
      steps
      |> Enum.reduce({nil, nil}, fn
        {level, :refine, v}, {lo, hi} ->
          assert lo < level and level < hi
          if v == :good, do: {level, hi}, else: {lo, level}

        {level, _, :good}, {_lo, hi} ->
          {level, hi}

        {level, _, :bad}, {lo, nil} ->
          {lo, level}

        _, acc ->
          acc
      end)

      assert outcome.last_good == Enum.max(good, fn -> nil end)
      assert outcome.first_bad == Enum.min(bad, fn -> nil end)

      if outcome.last_good && outcome.first_bad,
        do: assert(outcome.last_good <= threshold and threshold < outcome.first_bad)

      # The last good level is run once more, unless there was none.
      case outcome.last_good do
        nil -> assert [{_, :ramp, :bad}] = steps
        lo -> assert {^lo, :confirm, _} = List.last(steps)
      end

      assert length(ran) <= length(levels) + refine + 1
    end
  end

  test "a bad confirmation is recorded" do
    ramp = Ramp.new([10, 20], refine: 0)
    ramp = ramp |> Ramp.record(:good) |> Ramp.record(:bad)
    assert Ramp.next(ramp) == {:run, 10, :confirm}

    assert %{confirmation: :bad, last_good: 10, first_bad: 20} =
             ramp |> Ramp.record(:bad) |> Ramp.outcome()
  end

  test "an invalid step ends the ramp without a verdict on the level" do
    ramp = Ramp.new([10, 20, 40]) |> Ramp.record(:good) |> Ramp.record(:invalid)
    assert Ramp.next(ramp) == :done

    assert %{last_good: 10, first_bad: nil, stopped: :invalid, confirmation: nil} =
             Ramp.outcome(ramp)
  end

  test "without confirmation or refinement a bad level ends the ramp" do
    ramp =
      Ramp.new([10, 20, 40], refine: 0, confirm: false) |> Ramp.record(:good) |> Ramp.record(:bad)

    assert Ramp.next(ramp) == :done
    assert %{last_good: 10, first_bad: 20, stopped: :bad} = Ramp.outcome(ramp)
  end

  test "levels that run out are confirmed" do
    ramp = Ramp.new([10, 20]) |> Ramp.record(:good) |> Ramp.record(:good)
    assert Ramp.next(ramp) == {:run, 20, :confirm}
    assert %{stopped: :exhausted, first_bad: nil} = Ramp.outcome(ramp)
  end

  describe "the sequence of a round's steps" do
    # Good up to `threshold`: the `{level, phase}` of every step that runs.
    defp sequence(levels, threshold, opts \\ []) do
      {steps, _} =
        levels
        |> Ramp.new(opts)
        |> drive(fn level -> if level <= threshold, do: :good, else: :bad end)

      for {level, phase, _} <- steps, do: {level, phase}
    end

    test "climbs, bisects between the last good and the first bad level, then confirms" do
      assert sequence([50, 100, 200, 400], 150, refine: 2) ==
               [
                 {50, :ramp},
                 {100, :ramp},
                 {200, :ramp},
                 {150, :refine},
                 {175, :refine},
                 {150, :confirm}
               ]
    end

    test "the bisection narrows toward the threshold and stops when the levels touch" do
      assert sequence([100, 200], 130, refine: 5) ==
               [
                 {100, :ramp},
                 {200, :ramp},
                 {150, :refine},
                 {125, :refine},
                 {137, :refine},
                 {131, :refine},
                 {128, :refine},
                 {128, :confirm}
               ]

      assert sequence([10, 12], 10, refine: 5) ==
               [{10, :ramp}, {12, :ramp}, {11, :refine}, {10, :confirm}]
    end

    test "refine 0 goes straight to the confirmation, and confirm false ends the ramp" do
      assert sequence([50, 100, 200], 120, refine: 0) ==
               [{50, :ramp}, {100, :ramp}, {200, :ramp}, {100, :confirm}]

      assert sequence([50, 100, 200], 120, refine: 1, confirm: false) ==
               [{50, :ramp}, {100, :ramp}, {200, :ramp}, {150, :refine}]
    end

    test "a ladder that never fails confirms its last level; one that fails at once stops" do
      assert sequence([5, 7, 9], 100) == [{5, :ramp}, {7, :ramp}, {9, :ramp}, {9, :confirm}]
      assert sequence([5, 7, 9], 1) == [{5, :ramp}]
    end

    test "the cells plan (no refinement, no confirmation) climbs every level that is good" do
      assert sequence([500, 1000, 2000], 1500, refine: 0, confirm: false) ==
               [{500, :ramp}, {1000, :ramp}, {2000, :ramp}]
    end
  end
end
