defmodule Alambique.Replication.RetryPropertiesTest do
  @moduledoc """
  `Alambique.Replication.Retry.run/3`: a result that is not `{:retry, _}`
  stops the retrying and is returned as it is, the waits double up to
  `max_backoff_ms`, and no wait is started that would end past the deadline.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Replication.Retry

  # Calls `fun` as the n-th time and records when.
  defp recorder(script) do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    fun = fn ->
      times =
        Agent.get_and_update(agent, fn times ->
          {times, [System.monotonic_time(:millisecond) | times]}
        end)

      script.(length(times))
    end

    {fun, fn -> agent |> Agent.get(& &1) |> Enum.reverse() end}
  end

  defp ctx(backoff, max), do: %{backoff_ms: backoff, max_backoff_ms: max}

  property "the first result that is not {:retry, _} is returned as it is, after that many calls" do
    check all(
            retries <- integer(0..6),
            result <-
              one_of([
                constant(:ok),
                map(integer(), &{:ok, &1}),
                map(integer(), &{:error, &1}),
                constant(nil)
              ])
          ) do
      {fun, calls} =
        recorder(fn n -> if n < retries, do: {:retry, n}, else: result end)

      assert Retry.run(ctx(1, 2), Retry.deadline_in(60_000), fun) == result
      assert length(calls.()) == retries + 1
    end
  end

  property "a deadline that has passed allows one call and returns its reason" do
    check all(late <- integer(0..1_000), reason <- term()) do
      {fun, calls} = recorder(fn _ -> {:retry, reason} end)

      assert Retry.run(ctx(1, 2), Retry.deadline_in(-late), fun) == {:error, reason}
      assert length(calls.()) == 1
    end
  end

  property "a retry that would end past the deadline is not started" do
    check all(backoff <- integer(2..6), deadline_ms <- integer(1..60), max_runs: 40) do
      {fun, calls} = recorder(fn n -> {:retry, n} end)
      deadline = Retry.deadline_in(deadline_ms)

      assert {:error, last} = Retry.run(ctx(backoff, backoff * 4), deadline, fun)
      times = calls.()

      # The reason is the one of the last call.
      assert last == length(times) - 1
      # Waiting between calls is at least the schedule: backoff, doubled, capped.
      schedule = Stream.iterate(backoff, &min(&1 * 2, backoff * 4))
      # Every wait that was made began before the deadline would have passed.
      assert schedule |> Enum.take(length(times) - 1) |> Enum.sum() < deadline_ms

      times
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.zip(schedule)
      |> Enum.each(fn {[a, b], wait} -> assert b - a >= wait end)
    end
  end

  property "the waits double up to the cap and never beyond it" do
    check all(backoff <- integer(4..8), factor <- member_of([1, 2, 4]), max_runs: 12) do
      cap = backoff * factor
      calls = 7
      {fun, times} = recorder(fn n -> if n < calls, do: {:retry, n}, else: :done end)

      assert Retry.run(ctx(backoff, cap), Retry.deadline_in(60_000), fun) == :done

      waits = Stream.iterate(backoff, &min(&1 * 2, cap)) |> Enum.take(calls)
      [first | _] = times = times.()
      last = List.last(times)

      # Sleeping never ends early. Without the cap the waits would sum to
      # more, so staying below that bound tolerates a loaded machine.
      uncapped = Stream.iterate(backoff, &(&1 * 2)) |> Enum.take(calls) |> Enum.sum()
      assert last - first >= Enum.sum(waits)
      assert last - first < uncapped
    end
  end
end
