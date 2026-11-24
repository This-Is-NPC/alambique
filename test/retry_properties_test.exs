defmodule Alambique.Replication.RetryPropertiesTest do
  @moduledoc """
  `Alambique.Replication.Retry.run/3`: a result that is not `{:retry, _}`
  stops the retrying and is returned as it is, the waits double up to
  `max_backoff_ms`, and no wait is started that would end past the deadline.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Replication.Retry

  # How far past its deadline a call that is killed there may end, when the
  # machine is not stalled: well under the 500 ms that a wait of its own would add.
  @kill_ms 300

  # How far past its deadline a call may end. It only has to tell a kill at the
  # deadline from an attempt left running (the longest hang here is 5 s), so it
  # leaves room for a loaded scheduler.
  @slack_ms 1_000

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

  defp ctx(backoff, max, attempts \\ 1_000_000),
    do: %{
      backoff_ms: backoff,
      max_backoff_ms: max,
      max_attempts: attempts,
      store: nil,
      max_object_bytes: Alambique.Store.max_object_bytes(),
      max_control_bytes: Alambique.Store.max_control_bytes()
    }

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

      # The grace of an attempt past the deadline is the first backoff: long
      # enough that a loaded scheduler does not kill the call before it runs.
      assert Retry.run(ctx(1_000, 2_000), Retry.deadline_in(-late), fun) == {:error, reason}
      assert length(calls.()) == 1
    end
  end

  test "an attempt past the deadline gets the first backoff and is then killed" do
    started = System.monotonic_time(:millisecond)

    assert {:error, :timeout} =
             Retry.run(ctx(40, 80), Retry.deadline_in(-5), fn -> Process.sleep(:infinity) end)

    assert (System.monotonic_time(:millisecond) - started) in 40..(40 + @slack_ms)
  end

  test "once kills the attempt after the timeout and returns its result before" do
    assert Retry.once(500, fn -> :ok end) == :ok
    assert Retry.once(30, fn -> Process.sleep(:infinity) end) == {:error, :timeout}
  end

  property "a retry that would end past the deadline is not started" do
    check all(
            backoff <- integer(2..6),
            deadline_ms <- integer(1..60),
            cap <- member_of([1, 2, 3, 5, 1_000_000]),
            max_runs: 40
          ) do
      {fun, calls} = recorder(fn n -> {:retry, n} end)
      deadline = Retry.deadline_in(deadline_ms)

      assert {:error, last} = Retry.run(ctx(backoff, backoff * 4, cap), deadline, fun)
      times = calls.()

      # The attempts stop at the cap.
      assert length(times) <= cap

      # The reason is the one of the last call, or of the one before when the
      # deadline killed the last call after it was recorded.
      killed = if length(times) >= 2, do: length(times) - 2, else: :timeout
      assert last in [length(times) - 1, killed]
      # Waiting between calls is at least the schedule: backoff, doubled, capped.
      schedule = Stream.iterate(backoff, &min(&1 * 2, backoff * 4))
      # Every wait that was made began before the deadline would have passed.
      assert schedule |> Enum.take(max(length(times) - 1, 0)) |> Enum.sum() < deadline_ms

      times
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.zip(schedule)
      |> Enum.each(fn {[a, b], wait} -> assert b - a >= wait end)
    end
  end

  property "an attempt killed at the deadline ends with the reason of the last failure" do
    check all(failures <- integer(0..3), deadline_ms <- integer(60..120), max_runs: 25) do
      {fun, _calls} =
        recorder(fn n ->
          if n < failures, do: {:retry, {:failed, n}}, else: Process.sleep(:infinity)
        end)

      result = Retry.run(ctx(2, 4), Retry.deadline_in(deadline_ms), fun)

      if failures == 0 do
        assert result == {:error, :timeout}
      else
        assert {:error, {:failed, n}} = result
        assert n < failures
      end
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

  # A store whose requests never return; its config is the pid that is told
  # which process blocked.
  defmodule HangingStore do
    @behaviour Alambique.Store
    defp hang(test) do
      send(test, {:blocked, self()})
      Process.sleep(:infinity)
    end

    def put(t, _, _, _, _), do: hang(t)
    def get(t, _), do: hang(t)
    def head(t, _), do: hang(t)
    def get_range(t, _, _, _), do: hang(t)
    def list(t, _, _, _), do: hang(t)
    def delete(t, _), do: hang(t)
  end

  test "a store call that never returns is interrupted at the deadline" do
    ctx = Map.put(ctx(5, 10), :store, {HangingStore, self()})

    # The call returns although the store never answers, and the process that
    # was blocked in the store is dead: it was interrupted, not left running.
    # The wait is only the limit of a failure.
    task =
      Task.async(fn ->
        Alambique.Replication.Objects.fetch(ctx, "k", Retry.deadline_in(100))
      end)

    assert {:error, :timeout} = Task.await(task, 30_000)
    assert_received {:blocked, blocked}
    ref = Process.monitor(blocked)
    assert_receive {:DOWN, ^ref, :process, ^blocked, _}, 5_000
  end

  # Killed at the deadline, not before and not later, and left no process.
  property "an attempt that hangs is killed at the deadline and does not outlive it" do
    check all(
            deadline_ms <- integer(20..120),
            hang <- member_of([:infinity, :long]),
            max_runs: 25
          ) do
      parent = self()
      started = System.monotonic_time(:millisecond)

      result =
        Retry.run(ctx(5, 20), Retry.deadline_in(deadline_ms), fn ->
          send(parent, {:attempt, self()})
          Process.sleep(if hang == :infinity, do: :infinity, else: 5_000)
          :done
        end)

      elapsed = System.monotonic_time(:millisecond) - started
      assert result == {:error, :timeout}
      assert elapsed >= deadline_ms - 2
      assert elapsed < deadline_ms + @kill_ms

      assert_received {:attempt, pid}
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    end
  end

  test "an interrupted attempt leaves no process and no message behind" do
    parent = self()

    assert {:error, _} =
             Retry.run(ctx(5, 10), Retry.deadline_in(30), fn ->
               send(parent, {:attempt, self()})
               Process.sleep(:infinity)
             end)

    assert_received {:attempt, pid}
    refute Process.alive?(pid)
    refute_received _
  end

  test "an attempt that raises raises in the caller, and exits and throws propagate" do
    assert_raise RuntimeError, "boom", fn ->
      Retry.run(ctx(5, 10), Retry.deadline_in(1_000), fn -> raise "boom" end)
    end

    assert catch_throw(Retry.run(ctx(5, 10), Retry.deadline_in(1_000), fn -> throw(:x) end)) == :x
    assert catch_exit(Retry.run(ctx(5, 10), Retry.deadline_in(1_000), fn -> exit(:y) end)) == :y
  end
end
