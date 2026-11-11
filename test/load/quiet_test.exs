defmodule Alambique.Load.QuietTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Load.Quiet

  @t0 1_000_000
  @opts %{max_net_load: 6, min_available_gib: 6, settle_s: 15, max_wait_s: 60}

  # A host whose readings (net load, GiB available) come every 5 s on a
  # simulated clock; the last one repeats. Returns `{result, log, readings the
  # wait looked at}`.
  defp run(readings, opts \\ @opts) do
    {:ok, log} = Agent.start_link(fn -> [] end)
    {:ok, clock} = Agent.start_link(fn -> @t0 end)
    {:ok, seen} = Agent.start_link(fn -> [] end)

    io = %{
      now: fn -> Agent.get(clock, & &1) end,
      sleep: fn ms -> Agent.update(clock, &(&1 + ms)) end,
      log: fn line -> Agent.update(log, &[line | &1]) end,
      read: fn ->
        now = Agent.get(clock, & &1)
        index = div(now - @t0, 5_000)
        {load, mem} = Enum.at(readings, index, List.last(readings))
        Agent.update(seen, &[{index, load} | &1])
        %{at: @t0 + index * 5_000, net_load: load, mem_available_gib: mem}
      end
    }

    result = Quiet.wait(opts, io)
    {result, Agent.get(log, &Enum.reverse/1), Agent.get(seen, &Enum.reverse/1)}
  end

  defp quiet(load), do: {load, 16.0}

  test "a quiet host is quiet after the settle window, not at the first reading" do
    assert {{:quiet, 15.0}, [], _} = run([quiet(1.0)])
  end

  test "a busy reading restarts the window; the wait is logged once each way" do
    readings = [
      quiet(1.0),
      quiet(1.0),
      quiet(2.0),
      quiet(9.0),
      quiet(12.0),
      quiet(3.0),
      quiet(3.0),
      quiet(2.0)
    ]

    assert {{:quiet, waited}, [started, resumed], _} = run(readings)
    # Readings 1 and 2 are quiet, 3 and 4 busy, 5 to 7 fill the window of 3.
    assert waited == 35.0
    assert started =~ "busy (net load 9.0)"
    assert resumed =~ "quiet again (net load 2.0)"
  end

  test "a reading exactly at the limit is quiet" do
    assert {{:quiet, _}, [], _} = run([quiet(6.0)])
  end

  test "a host that stays busy times out with its last net load" do
    assert {{:busy, waited, 8.5}, [started], _} = run([quiet(8.5)])
    assert waited >= @opts.max_wait_s
    assert started =~ "busy (net load 8.5)"
  end

  test "low memory ends the wait at once, quiet or not" do
    assert {{:low_memory, 5.0}, _, _} = run([quiet(1.0), {1.0, 5.9}])
    assert {{:low_memory, 5.0}, _, _} = run([quiet(1.0), {20.0, 5.9}])
  end

  test "a reading older than the wait does not count" do
    {:ok, clock} = Agent.start_link(fn -> @t0 end)

    io = %{
      now: fn -> Agent.get(clock, & &1) end,
      sleep: fn ms -> Agent.update(clock, &(&1 + ms)) end,
      log: fn _ -> :ok end,
      read: fn -> %{at: @t0 - 1_000, net_load: 0.0, mem_available_gib: 16.0} end
    }

    assert {:busy, 60.0, +0.0} = Quiet.wait(@opts, io)
  end

  test "the settle window is one reading per host sample, at least one" do
    assert Quiet.settle_readings(30) == 6
    assert Quiet.settle_readings(11) == 3
    assert Quiet.settle_readings(0) == 1
  end

  property "quiet only after a whole window of readings at or below the limit" do
    check all(
            loads <- list_of(float(min: 0.0, max: 12.0), min_length: 1, max_length: 30),
            settle_s <- member_of([5, 15, 30])
          ) do
      opts = %{@opts | settle_s: settle_s, max_wait_s: 400}
      {result, _log, seen} = run(Enum.map(loads, &quiet/1), opts)
      needed = Quiet.settle_readings(settle_s)

      # The distinct readings the wait looked at, in order, without the first
      # (taken at the start of the wait, which does not count).
      looked = seen |> Enum.uniq_by(&elem(&1, 0)) |> Enum.reject(&(elem(&1, 0) == 0))
      seen_loads = Enum.map(looked, &elem(&1, 1))

      case result do
        {:quiet, _} ->
          window = Enum.take(seen_loads, -needed)
          assert length(window) == needed
          assert Enum.all?(window, &(&1 <= 6))

        {:busy, _, _} ->
          refute seen_loads
                 |> Enum.chunk_every(needed, 1, :discard)
                 |> Enum.any?(fn window -> Enum.all?(window, &(&1 <= 6)) end)
      end
    end
  end
end
