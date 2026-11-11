defmodule Alambique.Load.CooldownTest do
  use ExUnit.Case, async: true

  alias Alambique.Load.Cooldown

  @t0 1_000_000

  # A service whose readings, one per second of a simulated clock, come from
  # `readings` (`{process_mb, outstanding}` or nil); the last one repeats.
  defp run(readings, opts) do
    {:ok, log} = Agent.start_link(fn -> [] end)
    {:ok, clock} = Agent.start_link(fn -> @t0 end)

    io = %{
      now: fn -> Agent.get(clock, & &1) end,
      sleep: fn ms -> Agent.update(clock, &(&1 + ms)) end,
      log: fn line -> Agent.update(log, &[line | &1]) end,
      read: fn ->
        index = div(Agent.get(clock, & &1) - @t0, 1_000)

        case Enum.at(readings, index, List.last(readings)) do
          nil -> nil
          {mb, outstanding} -> %{process_mb: mb, outstanding: outstanding}
        end
      end
    }

    {Cooldown.wait(opts, io), Agent.get(log, &Enum.reverse/1)}
  end

  @opts %{max_mem_mb: 0.8 * 512, max_wait_s: 300}

  test "an idle service below the memory line is cooled at once" do
    assert {{:cooled, +0.0}, []} = run([{100.0, 0}], @opts)
  end

  test "waits for the memory to fall below the line, and says so once" do
    readings = [{500.0, 0}, {450.0, 0}, {420.0, 0}, {400.0, 0}]
    assert {{:cooled, 3.0}, [line]} = run(readings, @opts)
    assert line =~ "waiting for it to cool down"
  end

  test "memory at the line is not below it" do
    assert {{:no_cooldown, 5.0, 409.6}, _} =
             run([{409.6, 0}], %{max_mem_mb: 409.6, max_wait_s: 5})
  end

  test "waits for the operations of the previous step to finish" do
    readings = [{100.0, 40}, {100.0, 12}, {100.0, 1}, {100.0, 0}]
    assert {{:cooled, 3.0}, _} = run(readings, @opts)
  end

  test "a service that does not cool down in time is reported with its memory" do
    assert {{:no_cooldown, 300.0, 480.0}, _} = run([{480.0, 0}], @opts)
  end

  test "a measure that holds memory by design skips the memory check" do
    assert {{:cooled, +0.0}, _} = run([{490.0, 0}], %{@opts | max_mem_mb: nil})
    assert {{:no_cooldown, _, _}, _} = run([{490.0, 3}], %{max_mem_mb: nil, max_wait_s: 5})
  end

  test "a service that is gone is not a service that did not cool down" do
    assert {{:service_down, 1.0}, _} = run([{480.0, 2}, nil], @opts)
  end
end
