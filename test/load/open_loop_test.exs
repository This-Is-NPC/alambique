defmodule Alambique.Load.OpenLoopTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Load.{OpenLoop, Schedule, Stats}

  describe "schedule" do
    property "offsets ascend inside the step, and a fixed schedule has the planned count" do
      check all(
              rate <- integer(1..500),
              seconds <- integer(1..5),
              seed <- integer(),
              kind <- member_of([:fixed, :poisson])
            ) do
        offsets = Schedule.offsets(rate, seconds, kind, seed)
        assert offsets == Enum.sort(offsets)
        assert Enum.all?(offsets, &(&1 >= 0 and &1 < seconds * 1_000_000))
        if kind == :fixed, do: assert(length(offsets) == rate * seconds)
      end
    end

    test "poisson arrivals average the rate" do
      offsets = Schedule.offsets(200, 60, :poisson, 3)
      assert_in_delta length(offsets), 12_000, 600
    end
  end

  describe "run" do
    test "latency counts from the scheduled time, not from when the driver issued the operation" do
      # Three instant operations are due 300 ms in; the driver stalls for
      # 500 ms (inside its abort poll) and issues them about 300 ms late. They
      # finish at once, but their latency carries the delay.
      stalls = :counters.new(1, [])

      abort = fn ->
        if :counters.get(stalls, 1) == 0, do: Process.sleep(500)
        :counters.add(stalls, 1, 1)
        nil
      end

      result = OpenLoop.run([300_000, 310_000, 320_000], fn _ -> :ok end, abort: abort)

      assert result.ok == 3
      assert Enum.all?(result.latencies_us, &(&1 > 150_000))
    end

    test "a slow operation reports its own time, the others are not held behind it" do
      offsets = for i <- 0..19, do: i * 5_000
      op = fn i -> if i == 0, do: Process.sleep(150), else: :ok end
      result = OpenLoop.run(offsets, op, max_outstanding: 100)

      assert result.completed == 20 and result.ok == 20
      assert Stats.percentile(result.latencies_us, 100) >= 150_000
      assert Stats.percentile(result.latencies_us, 50) < 50_000
    end

    test "operations over the outstanding cap are dropped, not delayed silently" do
      # Every operation takes 30 ms and only one may run: planned stays the
      # schedule and completed falls short of it.
      offsets = for i <- 0..19, do: i * 5_000

      op = fn _ ->
        Process.sleep(30)
        :ok
      end

      result = OpenLoop.run(offsets, op, max_outstanding: 1)

      assert result.planned == 20
      assert result.issued + result.dropped == 20
      assert result.dropped > 0
      assert result.completed == result.issued
    end

    test "errors, raises and exits are counted as errors, with their reason" do
      ops = fn
        0 -> :ok
        1 -> {:error, :boom}
        2 -> raise "bad"
        3 -> exit(:gone)
      end

      result = OpenLoop.run([0, 0, 0, 0], ops)
      assert result.ok == 1 and result.errors == 3 and result.completed == 4
      assert length(result.error_samples) == 3
    end

    test "the library's busy refusals are counted apart from the errors" do
      ops = fn
        0 -> :ok
        1 -> {:error, :busy}
        2 -> {:error, {"q-7", :busy}}
        3 -> {:error, {"q-7", :timeout}}
      end

      result = OpenLoop.run([0, 0, 0, 0], ops)
      assert result.ok == 1 and result.busy == 2 and result.errors == 1
      assert result.completed == 4
      assert [%{error: error}] = result.error_samples
      assert error =~ "timeout"
    end

    test "operations still running after the drain are not completed" do
      result = OpenLoop.run([0, 0], fn _ -> Process.sleep(5_000) end, drain_ms: 100)
      assert result.completed == 0 and result.issued == 2
    end

    test "the abort function stops the step early and says why" do
      offsets = for i <- 0..99, do: i * 100_000
      result = OpenLoop.run(offsets, fn _ -> :ok end, abort: fn -> :memory end)
      assert result.aborted == :memory
      assert result.planned == 100 and result.issued < 100
    end
  end
end
