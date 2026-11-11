defmodule Alambique.Load.SummaryTest do
  use ExUnit.Case, async: true

  alias Alambique.Load.{Stats, Summary}

  defp step(seq, phase, level, verdict, mem) do
    %{
      seq: seq,
      phase: phase,
      level: level,
      verdict: if(verdict == :good, do: "good", else: "bad"),
      reason: "",
      quiet_wait_s: 0.0,
      verdict_term: verdict,
      delivery: %{ratio: 1.0},
      latency_ms: %{p50: 30.0, p95: 60.0, p99: 90.0},
      cost: %{
        cpu_ms_per_op: 2.5,
        store_trips_per_op: 3.0,
        store_bytes_put_per_op: 2048.0,
        store_bytes_got_per_op: 0.0
      },
      cgroup: %{cpu_cores_mean: 0.5, mem_process_mb: %{mean: mem, max: mem, end: mem}},
      extra: %{awake: level},
      service: nil
    }
  end

  @meta %{
    scenario: "A",
    measure: "commits",
    variant: "small",
    unit: "commits/s",
    cpus: 1.0,
    mem_limit_mb: 512,
    command: "mise run load -- A commits",
    limits: %{latency: {:p99, 250}}
  }

  test "the table names the criterion that stopped the ramp and the memory line" do
    steps = [
      step(1, :ramp, 50, :good, 100.0),
      step(2, :ramp, 100, :good, 110.0),
      step(3, :ramp, 200, {:bad, {:latency, :p99, 250}}, 130.0),
      step(4, :confirm, 100, :good, 110.0)
    ]

    summary =
      Summary.build(@meta, steps, %{
        last_good: 100,
        first_bad: 200,
        stopped: :bad,
        confirmation: :good
      })

    assert summary.stopped_by == "bad: p99 above 250 ms"
    assert summary.last_good.level == 100
    # The confirmation repeat is not a point of the line: 50, 100, 200.
    assert summary.memory_fit.points == 3
    assert_in_delta summary.memory_fit.per_unit_mb, 0.2, 1.0e-9
    assert_in_delta summary.memory_fit.fixed_mb, 90.0, 1.0e-9
    assert summary.limits.latency == %{percentile: :p99, limit_ms: 250}

    md = Summary.markdown(summary)
    assert md =~ "Stopped by: bad: p99 above 250 ms"
    assert md =~ "Last good level: 100 commits/s, confirmed by a repeat"
    assert md =~ "Reproduce: `mise run load -- A commits`"
    assert md =~ "| 100 | 2 ramp | good |"
    assert {:ok, _} = Jason.encode(summary)
  end

  test "an invalid step is said not to be a failure" do
    steps = [
      step(1, :ramp, 50, :good, 100.0),
      step(2, :ramp, 100, {:invalid, {:delivery, 0.9}}, 100.0)
    ]

    summary =
      Summary.build(@meta, steps, %{
        last_good: 50,
        first_bad: nil,
        stopped: :invalid,
        confirmation: nil
      })

    assert summary.stopped_by ==
             "invalid (not a failure): delivered 90.0% of the planned operations"
  end

  test "memory per cell is fitted on the cells awake, not on the level asked" do
    steps =
      for {n, mem} <- [{1_000, 150.0}, {2_000, 250.0}, {3_000, 350.0}],
          do: Map.update!(step(1, :ramp, n + 10, :good, mem), :extra, &%{&1 | awake: n})

    summary =
      Summary.build(%{@meta | measure: "cells"}, steps, %{
        last_good: 3_010,
        first_bad: nil,
        stopped: :exhausted,
        confirmation: nil
      })

    assert_in_delta summary.memory_fit.per_unit_mb, 0.1, 1.0e-9
  end

  describe "stats" do
    test "nearest-rank percentiles" do
      sorted = Enum.to_list(1..100)
      assert Stats.percentile(sorted, 50) == 50
      assert Stats.percentile(sorted, 99) == 99
      assert Stats.percentile(sorted, 100) == 100
      assert Stats.percentile([], 99) == nil
    end

    test "a line needs two distinct x" do
      assert Stats.linear_fit([{1, 1}]) == nil
      assert Stats.linear_fit([{2, 1}, {2, 5}]) == nil
      assert %{slope: 2.0, intercept: 1.0} = Stats.linear_fit([{0, 1}, {1, 3}, {2, 5}])
    end
  end

  test "the time waited for a quiet host is summed over the steps and said" do
    steps = [
      %{step(1, :ramp, 50, :good, 100.0) | quiet_wait_s: 30.0},
      %{step(2, :ramp, 100, {:invalid, {:host_busy, 9.2}}, 100.0) | quiet_wait_s: 1830.5}
    ]

    summary = Summary.build(@meta, steps, %{stopped: :invalid, confirmation: nil})

    assert summary.quiet_wait_s == 1860.5
    assert summary.stopped_by == "invalid (not a failure): host net load 9.2"
    assert hd(summary.rows).quiet_wait_s == 30.0
    assert Summary.markdown(summary) =~ "Waited 1861 s in all"
  end

  test "the heading says when the store was on disk, and a full store is named" do
    steps = [step(1, :ramp, 50, {:bad, :store_full}, 100.0)]
    outcome = %{stopped: :bad, confirmation: nil}

    tmpfs = Summary.markdown(Summary.build(Map.put(@meta, :store_on, "tmpfs"), steps, outcome))
    refute tmpfs =~ "store on disk"

    disk_meta = Map.merge(@meta, %{store_on: "disk", store_fstype: "ext4"})
    summary = Summary.build(disk_meta, steps, outcome)

    assert Summary.markdown(summary) =~ "1.0 CPU, 512 MB, store on disk (ext4)\n"
    assert summary.stopped_by =~ "bad: the store is out of space"
  end
end
