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
    limits: %{
      latency: {:p99, 250},
      fit_min_r2: 0.8,
      fit_min_span: 2.0,
      suspect_throttle_fraction: 0.1
    }
  }

  test "the table names the criterion that stopped the ramp and the memory line" do
    steps = [
      step(1, :ramp, 50, :good, 100.0),
      step(2, :ramp, 100, :good, 110.0),
      step(3, :ramp, 200, :good, 130.0),
      step(4, :ramp, 400, {:bad, {:latency, :p99, 250}}, 900.0),
      step(5, :confirm, 200, :good, 130.0)
    ]

    summary =
      Summary.build(@meta, steps, %{
        last_good: 200,
        first_bad: 400,
        stopped: :bad,
        confirmation: :good
      })

    assert summary.stopped_by == "bad: p99 above 250 ms"
    assert summary.last_good.level == 200
    # Only the good steps are points, and not the confirmation repeat: 50, 100, 200.
    assert summary.memory_fit.points == 3
    assert_in_delta summary.memory_fit.per_unit_mb, 0.2, 1.0e-9
    assert_in_delta summary.memory_fit.fixed_mb, 90.0, 1.0e-9
    assert_in_delta summary.memory_fit.r2, 1.0, 1.0e-9
    assert summary.limits.latency == %{percentile: :p99, limit_ms: 250}

    md = Summary.markdown(summary)
    assert md =~ "Stopped by: bad: p99 above 250 ms"
    assert md =~ "Last good level: 200 commits/s, confirmed by a repeat"
    assert md =~ "R² 1.0)"
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

    test "r2 is 1 on a line, low on noise, and 1 when y does not vary" do
      assert_in_delta Stats.linear_fit([{0, 1}, {1, 3}, {2, 5}]).r2, 1.0, 1.0e-9
      assert Stats.linear_fit([{0, 1}, {1, 9}, {2, 1}, {3, 9}]).r2 < 0.5
      assert Stats.linear_fit([{0, 4}, {1, 4}, {2, 4}]).r2 == 1.0
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

  describe "memory fit" do
    defp fit(points, limits \\ %{}) do
      steps =
        for {{level, mem}, i} <- Enum.with_index(points, 1),
            do: step(i, :ramp, level, :good, mem)

      meta = %{@meta | limits: Map.merge(@meta.limits, limits)}

      Summary.build(meta, steps, %{stopped: :exhausted, confirmation: nil})
    end

    test "a line that explains the points is reported with its R²" do
      summary = fit([{100, 110.0}, {200, 131.0}, {400, 168.0}, {800, 252.0}])
      assert summary.memory_fit.suppressed == nil
      assert summary.memory_fit.r2 > 0.99
      assert summary.memory_fit.per_unit_mb > 0
    end

    test "the slope is suppressed when R² is below the minimum" do
      summary = fit([{100, 300.0}, {200, 120.0}, {400, 310.0}, {800, 150.0}])
      fit = summary.memory_fit
      assert fit.r2 < 0.8
      assert fit.per_unit_mb == nil and fit.fixed_mb == nil
      assert fit.suppressed =~ "R²"
      assert Summary.markdown(summary) =~ "no slope reported"
    end

    test "the slope is suppressed when the levels span too little" do
      summary = fit([{100, 110.0}, {110, 112.0}, {120, 114.0}])
      assert summary.memory_fit.suppressed =~ "span"

      assert fit([{100, 110.0}, {110, 112.0}, {120, 114.0}], %{fit_min_span: 1.1}).memory_fit.suppressed ==
               nil
    end

    test "fewer than three good steps give no slope" do
      assert fit([{100, 110.0}, {200, 130.0}]).memory_fit.suppressed =~ "fewer than 3"
    end
  end

  test "a step whose CPU was throttled is marked suspect, not invalid" do
    suspect = %{
      step(2, :ramp, 100, :good, 110.0)
      | cgroup: %{
          cpu_cores_mean: 1.0,
          throttled_ms: 30_000.0,
          mem_process_mb: %{mean: 110.0, max: 110.0, end: 110.0}
        }
    }

    suspect = Map.put(suspect, :suspect, true)
    steps = [step(1, :ramp, 50, :good, 100.0), suspect]
    summary = Summary.build(@meta, steps, %{stopped: :exhausted, confirmation: nil})

    assert Enum.map(summary.rows, & &1.suspect) == [false, true]
    md = Summary.markdown(summary)
    assert md =~ "| 100 | 2 ramp | good, suspect |"
    assert md =~ "Suspect steps (CPU throttled for more than 10% of the step): 2."
  end

  test "the store's use at the end of a step is a column" do
    full =
      Map.put(step(1, :ramp, 50, :good, 100.0), :store, %{kind: :tmpfs, used_mb_end: 3_900.4})

    summary = Summary.build(@meta, [full], %{stopped: :exhausted, confirmation: nil})
    assert hd(summary.rows).store_mb_end == 3_900.4
    assert Summary.markdown(summary) =~ "| 3900 |"
  end

  test "the heading says when the store was on disk, and a full store is named" do
    steps = [%{step(1, :ramp, 50, {:invalid, :store_full}, 100.0) | reason: ":store_full"}]
    outcome = %{stopped: :invalid, confirmation: nil}

    tmpfs = Summary.markdown(Summary.build(Map.put(@meta, :store_on, "tmpfs"), steps, outcome))
    refute tmpfs =~ "store on disk"

    disk_meta = Map.merge(@meta, %{store_on: "disk", store_fstype: "ext4"})
    summary = Summary.build(disk_meta, steps, outcome)

    assert Summary.markdown(summary) =~ "1.0 CPU, 512 MB, store on disk (ext4)\n"
    assert summary.stopped_by =~ "the store is out of space"
    assert Summary.markdown(summary) =~ "The disk of the store is nearly full"
    assert tmpfs =~ "run the case again with ALAMBIQUE_LOAD_STORE_ON=disk"
  end
end
