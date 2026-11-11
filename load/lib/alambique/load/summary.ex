defmodule Alambique.Load.Summary do
  @moduledoc false
  # The summary of one ramp: a row per step, the cost at the last good level,
  # the fit of process memory against the level, and why the ramp stopped.
  # `steps` are the step maps of the driver in the order run, each with its
  # `verdict_term`.

  alias Alambique.Load.Stats

  @spec build(map, [map], map) :: map
  def build(meta, steps, outcome) do
    good = for %{verdict_term: :good} = s <- steps, do: s
    last_good = Enum.max_by(good, & &1.level, fn -> nil end)

    meta
    |> Map.merge(%{
      finished_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      quiet_wait_s: steps |> Enum.map(& &1.quiet_wait_s) |> Enum.sum(),
      cooldown_s: steps |> Enum.map(&(&1[:cooldown_s] || 0)) |> Enum.sum(),
      outcome: outcome,
      stopped_by: stopped_by(outcome, steps),
      rows: Enum.map(steps, &row/1),
      last_good:
        last_good && %{level: last_good.level, cost: last_good.cost, cgroup: last_good.cgroup},
      memory_fit: memory_fit(meta.measure, steps, meta.limits)
    })
    |> Map.update!(:limits, &limits/1)
  end

  defp limits(%{latency: {percentile, ms}} = limits),
    do: %{limits | latency: %{percentile: percentile, limit_ms: ms}}

  defp limits(limits), do: limits

  defp stopped_by(%{stopped: :bad}, steps) do
    case Enum.find(steps, &match?(%{verdict_term: {:bad, _}}, &1)) do
      %{verdict_term: {:bad, reason}} -> "bad: #{describe(reason)}"
      nil -> "bad"
    end
  end

  defp stopped_by(%{stopped: :invalid}, steps) do
    case List.last(steps) do
      %{verdict_term: {:invalid, reason}} -> "invalid (not a failure): #{describe(reason)}"
      _ -> "invalid"
    end
  end

  defp stopped_by(%{stopped: :exhausted}, _), do: "no step was bad: the levels ran out"
  defp stopped_by(_, _), do: "no step ran"

  defp describe({:latency, percentile, limit}), do: "#{percentile} above #{limit} ms"

  defp describe({:delivery, ratio}),
    do: "delivered #{Float.round(ratio * 100, 1)}% of the planned operations"

  defp describe({:swap, who, mb}), do: "the #{who} swapped (#{Float.round(mb * 1.0, 1)} MB)"
  defp describe({:host_busy, load}), do: "host net load #{Float.round(load * 1.0, 1)}"

  defp describe(:store_full), do: "the store is out of space"

  defp describe(:data_full),
    do: "the service data tmpfs is full or the next level would fill it (harness limit)"

  defp describe(:prepare_failed), do: "the preparation of the level failed"
  defp describe(:no_operations), do: "the step had no operations"

  defp describe({:no_cooldown, mb}),
    do: "the service did not cool down (process memory #{Float.round(mb * 1.0)} MB)"

  defp describe(:busy), do: "the library refused reads with :busy above the allowed share"

  defp describe(other), do: to_string(other)

  @min_fit_points 3

  # Process memory against the level, over the good ramp and refinement steps
  # (not the invalid or bad ones, and not the confirmation repeat). The line is
  # reported only when it explains the points (R² at least `fit_min_r2`) and
  # they span enough (largest level at least `fit_min_span` times the
  # smallest); otherwise `per_unit_mb` and `fixed_mb` are nil and `suppressed`
  # says why.
  defp memory_fit(measure, steps, limits) do
    points =
      for %{verdict_term: :good, phase: phase, cgroup: cg} = s <- steps,
          cg != nil,
          phase != :confirm,
          do: point(measure, s)

    with %{slope: slope, intercept: intercept, r2: r2} <- Stats.linear_fit(points) do
      {xs, _} = Enum.unzip(points)
      fit = %{points: length(points), r2: r2, fixed_mb: nil, per_unit_mb: nil, suppressed: nil}

      case suppressed(length(points), r2, Enum.min(xs), Enum.max(xs), limits) do
        nil -> %{fit | fixed_mb: intercept, per_unit_mb: slope}
        reason -> %{fit | suppressed: reason}
      end
    end
  end

  defp suppressed(n, _r2, _min, _max, _limits) when n < @min_fit_points,
    do: "fewer than #{@min_fit_points} good steps"

  defp suppressed(_n, r2, min_x, max_x, limits) do
    cond do
      r2 < limits.fit_min_r2 -> "R² #{Float.round(r2, 2)} is below #{limits.fit_min_r2}"
      max_x < limits.fit_min_span * min_x -> "the levels span less than #{limits.fit_min_span}x"
      true -> nil
    end
  end

  defp point("cells", s), do: {s.extra.awake, s.cgroup.mem_process_mb.end}
  defp point(_, s), do: {s.level, s.cgroup.mem_process_mb.mean}

  defp row(s) do
    %{
      seq: s.seq,
      phase: s.phase,
      level: s.level,
      verdict: s.verdict,
      reason: s.reason,
      quiet_wait_s: s.quiet_wait_s,
      cooldown_s: s[:cooldown_s],
      suspect: s[:suspect] == true,
      throttled_ms: s.cgroup && s.cgroup[:throttled_ms],
      busy_ratio: s.delivery[:busy_ratio],
      store_mb_end: get_in(s, [:store, :used_mb_end]),
      delivery_ratio: s.delivery.ratio,
      latency_ms: s.latency_ms,
      cpu_ms_per_op: s.cost && s.cost.cpu_ms_per_op,
      cpu_cores_mean: s.cgroup && s.cgroup.cpu_cores_mean,
      mem_process_mb_mean: s.cgroup && s.cgroup.mem_process_mb.mean,
      host_swap_mb_max: get_in(s, [:swap, :host_mb, :max]),
      mem_process_mb_max: s.cgroup && s.cgroup.mem_process_mb.max,
      store_trips_per_op: s.cost && s.cost.store_trips_per_op,
      store_bytes_put_per_op: s.cost && s.cost.store_bytes_put_per_op,
      store_bytes_got_per_op: s.cost && s.cost.store_bytes_got_per_op,
      awake: s.extra[:awake],
      dirty_io_utilization: get_in(s, [:service, :dirty_io, :utilization, :dirty_io]),
      user_statements_max: get_in(s, [:service, :dirty_io, :user_statements_max])
    }
  end

  @doc "The summary as a Markdown table with the criterion that stopped the ramp."
  @spec markdown(map) :: String.t()
  def markdown(summary) do
    cells? = summary.measure == "cells"
    query? = summary.measure == "query"

    head =
      [
        "level",
        "step",
        "verdict",
        "delivered",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "CPU ms/op",
        "mem MB",
        "swap MB",
        "trips/op",
        "put KB/op",
        "got KB/op",
        "busy %",
        "store MB"
      ] ++
        if(cells?, do: ["awake"], else: []) ++
        if(query?, do: ["dirty IO util", "stmts max"], else: [])

    rows =
      for r <- summary.rows do
        [
          r.level,
          "#{r.seq} #{r.phase}",
          r.verdict <>
            if(r.reason != "", do: " (#{r.reason})", else: "") <>
            if(r.suspect, do: ", suspect", else: ""),
          pct(r.delivery_ratio),
          num(r.latency_ms[:p50]),
          num(r.latency_ms[:p95]),
          num(r.latency_ms[:p99]),
          num(r.cpu_ms_per_op, 2),
          num(r.mem_process_mb_mean, 0),
          num(r.host_swap_mb_max, 0),
          num(r.store_trips_per_op, 2),
          num(r.store_bytes_put_per_op && r.store_bytes_put_per_op / 1024, 1),
          num(r.store_bytes_got_per_op && r.store_bytes_got_per_op / 1024, 1),
          pct(r.busy_ratio),
          num(r.store_mb_end)
        ] ++
          if(cells?, do: [r.awake || "-"], else: []) ++
          if(query?, do: [num(r.dirty_io_utilization, 2), r.user_statements_max || "-"], else: [])
      end

    lines = [
      "#### Scenario #{summary.scenario}: #{summary.measure} (#{summary.variant}), #{summary.cpus} CPU, #{summary.mem_limit_mb} MB" <>
        store_note(summary),
      "",
      table([head | rows]),
      "",
      "- Stopped by: #{summary.stopped_by}.",
      last_good_line(summary),
      store_full_line(summary),
      fit_line(summary),
      suspect_line(summary),
      wait_line(summary),
      "- Reproduce: `#{summary.command}`"
    ]

    lines |> Enum.reject(&is_nil/1) |> Enum.join("\n") |> Kernel.<>("\n")
  end

  defp store_note(%{store_on: "disk"} = summary),
    do: ", store on disk (#{summary[:store_fstype] || "unknown"})"

  defp store_note(_), do: ""

  defp last_good_line(%{last_good: nil}), do: "- No level was good."

  defp last_good_line(%{last_good: %{level: level, cost: cost}, outcome: outcome, unit: unit}) do
    confirmation =
      case outcome.confirmation do
        :good -> ", confirmed by a repeat"
        nil -> ", not repeated"
        other -> ", the repeat was #{other}"
      end

    "- Last good level: #{level} #{unit}#{confirmation}." <>
      if(cost,
        do:
          " CPU #{num(cost.cpu_ms_per_op, 2)} ms per operation (cores per 1000 operations per second).",
        else: ""
      )
  end

  defp store_full_line(%{rows: rows} = summary) do
    if Enum.any?(rows, &(&1.reason == ":store_full")) do
      if summary[:store_on] == "disk",
        do: "- The disk of the store is nearly full: free space and run the case again.",
        else: "- The store's tmpfs is full: run the case again with ALAMBIQUE_LOAD_STORE_ON=disk."
    end
  end

  # Steps whose CPU was throttled for a large part of their time: not invalid,
  # but the service was cut by the quota as much as it was loaded.
  defp suspect_line(%{rows: rows, limits: limits}) do
    case for(%{suspect: true, seq: seq} <- rows, do: seq) do
      [] ->
        nil

      seqs ->
        "- Suspect steps (CPU throttled for more than #{round(limits.suspect_throttle_fraction * 100)}% of the step): #{Enum.join(seqs, ", ")}."
    end
  end

  defp wait_line(%{quiet_wait_s: seconds}) when seconds > 0,
    do: "- Waited #{num(seconds)} s in all for the host to be quiet."

  defp wait_line(_), do: nil

  defp fit_line(%{memory_fit: nil}), do: nil

  defp fit_line(%{memory_fit: %{suppressed: nil} = fit, unit: unit}),
    do:
      "- Process memory: #{num(fit.fixed_mb, 0)} MB fixed and #{num(fit.per_unit_mb, 3)} MB per #{unit} (fit over #{fit.points} good steps, R² #{num(fit.r2, 2)})."

  defp fit_line(%{memory_fit: fit}),
    do:
      "- Process memory: no slope reported (#{fit.suppressed}; #{fit.points} good steps, R² #{num(fit.r2, 2)})."

  defp table([head | rest]) do
    [head, Enum.map(head, fn _ -> "---" end) | rest]
    |> Enum.map_join("\n", fn row -> "| " <> Enum.join(row, " | ") <> " |" end)
  end

  defp pct(nil), do: "-"
  defp pct(ratio), do: "#{Float.round(ratio * 100, 1)}%"

  defp num(value, digits \\ 0)
  defp num(nil, _), do: "-"
  defp num(value, 0), do: value |> Kernel.*(1.0) |> Float.round(0) |> trunc() |> to_string()
  defp num(value, digits), do: value |> Kernel.*(1.0) |> Float.round(digits) |> to_string()
end
