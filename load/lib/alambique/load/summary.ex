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
      outcome: outcome,
      stopped_by: stopped_by(outcome, steps),
      rows: Enum.map(steps, &row/1),
      last_good:
        last_good && %{level: last_good.level, cost: last_good.cost, cgroup: last_good.cgroup},
      memory_fit: memory_fit(meta.measure, steps)
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

  defp describe({:host_busy, load}), do: "host net load #{Float.round(load * 1.0, 1)}"

  defp describe(:store_full),
    do: "the store is out of space"

  defp describe(other), do: to_string(other)

  # Process memory against the level, over the ramp and refinement steps that
  # ran to the end (not the invalid ones, and not the confirmation repeat).
  defp memory_fit(measure, steps) do
    points =
      for %{verdict_term: v, phase: phase, cgroup: cg} = s <- steps,
          cg != nil,
          phase != :confirm,
          match?(:good, v) or match?({:bad, _}, v),
          do: point(measure, s)

    case Stats.linear_fit(points) do
      nil ->
        nil

      %{slope: slope, intercept: intercept} ->
        %{fixed_mb: intercept, per_unit_mb: slope, points: length(points)}
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
      delivery_ratio: s.delivery.ratio,
      latency_ms: s.latency_ms,
      cpu_ms_per_op: s.cost && s.cost.cpu_ms_per_op,
      cpu_cores_mean: s.cgroup && s.cgroup.cpu_cores_mean,
      mem_process_mb_mean: s.cgroup && s.cgroup.mem_process_mb.mean,
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
        "trips/op",
        "put KB/op",
        "got KB/op"
      ] ++
        if(cells?, do: ["awake"], else: []) ++
        if(query?, do: ["dirty IO util", "stmts max"], else: [])

    rows =
      for r <- summary.rows do
        [
          r.level,
          "#{r.seq} #{r.phase}",
          r.verdict <> if(r.reason != "", do: " (#{r.reason})", else: ""),
          pct(r.delivery_ratio),
          num(r.latency_ms[:p50]),
          num(r.latency_ms[:p95]),
          num(r.latency_ms[:p99]),
          num(r.cpu_ms_per_op, 2),
          num(r.mem_process_mb_mean, 0),
          num(r.store_trips_per_op, 2),
          num(r.store_bytes_put_per_op && r.store_bytes_put_per_op / 1024, 1),
          num(r.store_bytes_got_per_op && r.store_bytes_got_per_op / 1024, 1)
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
      fit_line(summary),
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

  defp wait_line(%{quiet_wait_s: seconds}) when seconds > 0,
    do: "- Waited #{num(seconds)} s in all for the host to be quiet."

  defp wait_line(_), do: nil

  defp fit_line(%{memory_fit: nil}), do: nil

  defp fit_line(%{memory_fit: fit, unit: unit}),
    do:
      "- Process memory: #{num(fit.fixed_mb, 0)} MB fixed and #{num(fit.per_unit_mb, 3)} MB per #{unit} (fit over #{fit.points} steps)."

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
