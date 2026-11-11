defmodule Alambique.Load.Driver do
  @moduledoc false
  # Runs one ramp of one measure against a service started by scripts/load-case
  # and writes a JSON file per step and a summary. It runs on the host, outside
  # the container whose CPU and memory it measures.

  alias Alambique.Load.{
    Cgroup,
    Env,
    Host,
    Measure,
    Ramp,
    Results,
    Sampler,
    State,
    Stats,
    Summary,
    Verdict
  }

  alias Alambique.Load.Measures

  # Times a step the host disturbed is run before it counts as invalid.
  @attempts 3

  @measures %{
    "commits" => Measures.Commits,
    "cells" => Measures.Cells,
    "wakes" => Measures.Wakes,
    "query" => Measures.Query
  }

  @doc "Runs the ramp; returns the exit status: 0 done, 2 stopped invalid, 3 host low on memory, 4 service down."
  def main do
    measure = Env.get("MEASURE")
    mod = Map.fetch!(@measures, measure)
    node = Env.get("NODE") |> String.to_atom()
    connect(node)

    {:ok, _} = State.start_link()

    {:ok, _} =
      Sampler.start_link(%{
        service: Cgroup.path(Env.get("SERVICE_ID")),
        store: Cgroup.path(Env.get("STORE_ID"))
      })

    limits = %{
      delivery_min: Env.float("DELIVERY_MIN", 0.95),
      max_net_load: Env.float("MAX_NET_LOAD", 6),
      max_error_ratio: Env.float("MAX_ERROR_RATIO", 0),
      latency: mod.latency(),
      mem_limit_mb: Env.int("MEM_LIMIT_MB", 512),
      mem_fraction: mod.mem_fraction(),
      min_available_gib: Env.float("MIN_AVAILABLE_GIB", 6)
    }

    ctx = %{node: node, measure: measure, limits: limits, abort: abort_fun(limits)}
    ctx = Map.put(ctx, :settings, settings(ctx))

    meta = %{
      scenario: Env.get("SCENARIO"),
      measure: measure,
      variant: Env.get("VARIANT", "default"),
      unit: mod.unit(),
      cpus: Env.float("CPUS", 1),
      mem_limit_mb: limits.mem_limit_mb,
      command: Env.get("COMMAND", ""),
      started_at: iso_now(),
      limits: limits,
      service: ctx.settings
    }

    out = Env.get("OUT")
    :ok = mod.setup(ctx)
    ramp = Ramp.new(mod.levels(), mod.ramp_opts())
    {ramp, steps} = climb(ctx, mod, ramp, 1, [], out, meta)

    summary = Summary.build(meta, Enum.reverse(steps), Ramp.outcome(ramp))
    Results.write!(Path.join(out, "summary.json"), summary)
    File.write!(Path.join(out, "summary.md"), Summary.markdown(summary))
    IO.puts(Summary.markdown(summary))
    exit_status(steps)
  end

  defp climb(ctx, mod, ramp, seq, steps, out, meta) do
    case Ramp.next(ramp) do
      :done ->
        {ramp, steps}

      {:run, level, phase} ->
        {step, verdict} = step_with_repeats(ctx, mod, level, phase, seq, out, meta)
        ramp = Ramp.record(ramp, Verdict.ramp_verdict(verdict))
        climb(ctx, mod, ramp, seq + 1, [Map.put(step, :verdict_term, verdict) | steps], out, meta)
    end
  end

  # A step the host disturbed runs again, up to @attempts times in all; the
  # disturbed attempts stay on disk, marked "repeat".
  defp step_with_repeats(ctx, mod, level, phase, seq, out, meta, attempt \\ 1) do
    {step, verdict} = run_step(ctx, mod, level, phase, seq, attempt)
    name = Results.step_name(seq, phase, level)

    case verdict do
      {:repeat, reason} when attempt < @attempts ->
        Results.write!(
          Path.join(out, String.replace(name, ".json", "-attempt#{attempt}.json")),
          step
        )

        IO.puts("#{meta.measure} #{level}: repeated, #{inspect(reason)}")
        step_with_repeats(ctx, mod, level, phase, seq, out, meta, attempt + 1)

      {:repeat, reason} ->
        step = %{step | verdict: "invalid", reason: inspect(reason)}
        Results.write!(Path.join(out, name), step)
        {step, {:invalid, reason}}

      _ ->
        Results.write!(Path.join(out, name), step)
        IO.puts("#{meta.measure} #{level} #{mod.unit()}: #{step.verdict} #{step.reason}")
        {step, verdict}
    end
  end

  defp run_step(ctx, mod, level, phase, seq, attempt) do
    prep = mod.prepare(ctx, level)
    if mod.probe?(), do: Measure.call(ctx, :probe_begin)
    {:ok, cg0} = Cgroup.read(Cgroup.path(Env.get("SERVICE_ID")))
    snap0 = Measure.call(ctx, :snapshot)
    Sampler.mark()
    started = System.monotonic_time(:millisecond)
    started_at = iso_now()

    result = mod.run(ctx, level, prep)

    ended = System.monotonic_time(:millisecond)
    samples = Sampler.collect()
    {cg1, snap1} = after_readings(ctx)
    probe = if mod.probe?(), do: Measure.call(ctx, :probe_finish)

    step = build(ctx, result, samples, {cg0, cg1}, {snap0, snap1}, probe)

    step =
      Map.merge(step, %{
        seq: seq,
        phase: phase,
        level: level,
        attempt: attempt,
        started_at: started_at,
        duration_s: (ended - started) / 1000
      })

    verdict = decide(ctx, result, step)
    {Map.merge(step, %{verdict: verdict_name(verdict), reason: reason(verdict)}), verdict}
  end

  defp after_readings(ctx) do
    case Cgroup.read(Cgroup.path(Env.get("SERVICE_ID"))) do
      {:ok, cg} -> {cg, Measure.call(ctx, :snapshot)}
      :error -> {nil, nil}
    end
  end

  defp decide(ctx, result, step) do
    cond do
      result.aborted in [:host_low_memory, :service_down] ->
        {:invalid, result.aborted}

      step.cgroup == nil ->
        {:invalid, :service_down}

      true ->
        Verdict.judge(
          %{
            planned: result.planned,
            completed: result.completed,
            errors: result.errors,
            latency_ms: step.latency_ms,
            net_load_max: step.host.net_load_max,
            mem_mb_max: step.cgroup.mem_process_mb.max,
            aborted: result.aborted
          },
          ctx.limits
        )
    end
  end

  defp build(ctx, result, samples, {cg0, cg1}, {snap0, snap1}, probe) do
    latency = latency_ms(result.latencies_us)
    cgroup = cgroup_stats(samples.cgroup, cg0, cg1)
    ops = result.ok

    %{
      schema: 1,
      scenario: Env.get("SCENARIO"),
      measure: ctx.measure,
      variant: Env.get("VARIANT", "default"),
      delivery: %{
        planned: result.planned,
        issued: result.issued,
        dropped: result.dropped,
        completed: result.completed,
        ok: result.ok,
        errors: result.errors,
        error_samples: result.error_samples,
        ratio: Verdict.delivery_ratio(result),
        aborted: result.aborted
      },
      latency_ms: latency,
      host: host_stats(samples.host),
      cgroup: cgroup,
      service: service_stats(snap0, snap1, probe),
      cost: cost(cgroup, snap0, snap1, ops),
      extra: Map.get(result, :extra, %{})
    }
  end

  defp latency_ms([]), do: %{}

  defp latency_ms(sorted_us) do
    ms = fn p -> Stats.percentile(sorted_us, p) / 1000 end

    %{
      p50: ms.(50),
      p95: ms.(95),
      p99: ms.(99),
      max: List.last(sorted_us) / 1000,
      mean: Stats.mean(sorted_us) / 1000
    }
  end

  defp host_stats([]) do
    host = Sampler.host() || %{net_load: 0.0, loadavg1: 0.0, mem_available_gib: 0.0}

    %{
      loadavg1_max: host.loadavg1,
      net_load_max: host.net_load,
      mem_available_gib_min: host.mem_available_gib,
      samples: 0
    }
  end

  defp host_stats(samples) do
    %{
      loadavg1_max: samples |> Enum.map(& &1.loadavg1) |> Enum.max(),
      net_load_max: samples |> Enum.map(& &1.net_load) |> Enum.max(),
      mem_available_gib_min: samples |> Enum.map(& &1.mem_available_gib) |> Enum.min(),
      samples: length(samples)
    }
  end

  defp cgroup_stats(_samples, _cg0, nil), do: nil
  defp cgroup_stats([], _cg0, _cg1), do: nil

  defp cgroup_stats(samples, cg0, cg1) do
    mem = Enum.map(samples, & &1.process_mb)
    cpu_s = (cg1.cpu_usec - cg0.cpu_usec) / 1_000_000
    span_s = max((List.last(samples).at - hd(samples).at) / 1000, 1)

    %{
      cpu_seconds: cpu_s,
      cpu_cores_mean: cpu_s / span_s,
      throttled_ms: (cg1.throttled_usec - cg0.throttled_usec) / 1000,
      throttled_periods: cg1.throttled_periods - cg0.throttled_periods,
      mem_process_mb: %{
        max: Enum.max(mem),
        mean: Stats.mean(mem),
        end: mem |> Enum.take(-10) |> Stats.mean()
      },
      shmem_mb_max: samples |> Enum.map(& &1.shmem_mb) |> Enum.max(),
      anon_mb_max: samples |> Enum.map(& &1.anon_mb) |> Enum.max(),
      file_mb_max: samples |> Enum.map(& &1.file_mb) |> Enum.max(),
      samples: length(samples)
    }
  end

  defp service_stats(_, nil, _), do: nil

  defp service_stats(snap0, snap1, probe) do
    %{
      store: Map.new(snap1.store, fn {k, v} -> {k, v - snap0.store[k]} end),
      native: %{before: snap0.native, after: snap1.native},
      awake_end: snap1.awake,
      beam_total_mb_end: snap1.beam_total_mb,
      dirty_io: probe
    }
  end

  defp cost(_cgroup, _, nil, _ops), do: nil
  defp cost(_cgroup, _, _, 0), do: nil
  defp cost(nil, _, _, _), do: nil

  defp cost(cgroup, snap0, snap1, ops) do
    store = Map.new(snap1.store, fn {k, v} -> {k, v - snap0.store[k]} end)

    %{
      cpu_ms_per_op: cgroup.cpu_seconds * 1000 / ops,
      store_trips_per_op: store.trips / ops,
      store_bytes_put_per_op: store.bytes_put / ops,
      store_bytes_got_per_op: store.bytes_got / ops,
      store_trips_by_op_per_op:
        for(
          op <- [:put, :get, :get_range, :head, :list, :delete],
          into: %{},
          do: {op, store[op] / ops}
        )
    }
  end

  defp abort_fun(limits) do
    fn ->
      host = Sampler.host()
      cgroup = Sampler.cgroup()

      cond do
        host && Host.low_memory?(host.mem_available_gib, limits.min_available_gib) ->
          :host_low_memory

        cgroup == :error ->
          :service_down

        match?({:ok, _}, cgroup) and
            elem(cgroup, 1).process_mb >= limits.mem_limit_mb * limits.mem_fraction ->
          :memory

        true ->
          nil
      end
    end
  end

  # The node answers before the instance has started (the service waits for the
  # store's bucket first).
  defp settings(ctx, attempts \\ 240) do
    Measure.call(ctx, :settings)
  rescue
    error ->
      if attempts == 0, do: reraise(error, __STACKTRACE__)
      Process.sleep(500)
      settings(ctx, attempts - 1)
  end

  defp connect(node, attempts \\ 240) do
    cond do
      Node.connect(node) == true -> :ok
      attempts == 0 -> raise "cannot connect to #{node}"
      true -> Process.sleep(500) && connect(node, attempts - 1)
    end
  end

  defp exit_status(steps) do
    case steps |> List.first() |> Map.get(:verdict_term) do
      {:invalid, :host_low_memory} -> 3
      {:invalid, :service_down} -> 4
      {:invalid, _} -> 2
      _ -> 0
    end
  end

  defp verdict_name(:good), do: "good"
  defp verdict_name({tag, _}), do: Atom.to_string(tag)
  defp reason(:good), do: ""
  defp reason({_, reason}), do: inspect(reason)

  defp iso_now, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
