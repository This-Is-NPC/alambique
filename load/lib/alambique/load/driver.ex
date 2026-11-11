defmodule Alambique.Load.Driver do
  @moduledoc false
  # Runs one ramp of one measure against a service started by scripts/load-case
  # and writes a JSON file per step and a summary. It runs on the host, outside
  # the container whose CPU and memory it measures.

  alias Alambique.Load.{
    Capacity,
    Cgroup,
    Cooldown,
    Env,
    Host,
    Measure,
    Quiet,
    Ramp,
    Results,
    Sampler,
    State,
    Stats,
    Summary,
    Verdict
  }

  alias Alambique.Load.Measures

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
        store: Cgroup.path(Env.get("STORE_ID")),
        store_dir: if(Env.get("STORE_ON", "tmpfs") == "disk", do: Env.get("STORE_PATH"))
      })

    limits = %{
      delivery_min: Env.float("DELIVERY_MIN", 0.95),
      max_net_load: Env.float("MAX_NET_LOAD", 6),
      max_driver_swap_mb: Env.float("MAX_DRIVER_SWAP_MB", 16),
      max_store_swap_mb: Env.float("MAX_STORE_SWAP_MB", 0),
      max_host_swap_mb: Env.float("MAX_HOST_SWAP_MB", 512),
      max_error_ratio: Env.float("MAX_ERROR_RATIO", 0),
      max_busy_ratio: Env.float("MAX_BUSY_RATIO", 0.01),
      latency: mod.latency(),
      mem_limit_mb: Env.int("MEM_LIMIT_MB", 512),
      mem_fraction: mod.mem_fraction(),
      cooldown_mem_fraction: Env.float("COOLDOWN_MEM_FRACTION", 0.8),
      cooldown_max_s: Env.int("COOLDOWN_MAX_S", 300),
      cooldown_memory?: not mod.holds_memory?(),
      store_mb: Env.int("STORE_MB", 4096),
      store_full_fraction: Env.float("STORE_FULL_FRACTION", 0.95),
      store_min_free_mb: Env.float("STORE_MIN_FREE_MB", 1024),
      suspect_throttle_fraction: Env.float("SUSPECT_THROTTLE_FRACTION", 0.1),
      fit_min_r2: Env.float("FIT_MIN_R2", 0.8),
      fit_min_span: Env.float("FIT_MIN_SPAN", 2),
      min_available_gib: Env.float("MIN_AVAILABLE_GIB", 6),
      settle_s: Env.int("SETTLE_S", 30),
      wait_quiet_step_s: Env.int("WAIT_QUIET_STEP_S", 1800),
      attempts: Env.int("ATTEMPTS", 5)
    }

    ctx = %{node: node, measure: measure, limits: limits, abort: abort_fun(limits)}
    ctx = Map.put(ctx, :settings, settings(ctx))

    meta = %{
      scenario: Env.get("SCENARIO"),
      measure: measure,
      variant: Env.get("VARIANT", "default"),
      store_on: Env.get("STORE_ON", "tmpfs"),
      store_fstype: Env.get("STORE_FSTYPE", "tmpfs"),
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

  # Every attempt starts when the service has cooled down from the previous
  # step and the host is quiet. A step the host disturbed, or that started on a
  # service that did not cool down, runs again, up to `limits.attempts` times
  # in all; the disturbed attempts that ran stay on disk, marked "repeat".
  # `waits` is the time spent waiting so far in this step, this attempt
  # included in the step it is written to.
  defp step_with_repeats(
         ctx,
         mod,
         level,
         phase,
         seq,
         out,
         meta,
         attempt \\ 1,
         waits \\ %{quiet: 0.0, cooldown: 0.0}
       ) do
    name = Results.step_name(seq, phase, level)
    key = {level, phase, seq, attempt}

    case wait_to_start(ctx, waits) do
      {:ready, waits} ->
        {step, verdict} = run_step(ctx, mod, level, phase, seq, attempt)
        step = Map.merge(step, %{quiet_wait_s: waits.quiet, cooldown_s: waits.cooldown})
        finish(ctx, mod, {step, verdict}, key, waits, out, meta, name)

      {:no_cooldown, waits, mb} when attempt < ctx.limits.attempts ->
        IO.puts("#{meta.measure} #{level}: repeated, no cooldown (#{round(mb)} MB)")
        step_with_repeats(ctx, mod, level, phase, seq, out, meta, attempt + 1, waits)

      {:no_cooldown, waits, mb} ->
        unrun_step(ctx, key, waits, {:invalid, {:no_cooldown, mb}}, %{})
        |> write_step(out, name, meta, mod)

      {:stop, waits, verdict} ->
        unrun_step(ctx, key, waits, verdict, %{}) |> write_step(out, name, meta, mod)
    end
  end

  # Cooldown first, then the quiet window: the service idles through both.
  defp wait_to_start(ctx, waits) do
    case Cooldown.wait(cooldown_opts(ctx.limits), cooldown_io(ctx)) do
      {:cooled, s} ->
        waits = %{waits | cooldown: waits.cooldown + s}

        case Quiet.wait(quiet_opts(ctx.limits), quiet_io()) do
          {:quiet, s} -> {:ready, %{waits | quiet: waits.quiet + s}}
          {:busy, s, load} -> {:stop, add_quiet(waits, s), {:invalid, {:host_busy, load}}}
          {:low_memory, s} -> {:stop, add_quiet(waits, s), {:invalid, :host_low_memory}}
        end

      {:no_cooldown, s, mb} ->
        {:no_cooldown, %{waits | cooldown: waits.cooldown + s}, mb}

      {:service_down, s} ->
        {:stop, %{waits | cooldown: waits.cooldown + s}, {:invalid, :service_down}}
    end
  end

  defp add_quiet(waits, s), do: %{waits | quiet: waits.quiet + s}

  defp finish(ctx, mod, {step, verdict}, {level, phase, seq, attempt}, waits, out, meta, name) do
    case verdict do
      {:repeat, reason} when attempt < ctx.limits.attempts ->
        Results.write!(
          Path.join(out, String.replace(name, ".json", "-attempt#{attempt}.json")),
          step
        )

        IO.puts("#{meta.measure} #{level}: repeated, #{inspect(reason)}")
        # What the disturbed attempt left is undone, so the repeat redoes all of it.
        mod.rewind(ctx, level)
        step_with_repeats(ctx, mod, level, phase, seq, out, meta, attempt + 1, waits)

      {:repeat, reason} ->
        step = %{step | verdict: "invalid", reason: inspect(reason)}
        write_step({step, {:invalid, reason}}, out, name, meta, mod)

      _ ->
        write_step({step, verdict}, out, name, meta, mod)
    end
  end

  defp write_step({step, verdict}, out, name, meta, mod) do
    Results.write!(Path.join(out, name), step)
    IO.puts("#{meta.measure} #{step.level} #{mod.unit()}: #{step.verdict} #{step.reason}")

    if verdict == {:invalid, :store_full},
      do:
        IO.puts(
          "the store is out of space: stopping. Run the case again with ALAMBIQUE_LOAD_STORE_ON=disk (a store on disk needs at least #{round(meta.limits.store_min_free_mb)} MB free)"
        )

    {step, verdict}
  end

  defp quiet_opts(limits) do
    %{
      max_net_load: limits.max_net_load,
      min_available_gib: limits.min_available_gib,
      settle_s: limits.settle_s,
      max_wait_s: limits.wait_quiet_step_s
    }
  end

  defp quiet_io do
    %{
      read: &Sampler.host/0,
      now: fn -> System.system_time(:millisecond) end,
      sleep: &Process.sleep/1,
      log: &IO.puts/1
    }
  end

  defp cooldown_opts(limits) do
    %{
      max_mem_mb:
        if(limits.cooldown_memory?, do: limits.mem_limit_mb * limits.cooldown_mem_fraction),
      max_wait_s: limits.cooldown_max_s
    }
  end

  # The service's process memory and the operations still running on it; nil
  # when the container or the node is gone.
  defp cooldown_io(ctx) do
    read = fn ->
      with {:ok, cgroup} <- Cgroup.read(Cgroup.path(Env.get("SERVICE_ID"))) do
        %{process_mb: cgroup.process_mb, outstanding: Measure.call(ctx, :outstanding)}
      else
        :error -> nil
      end
    end

    %{
      read: fn ->
        try do
          read.()
        catch
          :error, {:erpc, _} -> nil
        end
      end,
      now: fn -> System.system_time(:millisecond) end,
      sleep: &Process.sleep/1,
      log: &IO.puts/1
    }
  end

  # The step of an attempt that never ran: the host or the service did not
  # allow it, or its preparation failed. `extra` says why in detail.
  defp unrun_step(ctx, {level, phase, seq, attempt}, waits, verdict, extra) do
    step = %{
      schema: 1,
      scenario: Env.get("SCENARIO"),
      measure: ctx.measure,
      variant: Env.get("VARIANT", "default"),
      store_on: Env.get("STORE_ON", "tmpfs"),
      store_fstype: Env.get("STORE_FSTYPE", "tmpfs"),
      seq: seq,
      phase: phase,
      level: level,
      attempt: attempt,
      started_at: iso_now(),
      duration_s: 0.0,
      quiet_wait_s: waits.quiet,
      cooldown_s: waits.cooldown,
      suspect: false,
      delivery: %{
        planned: 0,
        issued: 0,
        dropped: 0,
        completed: 0,
        ok: 0,
        busy: 0,
        errors: 0,
        ratio: nil,
        busy_ratio: nil
      },
      latency_ms: %{},
      host: nil,
      swap: nil,
      cgroup: nil,
      store: nil,
      service: nil,
      cost: nil,
      extra: extra,
      verdict: verdict_name(verdict),
      reason: reason(verdict)
    }

    {step, verdict}
  end

  # A level that cannot be prepared (or a store with no room left) is an
  # invalid step with its reason, not a crash: the summary is always written.
  defp run_step(ctx, mod, level, phase, seq, attempt) do
    with :ok <- check_store(ctx.limits),
         {:ok, prep} <- prepare(ctx, mod, level) do
      measure_step(ctx, mod, prep, level, phase, seq, attempt)
    else
      {:invalid, reason, extra} ->
        unrun_step(
          ctx,
          {level, phase, seq, attempt},
          %{quiet: 0.0, cooldown: 0.0},
          {:invalid, reason},
          extra
        )
    end
  end

  defp check_store(limits) do
    reading = Sampler.store()

    if reading && Capacity.store_full?(reading, limits),
      do: {:invalid, :store_full, %{store: reading}},
      else: :ok
  end

  defp prepare(ctx, mod, level) do
    mod.prepare(ctx, level)
  rescue
    error -> {:invalid, :prepare_failed, %{prepare_error: Exception.message(error)}}
  catch
    kind, reason -> {:invalid, :prepare_failed, %{prepare_error: inspect({kind, reason})}}
  end

  defp measure_step(ctx, mod, prep, level, phase, seq, attempt) do
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

    duration_s = (ended - started) / 1000

    step =
      Map.merge(step, %{
        seq: seq,
        phase: phase,
        level: level,
        attempt: attempt,
        started_at: started_at,
        duration_s: duration_s,
        suspect:
          step.cgroup != nil and
            Verdict.suspect?(
              step.cgroup.throttled_ms,
              duration_s,
              ctx.limits.suspect_throttle_fraction
            )
      })

    verdict = decide(ctx, result, step, cg1 == nil)
    {Map.merge(step, %{verdict: verdict_name(verdict), reason: reason(verdict)}), verdict}
  end

  defp after_readings(ctx) do
    case Cgroup.read(Cgroup.path(Env.get("SERVICE_ID"))) do
      {:ok, cg} -> {cg, Measure.call(ctx, :snapshot)}
      :error -> {nil, nil}
    end
  end

  defp decide(ctx, result, step, service_gone?) do
    Verdict.decide(
      %{
        planned: result.planned,
        completed: result.completed,
        ok: result.ok,
        errors: result.errors,
        busy: result.busy,
        store_full: Verdict.store_full?(Map.get(result, :error_samples, [])),
        latency_ms: step.latency_ms,
        net_load_max: step.host.net_load_max,
        swap: swap_verdict_stats(step.swap),
        mem_mb_max: step.cgroup && step.cgroup.mem_process_mb.max,
        aborted: result.aborted,
        service_gone?: service_gone?
      },
      ctx.limits
    )
  end

  defp swap_verdict_stats(swap) do
    %{
      driver_max_mb: swap.driver_mb && swap.driver_mb.max,
      host_max_mb: swap.host_mb && swap.host_mb.max,
      store_growth_mb: swap.store_mb && swap.store_mb.growth,
      service_mb: swap.service_mb && swap.service_mb.max
    }
  end

  defp build(ctx, result, samples, {cg0, cg1}, {snap0, snap1}, probe) do
    latency = latency_ms(result.latencies_us)
    cgroup = cgroup_stats(samples.cgroup, cg0, cg1)
    swap = Host.swap_stats(samples.swap)
    ops = result.ok

    %{
      schema: 1,
      scenario: Env.get("SCENARIO"),
      measure: ctx.measure,
      variant: Env.get("VARIANT", "default"),
      store_on: Env.get("STORE_ON", "tmpfs"),
      store_fstype: Env.get("STORE_FSTYPE", "tmpfs"),
      delivery: %{
        planned: result.planned,
        issued: result.issued,
        dropped: result.dropped,
        completed: result.completed,
        ok: result.ok,
        busy: result.busy,
        errors: result.errors,
        error_samples: result.error_samples,
        ratio: Verdict.delivery_ratio(result),
        busy_ratio: if(result.completed > 0, do: result.busy / result.completed),
        aborted: result.aborted
      },
      latency_ms: latency,
      host: host_stats(samples.host),
      swap: swap,
      cgroup: cgroup,
      store: store_stats(samples.store),
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
      throttled_usec: cg1.throttled_usec - cg0.throttled_usec,
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

  # The store's space: used MiB at the end of the step and at most, and for a
  # store on disk the free MiB at least.
  defp store_stats([]), do: nil

  defp store_stats(samples) do
    last = List.last(samples)

    %{
      kind: last.kind,
      used_mb_end: last.used_mb,
      used_mb_max: samples |> Enum.map(& &1.used_mb) |> Enum.max(),
      free_mb_min: if(last.kind == :disk, do: samples |> Enum.map(& &1.free_mb) |> Enum.min())
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
      store = Sampler.store()

      cond do
        host && Host.low_memory?(host.mem_available_gib, limits.min_available_gib) ->
          :host_low_memory

        cgroup == :error ->
          :service_down

        store && Capacity.store_full?(store, limits) ->
          :store_full

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
