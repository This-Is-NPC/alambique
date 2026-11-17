defmodule Alambique.Load.Driver do
  @moduledoc false
  # Runs one ramp of one measure against a service started by scripts/load-case
  # and writes a JSON file per step and a summary. It runs on the host, outside
  # the container whose CPU and memory it measures.

  alias Alambique.Load.{
    Capacity,
    Cgroup,
    Config,
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

  @incomplete 10

  @doc """
  Runs the ramp of the case in `argv` (the path of its `case.json`) and returns
  the exit status: 0 done, 2 stopped invalid, 3 host low on memory, 4 service
  down. With `--replay` it starts no service: it only reads the steps that are
  done, writes the summary and returns 0 when they cover the ramp, or 10 when a
  step is left to run.
  """
  def main(argv \\ System.argv()) do
    {replay?, path} =
      case argv do
        ["--replay", path] -> {true, path}
        [path] -> {false, path}
      end

    cfg = Config.read!(path)
    Config.put(cfg)
    mod = Map.fetch!(@measures, cfg["measure"])
    limits = Config.limits(cfg, mod.holds_memory?())
    out = cfg["dir"]

    plan = %{
      cfg: cfg,
      mod: mod,
      limits: limits,
      out: out,
      meta: meta(cfg, limits, mod, out),
      replay?: replay?,
      skip?: cfg["skip_done"],
      timeout_ms: round(cfg["timeouts"]["step_s"] * 1000)
    }

    steps = cfg["steps"]
    ramp = Ramp.new(cfg["levels"], refine: steps["refine"], confirm: steps["confirm"])

    case climb(plan, nil, ramp, 1, []) do
      :incomplete ->
        @incomplete

      {ctx, ramp, steps} ->
        meta = %{plan.meta | service: (ctx && ctx.settings) || plan.meta.service}
        summary = Summary.build(meta, Enum.reverse(steps), Ramp.outcome(ramp))
        Results.write!(Path.join(out, "summary.json"), summary)
        File.write!(Path.join(out, "summary.md"), Summary.markdown(summary))
        IO.puts(Summary.markdown(summary))
        exit_status(steps)
    end
  end

  defp meta(cfg, limits, mod, out) do
    before = previous_summary(out)

    %{
      scenario: cfg["scenario"],
      measure: cfg["measure"],
      variant: cfg["variant"],
      store_on: cfg["store"]["backing"],
      store_fstype: Env.get("STORE_FSTYPE") || before["store_fstype"] || "tmpfs",
      unit: mod.unit(),
      cpus: cfg["cpus"],
      mem_limit_mb: limits.mem_limit_mb,
      command: cfg["command"],
      config_hash: cfg["hash"],
      started_at: iso_now(),
      limits: limits,
      service: before["service"]
    }
  end

  defp previous_summary(out) do
    with {:ok, text} <- File.read(Path.join(out, "summary.json")),
         {:ok, summary} <- Jason.decode(text) do
      summary
    else
      _ -> %{}
    end
  end

  # What the case needs to run steps: the node, the sampler and the measure's
  # setup. Started at the first step that is not done, so that a case whose
  # steps are all done starts nothing.
  defp start(%{cfg: cfg, mod: mod, limits: limits}) do
    node = Env.get("NODE") |> String.to_atom()
    connect(node)

    {:ok, _} = State.start_link()

    {:ok, _} =
      Sampler.start_link(%{
        service: Cgroup.path(Env.get("SERVICE_ID")),
        store: Cgroup.path(Env.get("STORE_ID")),
        store_dir: if(cfg["store"]["backing"] == "disk", do: Env.get("STORE_PATH"))
      })

    ctx = %{node: node, measure: cfg["measure"], limits: limits, abort: abort_fun(limits)}
    ctx = Map.put(ctx, :settings, settings(ctx))
    :ok = mod.setup(ctx)
    ctx
  end

  # `ctx` is nil until the first step that has to run.
  defp climb(plan, ctx, ramp, seq, steps) do
    case Ramp.next(ramp) do
      :done ->
        {ctx, ramp, steps}

      {:run, level, phase} ->
        name = Results.step_name(level, phase)

        case done_step(plan, name, seq) do
          {:ok, step} ->
            IO.puts("#{plan.cfg["measure"]} #{level}: done, #{step.verdict} #{step.reason}")
            advance(plan, ctx, ramp, seq, steps, step)

          :none when plan.replay? ->
            :incomplete

          :none ->
            ctx = ctx || start(plan)
            {step, verdict} = step_with_repeats(plan, ctx, level, phase, seq, name)
            advance(plan, ctx, ramp, seq, steps, Map.put(step, :verdict_term, verdict))
        end
    end
  end

  defp advance(plan, ctx, ramp, seq, steps, step) do
    ramp = Ramp.record(ramp, Verdict.ramp_verdict(step.verdict_term))
    climb(plan, ctx, ramp, seq + 1, [step | steps])
  end

  # A step with a result of the same settings that is not invalid is done: it
  # is read, not run (never with --no-skip-done).
  defp done_step(%{skip?: false}, _name, _seq), do: :none

  defp done_step(plan, name, seq) do
    case Results.read_done(Path.join(plan.out, name), plan.meta.config_hash) do
      {:ok, step} -> {:ok, %{step | seq: seq}}
      :none -> :none
    end
  end

  # Every attempt starts when the service has cooled down from the previous
  # step and the host is quiet. A step the host disturbed, or that started on a
  # service that did not cool down, runs again, up to `limits.attempts` times
  # in all; the disturbed attempts that ran stay on disk, marked "repeat".
  # `waits` is the time spent waiting so far in this step, this attempt
  # included in the step it is written to.
  defp step_with_repeats(
         plan,
         ctx,
         level,
         phase,
         seq,
         name,
         attempt \\ 1,
         waits \\ %{quiet: 0.0, cooldown: 0.0}
       ) do
    key = {level, phase, seq, attempt}

    case wait_to_start(ctx, waits) do
      {:ready, waits} ->
        {step, verdict} = run_step(plan, ctx, level, phase, seq, attempt)
        step = Map.merge(step, %{quiet_wait_s: waits.quiet, cooldown_s: waits.cooldown})
        finish(plan, ctx, {step, verdict}, key, waits, name)

      {:no_cooldown, waits, mb} when attempt < ctx.limits.attempts ->
        IO.puts("#{plan.meta.measure} #{level}: repeated, no cooldown (#{round(mb)} MB)")
        step_with_repeats(plan, ctx, level, phase, seq, name, attempt + 1, waits)

      {:no_cooldown, waits, mb} ->
        plan
        |> unrun_step(key, waits, {:invalid, {:no_cooldown, mb}}, %{})
        |> write_step(plan, name)

      {:stop, waits, verdict} ->
        plan |> unrun_step(key, waits, verdict, %{}) |> write_step(plan, name)
    end
  end

  # Cooldown first, then the quiet window: the service idles through both.
  defp wait_to_start(ctx, waits) do
    case cooldown(ctx) do
      {:cooled, s} ->
        waits = %{waits | cooldown: waits.cooldown + s}

        case quiet(ctx) do
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

  # A criterion that is off waits for nothing.
  defp cooldown(%{limits: %{cooldown?: false}}), do: {:cooled, 0.0}
  defp cooldown(ctx), do: Cooldown.wait(cooldown_opts(ctx.limits), cooldown_io(ctx))

  defp quiet(%{limits: %{wait_quiet?: false}}), do: {:quiet, 0.0}
  defp quiet(ctx), do: Quiet.wait(quiet_opts(ctx.limits), quiet_io())

  defp add_quiet(waits, s), do: %{waits | quiet: waits.quiet + s}

  defp finish(plan, ctx, {step, verdict}, {level, phase, seq, attempt}, waits, name) do
    case verdict do
      {:repeat, reason} when attempt < ctx.limits.attempts ->
        Results.write!(
          Path.join(plan.out, String.replace(name, ".json", "-attempt#{attempt}.json")),
          Map.put(step, :config_hash, plan.meta.config_hash)
        )

        IO.puts("#{plan.meta.measure} #{level}: repeated, #{inspect(reason)}")
        # What the disturbed attempt left is undone, so the repeat redoes all of it.
        plan.mod.rewind(ctx, level)
        step_with_repeats(plan, ctx, level, phase, seq, name, attempt + 1, waits)

      {:repeat, reason} ->
        step = %{step | verdict: "invalid", reason: inspect(reason)}
        write_step({step, {:invalid, reason}}, plan, name)

      _ ->
        write_step({step, verdict}, plan, name)
    end
  end

  defp write_step({step, verdict}, plan, name) do
    step = Map.put(step, :config_hash, plan.meta.config_hash)
    Results.write!(Path.join(plan.out, name), step)

    IO.puts(
      "#{plan.meta.measure} #{step.level} #{plan.mod.unit()}: #{step.verdict} #{step.reason}"
    )

    if verdict == {:invalid, :store_full},
      do:
        IO.puts(
          "the store is out of space: stopping. Run the case again with the store on disk (store.backing = \"disk\"; a store on disk needs at least #{round(plan.limits.store_min_free_mb)} MB free)"
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
  defp unrun_step(plan, {level, phase, seq, attempt}, waits, verdict, extra) do
    step = %{
      schema: 1,
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

    {Map.merge(identity(plan), step), verdict}
  end

  # What every step result says about its case.
  defp identity(plan),
    do: Map.take(plan.meta, [:scenario, :measure, :variant, :store_on, :store_fstype])

  # A step that runs past `round.step_timeout_s` is invalid with reason
  # `timeout`: its work is stopped and the ramp ends, as for any invalid step.
  defp run_step(plan, ctx, level, phase, seq, attempt) do
    task = Task.async(fn -> run_step_now(plan, ctx, level, phase, seq, attempt) end)

    case Task.yield(task, plan.timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} ->
        result

      nil ->
        Sampler.collect()

        unrun_step(
          plan,
          {level, phase, seq, attempt},
          %{quiet: 0.0, cooldown: 0.0},
          {:invalid, :timeout},
          %{timeout_s: plan.timeout_ms / 1000}
        )
    end
  end

  # A level that cannot be prepared (or a store with no room left) is an
  # invalid step with its reason, not a crash: the summary is always written.
  defp run_step_now(plan, ctx, level, phase, seq, attempt) do
    with :ok <- check_store(ctx.limits),
         {:ok, prep} <- prepare(ctx, plan.mod, level) do
      measure_step(plan, ctx, prep, level, phase, seq, attempt)
    else
      {:invalid, reason, extra} ->
        unrun_step(
          plan,
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

  defp measure_step(%{mod: mod} = plan, ctx, prep, level, phase, seq, attempt) do
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

    step = build(plan, result, samples, {cg0, cg1}, {snap0, snap1}, probe)

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
        store_full:
          Verdict.store_full?(
            Map.get(result, :error_samples, []),
            Capacity.strained?(step.store, ctx.limits)
          ),
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

  defp build(plan, result, samples, {cg0, cg1}, {snap0, snap1}, probe) do
    latency = latency_ms(result.latencies_us)
    cgroup = cgroup_stats(samples.cgroup, cg0, cg1)
    swap = Host.swap_stats(samples.swap)
    ops = result.ok

    Map.merge(identity(plan), %{
      schema: 1,
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
    })
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
