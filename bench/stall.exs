# Stalls of the write path under steady load (design section 12).
#
#   mise run bench:stall                 # 60 s per rate, in-memory store
#   mise run bench:stall -- 30           # seconds per rate
#   ALAMBIQUE_BENCH_STORE=seaweedfs mise run bench:stall   # after `mise run seaweedfs:up`
#
# Open loop: single-row inserts arrive at a fixed mean rate (Poisson) over N
# cells, one process per arrival. The instance has the feed on with
# `object_batches: 16, linger_ms: 250`, a 30 s lease, the default pipeline
# depth, compaction and retention. One BEAM node, one instance. The files of
# the cells go under `$TMPDIR`: point it at the disk under test (`/tmp` may be
# a tmpfs).
#
# Per second it prints the requests, p50, p99, max, the number over the stall
# limit, and what can explain a stall: the run queues (CPU, dirty CPU, dirty
# IO), the NIF calls and their longest, the longest span of the commit, the
# sync, the checkpoint, the lease renewal, the compaction and the feed, the
# longest store request by kind, the CPU time the cgroup quota throttled
# (`thr_ms`), the most threads of the process in uninterruptible sleep, state D
# (`Dthr`), the WAL size, the longest message queue and the garbage
# collections. After the timeline, every window with a request over the limit
# is listed with the spans and store requests over 100 ms near it, the kernel
# wait channel of the threads in D state (`folio_wait_bit` is a wait for a
# page-cache page under I/O) and the long schedules and collections of the
# BEAM. Linux only: the kernel columns are 0 elsewhere.
#
#   ALAMBIQUE_BENCH_RATES     arrivals per second, space-separated  (default "60 100 200 400")
#   ALAMBIQUE_BENCH_CELLS     cells written to                      (default "12")
#   ALAMBIQUE_BENCH_STALL_MS  requests of at least this are stalls  (default "1000")
#   ALAMBIQUE_BENCH_STORE     "memory", or "seaweedfs" for the disposable
#                             container from `mise run seaweedfs:up`
#                             (default "memory")
#   ALAMBIQUE_BENCH_OPTS      extra Alambique options, an Elixir keyword list
#                             evaluated at start, e.g. "[wal: [checkpoint_bytes: 1_000_000]]"
#                             (default none)
#
# Pin the CPUs and the dirty IO pool from outside, for example
# `ELIXIR_ERL_OPTIONS="+SDio 4" taskset -c 0,1 mise run bench:stall`, or with
# a quota: `systemd-run --user --scope -p CPUQuota=150% mise run bench:stall`.

defmodule TimingStore do
  @moduledoc false
  # Wraps a store and records {second, kind, duration_us} for every request.
  @behaviour Alambique.Store

  def wrap({_m, _c} = inner, table), do: {__MODULE__, %{inner: inner, table: table}}

  defp timed(%{table: t}, op, key, fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()
    done = System.monotonic_time(:microsecond)
    :ets.insert(t, {:store, started, op, kind(key), done - started})
    result
  end

  def kind(key) do
    cond do
      String.contains?(key, "lease") ->
        "lease"

      String.contains?(key, "feed") ->
        "feed"

      String.contains?(key, "snapshot") ->
        "snapshot"

      String.contains?(key, "ltx") ->
        "ltx"

      true ->
        key
        |> String.replace(~r/[0-9a-f]{6,}|\d+/, "N")
        |> String.split("/")
        |> Enum.take(-2)
        |> Enum.join("/")
    end
  end

  @impl true
  def location(%{inner: {m, c}}),
    do: if(function_exported?(m, :location, 1), do: m.location(c), else: c)

  @impl true
  def put(%{inner: {m, c}} = s, key, body, cond, meta),
    do: timed(s, :put, key, fn -> m.put(c, key, body, cond, meta) end)

  @impl true
  def get(%{inner: {m, c}} = s, key), do: timed(s, :get, key, fn -> m.get(c, key) end)

  @impl true
  def get_range(%{inner: {m, c}} = s, key, o, l),
    do: timed(s, :get_range, key, fn -> m.get_range(c, key, o, l) end)

  @impl true
  def head(%{inner: {m, c}} = s, key), do: timed(s, :head, key, fn -> m.head(c, key) end)

  @impl true
  def list(%{inner: {m, c}} = s, prefix, after_key, limit),
    do: timed(s, :list, prefix, fn -> m.list(c, prefix, after_key, limit) end)

  @impl true
  def delete(%{inner: {m, c}} = s, key), do: timed(s, :delete, key, fn -> m.delete(c, key) end)
end

defmodule Bench do
  @events [
    [:cell, :commit],
    [:cell, :checkpoint],
    [:cell, :wake],
    [:cell, :hibernate],
    [:replication, :sync],
    [:replication, :hydrate],
    [:lease, :acquire],
    [:lease, :renew],
    [:compaction, :merge],
    [:compaction, :snapshot],
    [:compaction, :pass],
    [:compactor, :tick],
    [:feed, :publish],
    [:feed, :prune],
    [:retention, :run],
    [:trash, :mark],
    [:trash, :reap],
    [:local_cache, :sweep],
    [:router, :forward]
  ]

  def run(seconds) do
    machine()

    IO.puts(
      "duration:  #{seconds} s per rate, #{cells()} cells, stall = request over #{stall_ms()} ms\n"
    )

    store =
      case System.get_env("ALAMBIQUE_BENCH_STORE", "memory") do
        "memory" -> memory()
        "seaweedfs" -> seaweedfs()
        other -> raise "ALAMBIQUE_BENCH_STORE must be memory or seaweedfs, not #{inspect(other)}"
      end

    for rate <- list("RATES", "60 100 200 400"), do: one(store, rate, seconds)
  end

  defp list(name, default),
    do:
      ("ALAMBIQUE_BENCH_" <> name)
      |> System.get_env(default)
      |> String.split()
      |> Enum.map(&String.to_integer/1)

  defp cells, do: "ALAMBIQUE_BENCH_CELLS" |> System.get_env("12") |> String.to_integer()
  defp stall_ms, do: "ALAMBIQUE_BENCH_STALL_MS" |> System.get_env("1000") |> String.to_integer()

  defp memory do
    {:ok, store} = AlambiqueTestkit.MemoryStore.start_link()
    {AlambiqueTestkit.MemoryStore, AlambiqueTestkit.MemoryStore.config(store)}
  end

  defp seaweedfs do
    config = %{
      endpoint: "http://127.0.0.1:18333",
      bucket: "bench-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower),
      access_key_id: "alambique",
      secret_access_key: "alambique-secret",
      path_style: true,
      receive_timeout: 30_000
    }

    created =
      Req.put(
        url: "#{config.endpoint}/#{config.bucket}",
        aws_sigv4: [
          access_key_id: config.access_key_id,
          secret_access_key: config.secret_access_key,
          service: "s3",
          region: "us-east-1"
        ],
        retry: false
      )

    case created do
      {:ok, %{status: status}} when status in [200, 409] ->
        {Alambique.Store.S3, config}

      other ->
        IO.puts("SeaweedFS is not running (mise run seaweedfs:up): #{inspect(other)}")
        System.halt(1)
    end
  end

  defp one(store, rate, seconds) do
    probes = :ets.new(:probes, [:public, :duplicate_bag, write_concurrency: true])
    dir = Alambique.TestDirs.new("bench")
    name = :"bench_stall_#{System.unique_integer([:positive])}"
    :persistent_term.put({__MODULE__, :probes}, probes)

    extra =
      case System.get_env("ALAMBIQUE_BENCH_OPTS") do
        nil -> []
        code -> code |> Code.eval_string() |> elem(0)
      end

    opts =
      Keyword.merge(
        [
          name: name,
          store: TimingStore.wrap(store, probes),
          prefix: "bench-" <> Integer.to_string(System.unique_integer([:positive])),
          data_dir: dir,
          migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}],
          lease_ttl_ms: 30_000,
          feed: [object_batches: 16, linger_ms: 250]
        ],
        extra
      )

    {:ok, sup} = Supervisor.start_link([{Alambique, opts}], strategy: :one_for_one)
    ids = for i <- 1..cells(), do: "room-#{i}"
    for id <- ids, do: {:ok, _} = Alambique.execute(name, id, "INSERT INTO t (v) VALUES ('warm')")

    handlers = attach(probes)
    monitor = start_monitor(probes)
    sampler = spawn_link(fn -> sample(probes, dir) end)

    :ets.delete_all_objects(probes)
    t0 = System.monotonic_time(:microsecond)
    :persistent_term.put({__MODULE__, :t0}, t0)
    stop = System.monotonic_time(:microsecond) + seconds * 1_000_000

    latencies = drive(name, ids, rate, stop, t0)

    Process.unlink(sampler)
    Process.exit(sampler, :kill)
    :erlang.system_monitor(:undefined)
    Process.exit(monitor, :kill)
    for h <- handlers, do: :telemetry.detach(h)

    IO.puts("=== rate #{rate}/s ===")
    report(probes, latencies, seconds, stall_ms() * 1000)

    Supervisor.stop(sup)
    File.rm_rf(dir)
    :ets.delete(probes)
  end

  defp attach(probes) do
    for event <- @events do
      id = {__MODULE__, event, make_ref()}
      :telemetry.attach(id, [:alambique | event], &__MODULE__.span/4, probes)
      id
    end
  end

  def span(event, %{duration: d}, meta, probes) do
    now = System.monotonic_time(:microsecond)
    us = System.convert_time_unit(d, :native, :microsecond)
    :ets.insert(probes, {:span, now - us, Enum.join(tl(event), "."), us, meta[:result]})
  end

  def span(_event, _m, _meta, _probes), do: :ok

  # Long schedules and collections of the BEAM, with the process or port.
  defp start_monitor(probes) do
    spawn(fn ->
      :erlang.system_monitor(self(), [{:long_schedule, 50}, {:long_gc, 50}])
      monitor_loop(probes)
    end)
  end

  defp monitor_loop(probes) do
    receive do
      {:monitor, who, kind, info} ->
        now = System.monotonic_time(:microsecond)

        what =
          if is_pid(who),
            do: Process.info(who, :current_function) |> inspect(),
            else: inspect(who)

        :ets.insert(probes, {:monitor, now, kind, who |> inspect(), what, inspect(info)})
        monitor_loop(probes)
    end
  end

  # Samples every 100 ms: run queues, NIF counters, the longest message queue,
  # the garbage collections, and every second the WAL size.
  defp sample(probes, dir) do
    now = System.monotonic_time(:millisecond)
    t = System.monotonic_time(:microsecond)
    ncpu = :erlang.system_info(:schedulers)
    dcpu = :erlang.system_info(:dirty_cpu_schedulers)
    all = :erlang.statistics(:run_queue_lengths_all)
    {cpu_q, rest} = Enum.split(all, ncpu)
    {dcpu_q, dio_q} = Enum.split(rest, dcpu)

    {gcs, words, _} = :erlang.statistics(:garbage_collection)
    stats = Alambique.NativeStats.read()

    mq =
      :erlang.processes()
      |> Enum.map(fn pid ->
        case Process.info(pid, :message_queue_len) do
          {_, n} -> n
          nil -> 0
        end
      end)
      |> Enum.max()

    wal =
      dir
      |> Path.join("**/*-wal")
      |> Path.wildcard()
      |> Enum.map(
        &(File.stat(&1)
          |> then(fn
            {:ok, s} -> s.size
            _ -> 0
          end))
      )
      |> Enum.max(fn -> 0 end)

    :ets.insert(
      probes,
      {:sample, t, Enum.sum(cpu_q), Enum.sum(dcpu_q), Enum.sum(dio_q), gcs, words, stats, mq, wal}
    )

    {throttled_us, nr_throttled} = cgroup_throttle()
    {dthreads, wchans} = d_state_threads()
    :ets.insert(probes, {:kernel, t, throttled_us, nr_throttled, dthreads, wchans})

    Process.sleep(max(100 - (System.monotonic_time(:millisecond) - now), 1))
    sample(probes, dir)
  end

  # Time this cgroup was throttled by its CPU quota, from cpu.stat.
  defp cgroup_throttle do
    with {:ok, cg} <- File.read("/proc/self/cgroup"),
         [_, path] <- Regex.run(~r/^0::(.*)$/m, cg),
         {:ok, stat} <- File.read("/sys/fs/cgroup" <> path <> "/cpu.stat") do
      get = fn key ->
        case Regex.run(~r/^#{key} (\d+)$/m, stat) do
          [_, v] -> String.to_integer(v)
          _ -> 0
        end
      end

      {get.("throttled_usec"), get.("nr_throttled")}
    else
      _ -> {0, 0}
    end
  end

  # Threads of this OS process in uninterruptible sleep (D), with their kernel wait channel.
  defp d_state_threads do
    d =
      for stat <- Path.wildcard("/proc/#{System.pid()}/task/*/stat"),
          {:ok, text} <- [File.read(stat)],
          [_, "D"] <- [Regex.run(~r/\) (\w) /, text)],
          do: Path.dirname(stat)

    {length(d),
     for(
       dir <- d,
       do:
         dir
         |> Path.join("wchan")
         |> File.read()
         |> then(fn
           {:ok, w} -> w
           _ -> "?"
         end)
     )}
  end

  defp drive(name, ids, rate, stop, t0) do
    table = :persistent_term.get({__MODULE__, :probes})
    done = :counters.new(1, [])
    n = length(ids)

    count =
      arrive(
        {table, done},
        name,
        List.to_tuple(ids),
        n,
        System.monotonic_time(:microsecond),
        rate,
        stop,
        0,
        t0
      )

    wait(done, count)
    for {:req, s, us, ok} <- :ets.tab2list(table), do: {s, us, ok}
  end

  # Each request records itself in the ETS table, so no mailbox grows.
  defp arrive({table, done} = sinks, name, ids, n, next, rate, stop, count, t0) do
    now = System.monotonic_time(:microsecond)

    cond do
      now >= stop ->
        count

      next > now ->
        Process.sleep(div(next - now, 1000))
        arrive(sinks, name, ids, n, next, rate, stop, count, t0)

      true ->
        id = elem(ids, :rand.uniform(n) - 1)

        spawn(fn ->
          s = System.monotonic_time(:microsecond)
          r = Alambique.execute(name, id, "INSERT INTO t (v) VALUES (?)", ["xxxxxxxxxxxxxxxx"])
          e = System.monotonic_time(:microsecond)
          :ets.insert(table, {:req, s - t0, e - s, match?({:ok, _}, r)})
          :counters.add(done, 1, 1)
        end)

        gap = trunc(-:math.log(1 - :rand.uniform()) / rate * 1_000_000)
        arrive(sinks, name, ids, n, next + gap, rate, stop, count + 1, t0)
    end
  end

  defp wait(done, count, tries \\ 300) do
    if :counters.get(done, 1) < count and tries > 0 do
      Process.sleep(100)
      wait(done, count, tries - 1)
    end
  end

  # ---- report -------------------------------------------------------------

  defp report(probes, latencies, seconds, stall_us) do
    objs = :ets.tab2list(probes)
    sec = fn us -> div(us, 1_000_000) end
    by = fn list, key -> Enum.group_by(list, key) end

    reqs = by.(latencies, fn {s, _, _} -> sec.(s) end)
    t0 = :persistent_term.get({__MODULE__, :t0})
    spans = for {:span, s, n, d, _} <- objs, do: {sec.(s - t0), n, d}
    spans_by = by.(spans, fn {s, _, _} -> s end)
    store = for {:store, s, op, kind, d} <- objs, do: {sec.(s - t0), "#{op}:#{kind}", d}
    store_by = by.(store, fn {s, _, _} -> s end)

    samples =
      for {:sample, t, c, dc, dio, gc, w, st, mq, wal} <- objs,
          do: {sec.(t - t0), {c, dc, dio, gc, w, st, mq, wal}}

    samples_by = Enum.group_by(samples, &elem(&1, 0), &elem(&1, 1))

    kernel_by =
      for({:kernel, t, th, _n, d, _w} <- objs, do: {sec.(t - t0), th, d})
      |> Enum.group_by(&elem(&1, 0), fn {_, th, d} -> {th, d} end)

    IO.puts(
      "   t  reqs  p50ms  p99ms  maxms  >1s | rq cpu/dcpu/dio | sqlite n/maxms  repl n/maxms  lazy n | " <>
        "commit  sync  ckpt  lease  compact  feed | store max ms (kind) | thr_ms Dthr | walMB  mq  gcs gcMwords"
    )

    Enum.reduce(0..(seconds + 1), nil, fn s, prev ->
      lat = Enum.map(Map.get(reqs, s, []), &elem(&1, 1)) |> Enum.sort()
      n = length(lat)
      pct = fn p -> if n == 0, do: 0.0, else: Enum.at(lat, min(n - 1, trunc(n * p))) / 1000 end
      over = Enum.count(lat, &(&1 >= stall_us))
      sm = Map.get(samples_by, s, [])
      first = List.first(sm)
      last = List.last(sm)

      {rq, dnat} =
        if sm == [] do
          {{0, 0, 0}, {0, 0.0, 0, 0.0, 0}}
        else
          rq =
            {sm |> Enum.map(&elem(&1, 0)) |> Enum.max(),
             sm |> Enum.map(&elem(&1, 1)) |> Enum.max(),
             sm |> Enum.map(&elem(&1, 2)) |> Enum.max()}

          {rq, native_delta(prev, last)}
        end

      sp = Map.get(spans_by, s, [])

      smax = fn pref ->
        sp
        |> Enum.filter(fn {_, nm, _} -> String.starts_with?(nm, pref) end)
        |> Enum.map(&elem(&1, 2))
        |> Enum.max(fn -> 0 end)
        |> Kernel./(1000)
      end

      st = Map.get(store_by, s, [])

      {sk, sd} =
        st
        |> Enum.max_by(&elem(&1, 2), fn -> {0, "-", 0} end)
        |> then(fn {_, k, d} -> {k, d / 1000} end)

      walmb =
        if sm == [],
          do: 0.0,
          else: sm |> Enum.map(&elem(&1, 7)) |> Enum.max() |> Kernel./(1_048_576)

      mq = if sm == [], do: 0, else: sm |> Enum.map(&elem(&1, 6)) |> Enum.max()

      {gcn, gcw} =
        if sm == [] or first == nil,
          do: {0, 0},
          else: {elem(last, 3) - elem(first, 3), elem(last, 4) - elem(first, 4)}

      {sn, smx, rn, rmx, ln} = dnat
      ker = Map.get(kernel_by, s, [])

      thr =
        case {List.first(ker), List.last(ker)} do
          {{a, _}, {b, _}} -> b - a
          _ -> 0
        end

      dmax = ker |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> 0 end)

      IO.puts(
        :io_lib.format(
          "~4B ~5B ~6.1f ~6.1f ~6.1f ~4B | ~3B/~3B/~3B | ~6B/~6.1f ~6B/~6.1f ~5B | ~6.1f ~5.1f ~5.1f ~6.1f ~7.1f ~5.1f | ~7.1f ~-18s | ~6.1f ~4B | ~5.1f ~4B ~4B ~6.1f",
          [
            s,
            n,
            pct.(0.5),
            pct.(0.99),
            (List.last(lat) || 0) / 1000,
            over,
            elem(rq, 0),
            elem(rq, 1),
            elem(rq, 2),
            sn,
            smx,
            rn,
            rmx,
            ln,
            smax.("cell.commit"),
            smax.("replication.sync"),
            smax.("cell.checkpoint"),
            smax.("lease"),
            smax.("compaction") |> max(smax.("compactor")),
            smax.("feed"),
            sd,
            to_string(sk),
            thr / 1000,
            dmax,
            walmb,
            mq,
            gcn,
            gcw / 1.0e6
          ]
        )
      )

      last || prev
    end)

    summary(latencies, stall_us)
    stalls(objs, latencies, stall_us, spans, store)
  end

  defp native_delta(nil, _), do: {0, 0.0, 0, 0.0, 0}

  defp native_delta(prev, last) do
    {_, _, _, _, _, a, _, _} = prev
    {_, _, _, _, _, b, _, _} = last

    {b.sqlite_calls - a.sqlite_calls, b.sqlite_call_ns_max / 1.0e6,
     b.replication_calls - a.replication_calls, b.replication_call_ns_max / 1.0e6,
     b.lazy_calls - a.lazy_calls}
  end

  defp summary(latencies, stall_us) do
    lat = latencies |> Enum.map(&elem(&1, 1)) |> Enum.sort()
    n = length(lat)
    p = fn q -> Enum.at(lat, min(n - 1, trunc(n * q))) / 1000 end
    failed = Enum.count(latencies, fn {_, _, ok} -> not ok end)

    IO.puts(
      :io_lib.format(
        "summary: n=~B failed=~B p50=~.1f p99=~.1f p999=~.1f max=~.1f ms, over ~B ms: ~B~n",
        [
          n,
          failed,
          p.(0.5),
          p.(0.99),
          p.(0.999),
          List.last(lat) / 1000,
          div(stall_us, 1000),
          Enum.count(lat, &(&1 >= stall_us))
        ]
      )
    )
  end

  # For every request over the limit: the window it covers and what ran in it.
  defp stalls(objs, latencies, stall_us, spans, store) do
    slow = latencies |> Enum.filter(fn {_, us, _} -> us >= stall_us end) |> Enum.sort()

    if slow != [] do
      IO.puts(
        "stalls (start s .. end s, ms) and spans or store requests over 100 ms that overlap:"
      )

      t0 = :persistent_term.get({__MODULE__, :t0})

      # Merge overlapping slow requests into windows.
      windows =
        Enum.reduce(slow, [], fn {s, us, _}, acc ->
          case acc do
            [{a, b, n} | rest] when s <= b -> [{a, max(b, s + us), n + 1} | rest]
            _ -> [{s, s + us, 1} | acc]
          end
        end)
        |> Enum.reverse()

      for {a, b, n} <- windows do
        IO.puts(:io_lib.format("  ~.2f .. ~.2f s  (~B slow requests)", [a / 1.0e6, b / 1.0e6, n]))

        near = fn s, d -> s - t0 + d >= a - 200_000 and s - t0 <= b + 200_000 end

        spans = for {:span, s, nm, d, _} <- objs, d >= 100_000, near.(s, d), do: {nm, d}

        stores =
          for {:store, s, op, kind, d} <- objs,
              d >= 100_000,
              near.(s, d),
              do: {"#{op}:#{kind}", d}

        for {title, list} <- [{"span ", spans}, {"store", stores}],
            {nm, ds} <- Enum.group_by(list, &elem(&1, 0), &elem(&1, 1)) |> Enum.sort() do
          IO.puts(
            :io_lib.format("      ~s ~-24s ~4B over 100 ms, longest ~7.1f ms", [
              title,
              nm,
              length(ds),
              Enum.max(ds) / 1000
            ])
          )
        end

        kernel = for {:kernel, s, th, n, d, w} <- objs, d > 0, near.(s, 0), do: {s, d, w, th, n}

        for {s, d, w, th, n} <- Enum.take(kernel, 8) do
          IO.puts(
            "      kernel at #{Float.round((s - t0) / 1.0e6, 2)} s: #{d} threads in D state, wchan #{inspect(Enum.frequencies(w))}, throttled #{th} us (#{n} periods)"
          )
        end

        IO.puts("      kernel: #{length(kernel)} samples with a thread in D state in this window")

        for {:monitor, s, kind, who, what, info} <-
              Enum.take(Enum.filter(objs, &match?({:monitor, _, _, _, _, _}, &1)), 6),
            near.(s, 0) do
          IO.puts(
            "      monitor #{kind} #{who} #{what} #{info} at #{Float.round((s - t0) / 1.0e6, 2)} s"
          )
        end
      end
    end

    kinds =
      Enum.group_by(store, fn {_, k, _} -> k end, fn {_, _, d} -> d end)

    IO.puts("store requests by kind: count, p50, p99, max ms")

    for {k, ds} <- Enum.sort(kinds) do
      s = Enum.sort(ds)
      n = length(s)

      IO.puts(
        :io_lib.format("  ~-28s ~6B ~7.1f ~7.1f ~7.1f", [
          k,
          n,
          Enum.at(s, div(n, 2)) / 1000,
          Enum.at(s, min(n - 1, trunc(n * 0.99))) / 1000,
          List.last(s) / 1000
        ])
      )
    end

    names = spans |> Enum.group_by(&elem(&1, 1), &elem(&1, 2))
    IO.puts("spans by event: count, p50, p99, max ms")

    for {k, ds} <- Enum.sort(names) do
      s = Enum.sort(ds)
      n = length(s)

      IO.puts(
        :io_lib.format("  ~-28s ~6B ~7.1f ~7.1f ~7.1f", [
          k,
          n,
          Enum.at(s, div(n, 2)) / 1000,
          Enum.at(s, min(n - 1, trunc(n * 0.99))) / 1000,
          List.last(s) / 1000
        ])
      )
    end

    IO.puts("")
  end

  defp machine do
    cpu =
      "/proc/cpuinfo"
      |> File.read!()
      |> then(&Regex.run(~r/model name\s*:\s*(.+)/, &1))
      |> then(fn [_, name] -> name end)

    {:ok, aff} = File.read("/proc/self/status")
    [_, allowed] = Regex.run(~r/Cpus_allowed_list:\s*(\S+)/, aff)

    IO.puts("""
    machine:   #{cpu}, #{System.schedulers_online()} schedulers online, dirty IO #{:erlang.system_info(:dirty_io_schedulers)}, dirty CPU #{:erlang.system_info(:dirty_cpu_schedulers)}, cpus allowed #{allowed}
    runtime:   Erlang/OTP #{:erlang.system_info(:otp_release)}, Elixir #{System.version()}
    """)
  end
end

seconds =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 60
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(seconds)
after
  Alambique.TestDirs.clean()
end
