# A node swap under continuous load, as the Atanor runtime saw it (design §6.3,
# §9): busy cells commit until the old node stops, then a fresh node wakes all
# of them at once on one CPU.
#
#   mise run seaweedfs:up
#   mise run bench:swap                      # write, then restore 3 times
#   mise run bench:swap -- write             # only the write phase; prints the bucket
#   ALAMBIQUE_BENCH_SWAP_BUCKET=<bucket> mise run bench:swap -- restore
#
# Both nodes are VMs pinned to one CPU (`+S 1:1 +SDcpu 1:1 +SDio 1`, `taskset`).
# The write phase runs 12 cells at 8 commits per second each for 90 s, with the
# compactor at its defaults, on SeaweedFS behind the delay proxy
# (`bench/delay_proxy.exs`, 25 ms per request). It ends with a graceful stop,
# which releases the leases (and, first, compacts the tails: it prints what each
# cell did). Each restore run is a new VM with a new data
# directory, which
# touches the 12 cells at once and prints per wake: the time, the level-0 and
# merged GETs of the wake (the `requests` of `[:alambique, :cell, :wake]`) and
# the files of the chain it walked. The compactor of the restoring node is off,
# so that every run restores the same chain, one file longer each time (the
# fence).
#
#   ALAMBIQUE_BENCH_LATENCY       added latency per request in ms        (default "25")
#   ALAMBIQUE_BENCH_SWAP_CELLS    busy cells                             (default "12")
#   ALAMBIQUE_BENCH_SWAP_RATE     commits per second of each cell        (default "8")
#   ALAMBIQUE_BENCH_SWAP_SECONDS  length of the write phase              (default "90")
#   ALAMBIQUE_BENCH_SWAP_RUNS     restore runs                           (default "3")
#   ALAMBIQUE_BENCH_SWAP_BUCKET   bucket to write to or restore from     (default: a new one)
#   ALAMBIQUE_BENCH_SWAP_READER_CPU  CPU of the restoring VM             (default "0")
#   ALAMBIQUE_BENCH_SWAP_WRITER_CPUS CPUs of the writing VM              (default "1")
#   ALAMBIQUE_BENCH_SWAP_OPEN     "1": a commit starts every 1/RATE s whatever the
#                                 earlier ones did, like clients' requests (default: one
#                                 commit at a time per cell)
#   ALAMBIQUE_BENCH_SWAP_WRITER_BURN  busy loops in the writing VM, to
#                                 saturate its CPU like an app that works      (default "0")
#   ALAMBIQUE_BENCH_SWAP_WRITER_BURN_DIRTY  loops of a dirty CPU NIF in it (default "0")
#   ALAMBIQUE_BENCH_SWAP_WRITER_OPTS  extra Alambique options of the writing
#                                 node, an Elixir keyword list, e.g.
#                                 "[compaction: [interval_ms: 3_600_000]]"
#   ALAMBIQUE_BENCH_PROXY_CPU     CPU of the proxy                       (default "5")
#   ALAMBIQUE_BENCH_CONCURRENCY   restore `fetch_concurrency`            (default "16")
#   ALAMBIQUE_BENCH_BUDGET        restore `max_inflight_bytes`           (default "2516582")
#   ALAMBIQUE_BENCH_POOL_SIZE     `pool_size` of the S3 store            (default: its own)

Code.require_file("seaweedfs_store.exs", __DIR__)

# The compactor of the writing node as seen from outside: per cell, the passes
# it ran and the longest tail its row held.
defmodule Watch do
  @moduledoc false

  def start(config) do
    me = self()
    cap = config.compaction.max_chain_segments
    table = :ets.new(:watch, [:public, :bag])
    id = {__MODULE__, make_ref()}

    :telemetry.attach(
      id,
      [:alambique, :compaction, :pass],
      fn _, %{duration: d}, %{cell: cell} = meta, _ ->
        ms = System.convert_time_unit(d, :native, :millisecond)
        :ets.insert(table, {:pass, cell, ms, meta[:more], System.monotonic_time(:millisecond)})
      end,
      nil
    )

    sampler = spawn_link(fn -> sample(config.compactor, table, me, cap) end)
    {id, table, sampler}
  end

  defp sample(compactor, table, parent, cap) do
    receive do
      :stop -> send(parent, :sampled)
    after
      100 ->
        for {cell, first, last, _} <- :ets.tab2list(compactor), first > 0 do
          :ets.insert(table, {:tail, cell, last - first + 1, System.monotonic_time(:millisecond)})
        end

        sample(compactor, table, parent, cap)
    end
  end

  # Samples that found a cell at the cap while fewer passes ran than the
  # compactor may run at once (concurrency + urgent_concurrency), which is
  # the compactor leaving work waiting.
  defp idle_while_over(all, cap) do
    intervals = for {:pass, _, ms, _, stop} <- all, do: {stop - ms, stop}
    samples = for {:tail, _, n, t} <- all, do: {t, n}

    over =
      samples
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.filter(fn {_, ns} -> Enum.any?(ns, &(&1 >= cap)) end)

    running = fn t -> Enum.count(intervals, fn {a, b} -> a <= t and t <= b end) end
    idle = Enum.count(over, fn {t, _} -> running.(t) < 6 end)

    IO.puts(
      "samples with a cell at the cap: #{length(over)}, with fewer than 6 passes running: #{idle}, " <>
        "mean passes running then: #{Float.round(Enum.sum(for {t, _} <- over, do: running.(t)) / max(length(over), 1), 2)}"
    )
  end

  def report({id, table, sampler}, cap) do
    send(sampler, :stop)
    receive do: (:sampled -> :ok)
    :telemetry.detach(id)
    IO.puts("cell  passes  pass ms (sum / max)  more  tail row max  wait for a pass ms (max)")
    all = :ets.tab2list(table)
    cells = for {:pass, cell, _, _, _} <- all, uniq: true, do: cell
    idle_while_over(all, cap)

    for cell <- Enum.sort(cells) do
      passes =
        for {:pass, ^cell, ms, more, stop} <- all, do: {stop - ms, ms, more}

      passes = Enum.sort(passes)
      samples = for {:tail, ^cell, n, t} <- all, do: {t, n}
      ms = Enum.map(passes, &elem(&1, 1))
      more = Enum.count(passes, &elem(&1, 2))

      # From the first sample at the cap to the start of the pass that took the row.
      waits =
        passes
        |> Enum.map(&elem(&1, 0))
        |> Enum.chunk_every(2, 1, [0])
        |> Enum.flat_map(fn [start, _] ->
          over = for {t, n} <- samples, n >= cap, t <= start, t > start - 30_000, do: t
          if over == [], do: [], else: [start - Enum.min(over)]
        end)

      IO.puts(
        "#{cell}  #{length(passes)}  #{Enum.sum(ms)} / #{Enum.max(ms)}  #{more}  " <>
          "#{samples |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> 0 end)}  " <>
          "#{Enum.max(waits, fn -> 0 end)}"
      )
    end
  end
end

defmodule Swap do
  @moduledoc false
  alias Alambique.Compaction.Plan
  alias Alambique.Replication.Objects

  @prefix "swap"
  # The VM of a node with one CPU.
  @small_node "+S 1:1 +SDcpu 1:1 +SDio 1"
  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}]

  def int(name, default), do: name |> System.get_env(default) |> String.to_integer()
  defp cells, do: for(i <- 1..int("ALAMBIQUE_BENCH_SWAP_CELLS", "12"), do: "room-#{i}")

  defp store(port) do
    config =
      BenchSeaweedfs.bucket(System.fetch_env!("ALAMBIQUE_BENCH_SWAP_BUCKET"))
      |> Map.put(:endpoint, "http://127.0.0.1:#{port}")

    config =
      case System.get_env("ALAMBIQUE_BENCH_POOL_SIZE") do
        nil -> config
        size -> Map.put(config, :pool_size, String.to_integer(size))
      end

    {Alambique.Store.S3, config}
  end

  # Extra Alambique options of a phase, an Elixir keyword list.
  defp opts(name) do
    case System.get_env("ALAMBIQUE_BENCH_SWAP_" <> name) do
      nil -> []
      code -> code |> Code.eval_string() |> elem(0)
    end
  end

  defp start(name, port, extra) do
    dir = Alambique.TestDirs.new("bench")

    opts =
      Keyword.merge(
        [
          name: name,
          store: store(port),
          prefix: @prefix,
          data_dir: dir,
          migrations: @schema,
          lease_ttl_ms: 30_000
        ],
        extra
      )

    {:ok, sup} = Supervisor.start_link([{Alambique, opts}], strategy: :one_for_one)
    {sup, dir}
  end

  # The phases, each in a VM of its own.

  def write(port) do
    seconds = int("ALAMBIQUE_BENCH_SWAP_SECONDS", "90")
    interval = div(1000, int("ALAMBIQUE_BENCH_SWAP_RATE", "8"))
    name = :bench_swap_writer
    {sup, dir} = start(name, port, opts("WRITER_OPTS"))
    row = String.duplicate("x", 100)

    burners =
      for _ <- 1..int("ALAMBIQUE_BENCH_SWAP_WRITER_BURN", "0")//1, do: spawn(&burn/0)

    burners =
      burners ++
        for _ <- 1..int("ALAMBIQUE_BENCH_SWAP_WRITER_BURN_DIRTY", "0")//1,
            do: spawn(&burn_dirty/0)

    watch = Watch.start(Alambique.Config.fetch!(name))
    t0 = System.monotonic_time(:millisecond)
    stop = t0 + seconds * 1000

    writers =
      for {cell, i} <- Enum.with_index(cells()) do
        Task.async(fn ->
          Process.sleep(div(interval * i, 12))
          commits(name, cell, row, stop, interval, 0)
        end)
      end

    counts = Task.await_many(writers, :infinity)
    IO.puts("write: #{Enum.sum(counts)} commits in #{seconds} s")
    for pid <- burners, do: Process.exit(pid, :kill)
    Watch.report(watch, Alambique.Config.fetch!(name).compaction.max_chain_segments)
    IO.puts("cell  level-0  merged  listed  walked")

    for cell <- cells() do
      {level0, merged, walked} = chain(name, cell)
      IO.puts("#{cell}  #{level0}  #{merged}  #{level0 + merged}  #{walked}")
    end

    id = {__MODULE__, make_ref()}
    :telemetry.attach(id, [:alambique, :cell, :hibernate], &__MODULE__.print_release/4, nil)
    {stop_us, :ok} = :timer.tc(fn -> Supervisor.stop(sup) end)
    :telemetry.detach(id)
    IO.puts("stop: #{div(stop_us, 1000)} ms for #{length(cells())} cells")
    File.rm_rf(dir)
  end

  # What the graceful stop did to each cell: its tail, and the compaction
  # before the release (`compaction: :deadline` means it was cut).
  def print_release(_event, m, meta, _config) do
    ms = &System.convert_time_unit(&1, :native, :millisecond)

    IO.puts(
      "release #{meta.cell} tail #{m.tail} compaction #{meta.compaction} " <>
        "#{ms.(m.compact_duration)} ms of #{ms.(m.duration)} ms"
    )
  end

  defp burn, do: burn()

  # A dirty CPU NIF in a loop: the pool a node shares with the work of its app.
  defp burn_dirty do
    :crypto.pbkdf2_hmac(:sha256, "p", "s", 100_000, 32)
    burn_dirty()
  end

  # Open loop: a commit starts every `interval` ms whatever the commits before
  # it have done, like requests that arrive from clients.
  defp commits(name, cell, row, stop, interval, n) do
    if System.get_env("ALAMBIQUE_BENCH_SWAP_OPEN") == "1",
      do: open(name, cell, row, stop, interval, n, [], System.monotonic_time(:millisecond)),
      else: closed(name, cell, row, stop, interval, n)
  end

  defp open(name, cell, row, stop, interval, n, tasks, due) do
    if due >= stop do
      for task <- tasks, do: {:ok, _} = Task.await(task, :infinity)
      n
    else
      task =
        Task.async(fn -> Alambique.execute(name, cell, "INSERT INTO t (v) VALUES (?)", [row]) end)

      wait = due + interval - System.monotonic_time(:millisecond)
      if wait > 0, do: Process.sleep(wait)
      open(name, cell, row, stop, interval, n + 1, [task | tasks], due + interval)
    end
  end

  defp closed(name, cell, row, stop, interval, n) do
    now = System.monotonic_time(:millisecond)

    if now >= stop do
      n
    else
      {:ok, _} = Alambique.execute(name, cell, "INSERT INTO t (v) VALUES (?)", [row])
      wait = interval - (System.monotonic_time(:millisecond) - now)
      if wait > 0, do: Process.sleep(wait)
      closed(name, cell, row, stop, interval, n + 1)
    end
  end

  # Level-0 files, merged files and the files a restore walks.
  defp chain(name, cell) do
    ctx = Alambique.CellHelpers.repl_ctx(name, cell)
    deadline = System.monotonic_time(:millisecond) + 60_000
    {:ok, {segments, snapshots}} = Objects.list_cell(ctx, cell, deadline)
    from = snapshots |> Enum.map(& &1.txid) |> Enum.max(fn -> 0 end)
    walked = segments |> Plan.index() |> Plan.chain(from) |> length()
    {Enum.count(segments, &(&1.level == 0)), Enum.count(segments, &(&1.level > 0)), walked}
  end

  # Level-0 files listed, and the level-0 and merged files of the chain a restore walks.
  defp chain_files(name, cell) do
    ctx = Alambique.CellHelpers.repl_ctx(name, cell)
    deadline = System.monotonic_time(:millisecond) + 60_000
    {:ok, {segments, snapshots}} = Objects.list_cell(ctx, cell, deadline)
    from = snapshots |> Enum.map(& &1.txid) |> Enum.max(fn -> 0 end)
    files = segments |> Plan.index() |> Plan.chain(from)

    {Enum.count(segments, &(&1.level == 0)), Enum.count(files, &(&1.level == 0)),
     Enum.count(files, &(&1.level > 0))}
  end

  def restore(port) do
    name = :bench_swap_reader
    id = {__MODULE__, make_ref()}

    :telemetry.attach_many(
      id,
      [[:alambique, :cell, :wake], [:alambique, :replication, :restore]],
      &__MODULE__.relay/4,
      self()
    )

    {sup, dir} =
      start(name, port,
        restore: [
          fetch_concurrency: int("ALAMBIQUE_BENCH_CONCURRENCY", "16"),
          max_inflight_bytes: int("ALAMBIQUE_BENCH_BUDGET", "2516582")
        ],
        compaction: [max_chain_segments: 1_000_000, max_age_ms: 3_600_000_000]
      )

    ids = cells()
    # Start from a quiet node: nothing else runs while the wakes do.
    Process.sleep(500)
    # What the chain of each cell holds before the wake reads it, for the audit below.
    expected = Map.new(ids, &{&1, chain_files(name, &1)})

    ids
    |> Enum.map(&Task.async(fn -> {:ok, _} = Alambique.touch(name, &1, timeout: 120_000) end))
    |> Task.await_many(:infinity)

    wakes = collect(length(ids), %{})
    :telemetry.detach(id)

    for {cell, wake} <- Enum.sort(wakes) do
      ms = System.convert_time_unit(wake.duration, :native, :millisecond)
      segment = Map.get(wake.requests, {:get, :segment}, 0)
      merged = Map.get(wake.requests, {:get, :merged}, 0)
      IO.puts("WAKE #{cell} #{ms} #{segment} #{merged} #{wake.walked}")
    end

    # Every file of the chain is read once: a wake makes as many level-0 and
    # merged GETs as its chain has files of each kind.
    IO.puts("AUDIT cell  listed level-0  chain level-0  level-0 GETs  chain merged  merged GETs")

    for {cell, wake} <- Enum.sort(wakes) do
      {listed, l0, merged} = expected[cell]
      got_l0 = Map.get(wake.requests, {:get, :segment}, 0)
      got_merged = Map.get(wake.requests, {:get, :merged}, 0)
      IO.puts("AUDIT #{cell}  #{listed}  #{l0}  #{got_l0}  #{merged}  #{got_merged}")
    end

    Supervisor.stop(sup)
    File.rm_rf(dir)
  end

  def relay(event, measurements, meta, test), do: send(test, {:event, event, measurements, meta})

  # A wake and the restore that came with it, by cell.
  defp collect(n, acc) do
    done = for {_, %{wake: _, walked: _}} <- acc, do: 1

    if length(done) == n do
      Map.new(acc, fn {cell, %{wake: wake, walked: walked}} ->
        {cell, Map.merge(wake, %{walked: walked})}
      end)
    else
      receive do
        {:event, [:alambique, :cell, :wake], %{duration: d}, %{cell: cell, requests: r}} ->
          collect(
            n,
            Map.update(
              acc,
              cell,
              %{wake: %{duration: d, requests: r}},
              &Map.put(&1, :wake, %{duration: d, requests: r})
            )
          )

        {:event, [:alambique, :replication, :restore], _, %{cell: cell, segments: s}} ->
          collect(n, Map.update(acc, cell, %{walked: s}, &Map.put(&1, :walked, s)))

        {:event, _, _, _} ->
          collect(n, acc)
      after
        120_000 -> raise "the wakes did not finish"
      end
    end
  end

  # The orchestrator: the proxy and the phases.

  def orchestrate(mode) do
    ms = int("ALAMBIQUE_BENCH_LATENCY", "25")
    bucket = System.get_env("ALAMBIQUE_BENCH_SWAP_BUCKET")
    bucket = bucket || "swap-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
    BenchSeaweedfs.bucket(bucket)
    port = BenchSeaweedfs.start_proxy(ms)
    IO.puts("bucket #{bucket}, latency #{ms} ms, proxy on port #{port}")

    if mode in ["all", "write"] do
      cpus = System.get_env("ALAMBIQUE_BENCH_SWAP_WRITER_CPUS", "1")
      IO.puts("\n== write phase ==")
      {0, _} = phase("write", bucket, port, cpus, [{"ELIXIR_ERL_OPTIONS", @small_node}])
    end

    if mode in ["all", "restore"] do
      cpu = System.get_env("ALAMBIQUE_BENCH_SWAP_READER_CPU", "0")

      runs =
        for run <- 1..int("ALAMBIQUE_BENCH_SWAP_RUNS", "3") do
          IO.puts("\n== restore run #{run} ==")
          {0, lines} = phase("restore", bucket, port, cpu, [{"ELIXIR_ERL_OPTIONS", @small_node}])
          wakes = for "WAKE " <> rest <- lines, do: wake(rest)
          summary(wakes)
        end

      IO.puts(
        "\nrun  max s  median s  segment GETs (median)  merged GETs (median)  walked (min-max)"
      )

      for {{max, median, segment, merged, lo, hi}, run} <- Enum.with_index(runs, 1) do
        IO.puts(
          :io_lib.format("~3w  ~5.2f  ~8.2f  ~21w  ~20w  ~w-~w", [
            run,
            max,
            median,
            segment,
            merged,
            lo,
            hi
          ])
        )
      end
    end

    IO.puts("\nbucket #{bucket} stays in the container (mise run seaweedfs:down removes it)")
  end

  defp wake(rest) do
    [_cell, ms, segment, merged, walked] = String.split(rest)
    Enum.map([ms, segment, merged, walked], &String.to_integer/1)
  end

  defp summary(wakes) do
    ms = for [ms, _, _, _] <- wakes, do: ms
    segment = for [_, s, _, _] <- wakes, do: s
    merged = for [_, _, m, _] <- wakes, do: m
    walked = for [_, _, _, w] <- wakes, do: w

    {Enum.max(ms) / 1000, median(ms) / 1000, median(segment), median(merged), Enum.min(walked),
     Enum.max(walked)}
  end

  defp median(list), do: list |> Enum.sort() |> Enum.at(div(length(list), 2))

  # Runs `mix run bench/swap.exs` as a phase on `cpus`, echoing its output;
  # returns its exit status and its lines.
  defp phase(name, bucket, port, cpus, env) do
    env =
      [
        {"ALAMBIQUE_BENCH_SWAP_PHASE", name},
        {"ALAMBIQUE_BENCH_SWAP_BUCKET", bucket},
        {"ALAMBIQUE_BENCH_SWAP_PORT", "#{port}"},
        {"MIX_ENV", "test"} | env
      ]
      |> Enum.map(fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)

    port =
      Port.open({:spawn_executable, System.find_executable("taskset")}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:line, 4096},
        {:env, env},
        args: ["-c", cpus, System.find_executable("mix"), "run", Path.join(__DIR__, "swap.exs")]
      ])

    drain(port, [])
  end

  defp drain(port, lines) do
    receive do
      {^port, {:data, {_, line}}} ->
        IO.puts(line)
        drain(port, [line | lines])

      {^port, {:exit_status, status}} ->
        {status, Enum.reverse(lines)}
    end
  end
end

case System.get_env("ALAMBIQUE_BENCH_SWAP_PHASE") do
  "write" -> Swap.write(Swap.int("ALAMBIQUE_BENCH_SWAP_PORT", "0"))
  "restore" -> Swap.restore(Swap.int("ALAMBIQUE_BENCH_SWAP_PORT", "0"))
  nil -> Swap.orchestrate(List.first(System.argv()) || "all")
end
