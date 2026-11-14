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
#   ALAMBIQUE_BENCH_SWAP_WRITER_OPTS  extra Alambique options of the writing
#                                 node, an Elixir keyword list, e.g.
#                                 "[compaction: [interval_ms: 3_600_000]]"
#   ALAMBIQUE_BENCH_PROXY_CPU     CPU of the proxy                       (default "5")
#   ALAMBIQUE_BENCH_CONCURRENCY   restore `fetch_concurrency`            (default "16")
#   ALAMBIQUE_BENCH_BUDGET        restore `max_inflight_bytes`           (default "2516582")
#   ALAMBIQUE_BENCH_POOL_SIZE     `pool_size` of the S3 store            (default: its own)

Code.require_file("seaweedfs_store.exs", __DIR__)

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

  defp commits(name, cell, row, stop, interval, n) do
    now = System.monotonic_time(:millisecond)

    if now >= stop do
      n
    else
      {:ok, _} = Alambique.execute(name, cell, "INSERT INTO t (v) VALUES (?)", [row])
      wait = interval - (System.monotonic_time(:millisecond) - now)
      if wait > 0, do: Process.sleep(wait)
      commits(name, cell, row, stop, interval, n + 1)
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
