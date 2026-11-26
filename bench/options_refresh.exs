# What a live refresh of the options of an awake cell costs
# (`Alambique.refresh_options/3`, docs/cost-of-a-cell.md).
#
#   mise run bench:options-refresh               # 1000 cells for the node-wide pass
#   mise run bench:options-refresh -- 2000       # another count
#
# It prints, on the in-memory store:
#
#   * the latency of one refresh (p50 and p99) with no callback, with a fast
#     callback that changes the page cache each time (a pragma and no timer),
#     with one that changes the idle timeout (a pragma and the timers), and
#     with one that never returns, which ends at `cell_options_timeout_ms`
#   * the lease renewals of a cell under a storm of refreshes whose callback
#     hangs: the gap between renewals against the gap of an idle cell, so the
#     lag a refresh adds to `:renew`
#   * the time the busy marker is held by a refresh: the pragma and the two
#     writes of the marker, against the whole resolve, which an in-cell
#     resolve would hold it for
#   * a node-wide refresh over the awake cells: its time, and the latency of
#     client calls before and during it
#
#   ALAMBIQUE_BENCH_CALLS     refreshes per latency case          (default "300")
#   ALAMBIQUE_BENCH_TIMEOUT_MS cell_options_timeout_ms             (default "200")
#   ALAMBIQUE_BENCH_STORM     seconds of the refresh storm         (default "8")
#   ALAMBIQUE_BENCH_CLIENTS   concurrent clients of the node-wide pass (default "8")
#
# The run is bounded by `timeout` in scripts/bench-options-refresh.

defmodule Bench.Options do
  @moduledoc false
  @key {__MODULE__, :mode}

  def mode(mode) do
    :persistent_term.put({__MODULE__, :n}, :atomics.new(1, []))
    :persistent_term.put(@key, mode)
  end

  # `:alternate` flips the cache limit, `:timers` flips the idle timeout too,
  # `:hang` never returns.
  def options(_cell) do
    case :persistent_term.get(@key, :alternate) do
      :alternate ->
        [awake: [cache_kib: flip(128, 192)]]

      :timers ->
        [idle_timeout_ms: flip(600_000, 590_000), awake: [cache_kib: flip(128, 192)]]

      :hang ->
        Process.sleep(:infinity)
    end
  end

  # Every call gives the other value, so every refresh changes something.
  defp flip(a, b) do
    n = :atomics.add_get(:persistent_term.get({__MODULE__, :n}), 1, 1)
    if rem(n, 2) == 0, do: a, else: b
  end
end

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"}]

  def run(count, calls, timeout_ms, storm_s, clients) do
    Bench.Options.mode(:alternate)
    IO.puts("== refresh latency, #{calls} calls per case, cell_options_timeout_ms #{timeout_ms}")
    latency(calls, timeout_ms)
    IO.puts("== :renew under a storm of refreshes with a hung callback, #{storm_s} s")
    storm(timeout_ms, storm_s)
    IO.puts("== busy marker held per refresh")
    marker(calls)
    IO.puts("== node-wide refresh over #{count} awake cells")
    node_wide(count, clients)
  end

  defp latency(calls, timeout_ms) do
    with_instance(:bench_refresh_absent, [], fn inst ->
      wake(inst, "c")
      line("no callback", us(calls, fn -> Alambique.refresh_options(inst, "c") end))
    end)

    with_instance(
      :bench_refresh_fast,
      [cell_options: {Bench.Options, :options}],
      fn inst ->
        wake(inst, "c")
        Bench.Options.mode(:alternate)

        line(
          "fast callback, cache changes",
          us(calls, fn -> Alambique.refresh_options(inst, "c") end)
        )

        Bench.Options.mode(:timers)

        line(
          "fast callback, cache and timers",
          us(calls, fn -> Alambique.refresh_options(inst, "c") end)
        )

        Bench.Options.mode(:hang)

        line(
          "callback at the timeout",
          us(min(calls, 5), fn -> Alambique.refresh_options(inst, "c") end)
        )
      end,
      timeout_ms
    )
  end

  # Renewals of one cell: the gaps between them, hung callback under load and idle.
  defp storm(timeout_ms, seconds) do
    handler = {__MODULE__, make_ref()}
    :telemetry.attach(handler, [:alambique, :lease, :renew], &__MODULE__.relay/4, self())

    try do
      with_instance(
        :bench_refresh_storm,
        [cell_options: {Bench.Options, :options}],
        fn inst ->
          wake(inst, "c")
          flush()
          Process.sleep(seconds * 1000)
          line("renewal gaps, idle cell", gaps(renewals("c")))

          Bench.Options.mode(:hang)
          stop = System.monotonic_time(:millisecond) + seconds * 1000

          loops =
            for _ <- 1..8 do
              spawn_monitor(fn -> loop(inst, stop) end)
            end

          Process.sleep(seconds * 1000)
          line("renewal gaps, 8 refresh loops", gaps(renewals("c")))
          for {pid, mon} <- loops, do: receive(do: ({:DOWN, ^mon, :process, ^pid, _} -> :ok))
        end,
        timeout_ms,
        3_000
      )
    after
      :telemetry.detach(handler)
    end
  end

  defp loop(inst, stop) do
    if System.monotonic_time(:millisecond) < stop do
      Alambique.refresh_options(inst, "c")
      loop(inst, stop)
    end
  end

  def relay(_event, _m, meta, test),
    do: send(test, {:renew, meta.cell, System.monotonic_time(:millisecond)})

  defp flush, do: receive(do: ({:renew, _, _} -> flush()), after: (0 -> :ok))

  defp renewals(cell) do
    receive do
      {:renew, ^cell, at} -> [at | renewals(cell)]
      {:renew, _, _} -> renewals(cell)
    after
      0 -> []
    end
  end

  defp gaps(times) do
    gaps = times |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)

    case gaps do
      [] -> "no renewal"
      _ -> "n=#{length(gaps)} p50 #{pct(gaps, 0.5)} ms, max #{Enum.max(gaps)} ms"
    end
  end

  # The pragma and the two writes of the marker, against the resolve.
  defp marker(calls) do
    with_instance(:bench_refresh_marker, [cell_options: {Bench.Options, :options}], fn inst ->
      wake(inst, "c")
      config = Alambique.Config.fetch!(inst)
      pid = Alambique.Cell.whereis(config, "c")
      Bench.Options.mode(:alternate)

      busy =
        for n <- 1..calls do
          Alambique.CellHelpers.in_cell(pid, fn state ->
            kib = 128 + rem(n, 2) * 64

            {t, :ok} =
              :timer.tc(fn ->
                Alambique.Inflight.busy(state.inflight, fn ->
                  Alambique.Sqlite.execute(state.conn, "PRAGMA cache_size = -#{kib}", write: true)
                end)
              end)

            t
          end)
        end

      line(
        "marker busy: pragma + 2 writes",
        "p50 #{pct(busy, 0.5)} us, p99 #{pct(busy, 0.99)} us"
      )

      resolve = us(calls, fn -> Alambique.refresh_options(inst, "c") end)
      line("whole refresh (an in-cell resolve would hold the marker this long)", resolve)
    end)
  end

  defp node_wide(count, clients) do
    with_instance(:bench_refresh_all, [cell_options: {Bench.Options, :options}], fn inst ->
      ids = Enum.map(1..count, &"cell-#{&1}")

      ids
      |> Task.async_stream(&wake(inst, &1), max_concurrency: 64, timeout: :infinity)
      |> Stream.run()

      Bench.Options.mode(:alternate)

      {base, _} = client_calls(inst, ids, clients, fn -> Process.sleep(1_000) end)
      line("client calls, no refresh", "n=#{length(base)} " <> latency_text(base))

      {during, {total, summary}} =
        client_calls(inst, ids, clients, fn ->
          {t, {:ok, summary}} = :timer.tc(fn -> Alambique.refresh_options(inst, :all) end)
          {t, summary}
        end)

      line("client calls, during the refresh", "n=#{length(during)} " <> latency_text(during))

      line(
        "refresh of all",
        "#{div(total, 1000)} ms for #{summary.cells} cells, #{summary.failed} failed"
      )
    end)
  end

  # `clients` loops of queries on random cells while `fun` runs; returns their
  # latencies and the result of `fun`.
  defp client_calls(inst, ids, clients, fun) do
    me = self()
    ids = List.to_tuple(ids)

    pids =
      for _ <- 1..clients do
        spawn_link(fn -> client(inst, ids, me, []) end)
      end

    result = fun.()
    for pid <- pids, do: send(pid, :stop)

    latencies =
      for pid <- pids, reduce: [] do
        acc ->
          receive do
            {^pid, us} -> us ++ acc
          end
      end

    {latencies, result}
  end

  defp client(inst, ids, owner, acc) do
    receive do
      :stop -> send(owner, {self(), acc})
    after
      0 ->
        id = elem(ids, :rand.uniform(tuple_size(ids)) - 1)
        {t, _} = :timer.tc(fn -> Alambique.query(inst, id, "SELECT 1") end)
        client(inst, ids, owner, [t | acc])
    end
  end

  defp us(calls, fun) do
    times =
      for _ <- 1..calls do
        {t, _} = :timer.tc(fun)
        t
      end

    latency_text(times)
  end

  defp latency_text(us), do: "p50 #{pct(us, 0.5)} us, p99 #{pct(us, 0.99)} us"

  defp pct(values, p) do
    sorted = Enum.sort(values)
    Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))
  end

  defp wake(inst, id), do: {:ok, _} = Alambique.query(inst, id, "SELECT 1")

  defp with_instance(name, extra, fun, timeout_ms \\ 200, ttl_ms \\ 60_000) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()

    extra =
      if extra[:cell_options], do: [cell_options_timeout_ms: timeout_ms] ++ extra, else: extra

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           [
             name: name,
             store: {MemoryStore, MemoryStore.config(store)},
             data_dir: dir,
             migrations: @schema,
             feed: false,
             compaction: false,
             idle_timeout_ms: 600_000,
             lease_ttl_ms: ttl_ms,
             skew_margin_ms: 400,
             awake: [cache_kib: 128, idle_cache_after_ms: 300_000, idle_cache_kib: 8]
           ] ++ extra}
        ],
        strategy: :one_for_one
      )

    try do
      fun.(name)
    after
      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  defp line(label, text), do: IO.puts("  #{String.pad_trailing(label, 36)} #{text}")
end

count =
  case System.argv() do
    [] -> 1_000
    [n | _] -> String.to_integer(n)
  end

env = fn name, default -> String.to_integer(System.get_env(name, default)) end
Alambique.TestDirs.sweep()

try do
  Bench.run(
    count,
    env.("ALAMBIQUE_BENCH_CALLS", "300"),
    env.("ALAMBIQUE_BENCH_TIMEOUT_MS", "200"),
    env.("ALAMBIQUE_BENCH_STORM", "8"),
    env.("ALAMBIQUE_BENCH_CLIENTS", "8")
  )
after
  Alambique.TestDirs.clean()
end
