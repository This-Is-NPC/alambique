# What the facts of a cell cost (design 7.3).
#
#   mise run bench:facts                # pages of 1000 and 10000 cells
#   mise run bench:facts -- 1000        # other cell counts for the page case
#   ALAMBIQUE_BENCH_N=500 ALAMBIQUE_BENCH_LOOP=10000 mise run bench:facts -- 100
#
# Every case prints one line. Cases with an instance use the in-memory store.
#
#   write / call   one cell, N sequential single-row INSERT transactions (write)
#                  or `SELECT 1` (call), after a warm-up, with `track_bytes`
#                  true and false, without a feed and with `hand_over: :sender`.
#                  `memory_calls_per_write` is the delta of `sqlite_memory_calls`
#                  of the native stats over the N operations.
#   unit           ns per operation over LOOP iterations, in this process:
#                  `Sqlite.memory/1` of an idle connection,
#                  `Process.info(self(), :memory)`, `Facts.publish/2` without
#                  measuring (on the state of an idle cell, which it rewrites
#                  with the values it already holds), `Facts.read/2`, and the
#                  empty loop that the others include.
#   cells          `cells(details: true)` and `cells(details: false)` pages of
#                  1000 and 10000 entries, for each cell count (arguments).
#                  Every cell is awake: raise `ulimit -n` for 10000 cells.
#   busy           `Sqlite.memory/1` called BUSY times while another process
#                  runs a long statement on the connection: its max and p99 and
#                  the number of `{:error, :busy}`.
#
#   ALAMBIQUE_BENCH_N          writes and calls per case     (default "2000")
#   ALAMBIQUE_BENCH_LOOP       iterations of a unit case     (default "100000")
#   ALAMBIQUE_BENCH_CACHE_KIB  `awake: [cache_kib:]` of the cells case (default "64")
#   ALAMBIQUE_BENCH_PAGES      repetitions of a page read    (default "5")
#   ALAMBIQUE_BENCH_BUSY       calls of the busy case        (default "1000")

defmodule Bench do
  alias Alambique.Cell.Facts
  alias Alambique.{NativeStats, Sqlite}
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}]
  @insert "INSERT INTO t (v) VALUES ('x')"
  @warmup 200

  def env(var, default), do: String.to_integer(System.get_env(var, default))

  # Starts an instance over a fresh store and data directory, runs `fun` with
  # its name and stops everything, also when `fun` fails.
  def with_instance(opts, fun) do
    dir = Alambique.TestDirs.new("bench")
    name = :"bench_facts_#{System.unique_integer([:positive])}"
    {:ok, store} = MemoryStore.start_link()

    try do
      {:ok, sup} =
        Supervisor.start_link(
          [
            {Alambique,
             [
               name: name,
               store: {MemoryStore, MemoryStore.config(store)},
               data_dir: dir,
               migrations: @schema,
               idle_timeout_ms: 600_000,
               lease_ttl_ms: 60_000
             ] ++ opts}
          ],
          strategy: :one_for_one
        )

      try do
        fun.(name)
      after
        Supervisor.stop(sup)
      end
    after
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  # Writes and calls

  def operations(n) do
    for kind <- [:write, :call], feed <- [:none, :sender], track <- [true, false] do
      awake = [track_bytes: track]
      opts = [awake: awake] ++ if(feed == :sender, do: [feed: [hand_over: :sender]], else: [])

      with_instance(opts, fn inst ->
        run =
          case kind do
            :write -> fn -> {:ok, _} = retry(fn -> Alambique.execute(inst, "c", @insert) end) end
            :call -> fn -> {:ok, _} = retry(fn -> Alambique.query(inst, "c", "SELECT 1") end) end
          end

        {:ok, _} = Alambique.execute(inst, "c", @insert)
        pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), "c")
        settle(pid)
        for _ <- 1..@warmup, do: run.()
        settle(pid)

        before = NativeStats.read().sqlite_memory_calls
        us = for _ <- 1..n, do: elem(:timer.tc(run), 0)
        settle(pid)
        calls = NativeStats.read().sqlite_memory_calls - before

        IO.puts(
          "#{kind} track_bytes=#{track} feed=#{feed} n=#{n} mean_us=#{mean(us)} " <>
            "p50_us=#{pct(us, 0.5)} p99_us=#{pct(us, 0.99)} " <>
            "memory_calls_per_#{kind}=#{Float.round(calls / n, 3)}"
        )
      end)
    end

    :ok
  end

  # A message sent to the cell after the work is handled after every earlier one.
  defp settle(pid), do: :sys.get_state(pid)

  defp retry(fun) do
    case fun.() do
      {:error, :overloaded} -> retry(fun)
      other -> other
    end
  end

  # Unit costs

  def units(loop, dir) do
    {:ok, conn} = Sqlite.open(Path.join(dir, "unit.db"))
    :ok = Sqlite.execute(conn, "CREATE TABLE t (a)")

    try do
      with_instance([], fn inst ->
        {:ok, _} = Alambique.execute(inst, "c", @insert)
        config = Alambique.Config.fetch!(inst)
        {pid, load} = Alambique.Cell.find(config, "c")
        # The idle cell has nothing in flight: publishing its own state again
        # writes the values the slots hold.
        state = :sys.get_state(pid)

        cases = [
          {"empty_loop", fn -> :ok end},
          {"sqlite_memory", fn -> Sqlite.memory(conn) end},
          {"process_info_memory", fn -> Process.info(self(), :memory) end},
          {"facts_publish_no_measure", fn -> Facts.publish(state, false) end},
          {"facts_read", fn -> Facts.read(load, pid) end}
        ]

        [{_, empty} | rest] = for {label, fun} <- cases, do: {label, time_ns(loop, fun)}
        IO.puts("unit empty_loop loop=#{loop} ns_per_op=#{empty}")

        for {label, ns} <- rest,
            do: IO.puts("unit #{label} loop=#{loop} ns_per_op=#{ns} net_of_loop_ns=#{ns - empty}")
      end)
    after
      Sqlite.close(conn)
    end
  end

  defp time_ns(loop, fun) do
    {us, _} = :timer.tc(fn -> Enum.each(1..loop, fn _ -> fun.() end) end)
    div(us * 1000, loop)
  end

  # Pages of cells

  def pages(count, cache_kib, reps) do
    opts = [awake: [max_cells: count + 100, cache_kib: cache_kib]]

    with_instance(opts, fn inst ->
      ids = Enum.map(1..count, &"cell-#{&1}")

      ids
      |> Task.async_stream(fn id -> {:ok, _} = retry(fn -> Alambique.touch(inst, id) end) end,
        max_concurrency: 64,
        ordered: false,
        timeout: :infinity
      )
      |> Stream.run()

      # Waking cells have no facts yet: wait until all serve.
      deadline = System.monotonic_time(:millisecond) + 600_000

      until(deadline, fn ->
        %{cells: cells} = Alambique.cells(inst, details: true, limit: count)
        length(cells) == count and Enum.all?(cells, & &1.last_used)
      end)

      for limit <- [1_000, 10_000], limit <= count, details <- [true, false] do
        Alambique.cells(inst, details: details, limit: limit)

        us =
          for _ <- 1..reps,
              do: elem(:timer.tc(fn -> Alambique.cells(inst, details: details, limit: limit) end), 0)

        mean = Enum.sum(us) / reps

        IO.puts(
          "cells count=#{count} limit=#{limit} details=#{details} reps=#{reps} " <>
            "ms_per_page=#{Float.round(mean / 1000, 2)} us_per_entry=#{Float.round(mean / limit, 3)}"
        )
      end
    end)
  end

  defp until(deadline, fun) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> raise "cells did not all wake in time"
      true -> Process.sleep(100) && until(deadline, fun)
    end
  end

  # A connection with a long statement

  def busy(calls, dir) do
    {:ok, conn} = Sqlite.open(Path.join(dir, "busy.db"))

    runner =
      Task.async(fn ->
        receive do
          :go ->
            Sqlite.run(
              conn,
              "WITH RECURSIVE c(n) AS " <>
                "(SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 2000000000) " <>
                "SELECT count(*) FROM c",
              []
            )
        end
      end)

    try do
      :ok = Sqlite.assign(conn, runner.pid)
      send(runner.pid, :go)
      deadline = System.monotonic_time(:millisecond) + 30_000
      until(deadline, fn -> Sqlite.memory(conn) == {:error, :busy} end)

      results = for _ <- 1..calls, do: :timer.tc(fn -> Sqlite.memory(conn) end)
      us = Enum.map(results, &elem(&1, 0))
      busy = Enum.count(results, &(elem(&1, 1) == {:error, :busy}))

      IO.puts(
        "busy calls=#{calls} busy=#{busy} max_us=#{Enum.max(us)} p99_us=#{pct(us, 0.99)} " <>
          "p50_us=#{pct(us, 0.5)}"
      )
    after
      Sqlite.interrupt(conn)
      Task.shutdown(runner, :brutal_kill)
      Sqlite.close(conn)
    end
  end

  defp mean(values), do: Float.round(Enum.sum(values) / length(values), 1)

  defp pct(values, p) do
    sorted = Enum.sort(values)
    Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))
  end
end

counts =
  case System.argv() do
    [] -> [1_000, 10_000]
    args -> Enum.map(args, &String.to_integer/1)
  end

n = Bench.env("ALAMBIQUE_BENCH_N", "2000")
loop = Bench.env("ALAMBIQUE_BENCH_LOOP", "100000")
cache_kib = Bench.env("ALAMBIQUE_BENCH_CACHE_KIB", "64")
reps = Bench.env("ALAMBIQUE_BENCH_PAGES", "5")
busy = Bench.env("ALAMBIQUE_BENCH_BUSY", "1000")

Alambique.TestDirs.sweep()
dir = Alambique.TestDirs.new("bench-units")

try do
  Bench.operations(n)
  Bench.units(loop, dir)
  for count <- counts, do: Bench.pages(count, cache_kib, reps)
  Bench.busy(busy, dir)
after
  Alambique.TestDirs.clean()
end
