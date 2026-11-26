# What the page arena and per-connection page caches cost and give back
# (docs/cost-of-a-cell.md). One case per run; scripts/bench-page-arena runs the
# cases for each mode, which sets the arena in the VM before it starts.
#
#   mise run bench:page-arena                      # every case, every mode
#   mise run bench:page-arena -- release hot       # some cases
#   ALAMBIQUE_BENCH_MODES="percell e64" mise run bench:page-arena -- miss
#
# Cases (the bench prints one `key=value` line per measurement):
#
#   release    Share of the memory a shrink frees that leaves the process, for
#              connections filled in turns (interleaved) or one at a time
#              (sequential), when 50, 75 and 90% of them shrink to
#              idle_cache_kib 64 and 0. Memory is the cgroup's memory.current,
#              else the RSS of the VM.
#   hot        Latency of a hot connection's point reads (p50, p99, max) while
#              other connections shrink and refill, against the same without.
#   miss       Page cache misses and scan time of a database above and below
#              cache_kib, in a fresh connection (cold) and again (warm).
#   regrow     Cycles of shrink and restore: the first scan after a restore and
#              the memory after the first and the last cycle (a leak grows).
#   misscost   Nanoseconds per page miss as the cache, and so the extents a
#              connection owns, grows.
#   lookaside  Statement time and the memory of a connection for lookaside
#              {0, 0}, SQLite's default and {512, 16}.
#   capacity   Awake cells a node holds before its memory reaches 95% of
#              ALAMBIQUE_BENCH_LIMIT_MIB, with no idle handling, the idle shrink and the idle close. The
#              limit is a threshold on the measured memory, not a kill: run
#              the script in a cgroup of that size to see the real one.
#
#   ALAMBIQUE_BENCH_CELLS      connections of release and hot     (default "250")
#   ALAMBIQUE_BENCH_KIB        page cache filled per connection   (default "1024")
#   ALAMBIQUE_BENCH_LIMIT_MIB  memory limit of capacity           (default "1024")
#   ALAMBIQUE_BENCH_SECONDS    seconds of each hot window         (default "5")

defmodule Bench do
  alias Alambique.{NativeStats, Sqlite}
  alias AlambiqueTestkit.MemoryStore

  @blob 3000

  def env_int(name, default), do: String.to_integer(System.get_env(name, "#{default}"))

  # Measurement

  defp stats, do: NativeStats.read()

  # The cgroup of the VM, else its RSS, in bytes.
  def memory do
    with {:ok, text} <- File.read("/proc/self/cgroup"),
         [_, path] <- Regex.run(~r/^0::(.*)$/m, text),
         {:ok, current} <- File.read("/sys/fs/cgroup#{path}/memory.current") do
      current |> String.trim() |> String.to_integer()
    else
      _ ->
        {:ok, text} = File.read("/proc/self/status")
        [_, kib] = Regex.run(~r/VmRSS:\s+(\d+) kB/, text)
        String.to_integer(kib) * 1024
    end
  end

  # Waits until the arena stops releasing (the trim thread has nothing more to
  # do), or a second without an arena.
  def settle do
    key = fn ->
      s = stats()
      {s[:page_arena_trims], s[:page_arena_released_bytes]}
    end

    wait_stable(key, key.(), 0)
  end

  defp wait_stable(key, last, waited) do
    Process.sleep(300)
    now = key.()

    cond do
      now == last and waited >= 600 -> :ok
      waited >= 15_000 -> :ok
      true -> wait_stable(key, now, if(now == last, do: waited + 300, else: 0))
    end
  end

  def pct(sorted, p), do: Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))

  def mib(bytes), do: Float.round(bytes / 1_048_576, 1)

  def line(case_name, fields) do
    IO.puts("#{case_name} " <> Enum.map_join(fields, " ", fn {k, v} -> "#{k}=#{v}" end))
  end

  # Connections

  # Opens a database of about `kib` KiB of 3000-byte rows, one per page, with
  # a page cache that holds it all.
  def open_db(dir, name, kib, opts \\ []) do
    {:ok, conn} = Sqlite.open(Path.join(dir, name), opts)
    :ok = Sqlite.execute(conn, "PRAGMA cache_size = -#{kib * 2}")
    :ok = Sqlite.execute(conn, "CREATE TABLE IF NOT EXISTS m (id INTEGER PRIMARY KEY, b BLOB)")
    conn
  end

  def rows(kib), do: div(kib * 1024, @blob + 1300)

  # Inserts rows `from..to` and reads everything back, so the pages are cached.
  def grow(conn, from, to) do
    :ok =
      Sqlite.execute(
        conn,
        "WITH RECURSIVE s(i) AS (SELECT #{from} UNION ALL SELECT i + 1 FROM s WHERE i < #{to}) " <>
          "INSERT INTO m SELECT i, zeroblob(#{@blob}) FROM s"
      )

    scan(conn)
  end

  def scan(conn), do: {:ok, _} = Sqlite.run(conn, "SELECT sum(length(b)) FROM m", [])

  # Fills `conns` to `kib` KiB each, in turns of ten rows (interleaved) or
  # one at a time (sequential).
  def fill(conns, kib, :sequential), do: for(c <- conns, do: grow(c, 1, rows(kib)))

  def fill(conns, kib, :interleaved) do
    total = rows(kib)

    for from <- 1..total//10, c <- conns do
      grow(c, from, min(from + 9, total))
    end
  end

  def shrink(conn, kib), do: :ok = Sqlite.execute(conn, "PRAGMA cache_size = -#{max(kib, 0)}")

  def close_all(conns), do: Enum.each(conns, &Sqlite.close/1)

  def tmp, do: Alambique.TestDirs.new("bench-arena")

  # Cases

  def release do
    n = env_int("ALAMBIQUE_BENCH_CELLS", 250)
    kib = env_int("ALAMBIQUE_BENCH_KIB", 1024)
    dir = tmp()

    for layout <- [:interleaved, :sequential], idle_kib <- [64, 0], share <- [50, 75, 90] do
      conns = for i <- 1..n, do: open_db(dir, "r#{i}.db", kib)
      fill(conns, kib, layout)
      settle()
      {mem_a, used_a} = {memory(), stats().sqlite_memory_used}
      shrinking = Enum.take(Enum.shuffle(conns), div(n * share, 100))
      Enum.each(shrinking, &shrink(&1, idle_kib))
      settle()
      {mem_b, used_b} = {memory(), stats().sqlite_memory_used}
      freed = used_a - used_b
      left = mem_a - mem_b

      line("release",
        layout: layout,
        idle_pct: share,
        idle_kib: idle_kib,
        freed_mib: mib(freed),
        left_mib: mib(left),
        share: if(freed > 0, do: Float.round(left / freed, 2), else: "n/a"),
        unreleasable_mib: mib(stats()[:page_arena_unreleasable_bytes] || 0)
      )

      close_all(conns)
      settle()
    end
  end

  def hot do
    n = env_int("ALAMBIQUE_BENCH_CELLS", 250)
    kib = env_int("ALAMBIQUE_BENCH_KIB", 1024)
    seconds = env_int("ALAMBIQUE_BENCH_SECONDS", 5)
    dir = tmp()
    conns = for i <- 1..n, do: open_db(dir, "h#{i}.db", kib)
    fill(conns, kib, :interleaved)
    hot = hd(conns)
    others = tl(conns)
    rows = rows(kib)

    for churn <- [false, true] do
      before = stats()
      stop = System.monotonic_time(:millisecond) + seconds * 1000
      churner = if churn, do: Task.async(fn -> churn(others, kib, stop) end)

      latencies =
        Stream.repeatedly(fn ->
          {us, {:ok, _}} =
            :timer.tc(fn ->
              Sqlite.run(hot, "SELECT length(b) FROM m WHERE id = ?", [:rand.uniform(rows)])
            end)

          us
        end)
        |> Enum.take_while(fn _ -> System.monotonic_time(:millisecond) < stop end)
        |> Enum.sort()

      if churner, do: Task.await(churner, :infinity)
      after_stats = stats()

      line("hot",
        trims: churn,
        reads: length(latencies),
        p50_us: pct(latencies, 0.5),
        p99_us: pct(latencies, 0.99),
        max_us: List.last(latencies),
        trims_run: (after_stats[:page_arena_trims] || 0) - (before[:page_arena_trims] || 0),
        trim_ns_max: after_stats[:page_arena_trim_ns_max] || 0
      )
    end

    close_all(conns)
  end

  # Shrinks and refills the connections in turn until `stop`.
  defp churn(conns, kib, stop) do
    if System.monotonic_time(:millisecond) < stop do
      for c <- conns, System.monotonic_time(:millisecond) < stop do
        shrink(c, 0)
        shrink(c, kib * 2)
        scan(c)
      end

      churn(conns, kib, stop)
    end
  end

  def miss do
    dir = tmp()
    size_kib = 16_384
    {:ok, seed} = Sqlite.open(Path.join(dir, "big.db"))
    :ok = Sqlite.execute(seed, "CREATE TABLE m (id INTEGER PRIMARY KEY, b BLOB)")
    grow(seed, 1, rows(size_kib))
    :ok = Sqlite.close(seed)

    for cache_kib <- [4_096, 32_768] do
      {:ok, conn} = Sqlite.open(Path.join(dir, "big.db"))
      :ok = Sqlite.execute(conn, "PRAGMA cache_size = -#{cache_kib}")

      for pass <- [:cold, :warm, :warm] do
        before = stats()
        {us, _} = :timer.tc(fn -> scan(conn) end)
        after_stats = stats()

        line("miss",
          cache_kib: cache_kib,
          file_kib: size_kib,
          pass: pass,
          misses: after_stats.page_cache_misses - before.page_cache_misses,
          hits: after_stats.page_cache_hits - before.page_cache_hits,
          ms: Float.round(us / 1000, 1)
        )
      end

      :ok = Sqlite.close(conn)
    end
  end

  def regrow do
    kib = env_int("ALAMBIQUE_BENCH_KIB", 1024)
    cycles = 20
    dir = tmp()

    for idle_kib <- [64, 0] do
      conn = open_db(dir, "g#{idle_kib}.db", kib)
      ballast = for i <- 1..50, do: open_db(dir, "b#{idle_kib}-#{i}.db", kib)
      fill([conn | ballast], kib, :interleaved)

      mems =
        for cycle <- 1..cycles do
          shrink(conn, idle_kib)
          settle()
          idle = memory()
          shrink(conn, kib * 2)
          before = stats()
          {us, _} = :timer.tc(fn -> scan(conn) end)
          after_stats = stats()
          settle()

          if cycle in [1, cycles] do
            line("regrow",
              idle_kib: idle_kib,
              cycle: cycle,
              scan_ms: Float.round(us / 1000, 2),
              misses: after_stats.page_cache_misses - before.page_cache_misses,
              idle_mem_mib: mib(idle),
              full_mem_mib: mib(memory())
            )
          end

          idle
        end

      line("regrow", idle_kib: idle_kib, leak_mib_over_cycles: mib(List.last(mems) - hd(mems)))
      close_all([conn | ballast])
      settle()
    end
  end

  def misscost do
    dir = tmp()

    for cache_mib <- [1, 4, 16, 64] do
      kib = cache_mib * 1024
      conn = open_db(dir, "c#{cache_mib}.db", kib)
      # The database is twice the cache: every scan misses what it loaded
      # before, so each miss allocates (or reuses) a page.
      grow(conn, 1, rows(kib * 2))
      :ok = Sqlite.execute(conn, "PRAGMA cache_size = -#{kib}")
      scan(conn)
      before = stats()
      {us, _} = :timer.tc(fn -> for _ <- 1..3, do: scan(conn) end)
      after_stats = stats()
      misses = after_stats.page_cache_misses - before.page_cache_misses

      line("misscost",
        cache_mib: cache_mib,
        misses: misses,
        ns_per_miss: if(misses > 0, do: div(us * 1000, misses), else: "n/a"),
        arena_mapped_mib: mib(after_stats[:page_arena_mapped_bytes] || 0)
      )

      Sqlite.close(conn)
    end
  end

  def lookaside do
    dir = tmp()
    calls = 20_000

    for {name, setting} <- [none: {0, 0}, default: nil, small: {512, 16}] do
      before = stats().sqlite_memory_used
      conns = for i <- 1..200, do: open_db(dir, "l#{name}#{i}.db", 64, lookaside: setting)
      per_connection = div(stats().sqlite_memory_used - before, 200)
      close_all(conns)

      conn = open_db(dir, "q#{name}.db", 1024, lookaside: setting)
      grow(conn, 1, 2000)

      times =
        for _ <- 1..calls do
          {us, {:ok, _}} =
            :timer.tc(fn ->
              Sqlite.run(
                conn,
                "SELECT m.id, length(m.b) FROM m JOIN m AS x ON x.id = m.id " <>
                  "WHERE m.id BETWEEN ? AND ? ORDER BY 2 LIMIT 20",
                [:rand.uniform(1900), 1900 + :rand.uniform(100)]
              )
            end)

          us
        end

      sorted = Enum.sort(times)

      line("lookaside",
        setting: name,
        p50_us: pct(sorted, 0.5),
        p99_us: pct(sorted, 0.99),
        calls_per_s: div(calls * 1_000_000, Enum.sum(times)),
        bytes_per_connection: per_connection
      )

      Sqlite.close(conn)
    end
  end

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]

  def capacity do
    limit = env_int("ALAMBIQUE_BENCH_LIMIT_MIB", 1024) * 1_048_576

    for idle <- [:none, :shrink, :close] do
      dir = tmp()
      {:ok, store} = MemoryStore.start_link()
      name = :"bench_capacity_#{idle}"

      awake =
        [cache_kib: 512, max_cells: 100_000] ++
          case idle do
            :none -> []
            :shrink -> [idle_cache_after_ms: 500, idle_cache_kib: 64]
            :close -> [idle_close_after_ms: 500]
          end

      {:ok, sup} =
        Supervisor.start_link(
          [
            {Alambique,
             [
               name: name,
               store: {MemoryStore, MemoryStore.config(store)},
               data_dir: dir,
               migrations: @schema,
               idle_timeout_ms: 3_600_000,
               lease_ttl_ms: 600_000,
               awake: awake
             ]}
          ],
          strategy: :one_for_one
        )

      base = memory()
      cells = wake_until(name, idle, 0, base + trunc(limit * 0.95))

      line("capacity",
        idle: idle,
        cells: cells,
        limit_mib: div(limit, 1_048_576),
        mem_mib: mib(memory() - base)
      )

      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
      settle()
    end
  end

  # Waits until each cell of `ids` has done what `idle` asks of an idle cell
  # (its connection closed, its cache shrunk), so the memory is measured after
  # it; a cell that never does fails the case.
  defp wait_idle(_name, :none, _ids), do: :ok

  defp wait_idle(name, idle, ids) do
    config = Alambique.Config.fetch!(name)
    deadline = System.monotonic_time(:millisecond) + 60_000

    for id <- ids do
      pid = Alambique.Cell.whereis(config, id)
      wait_until(deadline, "#{id} did not go #{idle}", fn -> idle?(:sys.get_state(pid), idle) end)
    end
  end

  defp idle?(state, :close), do: state.kept != nil
  defp idle?(state, :shrink), do: state.idle_cache == :shrunk

  defp wait_until(deadline, failure, fun) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> raise failure
      true -> Process.sleep(50) && wait_until(deadline, failure, fun)
    end
  end

  # Wakes cells in batches of 50, each with a filled cache, until the memory
  # passes `ceiling`, after the idle shrink or close had its turn.
  defp wake_until(name, idle, woken, ceiling) do
    batch = for i <- (woken + 1)..(woken + 50), do: "cell-#{i}"

    batch
    |> Task.async_stream(
      fn id ->
        {:ok, _} =
          Alambique.execute(
            name,
            id,
            "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 160) " <>
              "INSERT INTO t SELECT i, zeroblob(3000) FROM s"
          )

        {:ok, _} = Alambique.query(name, id, "SELECT sum(length(v)) FROM t")
      end,
      max_concurrency: 16,
      timeout: :infinity
    )
    |> Stream.run()

    wait_idle(name, idle, batch)

    if memory() >= ceiling or woken >= 20_000,
      do: woken + 50,
      else: wake_until(name, idle, woken + 50, ceiling)
  end
end

case System.argv() do
  [name] ->
    apply(Bench, String.to_atom(name), [])

  _ ->
    IO.puts(
      :stderr,
      "usage: mix run bench/page_arena.exs release|hot|miss|regrow|misscost|lookaside|capacity"
    )
end
