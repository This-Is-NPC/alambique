# Memory of awake cells by purpose, and what a smaller page cache costs
# (design section 12).
#
#   mise run bench:memory               # 1, 100 and 500 cells
#   mise run bench:memory -- 50 200     # other counts
#   ALAMBIQUE_BENCH_CACHE_KIB=512 mise run bench:memory
#
# Each count is a fresh instance with the in-memory store. Every cell gets
# the same small schema (three tables, three indexes) and `ROWS` notes, then
# a mixed workload of reads (a point read, an aggregate and a join), a full
# table scan every tenth call and writes, so its page cache holds what a
# working cell holds. With the cells still awake it prints the memory per
# cell by purpose (the average of `sqlite3_db_status` over the connections),
# the growth of the global counters of SQLite and of the BEAM over the empty
# instance, and the p50 and p99 of the reads, scans and writes of the
# workload (one call at a time). A call refused with `:overloaded` is retried.
#
#   ALAMBIQUE_BENCH_ROWS       notes per cell              (default "2000")
#   ALAMBIQUE_BENCH_OPS        calls per cell              (default "40")
#   ALAMBIQUE_BENCH_CACHE_KIB  `awake: [cache_kib:]`       (default: the library's)
#   ALAMBIQUE_BENCH_MAX_BYTES  `awake: [max_bytes:]`       (default: none)
#   ALAMBIQUE_BENCH_OPTS       extra Alambique options, an Elixir keyword list
#                              evaluated at start          (default none)

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @instance :bench_memory
  @schema [
    {1,
     """
     CREATE TABLE notes (id INTEGER PRIMARY KEY, owner INTEGER NOT NULL, body TEXT NOT NULL, at INTEGER NOT NULL);
     CREATE INDEX notes_owner ON notes (owner, at);
     CREATE TABLE tags (note_id INTEGER NOT NULL, tag TEXT NOT NULL, PRIMARY KEY (note_id, tag));
     CREATE INDEX tags_tag ON tags (tag);
     CREATE TABLE owners (id INTEGER PRIMARY KEY, name TEXT NOT NULL, plan TEXT NOT NULL);
     CREATE INDEX owners_plan ON owners (plan);
     """}
  ]

  def run(count, rows, ops, extra) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           [
             name: @instance,
             store: {MemoryStore, MemoryStore.config(store)},
             data_dir: dir,
             migrations: @schema,
             idle_timeout_ms: 600_000,
             lease_ttl_ms: 60_000
           ] ++ extra}
        ],
        strategy: :one_for_one
      )

    try do
      before = totals()
      ids = Enum.map(1..count, &"cell-#{&1}")
      seed(ids, rows)
      lat = workload(ids, rows, ops)

      report(
        count,
        rows,
        Alambique.cells(@instance).count,
        before,
        per_cell(),
        totals(),
        lat
      )
    after
      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  defp each(ids, concurrency, fun) do
    ids
    |> Task.async_stream(fun, max_concurrency: concurrency, ordered: false, timeout: :infinity)
    |> Enum.map(fn {:ok, v} -> v end)
  end

  # One transaction per cell, with the rows made by SQLite.
  defp seed(ids, rows) do
    statements = [
      "INSERT INTO owners VALUES (1, 'o', 'free'), (2, 'p', 'pro')",
      """
      WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < #{rows})
        INSERT INTO notes SELECT i, i % 50, printf('%0120d', i), i FROM s
      """,
      "INSERT INTO tags SELECT id, 't' || (id % 20) FROM notes"
    ]

    each(ids, 64, fn id ->
      {:ok, :ok} =
        retry(fn ->
          Alambique.transaction(@instance, id, fn tx ->
            for sql <- statements, do: {:ok, _} = Alambique.Tx.execute(tx, sql)
            :ok
          end)
        end)
    end)
  end

  @scan "SELECT count(*), sum(length(body)) FROM notes"
  @read_join "SELECT n.id FROM notes n JOIN tags t ON t.note_id = n.id WHERE t.tag = ? LIMIT 50"

  # Latencies in microseconds, one call at a time: reads and writes of every
  # cell, so that each cell's cache sees its own mix.
  defp workload(ids, rows, ops) do
    ids
    |> each(1, fn id ->
      for n <- 1..ops do
        i = :rand.uniform(rows)

        case rem(n, 10) do
          w when w in [0, 5] ->
            {:write,
             time(fn ->
               Alambique.execute(@instance, id, "UPDATE notes SET at = at + 1 WHERE id = ?", [i])
             end)}

          1 ->
            {:scan, time(fn -> Alambique.query(@instance, id, @scan) end)}

          r ->
            {sql, params} =
              case rem(r, 3) do
                0 -> {"SELECT body FROM notes WHERE id = ?", [i]}
                1 -> {"SELECT count(*), max(at) FROM notes WHERE owner = ?", [rem(i, 50)]}
                2 -> {@read_join, ["t#{rem(i, 20)}"]}
              end

            {:read, time(fn -> Alambique.query(@instance, id, sql, params) end)}
        end
      end
    end)
    |> List.flatten()
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
  end

  defp time(fun) do
    {us, {:ok, _}} = :timer.tc(fn -> retry(fun) end)
    us
  end

  defp retry(fun) do
    case fun.() do
      {:error, :overloaded} ->
        Process.sleep(10)
        retry(fun)

      other ->
        other
    end
  end

  # The connections of the awake cells, by purpose.
  defp per_cell do
    config = Alambique.Config.fetch!(@instance)

    mems =
      for id <- Alambique.cells(@instance, limit: 1_000_000).cells do
        {:ok, mem} =
          config
          |> Alambique.Cell.whereis(id)
          |> :sys.get_state()
          |> Map.fetch!(:conn)
          |> Alambique.Sqlite.memory()

        mem
      end

    mems
    |> Enum.reduce(%{}, fn m, acc -> Map.merge(acc, m, fn _, a, b -> a + b end) end)
    |> Map.new(fn {k, v} -> {k, v / max(length(mems), 1)} end)
  end

  defp totals do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    s = Alambique.Replication.Native.stats()

    %{
      sqlite_used: s.sqlite_memory_used,
      sqlite_high: s.sqlite_memory_highwater,
      rust: s.rust_alloc_bytes - s.rust_free_bytes,
      mem: Map.new(:erlang.memory()),
      rss_anon: rss_kb("RssAnon") * 1024,
      types: Map.new(:recon_alloc.memory(:allocated_types))
    }
  end

  defp rss_kb(field) do
    "/proc/self/status"
    |> File.read!()
    |> then(&Regex.run(~r/#{field}:\s+(\d+) kB/, &1))
    |> then(fn [_, kb] -> String.to_integer(kb) end)
  end

  defp pct(values, p) do
    sorted = Enum.sort(values)
    Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))
  end

  defp kib(bytes), do: Float.round(bytes / 1024, 1)
  defp mib(bytes), do: Float.round(bytes / 1_048_576, 1)
  defp line(label, text), do: IO.puts("  #{String.pad_trailing(label, 22)} #{text}")

  defp report(count, rows, awake, before, cell, now, lat) do
    d = fn get -> get.(now) - get.(before) end
    per = fn bytes -> "#{kib(bytes / count)} KiB per cell" end

    IO.puts("== #{count} cells, #{rows} notes each, #{awake} awake at the end")
    tracked = Alambique.Admission.bytes(Alambique.Config.fetch!(@instance))

    if tracked > 0,
      do: IO.puts("budget accounting: #{mib(tracked)} MiB recorded for the awake cells")

    IO.puts("per cell, KiB (sqlite3_db_status):")
    for {k, v} <- Enum.sort(cell), do: line(to_string(k), kib(v))
    line("sum", kib(Enum.sum(Map.values(cell))))

    IO.puts("growth over the empty instance, MiB:")
    sqlite = d.(& &1.sqlite_used)
    line("sqlite memory_used", "#{mib(sqlite)}  (#{per.(sqlite)})")
    line("sqlite highwater", "#{mib(now.sqlite_high)}  (peak since the previous read)")
    line("rust", mib(d.(& &1.rust)))
    system = d.(& &1.mem.system)
    store = d.(& &1.mem.ets)
    line("erlang system", "#{mib(system)}  (#{per.(system)})")
    binary = d.(& &1.mem.binary)
    line("  ets + binary", "#{mib(store + binary)}  (the in-memory store of the bench)")

    line(
      "  rest of system",
      "#{mib(system - store - binary)}  (#{per.(system - store - binary)})"
    )

    line("erlang processes", mib(d.(& &1.mem.processes)))
    line("rss anon", "#{mib(d.(& &1.rss_anon))}  (#{per.(d.(& &1.rss_anon))})")

    IO.puts("allocators, carriers growth, MiB:")

    for {type, size} <- Enum.sort(now.types), size - Map.get(before.types, type, 0) > 1_048_576 do
      line(to_string(type), mib(size - Map.get(before.types, type, 0)))
    end

    for {kind, us} <- Enum.sort(lat) do
      IO.puts("#{kind}s: n=#{length(us)} p50 #{pct(us, 0.5)} us, p99 #{pct(us, 0.99)} us")
    end

    IO.puts("")
  end
end

counts =
  case System.argv() do
    [] -> [1, 100, 500]
    args -> Enum.map(args, &String.to_integer/1)
  end

rows = String.to_integer(System.get_env("ALAMBIQUE_BENCH_ROWS", "2000"))
ops = String.to_integer(System.get_env("ALAMBIQUE_BENCH_OPS", "40"))

extra =
  case System.get_env("ALAMBIQUE_BENCH_OPTS") do
    nil -> []
    code -> code |> Code.eval_string() |> elem(0)
  end

awake =
  for {var, key} <- [
        {"ALAMBIQUE_BENCH_CACHE_KIB", :cache_kib},
        {"ALAMBIQUE_BENCH_MAX_BYTES", :max_bytes}
      ],
      value = System.get_env(var),
      do: {key, String.to_integer(value)}

extra = Keyword.update(extra, :awake, awake, &(awake ++ &1))

Alambique.TestDirs.sweep()

try do
  for count <- counts, do: Bench.run(count, rows, ops, extra)
after
  Alambique.TestDirs.clean()
end
