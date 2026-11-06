# Cross-cell query over many cells (design §14).
#
#   mise run bench:query            # 1 000 cells
#   mise run bench:query -- 5000    # another count
#
# Writes N cells (three rows each, then hibernates them), then runs one query
# over all of them: cold (no replica cached), warm (nothing changed), and after
# one more write to 1% of the cells (incremental refresh). The in-memory store
# has no network latency; the request counts show what S3 would be asked.

defmodule Bench do
  alias Alambique.{CountingStore, Query}
  alias AlambiqueTestkit.MemoryStore

  @sql "SELECT count(*) FROM notes"

  def run(count) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()
    counts = %{
      inner: {MemoryStore, MemoryStore.config(store)},
      table: :ets.new(:counting_store, [:public, :duplicate_bag])
    }

    counting = {CountingStore, counts}

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: :bench_query,
           store: counting,
           data_dir: Path.join(dir, "writer"),
           migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}],
           lease_ttl_ms: 60_000},
          Supervisor.child_spec(
            {Alambique,
             name: :bench_reader,
             store: counting,
             data_dir: Path.join(dir, "reader"),
             migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}],
             query: [cache_bytes: 4_294_967_296]},
            id: :reader
          )
        ],
        strategy: :one_for_one
      )

    try do
      cpu = "/proc/cpuinfo" |> File.read!() |> then(&Regex.run(~r/model name\s*:\s*(.+)/, &1))
      IO.puts("machine:   #{List.last(cpu)}, #{System.schedulers_online()} schedulers")
      IO.puts("cells:     #{count}, three rows each, query: #{@sql}\n")

      ids = for i <- 1..count, do: "cell-#{i}"
      populate(ids, 3)
      for id <- ids, do: :ok = Alambique.hibernate(:bench_query, id)

      phase("cold", counts, fn -> query(ids) end)
      phase("warm (nothing changed)", counts, fn -> query(ids) end)
      phase("warm, max_age_ms 60 000", counts, fn -> query(ids, max_age_ms: 60_000) end)

      changed = Enum.take(ids, max(div(count, 100), 1))
      populate(changed, 1)
      phase("after #{length(changed)} cells wrote once", counts, fn -> query(ids) end)

      phase("cells/2 listing only", counts, fn -> Enum.count(Query.cells(:bench_reader)) end)
    after
      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  defp populate(ids, rows) do
    ids
    |> Task.async_stream(
      fn id ->
        for _ <- 1..rows,
            do: {:ok, _} = Alambique.execute(:bench_query, id, "INSERT INTO notes (body) VALUES ('x')")

        :ok = Alambique.hibernate(:bench_query, id)
      end,
      max_concurrency: 128,
      timeout: :infinity
    )
    |> Stream.run()
  end

  defp query(ids, opts \\ []) do
    result = Query.all(:bench_reader, ids, @sql, [], opts)
    if result.errors != [], do: raise("errors: #{inspect(Enum.take(result.errors, 3))}")
    length(result.rows)
  end

  defp phase(label, counts, fun) do
    CountingStore.reset(counts)
    {us, n} = :timer.tc(fun)

    IO.puts(
      :io_lib.format("~-34s ~8.2f s  gets ~6B  lists ~6B  puts ~3B  (~B results)", [
        label,
        us / 1_000_000,
        CountingStore.count(counts, :get),
        CountingStore.count(counts, :list),
        CountingStore.count(counts, :put),
        n
      ])
    )
  end
end

count =
  case System.argv() do
    [n | _] -> String.to_integer(n)
    [] -> 1_000
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(count)
after
  Alambique.TestDirs.clean()
end
