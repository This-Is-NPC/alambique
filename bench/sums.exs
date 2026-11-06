# Cost of a one-row commit in cells of many pages (design §15).
#
#   mise run bench:sums
#   ALAMBIQUE_BENCH_PAGES="10000 100000" mise run bench:sums
#
# Builds a fresh cell of at least P pages (default "10000 100000 1000000") with one
# table of 4 000-byte blobs, one row per page (SQLite's default page size is 4 096), then
# times 200 commits that each update a single row, against the in-memory store
# with no added latency. The page-sums vector grows with P, so µs per commit
# shows what it costs. Reports median and p99 of the commits.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @commits 200
  @batch 2_000

  def run(pages) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()
    name = :"bench_sums_#{pages}"

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: name,
           store: {MemoryStore, MemoryStore.config(store)},
           prefix: "bench-sums",
           data_dir: dir,
           migrations: [{1, "CREATE TABLE b (id INTEGER PRIMARY KEY, v BLOB)"}],
           idle_timeout_ms: 600_000,
           lease_ttl_ms: 60_000,
           compaction: false,
           retention: false}
        ],
        strategy: :one_for_one
      )

    try do
      {build_us, rows} = :timer.tc(fn -> build(name, pages) end)
      {:ok, size} = size(name)

      lat =
        for i <- 1..@commits do
          {us, {:ok, _}} =
            :timer.tc(fn ->
              Alambique.execute(name, "sums", "UPDATE b SET v = zeroblob(4000) WHERE id = ?", [
                rem(i * 7919, rows) + 1
              ])
            end)

          us
        end
        |> Enum.sort()

      IO.puts(
        :io_lib.format("~9B  ~9B  ~8.1f  ~10B  ~10B", [
          pages,
          size,
          build_us / 1_000_000,
          Enum.at(lat, div(@commits, 2)),
          Enum.at(lat, trunc(@commits * 0.99))
        ])
      )
    after
      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  # Inserts rows of about one page each, @batch per transaction, until the
  # database has `pages` pages (the change capture stores the rows too, so
  # it takes fewer rows than pages). Returns the row count.
  defp build(name, pages, first \\ 1) do
    last = first + @batch - 1

    {:ok, _} =
      Alambique.transaction(name, "sums", fn tx ->
        for id <- first..last do
          {:ok, _} =
            Alambique.Tx.execute(tx, "INSERT INTO b (id, v) VALUES (?, zeroblob(4000))", [id])
        end

        :ok
      end)

    {:ok, count} = size(name)
    if count >= pages, do: last, else: build(name, pages, last + 1)
  end

  defp size(name) do
    with {:ok, %{rows: [[count]]}} <- Alambique.query(name, "sums", "PRAGMA page_count") do
      {:ok, count}
    end
  end
end

pages =
  "ALAMBIQUE_BENCH_PAGES"
  |> System.get_env("10000 100000 1000000")
  |> String.split()
  |> Enum.map(&String.to_integer/1)

Alambique.TestDirs.sweep()
IO.puts("pages (target)  pages (db)  build s  median us  p99 us")

try do
  Enum.each(pages, &Bench.run/1)
after
  Alambique.TestDirs.clean()
end
