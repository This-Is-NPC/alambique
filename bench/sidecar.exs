# What the page-hash sidecar adds to the commit of a cell (design §16).
#
#   mise run bench:sidecar          # 300 commits for each case
#   mise run bench:sidecar -- 1000
#
# Each commit changes a few rows of a WAL database and syncs; the time of
# `Replicator.sync/2` is the time before the commit is acknowledged. Every
# segment is uploaded and, with the sidecar on, followed by its sidecar (the
# size rule is off, so every segment has one). Stores: the in-memory store
# with 0 and 20 ms added to every put.

defmodule Bench do
  alias Alambique.Replication
  alias Alambique.Replication.Replicator
  alias Alambique.Sqlite
  alias AlambiqueTestkit.MemoryStore

  def run(commits) do
    IO.puts("commits per case: #{commits}")
    IO.puts("put latency  sidecar     p50       p99      max   store puts per commit")

    for latency <- [0, 20], sidecar <- [false, true] do
      {:ok, pid} = MemoryStore.start_link()
      config = MemoryStore.config(pid)
      store = {MemoryStore, config}

      if latency > 0,
        do: MemoryStore.inject(config, kind: :latency, ms: latency, ops: [:put])

      measure(latency, sidecar, store, commits)
      GenServer.stop(pid)
    end
  end

  defp measure(latency, sidecar, store, commits) do
    dir = Alambique.TestDirs.new("bench-sidecar")
    path = Path.join(dir, "cell.db")
    {:ok, conn} = Sqlite.open(path)
    :ok = Sqlite.execute(conn, "PRAGMA page_size = 4096")
    {:ok, _} = Sqlite.run(conn, "PRAGMA journal_mode = WAL", [])
    :ok = Sqlite.execute(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)")

    ctx =
      Replication.ctx(
        store: store,
        prefix: "bench",
        node: "bench",
        timeout_ms: 60_000,
        page_sidecar: if(sidecar, do: 0)
      )

    state = Replicator.new("cell", path, 4096, 1)
    {:ok, state} = Replicator.sync(state, ctx)

    {times, _state} =
      Enum.map_reduce(1..commits, state, fn i, state ->
        :ok =
          Sqlite.execute(
            conn,
            "INSERT OR REPLACE INTO t VALUES (#{rem(i, 50)}, randomblob(1000))"
          )

        {us, {:ok, state}} = :timer.tc(fn -> Replicator.sync(state, ctx) end)
        {us, state}
      end)

    sorted = Enum.sort(times)
    at = fn q -> Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * q))) end
    {:ok, keys, nil} = MemoryStore.list(elem(store, 1), "bench/", nil, 100_000)
    puts = length(keys) / (commits + 1)

    IO.puts(
      String.pad_leading("#{latency} ms", 11) <>
        String.pad_leading(to_string(sidecar), 9) <>
        String.pad_leading(fmt(at.(0.5)), 9) <>
        String.pad_leading(fmt(at.(0.99)), 10) <>
        String.pad_leading(fmt(List.last(sorted)), 9) <>
        String.pad_leading(Float.to_string(Float.round(puts, 2)), 8)
    )

    Sqlite.close(conn)
    File.rm_rf(dir)
  end

  defp fmt(us) when us >= 1000, do: "#{Float.round(us / 1000, 2)} ms"
  defp fmt(us), do: "#{us} us"
end

commits =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 300
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(commits)
after
  Alambique.TestDirs.clean()
end
