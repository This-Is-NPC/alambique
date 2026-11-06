# Lazy restore against a full restore of a large cell (design §16).
#
#   mise run bench:lazy             # 2048 MB
#   mise run bench:lazy -- 512      # size of the database in MB
#
# A synthetic database of incompressible rows is generated in a temporary
# directory and replicated as TXID 1 plus 20 small batches. It is then
# restored in full, and lazily: time to the page map, to the first query
# (open, then a point lookup) and to a complete file. Stores: the in-memory
# store with 0 and 20 ms added to every request, and the disposable SeaweedFS
# from `mise run seaweedfs:up`.

defmodule Bench do
  alias Alambique.Cell.Db
  alias Alambique.Replication
  alias Alambique.Replication.{Hydrator, Lazy, Replicator}
  alias Alambique.Sqlite
  alias AlambiqueTestkit.MemoryStore

  @row 3_500

  def run(megabytes) do
    machine()

    dir = Alambique.TestDirs.new("bench-lazy")

    try do
      {conn, path} = generate(dir, megabytes)
      IO.puts("")

      IO.puts("store                  full restore  page map  first query  complete file  misses")

      for latency <- [0, 20] do
        {:ok, pid} = MemoryStore.start_link()
        config = MemoryStore.config(pid)
        store = {MemoryStore, config}

        if latency > 0 do
          MemoryStore.inject(config,
            kind: :latency,
            ms: latency,
            ops: [:get, :get_range, :head, :list]
          )
        end

        measure("memory +#{latency} ms", store, conn, path, dir)
        GenServer.stop(pid)
      end

      seaweedfs(conn, path, dir)
      Sqlite.close(conn)
    after
      File.rm_rf(dir)
    end
  end

  defp machine do
    {:ok, cpu} = File.read("/proc/cpuinfo")
    [_, model] = Regex.run(~r/model name\s*:\s*(.*)/, cpu)
    IO.puts("machine:   #{model}, #{System.schedulers_online()} schedulers")
    IO.puts("runtime:   Erlang/OTP #{System.otp_release()}, Elixir #{System.version()}")
  end

  # Rows of random bytes until the file has `megabytes`, then a WAL database.
  defp generate(dir, megabytes) do
    path = Path.join(dir, "source.db")
    started = now()
    {:ok, conn} = Sqlite.open(path)
    :ok = Sqlite.execute(conn, "PRAGMA page_size = 4096")
    :ok = Sqlite.execute(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB, k INTEGER)")
    :ok = Sqlite.execute(conn, "CREATE INDEX t_k ON t (k)")
    rows = div(megabytes * 1_048_576, @row + 40)
    step = 50_000

    for from <- 1..rows//step do
      to = min(from + step - 1, rows)

      :ok =
        Sqlite.execute(
          conn,
          "WITH RECURSIVE n(x) AS (SELECT #{from} UNION ALL SELECT x + 1 FROM n WHERE x < #{to}) " <>
            "INSERT INTO t SELECT x, randomblob(#{@row}), x % 1000 FROM n"
        )
    end

    {:ok, _} = Sqlite.run(conn, "PRAGMA journal_mode = WAL", [])
    size = File.stat!(path).size
    IO.puts("database:  #{rows} rows, #{mb(size)} MB (generated in #{secs(started)})")

    {conn, path}
  end

  def relay(_event, measurements, _meta, to), do: send(to, {:hydrate, measurements})

  defp measure(label, store, conn, path, dir) do
    ctx = ctx(store)
    upload(Replicator.new("cell", path, 4096, 1), ctx, conn)

    full_path = Path.join(dir, "full-#{System.unique_integer([:positive])}.db")
    t = now()
    {:ok, _} = Replication.restore(ctx, "cell", full_path, :latest)
    full_ms = ms(t)
    File.rm(full_path)

    lazy_path = Path.join(dir, "lazy-#{System.unique_integer([:positive])}.db")
    handler = {__MODULE__, make_ref()}
    me = self()

    :telemetry.attach(
      handler,
      [:alambique, :replication, :hydrate],
      &__MODULE__.relay/4,
      me
    )

    t = now()

    {:ok, result} =
      Replication.restore(ctx, "cell", lazy_path, :latest, lazy: %{min_bytes: 0, wait_ms: 30_000})

    plan_ms = ms(t)
    lazy = Lazy.new(result, ctx)
    {:ok, hydrator} = Hydrator.start_link(lazy, concurrency: 8, fetch_ms: 30_000)
    {:ok, db} = Db.open(lazy_path)
    {:ok, %{rows: [[_]]}} = Db.run(db, "SELECT length(v) FROM t WHERE id = (SELECT max(id) / 2 FROM t)", [])
    {:ok, %{rows: [[_]]}} = Db.run(db, "SELECT count(*) FROM t WHERE k = 7", [])
    first_ms = ms(t)

    receive do
      {:hydrated, ^hydrator} -> :ok
      {:hydration_failed, ^hydrator, reason} -> raise "hydration failed: #{inspect(reason)}"
    after
      900_000 -> raise "hydration did not finish"
    end

    complete_ms = ms(t)
    :telemetry.detach(handler)

    misses =
      receive do
        {:hydrate, m} -> m.misses
      after
        1_000 -> -1
      end

    Db.close(db)
    File.rm_rf(lazy_path)

    IO.puts(
      String.pad_trailing(label, 22) <>
        String.pad_leading(fmt(full_ms), 13) <>
        String.pad_leading(fmt(plan_ms), 10) <>
        String.pad_leading(fmt(first_ms), 13) <>
        String.pad_leading(fmt(complete_ms), 15) <>
        String.pad_leading(Integer.to_string(misses), 8)
    )
  end

  defp seaweedfs(conn, path, dir) do
    config = %{
      endpoint: "http://127.0.0.1:18333",
      bucket: "lazy-bench-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower),
      access_key_id: "alambique",
      secret_access_key: "alambique-secret",
      path_style: true,
      receive_timeout: 600_000
    }

    {:ok, %{status: status}} =
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

    true = status in [200, 409]
    measure("SeaweedFS (local)", {Alambique.Store.S3, config}, conn, path, dir)
  end

  defp ctx(store) do
    Replication.ctx(store: store, prefix: "bench", node: "bench", timeout_ms: 600_000)
  end

  # TXID 1 is the whole database; then 20 batches of a few rows.
  defp upload(state, ctx, conn) do
    {:ok, state} = Replicator.sync(state, ctx)

    Enum.reduce(1..20, state, fn i, state ->
      :ok =
        Sqlite.execute(
          conn,
          "UPDATE t SET v = randomblob(#{@row}) WHERE id BETWEEN #{i * 997} AND #{i * 997 + 20}"
        )

      {:ok, state} = Replicator.sync(state, ctx)
      state
    end)
  end

  defp now, do: System.monotonic_time(:millisecond)
  defp ms(t), do: now() - t
  defp secs(t), do: "#{Float.round(ms(t) / 1000, 1)} s"
  defp mb(bytes), do: div(bytes, 1_048_576)
  defp fmt(ms) when ms >= 1000, do: "#{Float.round(ms / 1000, 2)} s"
  defp fmt(ms), do: "#{ms} ms"
end

megabytes =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 2048
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(megabytes)
after
  Alambique.TestDirs.clean()
end
