# Wake of a hibernated cell, with latency on every store trip (design §7).
#
#   mise run bench:wake                  # 8 wakes per cell
#   mise run bench:wake -- 20            # wakes per cell
#
# A mailbox-like cell (about 5 MB: small rows and some 75 KB bodies) is
# hibernated and woken by the call that opens the mailbox, the 50 newest
# messages. The measured time is that call, wake included. The in-memory
# store adds a fixed delay to every trip. A wrapper store records every trip
# (operation, kind of object, start and end), so the table shows the trips
# of the wake in order; the trips counted are those until the answer, and the
# ones after it (the hydrator, the feed) are left out.
#
# Modes (each on its own instance):
#
#   local     the local file survives the hibernation and is reused
#   hydrated  the local file is gone and the cell is downloaded before it serves
#   lazy      the local file is gone and the pages are read on demand
#
#   ALAMBIQUE_BENCH_LATENCY  added latency per trip in ms         (default "25")
#   ALAMBIQUE_BENCH_MODES    modes to run                         (default "local hydrated lazy")
#   ALAMBIQUE_BENCH_CELLS    cells                                (default "3")
#   ALAMBIQUE_BENCH_TAIL     commits after the last snapshot      (default "12")
#   ALAMBIQUE_BENCH_TRACE    "1" prints the trips of one wake     (default "1")
#   ALAMBIQUE_BENCH_OPTS     extra Alambique options, an Elixir keyword list
#                            evaluated at start, e.g. "[restore: [fetch_concurrency: 4]]"
#                            (default none)

defmodule Traced do
  @moduledoc false
  # Wraps a store and records {{start_us, ref}, op, key, end_us} per trip.
  @behaviour Alambique.Store

  def wrap({_m, _c} = inner),
    do: {__MODULE__, %{inner: inner, table: :ets.new(:trace, [:ordered_set, :public])}}

  def events(%{table: t}), do: :ets.tab2list(t)

  def last_us(%{table: t}) do
    case :ets.last(t) do
      :"$end_of_table" -> 0
      {started, _} -> started
    end
  end

  defp timed(%{table: t}, op, key, fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()
    :ets.insert(t, {{started, make_ref()}, op, key, System.monotonic_time(:microsecond)})
    result
  end

  def kind(key) do
    cond do
      String.ends_with?(key, "/lease") -> :lease
      String.ends_with?(key, "/frozen") -> :frozen
      String.contains?(key, "/schema/") -> :schema
      String.contains?(key, "/feed/") -> :feed
      String.contains?(key, "/0009/") -> :snapshot
      String.contains?(key, "/0000/") -> :segment
      Regex.match?(~r|/000[1-8]/|, key) -> :merged
      true -> :other
    end
  end

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}} = s, key, body, cond, meta),
    do: timed(s, :put, key, fn -> m.put(c, key, body, cond, meta) end)

  @impl true
  def get(%{inner: {m, c}} = s, key), do: timed(s, :get, key, fn -> m.get(c, key) end)

  @impl true
  def get_range(%{inner: {m, c}} = s, key, offset, length),
    do: timed(s, :get, key, fn -> m.get_range(c, key, offset, length) end)

  @impl true
  def head(%{inner: {m, c}} = s, key), do: timed(s, :head, key, fn -> m.head(c, key) end)

  @impl true
  def list(%{inner: {m, c}} = s, prefix, start_after, limit),
    do: timed(s, :list, prefix, fn -> m.list(c, prefix, start_after, limit) end)

  @impl true
  def delete(%{inner: {m, c}} = s, key), do: timed(s, :delete, key, fn -> m.delete(c, key) end)
end

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @migration """
  CREATE TABLE messages (id INTEGER PRIMARY KEY, date INTEGER NOT NULL, subject TEXT NOT NULL, snippet TEXT NOT NULL, body BLOB NOT NULL);
  CREATE INDEX messages_date ON messages (date);
  """
  @open "SELECT id, date, subject, snippet FROM messages ORDER BY date DESC LIMIT 50"
  @messages 500

  def run(wakes) do
    ms = "LATENCY" |> env("25") |> String.to_integer()
    cells = "CELLS" |> env("3") |> String.to_integer()

    IO.puts("latency per trip: #{ms} ms; #{cells} cells, #{wakes} wakes each")
    IO.puts("options: #{inspect(extra_opts())}\n")

    for mode <- String.split(env("MODES", "local hydrated lazy")) do
      one_mode(String.to_atom(mode), ms, cells, wakes)
    end

    :ok
  end

  defp env(name, default), do: System.get_env("ALAMBIQUE_BENCH_" <> name, default)

  defp extra_opts do
    case env("OPTS", "") do
      "" -> []
      code -> code |> Code.eval_string() |> elem(0)
    end
  end

  defp merge_opt(_key, a, b) do
    if Keyword.keyword?(a) and Keyword.keyword?(b), do: Keyword.merge(a, b), else: b
  end

  defp mode_opts(:lazy), do: [lazy_restore: [min_bytes: 1]]
  defp mode_opts(:hydrated), do: [lazy_restore: false]
  defp mode_opts(:local), do: []

  defp one_mode(mode, ms, cells, wakes) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, mem} = MemoryStore.start_link()
    config = MemoryStore.config(mem)
    {Traced, traced} = store = Traced.wrap({MemoryStore, config})
    name = :"bench_wake_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           [
             name: name,
             store: store,
             prefix: "bench",
             data_dir: dir,
             migrations: [{1, @migration}],
             compaction: false,
             idle_timeout_ms: 600_000,
             lease_ttl_ms: 60_000
           ]
           |> Keyword.merge(mode_opts(mode))
           |> Keyword.merge(extra_opts(), &merge_opt/3)}
        ],
        strategy: :one_for_one
      )

    try do
      ids = for i <- 1..cells, do: "mail-#{i}"
      for id <- ids, do: load(name, id)

      MemoryStore.inject(config, kind: :latency, ms: ms)

      samples =
        for round <- 1..wakes, id <- ids do
          wake(name, id, dir, mode, traced, round == wakes and id == hd(ids))
        end

      report(mode, samples)
    after
      Supervisor.stop(sup)
      GenServer.stop(mem)
      File.rm_rf(dir)
    end
  end

  # About 5 MB: small rows, and every 15th message has a 75 KB body. A merge
  # and a snapshot follow, then `tail` more commits (new mail since the last
  # snapshot) stay as single-TXID files.
  defp load(name, id) do
    insert(name, id, 1..@messages)
    :ok = Alambique.hibernate(name, id)
    config = Alambique.Config.fetch!(name)
    {:ok, location} = Alambique.Placement.locate(config, id, fresh: true)
    ctx = Alambique.Config.repl_ctx(config, location)
    {:ok, _} = Alambique.Compaction.run(ctx, id, snapshot_interval_txids: 1)

    tail = "TAIL" |> env("12") |> String.to_integer()
    for i <- 1..tail, do: insert(name, id, [@messages + i])
    :ok = Alambique.hibernate(name, id)
  end

  defp insert(name, id, ids) do
    for chunk <- Enum.chunk_every(ids, 10) do
      {:ok, :ok} =
        Alambique.transaction(name, id, fn tx ->
          for i <- chunk do
            size = if rem(i, 15) == 0, do: 75_000, else: 1_500

            {:ok, _} =
              Alambique.Tx.execute(
                tx,
                "INSERT INTO messages (id, date, subject, snippet, body) VALUES (?, ?, ?, ?, ?)",
                [
                  i,
                  1_700_000_000 + i * 60,
                  "subject #{i}",
                  String.duplicate("s", 120),
                  {:blob, :crypto.strong_rand_bytes(size)}
                ]
              )
          end

          :ok
        end)
    end
  end

  defp wake(name, id, dir, mode, traced, trace?) do
    :ok = Alambique.hibernate(name, id)
    if mode != :local, do: File.rm_rf(Path.join(dir, id <> ".cell"))
    settle(traced)

    t0 = System.monotonic_time(:microsecond)
    {:ok, %{rows: rows}} = Alambique.query(name, id, @open)
    t1 = System.monotonic_time(:microsecond)
    50 = length(rows)

    during =
      for {{s, _}, op, key, e} <- Traced.events(traced), s >= t0, s <= t1, do: {s - t0, e - t0, op, key}

    if trace?, do: print_trace(mode, t1 - t0, Enum.sort(during))
    %{us: t1 - t0, trips: Enum.map(during, fn {_, _, op, key} -> {op, Traced.kind(key)} end)}
  end

  # Waits until the store has been quiet, so that the work the hibernation
  # and the previous hydrator left does not touch the next wake.
  defp settle(traced) do
    Process.sleep(1_500)
    last = Traced.last_us(traced)
    Process.sleep(1_000)
    if Traced.last_us(traced) != last, do: settle(traced)
  end

  defp print_trace(mode, total, rows) do
    if env("TRACE", "1") == "1" do
      IO.puts("-- trips of one #{mode} wake and open, #{div(total, 1000)} ms in all")
      IO.puts("   start   end  op     kind      key")

      for {s, e, op, key} <- rows do
        IO.puts(
          :io_lib.format("  ~5B ~5B  ~-5s  ~-8s  ~s", [
            div(s, 1000),
            div(e, 1000),
            op,
            Traced.kind(key),
            key |> String.split("/") |> Enum.take(-2) |> Enum.join("/")
          ])
        )
      end

      IO.puts("")
    end
  end

  defp report(mode, samples) do
    us = samples |> Enum.map(& &1.us) |> Enum.sort()
    pct = fn p -> Enum.at(us, min(length(us) - 1, ceil(p / 100 * length(us)) - 1)) / 1000 end
    trips = Enum.flat_map(samples, & &1.trips)
    n = length(samples)

    IO.puts("== #{mode}: #{n} wakes")

    ms = fn v -> v |> Float.round(0) |> trunc() end

    IO.puts(
      "  wake + open (ms): p50 #{ms.(pct.(50))}  p95 #{ms.(pct.(95))}  " <>
        "p99 #{ms.(pct.(99))}  max #{ms.(List.last(us) / 1000)}"
    )

    IO.puts("  trips per wake: #{Float.round(length(trips) / n, 1)}")

    for {{op, kind}, c} <- trips |> Enum.frequencies() |> Enum.sort() do
      IO.puts(:io_lib.format("    ~-5s ~-8s ~5.1f", [op, kind, c / n]))
    end

    IO.puts("")
  end
end

wakes =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 8
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(wakes)
after
  Alambique.TestDirs.clean()
end
