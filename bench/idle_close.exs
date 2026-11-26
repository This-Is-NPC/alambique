# What closing the SQLite connection of an idle awake cell saves and costs
# (`awake: [idle_close_after_ms:]`, docs/cost-of-a-cell.md).
#
#   mise run bench:idle-close               # 250 cells of 0 and 2000 notes
#   mise run bench:idle-close -- 500        # another count
#   ALAMBIQUE_BENCH_ROWS="0 2000" mise run bench:idle-close
#
# For each size of cell, two fresh instances over the in-memory store hold the
# same cells idle: one with the connection open (the shrink off), one with it
# closed. Per cell it prints the memory of each (RssAnon, the BEAM's memory,
# the memory SQLite and the driver allocated) and the descriptors, then on
# the closed cells:
#
#   * the time of the close (checkpoint, fsync and close, from the event
#     `[:alambique, :cell, :idle_close]`) and of the reopen (the event
#     `[:alambique, :cell, :reopen]`: the check of the files, Db.open and the
#     -shm rebuild), with the -shm file kept and removed, and with frames in
#     the WAL; the p50 and p99 of the first call
#   * the first call of a request that needs SQLite and of one that does not
#     (`position/3`), against the same call on a cell that hibernated with its
#     file kept and on a warm cell
#   * lease renewals per second of the closed cells
#   * a cycle: every cell called every `idle_close_after_ms` plus a margin, so
#     that each call reopens and each pause closes
#
#   ALAMBIQUE_BENCH_ROWS    notes per cell, one run each     (default "0 2000")
#   ALAMBIQUE_BENCH_CLOSE   idle_close_after_ms              (default "300")
#   ALAMBIQUE_BENCH_CYCLES  rounds of the cycle              (default "3")
#   ALAMBIQUE_BENCH_RENEWAL seconds the renewals are counted (default "10")
#
# The run is bounded by `timeout` in scripts/bench-idle-close.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @instance :bench_idle_close
  @schema [
    {1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, owner INTEGER NOT NULL, body TEXT NOT NULL)"}
  ]

  def run(count, rows, close_ms, cycles, renewal_s) do
    IO.puts("== #{count} cells, #{rows} notes each, idle_close_after_ms #{close_ms}")

    open = measure(count, rows, nil, fn ids, _ -> settle(ids, :open) end)
    closed = measure(count, rows, close_ms, fn ids, _ -> settle(ids, :closed) end)
    memory(count, open, closed)

    scenarios(count, rows, close_ms, cycles, renewal_s)
  end

  # One instance with `count` cells idle in the state `settle` waits for.
  defp measure(count, rows, close_ms, settle) do
    with_instance(close_ms, fn ->
      ids = Enum.map(1..count, &"cell-#{&1}")
      seed(ids, rows)
      before = totals()
      settle.(ids, nil)
      now = totals()
      %{before: before, now: now, fds: fds()}
    end)
  end

  defp memory(count, open, closed) do
    per = fn bytes -> Float.round(bytes / count / 1024, 1) end

    IO.puts("per idle cell, open vs closed (KiB):")

    for {label, get} <- [
          {"rss anon", & &1.rss_anon},
          {"erlang total", & &1.mem.total},
          {"erlang system", & &1.mem.system},
          {"erlang processes", & &1.mem.processes},
          {"sqlite memory_used", & &1.sqlite_used},
          {"driver (rust) alloc", & &1.rust},
          {"allocator blocks in use", & &1.blocks},
          {"allocator carriers", & &1.carriers},
          {"  driver_alloc carriers", &Map.get(&1.types, :driver_alloc, 0)},
          {"  binary_alloc carriers", &Map.get(&1.types, :binary_alloc, 0)},
          {"  eheap_alloc carriers", &Map.get(&1.types, :eheap_alloc, 0)}
        ] do
      o = get.(open.now)
      c = get.(closed.now)
      line(label, "#{per.(o)} -> #{per.(c)}  (#{per.(o - c)} saved)")
    end

    line("descriptors", "#{open.fds} -> #{closed.fds}  (#{Float.round((open.fds - closed.fds) / count, 2)} per cell)")
  end

  defp scenarios(count, rows, close_ms, cycles, renewal_s) do
    handler = {__MODULE__, make_ref()}

    :telemetry.attach_many(
      handler,
      [[:alambique, :cell, :idle_close], [:alambique, :cell, :reopen], [:alambique, :lease, :renew]],
      &__MODULE__.relay/4,
      self()
    )

    try do
      with_instance(close_ms, fn ->
        ids = Enum.map(1..count, &"cell-#{&1}")
        seed(ids, rows)
        settle(ids, :closed)
        closes = events([:alambique, :cell, :idle_close])
        report_events("close (checkpoint, fsync, close)", closes, &(&1.measurements.duration))
        report_bytes(closes)

        # First calls on closed cells, one at a time.
        flush([:alambique, :cell, :reopen])
        {no_sql, _} = first_calls(ids, fn id -> Alambique.position(@instance, id) end)
        line("first call, no SQL", latency(no_sql))
        {sql, _} = first_calls(ids, fn id -> Alambique.query(@instance, id, "SELECT 1") end)
        line("first call, SQL (closed)", latency(sql))
        reopens = events([:alambique, :cell, :reopen])
        report_events("reopen (clean)", reopens, &(&1.measurements.duration))

        # -shm removed while closed: the index is built again.
        settle(ids, :closed)
        flush([:alambique, :cell, :reopen])
        for id <- ids, do: File.rm(path(id) <> "-shm")
        {shm, _} = first_calls(ids, fn id -> Alambique.query(@instance, id, "SELECT 1") end)
        line("first call, -shm removed", latency(shm))
        report_events("reopen (-shm removed)", events([:alambique, :cell, :reopen]), &(&1.measurements.duration))

        # Frames committed behind the cell while it was closed.
        settle(ids, :closed)
        flush([:alambique, :cell, :reopen])
        dirty = Enum.take(ids, min(count, 50))

        for id <- dirty,
            do: 0 = Alambique.Foreign.exec(path(id), "INSERT INTO notes VALUES (-1, 0, 'x')")

        {frames, _} = first_calls(dirty, fn id -> Alambique.query(@instance, id, "SELECT 1") end)
        line("first call, WAL with frames", latency(frames))

        # Warm and hibernated, for comparison.
        {warm, _} = first_calls(ids, fn id -> Alambique.query(@instance, id, "SELECT 1") end)
        line("call on an open cell", latency(warm))

        for id <- ids, do: :ok = Alambique.hibernate(@instance, id)
        {asleep, _} = first_calls(ids, fn id -> Alambique.query(@instance, id, "SELECT 1") end)
        line("first call, hibernated (local file)", latency(asleep))

        renewals(ids, close_ms, renewal_s)
        cycle(ids, cycles)
      end)
    after
      :telemetry.detach(handler)
    end
  end

  def relay(event, measurements, meta, test),
    do: send(test, {:event, event, %{measurements: measurements, meta: meta}})

  # The events of one kind; those of other kinds stay in the mailbox.
  defp events(event) do
    receive do
      {:event, ^event, e} -> [e | events(event)]
    after
      0 -> []
    end
  end

  defp flush(event), do: events(event) && :ok

  defp report_events(label, events, get) do
    us = Enum.map(events, &System.convert_time_unit(get.(&1), :native, :microsecond))

    case us do
      [] -> line(label, "no events")
      _ -> line(label, "n=#{length(us)} #{latency(us)}")
    end
  end

  defp report_bytes(closes) do
    bytes = Enum.map(closes, & &1.measurements.bytes)
    if bytes != [], do: line("  WAL bytes at the close", "p50 #{pct(bytes, 0.5)}, p99 #{pct(bytes, 0.99)}")
  end

  defp first_calls(ids, fun) do
    us =
      for id <- ids do
        {t, result} = :timer.tc(fn -> retry(fn -> fun.(id) end) end)
        {:ok, _} = result
        t
      end

    {us, :ok}
  end

  # Renewal PUTs per second of the closed cells, from the lease events.
  defp renewals(ids, close_ms, seconds) do
    settle(ids, :closed)
    flush([:alambique, :lease, :renew])
    Process.sleep(seconds * 1000)
    n = length(events([:alambique, :lease, :renew]))

    line(
      "lease renewals",
      "#{Float.round(n / seconds, 1)} per second for #{length(ids)} closed cells " <>
        "(#{Float.round(n / seconds / length(ids), 3)} per cell, close after #{close_ms} ms)"
    )
  end

  # Each cell is called a little after it closed, so every call reopens.
  defp cycle(ids, cycles) do
    settle(ids, :closed)
    flush([:alambique, :cell, :reopen])
    flush([:alambique, :cell, :idle_close])
    started = System.monotonic_time(:millisecond)

    us =
      for _ <- 1..cycles do
        round =
          ids
          |> Task.async_stream(
            fn id ->
              {t, {:ok, _}} = :timer.tc(fn -> retry(fn -> Alambique.query(@instance, id, "SELECT 1") end) end)
              t
            end,
            max_concurrency: 16,
            timeout: :infinity
          )
          |> Enum.map(fn {:ok, t} -> t end)

        settle(ids, :closed)
        round
      end
      |> List.flatten()

    elapsed = System.monotonic_time(:millisecond) - started
    line("cycle", "#{length(us)} calls, each reopening, in #{elapsed} ms: #{latency(us)}")
    report_events("  reopen in the cycle", events([:alambique, :cell, :reopen]), &(&1.measurements.duration))
    report_events("  close in the cycle", events([:alambique, :cell, :idle_close]), &(&1.measurements.duration))
  end

  # Waits until every cell is `:open` (has a connection) or `:closed`.
  defp settle(ids, want) do
    config = Alambique.Config.fetch!(@instance)
    deadline = System.monotonic_time(:millisecond) + 120_000

    until(deadline, fn ->
      Enum.all?(ids, fn id ->
        case Alambique.Cell.whereis(config, id) do
          nil -> false
          pid -> (:sys.get_state(pid, 30_000).conn == nil) == (want == :closed)
        end
      end)
    end)
  end

  defp until(deadline, fun) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> raise "cells did not settle"
      true -> Process.sleep(100) && until(deadline, fun)
    end
  end

  defp with_instance(close_ms, fun) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()

    awake = if close_ms, do: [idle_close_after_ms: close_ms], else: []

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           [
             name: @instance,
             store: {MemoryStore, MemoryStore.config(store)},
             data_dir: dir,
             migrations: @schema,
             feed: false,
             compaction: false,
             sql: [orphan_frames: :replicate],
             idle_timeout_ms: 600_000,
             lease_ttl_ms: 6_000,
             awake: awake
           ]}
        ],
        strategy: :one_for_one
      )

    try do
      fun.()
    after
      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  defp path(id), do: Alambique.Cell.Local.db_path(Alambique.Config.fetch!(@instance), id)

  defp seed(ids, rows) do
    ids
    |> Task.async_stream(
      fn id ->
        sql =
          if rows == 0,
            do: "INSERT INTO notes VALUES (0, 0, 'x')",
            else:
              "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < #{rows}) " <>
                "INSERT INTO notes SELECT i, i % 50, printf('%0120d', i) FROM s"

        {:ok, _} = retry(fn -> Alambique.execute(@instance, id, sql) end)
        {:ok, _} = retry(fn -> Alambique.query(@instance, id, "SELECT count(*) FROM notes") end)
      end,
      max_concurrency: 64,
      timeout: :infinity
    )
    |> Stream.run()
  end

  defp retry(fun) do
    case fun.() do
      {:error, reason} when reason in [:overloaded, :busy] ->
        Process.sleep(10)
        retry(fun)

      other ->
        other
    end
  end

  defp totals do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    s = Alambique.Replication.Native.stats()

    %{
      sqlite_used: s.sqlite_memory_used,
      rust: s.rust_alloc_bytes - s.rust_free_bytes,
      mem: Map.new(:erlang.memory()),
      rss_anon: rss_kb("RssAnon") * 1024,
      # Blocks in use against the carriers that hold them: freed blocks stay
      # in carriers, which is why RssAnon falls less than the memory freed.
      blocks: :recon_alloc.memory(:used),
      carriers: :recon_alloc.memory(:allocated),
      types: Map.new(:recon_alloc.memory(:allocated_types))
    }
  end

  defp fds, do: length(File.ls!("/proc/self/fd"))

  defp rss_kb(field) do
    "/proc/self/status"
    |> File.read!()
    |> then(&Regex.run(~r/#{field}:\s+(\d+) kB/, &1))
    |> then(fn [_, kb] -> String.to_integer(kb) end)
  end

  defp latency(us), do: "p50 #{pct(us, 0.5)} us, p99 #{pct(us, 0.99)} us"

  defp pct(values, p) do
    sorted = Enum.sort(values)
    Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))
  end

  defp line(label, text), do: IO.puts("  #{String.pad_trailing(label, 36)} #{text}")
end

count =
  case System.argv() do
    [] -> 250
    [n | _] -> String.to_integer(n)
  end

sizes =
  "ALAMBIQUE_BENCH_ROWS" |> System.get_env("0 2000") |> String.split() |> Enum.map(&String.to_integer/1)

env = fn name, default -> String.to_integer(System.get_env(name, default)) end
close_ms = env.("ALAMBIQUE_BENCH_CLOSE", "300")
cycles = env.("ALAMBIQUE_BENCH_CYCLES", "3")
renewal = env.("ALAMBIQUE_BENCH_RENEWAL", "10")

Alambique.TestDirs.sweep()

try do
  for rows <- sizes, do: Bench.run(count, rows, close_ms, cycles, renewal)
after
  Alambique.TestDirs.clean()
end
