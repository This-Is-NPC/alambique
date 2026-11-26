# What the hand-over of the feed's rows costs, by mode (design §13).
#
#   mise run bench:feed-handover                # every case
#   mise run bench:feed-handover -- raw cell    # only the named cases
#   ALAMBIQUE_BENCH_REPS=30 ALAMBIQUE_BENCH_RUNS=3 mise run bench:feed-handover
#
# Modes: `cell` (hand_over: :cell), `sender_burst` and `sender_read`
# (hand_over: :sender with reader_close: :after_burst and :after_read) and
# `native` (hand_over: :native). Cases with an instance use the in-memory store.
#
#   raw      One batch of ROWS rows on a file, outside any cell, per value size:
#            `step` is the query alone (every column read, no term built);
#            `cell` is `Capture.rows_from/4` (step and the list of lists),
#            `shape` the rest of `Pieces.read/5`, `copy` the send of the batches
#            to another process; `native` is `Capture.send_rows/6` (step, the
#            text of the records and the send in the library). All in
#            microseconds per batch and per row; REPS repetitions, mean and p50.
#   cell     One cell, a batch of ROWS rows written REPS times: the reductions,
#            the growth of the heap and the bytes of binaries of the cell
#            process from the write to the publication of the batch, and the
#            time of the write itself, per mode.
#   latency  The first one-row write after a batch of ROWS rows, at once and
#            DELAY_MS later, per mode: mean, p50 and p99, over RUNS runs of REPS.
#   reader   `Db.open_reader/2` (the open of a read-only connection) per range,
#            and the files and memory of N cells that publish at once with each
#            reader_close mode: the peak of the files of the process and of
#            `:erlang.memory(:total)` while they do, and the checkpoints that
#            found the reader busy.
#   dirty    The share of time the dirty IO schedulers were busy while N cells
#            publish a batch of ROWS_N rows each, per mode.
#
# Not covered: the wait of a close on a reader blocked on a lazy page (it needs
# a slow store with `verify_writes: false`; see bench/lazy.exs).
#
#   ALAMBIQUE_BENCH_ROWS      rows of the batch                 (default "4500")
#   ALAMBIQUE_BENCH_REPS      repetitions per case              (default "10")
#   ALAMBIQUE_BENCH_RUNS      runs of the latency case          (default "3")
#   ALAMBIQUE_BENCH_DELAY_MS  the second delay of `latency`     (default "2000")
#   ALAMBIQUE_BENCH_CELLS     cells of `reader` and `dirty`     (default "300")
#   ALAMBIQUE_BENCH_ROWS_N    rows per cell of `dirty`          (default "500")
#   ALAMBIQUE_BENCH_SIZES     value sizes in bytes of `raw`     (default "20 100")

defmodule Bench do
  alias Alambique.Cell.Db
  alias Alambique.Feed.{Capture, Pieces}
  alias Alambique.Sqlite
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}]

  @modes [
    cell: [hand_over: :cell],
    sender_burst: [hand_over: :sender, reader_close: :after_burst],
    sender_read: [hand_over: :sender, reader_close: :after_read],
    native: [hand_over: :native]
  ]

  def env(var, default), do: String.to_integer(System.get_env(var, default))

  def sizes,
    do:
      "ALAMBIQUE_BENCH_SIZES"
      |> System.get_env("20 100")
      |> String.split()
      |> Enum.map(&String.to_integer/1)

  def mean(list), do: Float.round(Enum.sum(list) / length(list), 1)

  def pct(list, p) do
    sorted = Enum.sort(list)
    Enum.at(sorted, min(round(p * (length(sorted) - 1)), length(sorted) - 1))
  end

  def stats(list), do: "mean=#{mean(list)} p50=#{pct(list, 0.5)} p99=#{pct(list, 0.99)}"

  # Instances

  def with_instance(feed, opts \\ [], fun) do
    dir = Alambique.TestDirs.new("bench")
    name = :"bench_handover_#{System.unique_integer([:positive])}"
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
               lease_ttl_ms: 60_000,
               feed: [window_ms: 60_000] ++ feed
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

  def pid(inst, cell), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

  # One transaction of `rows` rows of `bytes` bytes, ids from `from`.
  def big_write(inst, cell, from, rows, bytes) do
    sql =
      "WITH RECURSIVE c(i) AS (SELECT #{from} UNION ALL SELECT i + 1 FROM c WHERE i < #{from + rows - 1}) " <>
        "INSERT INTO t (id, v) SELECT i, hex(zeroblob(#{div(bytes, 2)})) FROM c"

    {:ok, _} = retry(fn -> Alambique.execute(inst, cell, sql) end)
  end

  def retry(fun) do
    case fun.() do
      {:error, :overloaded} -> retry(fun)
      other -> other
    end
  end

  # Until the cell published what it acknowledged.
  def published(inst, cell) do
    pid = pid(inst, cell)

    wait(fn ->
      s = :sys.get_state(pid)
      s.feed_backlog == 0 and s.feed_published == s.pipe.acked.txid and s.feed_tick == nil
    end)
  end

  def wait(fun, timeout \\ 120_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        fun.() -> :done
        System.monotonic_time(:millisecond) > deadline -> raise "timeout"
        true -> Process.sleep(5) && :wait
      end
    end)
    |> Enum.find(&(&1 == :done))
  end

  # raw

  def raw(rows, reps) do
    for bytes <- sizes() do
      dir = Alambique.TestDirs.new("bench-raw")
      {:ok, conn} = Sqlite.open(Path.join(dir, "raw.db"))

      try do
        :ok = Sqlite.execute(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        :ok = Capture.install(conn, Capture.everything())

        :ok =
          Sqlite.execute(
            conn,
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < #{rows}) " <>
              "INSERT INTO _alambique_feed (txid, ts, tbl, op, key, vals) SELECT 2, 1000, 't', 'insert', " <>
              "json_object('id', i), json_object('id', i, 'v', hex(zeroblob(#{div(bytes, 2)}))) FROM c"
          )

        sink = spawn_link(fn -> sink() end)
        raw_case(conn, sink, rows, bytes, reps)
      after
        Sqlite.close(conn)
        File.rm_rf(dir)
      end
    end

    :ok
  end

  defp sink do
    receive do
      _ -> sink()
    end
  end

  @step "SELECT count(*), sum(length(txid) + length(ts) + length(tbl) + length(op) + length(key) + coalesce(length(vals), 0) + seq) FROM _alambique_feed WHERE txid >= ?1 AND txid <= ?3 AND (txid > ?1 OR seq > ?2)"

  defp raw_case(conn, sink, rows, bytes, reps) do
    position = {1, -1}

    time = fn fun -> Enum.map(1..reps, fn _ -> elem(:timer.tc(fun), 0) end) end
    # A first run of each warms the page cache.
    _ = Capture.rows_from(conn, position, 1_000, nil)

    step = time.(fn -> {:ok, _} = Sqlite.run(conn, @step, [1, -1, 1_000]) end)
    cell = time.(fn -> {:ok, _} = Capture.rows_from(conn, position, 1_000, nil) end)
    whole = time.(fn -> {:ok, _, _} = Pieces.read(conn, position, 1_000, nil, nil) end)

    {:ok, pieces, _} = Pieces.read(conn, position, 1_000, nil, nil)
    copy = time.(fn -> send(sink, {:batches, pieces}) end)

    native =
      time.(fn ->
        {:ok, _, _, _} =
          Capture.send_rows(conn, position, 1_000, {nil, nil, nil}, sink, make_ref())
      end)

    shape = Enum.zip_with(whole, cell, &(&1 - &2))
    per = fn list -> Float.round(mean(list) / rows * 1000, 1) end

    for {name, list} <- [
          step: step,
          cell_select: cell,
          cell_shape: shape,
          cell_copy: copy,
          native: native
        ] do
      IO.puts(
        "raw rows=#{rows} value_bytes=#{bytes} part=#{name} us_per_batch: #{stats(list)} ns_per_row=#{per.(list)}"
      )
    end
  end

  # cell

  def cell(rows, reps) do
    for {mode, feed} <- @modes, bytes <- sizes() do
      with_instance(feed, fn inst ->
        big_write(inst, "c", 1, 10, bytes)
        published(inst, "c")
        pid = pid(inst, "c")

        samples =
          for rep <- 1..reps do
            :sys.get_state(pid)
            before = snapshot(pid)

            {write_us, _} =
              :timer.tc(fn -> big_write(inst, "c", 100_000 + rep * rows, rows, bytes) end)

            published(inst, "c")
            after_ = snapshot(pid)

            {write_us, after_.reductions - before.reductions, after_.heap - before.heap,
             after_.binaries - before.binaries}
          end

        {w, r, h, b} = {col(samples, 0), col(samples, 1), col(samples, 2), col(samples, 3)}

        IO.puts(
          "cell mode=#{mode} rows=#{rows} value_bytes=#{bytes} write_us: #{stats(w)} " <>
            "reductions: #{stats(r)} heap_words_growth: #{stats(h)} binary_bytes_growth: #{stats(b)} " <>
            "reductions_per_row=#{Float.round(mean(r) / rows, 1)}"
        )
      end)
    end

    :ok
  end

  defp col(samples, i), do: Enum.map(samples, &elem(&1, i))

  defp snapshot(pid) do
    {:binary, bins} = Process.info(pid, :binary)
    {:reductions, red} = Process.info(pid, :reductions)
    {:total_heap_size, heap} = Process.info(pid, :total_heap_size)
    %{reductions: red, heap: heap, binaries: bins |> Enum.map(&elem(&1, 1)) |> Enum.sum()}
  end

  # latency

  def latency(rows, reps, runs, delay) do
    bytes = hd(sizes())

    for {mode, feed} <- @modes, wait <- [0, delay] do
      us =
        for _run <- 1..runs, reduce: [] do
          acc ->
            with_instance(feed, fn inst ->
              big_write(inst, "c", 1, 10, bytes)
              published(inst, "c")

              acc ++
                for rep <- 1..reps do
                  big_write(inst, "c", 100_000 + rep * rows, rows, bytes)
                  if wait > 0, do: Process.sleep(wait)

                  {t, _} =
                    :timer.tc(fn ->
                      {:ok, _} =
                        retry(fn ->
                          Alambique.execute(
                            inst,
                            "c",
                            "INSERT INTO t (id, v) VALUES (-#{System.unique_integer([:positive])}, 'x')"
                          )
                        end)
                    end)

                  published(inst, "c")
                  t
                end
            end)
        end

      IO.puts(
        "latency mode=#{mode} rows=#{rows} after_ms=#{wait} runs=#{runs} reps=#{reps} first_write_us: #{stats(us)}"
      )
    end

    :ok
  end

  # reader

  def reader(cells, rows, reps) do
    dir = Alambique.TestDirs.new("bench-reader")
    path = Path.join(dir, "r.db")
    {:ok, w} = Db.open(path)
    :ok = Db.writable_script(w, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

    open_us =
      for _ <- 1..(reps * 10) do
        {t, {:ok, r}} = :timer.tc(fn -> Db.open_reader(path, 64) end)
        Db.close(r)
        t
      end

    Db.close(w)
    File.rm_rf(dir)
    IO.puts("reader open_close_us_per_range: #{stats(open_us)}")

    for {mode, feed} <- Enum.filter(@modes, &(elem(&1, 0) != :cell)) do
      with_instance(feed, [awake: [max_bytes: 4_000_000_000]], fn inst ->
        ids = Enum.map(1..cells, &"c#{&1}")

        for id <- ids,
            do:
              {:ok, _} =
                retry(fn -> Alambique.execute(inst, id, "INSERT INTO t (v) VALUES ('x')") end)

        for id <- ids, do: published(inst, id)

        {fds0, mem0} = {fds(), :erlang.memory(:total)}
        {:ok, peak} = Agent.start_link(fn -> {fds0, mem0} end)
        sampler = spawn_link(fn -> sample(peak) end)

        busy =
          ids
          |> Task.async_stream(
            fn id ->
              big_write(inst, id, 1_000, rows, 20)
              Alambique.checkpoint(inst, id)
            end,
            max_concurrency: 50,
            timeout: :infinity
          )
          |> Enum.count(&match?({:ok, {:error, :busy}}, &1))

        for id <- ids, do: published(inst, id)
        Process.unlink(sampler)
        Process.exit(sampler, :kill)
        {pf, pm} = Agent.get(peak, & &1)
        Agent.stop(peak)

        IO.puts(
          "reader mode=#{mode} cells=#{cells} rows=#{rows} peak_files_per_cell=#{Float.round((pf - fds0) / cells, 2)} " <>
            "peak_bytes_per_cell=#{div(pm - mem0, cells)} checkpoints_busy=#{busy} files_after=#{fds() - fds0}"
        )
      end)
    end

    :ok
  end

  defp fds, do: length(File.ls!("/proc/self/fd"))

  defp sample(peak) do
    Agent.update(peak, fn {f, m} -> {max(f, fds()), max(m, :erlang.memory(:total))} end)
    Process.sleep(10)
    sample(peak)
  end

  # dirty

  def dirty(cells, rows) do
    :erlang.system_flag(:microstate_accounting, true)

    for {mode, feed} <- @modes do
      with_instance(feed, [awake: [max_bytes: 4_000_000_000]], fn inst ->
        ids = Enum.map(1..cells, &"c#{&1}")

        for id <- ids,
            do:
              {:ok, _} =
                retry(fn -> Alambique.execute(inst, id, "INSERT INTO t (v) VALUES ('x')") end)

        for id <- ids, do: published(inst, id)

        :erlang.system_flag(:microstate_accounting, :reset)
        started = System.monotonic_time(:millisecond)

        ids
        |> Task.async_stream(&big_write(inst, &1, 1_000, rows, 20),
          max_concurrency: 50,
          timeout: :infinity
        )
        |> Stream.run()

        for id <- ids, do: published(inst, id)
        elapsed = System.monotonic_time(:millisecond) - started

        io =
          Enum.filter(
            :erlang.statistics(:microstate_accounting),
            &(&1.type == :dirty_io_scheduler)
          )

        total = io |> Enum.flat_map(&Map.values(&1.counters)) |> Enum.sum()
        sleep = io |> Enum.map(& &1.counters.sleep) |> Enum.sum()

        IO.puts(
          "dirty mode=#{mode} cells=#{cells} rows=#{rows} elapsed_ms=#{elapsed} " <>
            "dirty_io_busy_percent=#{Float.round(100 * (1 - sleep / max(total, 1)), 1)}"
        )
      end)
    end

    :ok
  end
end

cases = System.argv() |> Enum.map(&String.to_atom/1)
run? = fn name -> cases == [] or name in cases end

{rows, reps} =
  {Bench.env("ALAMBIQUE_BENCH_ROWS", "4500"), Bench.env("ALAMBIQUE_BENCH_REPS", "10")}

cells = Bench.env("ALAMBIQUE_BENCH_CELLS", "300")

if run?.(:raw), do: Bench.raw(rows, reps * 3)
if run?.(:cell), do: Bench.cell(rows, reps)

if run?.(:latency),
  do:
    Bench.latency(
      rows,
      reps,
      Bench.env("ALAMBIQUE_BENCH_RUNS", "3"),
      Bench.env("ALAMBIQUE_BENCH_DELAY_MS", "2000")
    )

if run?.(:reader), do: Bench.reader(cells, Bench.env("ALAMBIQUE_BENCH_ROWS_N", "500"), reps)
if run?.(:dirty), do: Bench.dirty(cells, Bench.env("ALAMBIQUE_BENCH_ROWS_N", "500"))
