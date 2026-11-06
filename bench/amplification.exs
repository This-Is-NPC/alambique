# Bytes the store receives and keeps for one mailbox, against what the
# application wrote (design section 9, docs/how-to-size-storage.md).
#
#   mise run bench:amplification               # 1 simulated hour
#   mise run bench:amplification -- 240        # simulated hours (10 days)
#   ALAMBIQUE_BENCH_SPEED=3600 mise run bench:amplification -- 240
#
# One cell holds a mail box: 13 messages of 75 KB per hour (the peak hour of
# a mailbox), each one a transaction that inserts the message with its
# label and moves the counters of the mailbox, and half a slot later a second
# transaction that marks it read. The schema has a table of messages with five
# indexes. The instance has the in-memory store, the default options and the
# change feed on (no consumer, as in a service that publishes and never
# prunes).
#
# Compressed time. The instance clock (`:clock`) is a manual clock that the
# bench moves by the simulated time of each slot, so snapshots, retention, the
# grace period before deletion, the late window of the feed and the age of a
# feed object follow the simulated time. A timer that the VM runs in real time
# cannot follow it, so the bench divides these by SPEED (never below 1 ms),
# which keeps the ratio between a timer and the writes: `compaction`
# `interval_ms`, `max_age_ms` and `retry_ms`, the `trash` `interval_ms` and the
# `feed` `prune_interval_ms`. Everything else is the library's default. The
# lease TTL is longer than the whole run, so the cell keeps its lease on the
# manual clock (renewals are objects of about 130 bytes every ttl/3; their
# count is added from the formula in the report).
#
# A simulated hour takes 3600 / SPEED seconds: SPEED 60 is the pace at which
# the timers and the commits keep a realistic order, SPEED 3600 runs a day in
# 24 s.
#
# The store is wrapped to count what is put, by kind of object, and what is
# stored after every put and delete. The report has, for each stage (after
# each of the first hours and at the end): the bytes the application wrote
# (the size of the message bodies, `logical`), the bytes put to the store by
# kind, the bytes left in the store by kind, the size of the database and the
# latency of a write acknowledgement.
#
#   ALAMBIQUE_BENCH_SPEED       simulated seconds per real second     (default "60")
#   ALAMBIQUE_BENCH_MESSAGES    messages per hour                     (default "13")
#   ALAMBIQUE_BENCH_BODY_BYTES  size of a body                        (default "75000")
#   ALAMBIQUE_BENCH_STORE_MS    latency added to every store request  (default "0")
#   ALAMBIQUE_BENCH_STAGES      simulated hours at which to report, space-separated;
#                               the last one is also the length of the run
#                               (default: the argument, or "1")
#   ALAMBIQUE_BENCH_OPTS        extra Alambique options, an Elixir keyword list
#                               evaluated at start; `compaction`, `retention`,
#                               `feed` and `trash` are merged into the bench's
#                               (default none)

defmodule Meter do
  @moduledoc false
  # Wraps a store: counts the bytes of every successful put by kind of object
  # and keeps the size of every object that is stored.
  @behaviour Alambique.Store

  @kinds [:segment, :fence, :merged, :snapshot, :feed, :lease, :trash, :other]

  def kinds, do: @kinds

  def wrap({_m, _c} = inner),
    do: {__MODULE__, %{inner: inner, table: :ets.new(:meter, [:public, :set])}}

  def kind(key, size) do
    cond do
      String.contains?(key, "/feed/") ->
        :feed

      String.ends_with?(key, "/lease") ->
        :lease

      String.contains?(key, "/trash") ->
        :trash

      match = Regex.run(~r|/cells/[^/]+/(000\d)/[0-9a-f]{16}-[0-9a-f]{16}\.ltx\z|, key) ->
        level(match, size)

      true ->
        :other
    end
  end

  defp level([_, "0000"], size), do: if(size < 256, do: :fence, else: :segment)
  defp level([_, "0009"], _size), do: :snapshot
  defp level(_, _size), do: :merged

  # `%{written: %{kind => {puts, bytes}}, stored: %{kind => {objects, bytes}}}`
  def read(%{table: t}) do
    written =
      for kind <- @kinds, into: %{} do
        {kind,
         case(:ets.lookup(t, {:written, kind}),
           do: (
             [{_, n, b}] -> {n, b}
             [] -> {0, 0}
           )
         )}
      end

    stored =
      t
      |> :ets.select([{{{:stored, :_}, :"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}])
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    stored =
      for kind <- @kinds, into: %{} do
        sizes = Map.get(stored, kind, [])
        {kind, {length(sizes), Enum.sum(sizes)}}
      end

    %{written: written, stored: stored}
  end

  # Counts the puts and deletes, to see when the store is quiet (the reaper
  # and the compactor list it on their timers even when idle).
  def requests(%{table: t}), do: :ets.lookup_element(t, :requests, 2, 0)

  defp touch(%{table: t}), do: :ets.update_counter(t, :requests, 1, {:requests, 0})

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}, table: t} = s, key, body, condition, meta) do
    touch(s)

    case m.put(c, key, body, condition, meta) do
      {:ok, _etag} = ok ->
        size = IO.iodata_length(body)
        kind = kind(key, size)
        :ets.update_counter(t, {:written, kind}, [{2, 1}, {3, size}], {{:written, kind}, 0, 0})
        :ets.insert(t, {{:stored, key}, kind, size})
        ok

      other ->
        other
    end
  end

  @impl true
  def get(%{inner: {m, c}, table: t}, key) do
    result = m.get(c, key)
    with {:ok, body, _etag, _meta} <- result, do: read(t, byte_size(body))
    result
  end

  @impl true
  def get_range(%{inner: {m, c}, table: t}, key, offset, length) do
    result = m.get_range(c, key, offset, length)
    with {:ok, body, _size, _meta} <- result, do: read(t, byte_size(body))
    result
  end

  defp read(t, size), do: :ets.update_counter(t, :read, [{2, 1}, {3, size}], {:read, 0, 0})

  @doc "`{requests, bytes}` of the gets so far."
  def reads(%{table: t}) do
    case :ets.lookup(t, :read) do
      [{_, n, bytes}] -> {n, bytes}
      [] -> {0, 0}
    end
  end

  @impl true
  def head(%{inner: {m, c}}, key), do: m.head(c, key)

  @impl true
  def list(%{inner: {m, c}}, prefix, start_after, limit),
    do: m.list(c, prefix, start_after, limit)

  @impl true
  def delete(%{inner: {m, c}, table: t} = s, key) do
    touch(s)

    case m.delete(c, key) do
      :ok ->
        :ets.delete(t, {:stored, key})
        :ok

      other ->
        other
    end
  end
end

defmodule Bytes do
  @moduledoc false
  # The bytes the telemetry events report, by the kind of object the store
  # calls them, to compare with what the store received.

  @table :bench_telemetry
  @events [
    [:alambique, :replication, :sync],
    [:alambique, :feed, :publish],
    [:alambique, :compaction, :merge],
    [:alambique, :compaction, :snapshot]
  ]

  def attach do
    :ets.new(@table, [:named_table, :public, :set])
    :telemetry.attach_many("bench-amplification", @events, &__MODULE__.handle/4, nil)
  end

  def detach, do: :telemetry.detach("bench-amplification")

  def read do
    for kind <- [:segment, :fence, :merged, :snapshot, :feed], into: %{} do
      {kind, :ets.lookup_element(@table, kind, 2, 0)}
    end
  end

  def handle([:alambique, :replication, :sync], %{bytes: bytes}, %{result: :ok} = meta, _),
    do: add(if(meta.kind == :fence, do: :fence, else: :segment), bytes)

  def handle([:alambique, :feed, :publish], %{bytes: bytes}, _meta, _), do: add(:feed, bytes)

  def handle([:alambique, :compaction, :merge], %{bytes: bytes}, _meta, _),
    do: add(:merged, bytes)

  def handle([:alambique, :compaction, :snapshot], %{bytes: bytes}, _meta, _),
    do: add(:snapshot, bytes)

  defp add(kind, bytes), do: :ets.update_counter(@table, kind, bytes, {kind, 0})
end

defmodule Bench do
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @instance :bench_amplification
  @cell "mailbox"
  @hour_ms 3_600_000
  @schema [
    {1,
     """
     CREATE TABLE mailboxes (
       id INTEGER PRIMARY KEY,
       name TEXT NOT NULL UNIQUE,
       uid_next INTEGER NOT NULL DEFAULT 1,
       total INTEGER NOT NULL DEFAULT 0,
       unseen INTEGER NOT NULL DEFAULT 0
     );
     CREATE TABLE messages (
       id INTEGER PRIMARY KEY,
       mailbox_id INTEGER NOT NULL REFERENCES mailboxes (id),
       uid INTEGER NOT NULL,
       thread_id INTEGER,
       message_id TEXT NOT NULL,
       subject TEXT NOT NULL,
       from_addr TEXT NOT NULL,
       to_addrs TEXT NOT NULL,
       received_at INTEGER NOT NULL,
       size INTEGER NOT NULL,
       flags INTEGER NOT NULL DEFAULT 0,
       body TEXT NOT NULL
     );
     CREATE UNIQUE INDEX messages_uid ON messages (mailbox_id, uid);
     CREATE INDEX messages_received ON messages (mailbox_id, received_at);
     CREATE INDEX messages_thread ON messages (thread_id);
     CREATE INDEX messages_message_id ON messages (message_id);
     CREATE INDEX messages_from ON messages (from_addr);
     CREATE TABLE message_labels (
       message_id INTEGER NOT NULL,
       label TEXT NOT NULL,
       PRIMARY KEY (message_id, label)
     ) WITHOUT ROWID;
     CREATE INDEX message_labels_label ON message_labels (label, message_id);
     INSERT INTO mailboxes (id, name) VALUES (1, 'INBOX');
     """}
  ]

  def run(stages, opts) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, mem} = MemoryStore.start_link()
    meter = Meter.wrap({MemoryStore, MemoryStore.config(mem)})
    {Meter, meter_state} = meter

    if opts.store_ms > 0,
      do: MemoryStore.inject(MemoryStore.config(mem), kind: :latency, ms: opts.store_ms)

    start = :os.system_time(:millisecond)
    world = ManualClock.new(start)
    speed = opts.speed

    {:ok, sup} =
      Supervisor.start_link(
        [{Alambique, instance_opts(meter, dir, world, speed, opts.extra)}],
        strategy: :one_for_one
      )

    Bytes.attach()

    try do
      {:ok, _} = Alambique.touch(@instance, @cell)
      wait_quiet(meter_state, speed)
      header(opts, stages)

      loop(stages, 1, meter_state, world, opts, %{
        latencies: [],
        logical: 0,
        from: 0,
        mark: mark(meter_state)
      })
    after
      Bytes.detach()
      Supervisor.stop(sup)
      GenServer.stop(mem)
      File.rm_rf(dir)
    end
  end

  defp instance_opts(store, dir, world, speed, extra) do
    scaled = fn ms -> max(div(ms, speed), 1) end

    sections = [
      compaction: [
        interval_ms: scaled.(5_000),
        max_age_ms: scaled.(300_000),
        retry_ms: scaled.(30_000)
      ],
      trash: [interval_ms: scaled.(300_000)],
      feed: [prune_interval_ms: scaled.(3_600_000)]
    ]

    base = [
      name: @instance,
      store: store,
      prefix: "bench",
      data_dir: dir,
      migrations: @schema,
      clock: {ManualClock, ManualClock.config(world, 0)},
      lease_ttl_ms: 365 * 24 * @hour_ms,
      idle_timeout_ms: 365 * 24 * @hour_ms
    ]

    merged =
      Enum.reduce(sections, extra, fn {key, defaults}, acc ->
        Keyword.update(acc, key, defaults, &merge_section(defaults, &1))
      end)

    Keyword.merge(base, merged)
  end

  # `false` turns the section off, as in the instance options.
  defp merge_section(_defaults, false), do: false
  defp merge_section(defaults, given), do: Keyword.merge(defaults, given)

  # One simulated hour per iteration; reports at the end of each stage.
  defp loop([], _hour, _meter, _world, _opts, _acc), do: :ok

  defp loop([stage | rest], hour, meter, world, opts, acc) do
    acc = hour(hour, meter, world, opts, acc)

    if hour == stage do
      wait_quiet(meter, opts.speed)
      report(stage, meter, acc)

      loop(rest, hour + 1, meter, world, opts, %{
        acc
        | mark: mark(meter),
          latencies: [],
          logical: 0,
          from: hour
      })
    else
      loop([stage | rest], hour + 1, meter, world, opts, acc)
    end
  end

  # `messages` slots; in each, a message arrives and half a slot later the
  # user reads the previous one.
  defp hour(hour, _meter, world, opts, acc) do
    slot_ms = div(@hour_ms, opts.messages)
    half_real = div(slot_ms, 2 * opts.speed)

    Enum.reduce(1..opts.messages, acc, fn n, acc ->
      Process.sleep(half_real)
      ManualClock.advance(world, div(slot_ms, 2))
      acc = insert(acc, hour, n, world, opts)
      Process.sleep(half_real)
      ManualClock.advance(world, slot_ms - div(slot_ms, 2))
      read(acc, hour, n, opts)
    end)
  end

  defp insert(acc, hour, n, world, opts) do
    id = (hour - 1) * opts.messages + n
    body = Alambique.BenchText.body(id, opts.body_bytes)
    at = ManualClock.world_ms(world)

    {us, {:ok, :ok}} =
      :timer.tc(fn ->
        Alambique.transaction(@instance, @cell, fn tx ->
          {:ok, _} =
            Alambique.Tx.execute(
              tx,
              """
              INSERT INTO messages (mailbox_id, uid, thread_id, message_id, subject, from_addr,
                                    to_addrs, received_at, size, body)
              SELECT 1, uid_next, ?, ?, ?, ?, 'user@example.org', ?, ?, ? FROM mailboxes WHERE id = 1
              """,
              [
                rem(id, 7),
                "<m#{id}.#{at}@mail.example.net>",
                "Subject #{id}",
                "sender#{rem(id, 40)}@example.com",
                at,
                byte_size(body),
                body
              ]
            )

          {:ok, _} =
            Alambique.Tx.execute(
              tx,
              "INSERT INTO message_labels (message_id, label) VALUES (last_insert_rowid(), 'inbox')"
            )

          {:ok, _} =
            Alambique.Tx.execute(
              tx,
              "UPDATE mailboxes SET uid_next = uid_next + 1, total = total + 1, unseen = unseen + 1 WHERE id = 1"
            )

          :ok
        end)
      end)

    %{acc | latencies: [us | acc.latencies], logical: acc.logical + byte_size(body)}
  end

  defp read(acc, hour, n, opts) do
    id = max((hour - 1) * opts.messages + n - 1, 1)

    {us, _} =
      :timer.tc(fn ->
        Alambique.transaction(@instance, @cell, fn tx ->
          {:ok, _} =
            Alambique.Tx.execute(tx, "UPDATE messages SET flags = flags | 1 WHERE id = ?", [id])

          {:ok, _} =
            Alambique.Tx.execute(
              tx,
              "UPDATE mailboxes SET unseen = max(unseen - 1, 0) WHERE id = 1"
            )

          :ok
        end)
      end)

    %{acc | latencies: [us | acc.latencies]}
  end

  # Quiet: no store request for longer than the slowest timer of the
  # background work, which runs on real time.
  defp wait_quiet(meter, speed) do
    quiet = max(div(300_000, speed) * 3, 1_000) + 500
    wait_quiet(meter, quiet, Meter.requests(meter), System.monotonic_time(:millisecond))
  end

  defp wait_quiet(meter, quiet, seen, since) do
    Process.sleep(100)
    now = Meter.requests(meter)
    clock = System.monotonic_time(:millisecond)

    cond do
      now != seen -> wait_quiet(meter, quiet, now, clock)
      clock - since >= quiet -> :ok
      true -> wait_quiet(meter, quiet, seen, since)
    end
  end

  # Report

  defp header(opts, stages) do
    IO.puts(
      "== #{opts.messages} messages of #{opts.body_bytes} bytes per hour, speed #{opts.speed}, " <>
        "store latency #{opts.store_ms} ms, stages (hours) #{inspect(stages)}"
    )

    IO.puts("extra options: #{inspect(opts.extra)}")
  end

  defp mark(meter), do: Map.put(Meter.read(meter), :telemetry, Bytes.read())

  # What a node reads to wake the cell from the store alone: a restore of the
  # latest state into an empty file.
  defp restore_cost(meter_state, store) do
    ctx =
      Alambique.Replication.ctx(store: store, prefix: "bench", node: "bench", timeout_ms: 120_000)

    path = Path.join(Alambique.TestDirs.new("restore"), "restore.db")
    {n0, b0} = Meter.reads(meter_state)
    started = System.monotonic_time(:millisecond)
    {:ok, result} = Alambique.Replication.restore(ctx, @cell, path, :latest)
    ms = System.monotonic_time(:millisecond) - started
    {n1, b1} = Meter.reads(meter_state)
    File.rm_rf(Path.dirname(path))
    {n1 - n0, b1 - b0, result.segments, ms}
  end

  defp report(hour, meter, acc) do
    now = Meter.read(meter)
    {reads, read_bytes, files, restore_ms} = restore_cost(meter, {Meter, meter})
    telemetry = Bytes.read()
    hours = hour - acc.from

    {:ok, %{rows: [[db]]}} =
      Alambique.query(
        @instance,
        @cell,
        "SELECT page_count * page_size FROM pragma_page_count, pragma_page_size"
      )

    written = diff(now.written, acc.mark.written)
    put_total = written |> Map.values() |> Enum.map(&elem(&1, 1)) |> Enum.sum()
    stored_total = now.stored |> Map.values() |> Enum.map(&elem(&1, 1)) |> Enum.sum()
    {leases, lease_stored} = now.stored.lease
    lease = leases * 360 * lease_stored

    IO.puts("\n-- stage: simulated hour #{hour} (the window of #{hours} h before)")

    IO.puts(
      "logical (bodies written)  #{mib(acc.logical)} MiB   (#{mib(acc.logical / hours)} MiB per hour)"
    )

    IO.puts("database                  #{mib(db)} MiB")

    IO.puts(
      "kind        puts   put MiB  per hour MiB  x logical   stored objs  stored MiB  telemetry MiB"
    )

    for kind <- Meter.kinds() do
      {puts, bytes} = Map.fetch!(written, kind)
      {objects, stored} = Map.fetch!(now.stored, kind)

      IO.puts(
        "#{String.pad_trailing(to_string(kind), 10)} #{pad(puts, 6)} #{pad(mib(bytes), 9)} #{pad(mib(bytes / hours), 13)} " <>
          "#{pad(ratio(bytes, acc.logical), 9)}  #{pad(objects, 11)} #{pad(mib(stored), 11)}  " <>
          pad(telemetry_mib(kind, telemetry, acc.mark.telemetry), 13)
      )
    end

    IO.puts(
      "total      #{pad("", 6)} #{pad(mib(put_total), 9)} #{pad(mib(put_total / hours), 13)} " <>
        "#{pad(ratio(put_total, acc.logical), 9)}  #{pad("", 11)} #{pad(mib(stored_total), 11)}"
    )

    IO.puts(
      "lease renewals are not simulated: #{mib(lease)} MiB per hour (one object of #{lease_stored} bytes every ttl / 3 = 10 s)"
    )

    IO.puts("stored / database: #{ratio(stored_total, db)}")

    lat = Enum.sort(acc.latencies)

    IO.puts(
      "restore of the latest state: #{mib(read_bytes)} MiB (#{ratio(read_bytes, db)} times the database) " <>
        "in #{reads} reads, #{files} files applied, #{restore_ms} ms at the store's latency"
    )

    IO.puts(
      "write ack (us), n=#{length(lat)}: p50 #{pct(lat, 0.5)} p95 #{pct(lat, 0.95)} p99 #{pct(lat, 0.99)} max #{List.last(lat)}"
    )
  end

  # `-` for the kinds no event reports: the lease, the trash manifests and the markers.
  defp telemetry_mib(kind, now, before) do
    if Map.has_key?(now, kind), do: mib(now[kind] - before[kind]), else: "-"
  end

  defp diff(now, before),
    do:
      Map.new(now, fn {kind, {n, b}} ->
        {kind, {n - elem(before[kind], 0), b - elem(before[kind], 1)}}
      end)

  defp pct([], _p), do: nil
  defp pct(sorted, p), do: Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))

  defp mib(bytes), do: Float.round(bytes / 1_048_576, 2)
  defp ratio(_a, 0), do: "-"
  defp ratio(a, b), do: Float.round(a / b, 2)
  defp pad(value, width), do: value |> to_string() |> String.pad_leading(width)
end

args = System.argv()

stages =
  case System.get_env("ALAMBIQUE_BENCH_STAGES") do
    nil -> [String.to_integer(List.first(args) || "1")]
    list -> list |> String.split() |> Enum.map(&String.to_integer/1)
  end

env = fn name, default ->
  String.to_integer(System.get_env("ALAMBIQUE_BENCH_" <> name, default))
end

extra =
  case System.get_env("ALAMBIQUE_BENCH_OPTS") do
    nil -> []
    code -> code |> Code.eval_string() |> elem(0)
  end

opts = %{
  speed: env.("SPEED", "60"),
  messages: env.("MESSAGES", "13"),
  body_bytes: env.("BODY_BYTES", "75000"),
  store_ms: env.("STORE_MS", "0"),
  extra: extra
}

Alambique.TestDirs.sweep()

try do
  Bench.run(stages, opts)
after
  Alambique.TestDirs.clean()
end
