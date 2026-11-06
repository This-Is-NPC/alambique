defmodule Alambique.ReplicaPropertiesTest do
  @moduledoc """
  Read replicas (design §21) under random writes, store latency and owner
  changes.

  Three instances share one store: `o` and `n` take turns owning the cell, `r`
  never owns it and keeps the replica. In one VM `r` cannot forward to the
  owner (it is not in its `:pg` scope), so a read that `r` sends to the owner
  answers `{:error, {:unavailable, _, _}}`; the properties check the reads
  that the replica answered.

    * R-bound: a read started at `q` with bound `B` contains every write
      acknowledged before `q - B - margin` (and nothing nobody wrote). Owner
      killed, takeovers, rewinds and store latency are part of the run.
    * R-readonly: no statement sent to a replica changes its file or the store.
    * R-bounds: more cells than `max_cells`, or bigger than `max_bytes`: the
      limits hold at every step.
    * R-switch: turning the switch off stops the replicas and removes their
      files within `marker_ttl_ms + poll_ms`.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AuditStore, Cell, Config, Replica}
  alias Alambique.Replica.Manager
  alias AlambiqueTestkit.MemoryStore

  @moduletag timeout: 600_000
  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  @replicas [poll_ms: 15, marker_ttl_ms: 100, refresh_timeout_ms: 400, idle_ms: 600_000]

  setup do
    ctx = setup_store()
    served = :counters.new(1, [])
    Map.put(ctx, :served, served)
  end

  # World

  defp world(ctx, replicas \\ []) do
    tag = System.unique_integer([:positive])
    prefix = "rp#{tag}"
    audit = AuditStore.wrap({MemoryStore, ctx.store_config}, fn -> 0 end)

    opts = [prefix: prefix, migrations: @schema, compaction: false, lease_ttl_ms: 20_000]
    plain = %{ctx | store: {MemoryStore, ctx.store_config}}
    audited = %{ctx | store: audit}
    replicas = Keyword.merge(@replicas, replicas)

    %{
      ctx: ctx,
      tag: tag,
      prefix: prefix,
      audit: elem(audit, 1),
      o: start_instance(plain, [name: :"o#{tag}"] ++ opts),
      n: start_instance(plain, [name: :"n#{tag}"] ++ opts),
      r: start_instance(audited, [name: :"r#{tag}", replicas: replicas] ++ opts)
    }
  end

  defp teardown(w) do
    MemoryStore.clear(w.ctx.store_config)
    for name <- [w.r, w.n, w.o], do: stop_supervised(name)
  end

  defp insert(inst, cell, n, blob \\ nil),
    do:
      Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, ?)", [n, blob],
        timeout: 5_000
      )

  defp us, do: System.monotonic_time(:microsecond)

  defp bounded(w, cell, ms, sql \\ "SELECT n FROM t ORDER BY n"),
    do: Alambique.query(w.r, cell, sql, [], consistency: {:bounded, ms}, timeout: 5_000)

  # Waits until a replica of `cell` on `r` answers.
  defp ready(w, cell) do
    eventually(fn -> match?({:ok, _}, bounded(w, cell, 60_000)) end, 10_000)
  end

  defp replica_files(w) do
    Path.wildcard(Path.join(Replica.dir(Config.fetch!(w.r)), "*/replica.db"))
  end

  defp dir_bytes(w) do
    Replica.dir(Config.fetch!(w.r))
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.flat_map(fn path ->
      # A restore renames its temporary file, and a stop removes the directory.
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} -> [size]
        _ -> []
      end
    end)
    |> Enum.sum()
  end

  # R-bound

  defp bound_command do
    frequency([
      {8, constant(:write)},
      {8, map(member_of([1, 5, 20, 100]), &{:probe, &1})},
      {6, map(member_of([1, 5, 20, 100, 400]), &{:read, &1})},
      {3, map(integer(0..30), &{:latency, &1})},
      {3, map(integer(0..40), &{:sleep, &1})},
      {1, constant(:kill)},
      {1, constant(:take_over)},
      {1, constant(:mark)},
      {1, constant(:rewind)}
    ])
  end

  property "R-bound: a read has every write acknowledged before its bound", ctx do
    check all(
            commands <- list_of(bound_command(), min_length: 8, max_length: 25),
            max_runs: 30,
            max_shrinking_steps: 0
          ) do
      run_bound(ctx, commands)
    end

    assert :counters.get(ctx.served, 1) > 0, "no read was answered by a replica"
  end

  defp run_bound(ctx, commands) do
    w = world(ctx)
    cell = "bound#{w.tag}"

    state = %{
      cell: cell,
      owner: w.o,
      seq: 0,
      acked: %{},
      removed: MapSet.new(),
      attempted: MapSet.new([0]),
      marks: [],
      fault: nil
    }

    try do
      {:ok, _} = insert(w.o, cell, 0)
      state = %{state | acked: %{0 => us()}}
      :ok = Alambique.replicas(w.o, cell, true)
      ready(w, cell)
      Enum.reduce(commands, state, &step(&1, &2, w, ctx))
    after
      teardown(w)
    end
  end

  defp step(:write, s, _w, _ctx), do: write(s)

  defp step({:latency, ms}, s, _w, ctx) do
    if s.fault, do: MemoryStore.remove(ctx.store_config, s.fault)
    fault = if ms > 0, do: MemoryStore.inject(ctx.store_config, kind: :latency, ms: ms)
    %{s | fault: fault}
  end

  defp step({:sleep, ms}, s, _w, _ctx) do
    Process.sleep(ms)
    s
  end

  defp step(:kill, s, _w, _ctx) do
    with pid when is_pid(pid) <- Cell.whereis(Config.fetch!(s.owner), s.cell) do
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        5_000 -> raise "owner did not stop"
      end
    end

    s
  end

  defp step(:take_over, s, w, _ctx) do
    other = if s.owner == w.o, do: w.n, else: w.o

    case Alambique.take_over(other, s.cell, :test, timeout: 5_000) do
      {:ok, _} -> %{s | owner: other}
      {:error, _} -> s
    end
  end

  defp step(:mark, s, _w, _ctx) do
    case Alambique.position(s.owner, s.cell, timeout: 5_000) do
      {:ok, %{txid: txid}} -> %{s | marks: [{txid, Map.keys(s.acked)} | s.marks]}
      {:error, _} -> s
    end
  end

  # Undoes everything acknowledged after the newest mark.
  defp step(:rewind, %{marks: []} = s, _w, _ctx), do: s

  defp step(:rewind, %{marks: [{txid, kept} | _]} = s, _w, _ctx) do
    case Alambique.rewind(s.owner, s.cell, {:txid, txid}, timeout: 10_000) do
      {:ok, _} ->
        gone = Map.keys(s.acked) -- kept
        %{s | removed: MapSet.union(s.removed, MapSet.new(gone)), marks: []}

      {:error, _} ->
        s
    end
  end

  defp step({:read, ms}, s, w, ctx), do: check_read(s, w, ctx, ms)

  # A write, then a read that starts once the write is older than the bound:
  # the replica must have it.
  defp step({:probe, ms}, s, w, ctx) do
    s = write(s)
    Process.sleep(ms + 2)
    check_read(s, w, ctx, ms)
  end

  defp write(s) do
    n = s.seq + 1
    s = %{s | seq: n, attempted: MapSet.put(s.attempted, n)}

    case insert(s.owner, s.cell, n) do
      {:ok, _} -> %{s | acked: Map.put(s.acked, n, us())}
      {:error, _} -> s
    end
  end

  defp margin(ms), do: div(ms * 1_000 * 200 + 999_999, 1_000_000) + 1

  defp check_read(s, w, ctx, ms) do
    q = us()

    case bounded(w, s.cell, ms) do
      {:ok, %{rows: rows}} ->
        :counters.add(ctx.served, 1, 1)
        got = rows |> List.flatten() |> MapSet.new()

        must =
          for {n, at} <- s.acked, at < q - ms * 1_000 - margin(ms), n not in s.removed, do: n

        missing = Enum.reject(must, &MapSet.member?(got, &1))

        assert missing == [],
               "read with bound #{ms} ms lacks acknowledged writes #{inspect(missing)}"

        invented = MapSet.difference(got, s.attempted)
        assert MapSet.size(invented) == 0, "rows #{inspect(invented)} were never written"

      {:error, _} ->
        :ok
    end

    s
  end

  # R-readonly

  defp statement do
    one_of([
      member_of([
        "INSERT INTO t (n) VALUES (100)",
        "REPLACE INTO t (n) VALUES (1)",
        "UPDATE t SET v = x'00'",
        "DELETE FROM t",
        "DROP TABLE t",
        "CREATE TABLE u (x)",
        "CREATE INDEX i ON t (v)",
        "ALTER TABLE t ADD COLUMN w",
        "ATTACH DATABASE ':memory:' AS m",
        "ATTACH DATABASE '/tmp/alambique-replica-attach.db' AS m",
        "DETACH DATABASE m",
        "PRAGMA user_version = 9",
        "PRAGMA query_only = OFF",
        "PRAGMA journal_mode = DELETE",
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "PRAGMA writable_schema = ON",
        "PRAGMA page_size = 512",
        "PRAGMA locking_mode = EXCLUSIVE",
        "PRAGMA synchronous = OFF",
        "PRAGMA cache_size = 5",
        "PRAGMA mmap_size = 1048576",
        "VACUUM",
        "VACUUM INTO '/tmp/alambique-replica-vacuum.db'",
        "ANALYZE",
        "REINDEX",
        "BEGIN",
        "BEGIN IMMEDIATE",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT s",
        "RELEASE s",
        "WITH x AS (SELECT 1) DELETE FROM t",
        "SELECT 1; DELETE FROM t",
        "SELECT writefile('/tmp/alambique-replica-write', 'x')",
        "SELECT load_extension('x')",
        "PRAGMA integrity_check",
        "PRAGMA user_version",
        "SELECT count(*) FROM t",
        "SELECT * FROM sqlite_master"
      ]),
      map(string(:printable, max_length: 40), & &1)
    ])
  end

  property "R-readonly: no statement changes the replica or the store", ctx do
    check all(
            sqls <- list_of(statement(), min_length: 3, max_length: 12),
            max_runs: 10,
            max_shrinking_steps: 0
          ) do
      run_readonly(ctx, sqls)
    end
  end

  defp run_readonly(ctx, sqls) do
    w = world(ctx)
    cell = "ro#{w.tag}"

    try do
      for n <- 1..3, do: {:ok, _} = insert(w.o, cell, n, "blob#{n}")
      :ok = Alambique.replicas(w.o, cell, true)
      ready(w, cell)

      [file] = replica_files(w)
      before = {File.read!(file), AuditStore.puts(w.audit), AuditStore.deletes(w.audit)}
      stored = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n", w.prefix)

      for sql <- sqls do
        _ = bounded(w, cell, 60_000, sql)

        assert {File.read!(file), AuditStore.puts(w.audit), AuditStore.deletes(w.audit)} == before,
               "#{inspect(sql)} changed the replica file or wrote to the store"
      end

      assert {:ok, %{rows: [[1], [2], [3]]}} = bounded(w, cell, 60_000)
      assert durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n", w.prefix) == stored
    after
      File.rm("/tmp/alambique-replica-vacuum.db")
      File.rm("/tmp/alambique-replica-attach.db")
      teardown(w)
    end
  end

  # R-bounds

  property "R-bounds: more cells or bytes than the limits never exceed them", ctx do
    check all(
            max_cells <- integer(1..3),
            sizes <- list_of(integer(0..30_000), min_length: 4, max_length: 6),
            reads <- list_of(integer(0..5), min_length: 10, max_length: 40),
            max_runs: 8,
            max_shrinking_steps: 0
          ) do
      run_bounds(ctx, max_cells, sizes, reads)
    end
  end

  defp run_bounds(ctx, max_cells, sizes, reads) do
    max_bytes = 70_000
    w = world(ctx, max_cells: max_cells, max_bytes: max_bytes)
    config = Config.fetch!(w.r)

    cells =
      for {size, i} <- Enum.with_index([0 | sizes]) do
        cell = "b#{i}x#{w.tag}"
        {:ok, _} = insert(w.o, cell, 1, :binary.copy("x", size))
        :ok = Alambique.replicas(w.o, cell, true)
        cell
      end

    try do
      for i <- reads do
        cell = Enum.at(cells, rem(i, length(cells)))

        case bounded(w, cell, 60_000, "SELECT n FROM t") do
          {:ok, %{rows: [[1]]}} -> :ok
          {:error, {:unavailable, _, _}} -> :ok
        end

        usage = Manager.usage(config)
        assert usage.cells <= max_cells
        assert usage.bytes <= max_bytes
        assert length(replica_files(w)) <= max_cells
        assert dir_bytes(w) <= max_bytes
        Process.sleep(5)
      end

      # Every cell fits alone, so the limits did not simply keep replicas out.
      # Replicas start on a read, and refusals expire, so the reads go on.
      assert eventually(
               fn ->
                 bounded(w, hd(cells), 60_000, "SELECT n FROM t")
                 Manager.usage(config).cells >= 1
               end,
               10_000
             )
    after
      teardown(w)
    end
  end

  # R-switch

  property "R-switch: replicas stop and remove their files when the switch is off", ctx do
    check all(
            flips <- list_of(boolean(), min_length: 1, max_length: 4),
            max_runs: 6,
            max_shrinking_steps: 0
          ) do
      run_switch(ctx, flips)
    end
  end

  defp run_switch(ctx, flips) do
    w = world(ctx)
    cell = "sw#{w.tag}"
    config = Config.fetch!(w.r)
    settings = config.replicas
    # The next refresh after the marker's reading expires, plus its own time.
    limit = settings.marker_ttl_ms + settings.poll_ms + 1_000

    try do
      {:ok, _} = insert(w.o, cell, 1)

      for on? <- flips ++ [false] do
        :ok = Alambique.replicas(w.o, cell, true)
        ready(w, cell)
        assert [_] = replica_files(w)

        if on? do
          Process.sleep(2 * settings.marker_ttl_ms)
          assert [_] = replica_files(w)
        else
          :ok = Alambique.replicas(w.o, cell, false)
          started = System.monotonic_time(:millisecond)
          eventually(fn -> replica_files(w) == [] end, limit)
          assert System.monotonic_time(:millisecond) - started <= limit
          # The manager drops the row when the process is down.
          assert eventually(fn -> Manager.usage(config) == %{cells: 0, bytes: 0} end, limit)
          assert {:ok, false} = Alambique.replicas(w.r, cell)
        end
      end
    after
      teardown(w)
    end
  end
end
