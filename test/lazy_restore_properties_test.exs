defmodule Alambique.LazyRestorePropertiesTest do
  @moduledoc """
  Invariant H1 (design §16): reads served while a lazily restored file is
  still being filled equal the same reads on a fully restored copy, with
  writes and checkpoints in between, store faults and latency, and a crash;
  once the file is complete its checksum equals a full restore's.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.ReplicationHelpers

  alias Alambique.Cell.Db
  alias Alambique.Compaction
  alias Alambique.Replication
  alias Alambique.Replication.{Hydrator, Lazy, Native, Replicator}
  alias Alambique.Sqlite
  alias AlambiqueTestkit.MemoryStore

  @io_error "disk I/O error"

  # Generators

  defp simple_op_gen do
    one_of([
      gen all(k <- integer(1..40), n <- member_of([10, 300, 1500, 5000, 9000])) do
        {:put, k, n}
      end,
      map(integer(1..40), &{:del, &1}),
      map(integer(1..2), &{:fill, &1}),
      map(integer(1..2), &{:drop, &1}),
      map(integer(1..2), &{:create, &1})
    ])
  end

  defp op_gen do
    frequency([
      {6, simple_op_gen()},
      {2, map(list_of(simple_op_gen(), min_length: 1, max_length: 4), &{:txn, &1})},
      {1, map(list_of(simple_op_gen(), min_length: 1, max_length: 3), &{:rollback, &1})},
      {1, constant(:vacuum)}
    ])
  end

  defp read_gen do
    one_of([
      map(integer(1..40), &{:point, &1}),
      gen all(a <- integer(1..40), b <- integer(1..40)) do
        {:range, min(a, b), max(a, b)}
      end,
      constant(:aggregate),
      constant(:everything),
      constant(:schema),
      map(integer(1..2), &{:table, &1}),
      constant(:integrity)
    ])
  end

  defp command_gen do
    frequency([
      {6, map(read_gen(), &{:read, &1})},
      {4, map(list_of(op_gen(), min_length: 1, max_length: 3), &{:write, &1})},
      {3, constant(:sync)},
      {2, constant(:checkpoint)},
      {2,
       gen all(kind <- member_of([:error, :timeout]), times <- member_of([3, 40, :infinity])) do
         {:fault, kind, times}
       end},
      {1, constant(:heal)},
      {1, constant(:crash)}
    ])
  end

  # Statements. Values are deterministic, so that two databases that ran the
  # same operations hold the same bytes.

  defp blob(k, n), do: "CAST(replace(hex(zeroblob(#{n})), '00', printf('%02x', #{k})) AS BLOB)"

  defp sql({:put, k, n}), do: "INSERT OR REPLACE INTO kv (k, v) VALUES (#{k}, #{blob(k, n)})"
  defp sql({:del, k}), do: "DELETE FROM kv WHERE k = #{k}"
  defp sql({:create, i}), do: "CREATE TABLE IF NOT EXISTS t#{i} (a INTEGER PRIMARY KEY, b TEXT)"

  defp sql({:fill, i}),
    do: "INSERT INTO t#{i} (b) SELECT hex(k) || hex(length(v)) FROM kv LIMIT 20"

  defp sql({:drop, i}), do: "DROP TABLE IF EXISTS t#{i}"
  defp sql(:vacuum), do: "VACUUM"

  defp read_sql({:point, k}), do: "SELECT k, length(v), substr(v, 1, 16) FROM kv WHERE k = #{k}"

  defp read_sql({:range, a, b}),
    do: "SELECT k, length(v) FROM kv WHERE k BETWEEN #{a} AND #{b} ORDER BY k"

  defp read_sql(:aggregate), do: "SELECT count(*), sum(length(v)), min(k), max(k) FROM kv"
  defp read_sql(:everything), do: "SELECT k, v FROM kv ORDER BY k"
  defp read_sql(:schema), do: "SELECT type, name FROM sqlite_master ORDER BY name"
  defp read_sql({:table, i}), do: "SELECT count(*), sum(length(b)) FROM t#{i}"
  defp read_sql(:integrity), do: "PRAGMA integrity_check"

  defp run_sql(conn, sql), do: Db.writable(conn, fn -> Sqlite.execute(conn, sql) end)

  # One operation: `:ok` or `{:error, message}`; a failed operation leaves nothing behind.
  defp apply_op(conn, {kind, ops}) when kind in [:txn, :rollback] do
    ending = if kind == :txn, do: "COMMIT", else: "ROLLBACK"
    statements = ["BEGIN IMMEDIATE"] ++ Enum.map(ops, &sql/1) ++ [ending]

    result =
      Enum.reduce_while(statements, :ok, fn text, :ok ->
        case run_sql(conn, text) do
          :ok -> {:cont, :ok}
          error -> {:halt, error}
        end
      end)

    if result != :ok, do: run_sql(conn, "ROLLBACK")
    result
  end

  defp apply_op(conn, op), do: run_sql(conn, sql(op))

  defp read(conn, r) do
    case Db.run(conn, read_sql(r), []) do
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, _} = error -> error
    end
  end

  # Run

  defp run(seed, compact?, latency, commands) do
    Process.flag(:trap_exit, true)
    {_pid, {_, cfg} = store} = start_store()
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 150)

    try do
      cell = seed_store(ctx, dir, seed, compact?)
      model = open(ctx, cfg, dir, cell, latency)

      try do
        model = Enum.reduce_while(commands, model, &step/2)
        finish(model)
      after
        stop(model)
      end
    after
      File.rm_rf(dir)
    end
  end

  # The store holds a history of several files; `seed` is split over them.
  defp seed_store(ctx, dir, seed, compact?) do
    {conn, state} = new_replicator(dir, "owner.db")
    Sqlite.execute(conn, "CREATE TABLE IF NOT EXISTS t1 (a INTEGER PRIMARY KEY, b TEXT)")
    # Enough pages for hydration to take a while.
    for k <- 1..30, do: apply_op(conn, {:put, k, 3000})

    state =
      seed
      |> Enum.chunk_every(3)
      |> Enum.reduce(state, fn chunk, state ->
        Enum.each(chunk, &apply_op(conn, &1))
        {:ok, state} = Replicator.sync(state, ctx)
        state
      end)

    {:ok, state} = Replicator.sync(state, ctx)
    if compact?, do: {:ok, _} = Compaction.run(ctx, "cell", fanout: 2, snapshot_interval_txids: 2)
    assert state.txid >= 1
    Sqlite.close(conn)
    "cell"
  end

  defp open(ctx, cfg, dir, cell, latency) do
    # The reference: the same state, fully restored.
    ref_path = Path.join(dir, "reference.db")
    {:ok, _} = Replication.restore(ctx, cell, ref_path, :latest)
    {:ok, reference} = Sqlite.open(ref_path)

    path = Path.join(dir, "lazy.db")
    lazy_opts = %{min_bytes: 0, wait_ms: 300}
    {:ok, result} = Replication.restore(ctx, cell, path, :latest, lazy: lazy_opts)
    # Small reads, so that the file takes many requests to fill.
    lazy = Lazy.new(result, ctx, miss_pages: 2, batch_bytes: 256)

    if latency > 0,
      do: MemoryStore.inject(cfg, kind: :latency, ms: latency, ops: [:get_range])

    {:ok, hydrator} = Hydrator.start_link(lazy, concurrency: 2, fetch_ms: 60)
    {:ok, conn} = retry_io(fn -> Db.open(path) end)
    repl = Replicator.from_restore(result, 2, lazy: lazy)

    %{
      ctx: ctx,
      cfg: cfg,
      dir: dir,
      cell: cell,
      path: path,
      conn: conn,
      reference: reference,
      hydrator: hydrator,
      hydrated?: false,
      repl: repl,
      faulted?: false,
      dirty?: false,
      latency: latency,
      wait_ms: lazy_opts.wait_ms
    }
  end

  defp stop(model) do
    if Process.alive?(model.hydrator), do: Hydrator.stop(model.hydrator)
    Sqlite.close(model.conn)
    Sqlite.close(model.reference)
  end

  # A hydrator that finished tells us once.
  defp poll(%{hydrated?: true} = model), do: model

  defp poll(model) do
    hydrator = model.hydrator

    receive do
      {:hydrated, ^hydrator} ->
        assert {:ok, repl} = Replicator.hydrated(model.repl)
        %{model | repl: repl, hydrated?: true}

      {:hydration_failed, ^hydrator, reason} ->
        flunk("hydration failed: #{inspect(reason)}")
    after
      0 -> model
    end
  end

  # Without a fault a read that waits can still run out of time on a loaded
  # machine; it leaves nothing behind, so it is asked again.
  defp retry_unfaulted(%{faulted?: true}, fun), do: fun.()
  defp retry_unfaulted(_model, fun), do: retry_io(fun)

  defp retry_io(fun, tries \\ 20) do
    case fun.() do
      {:error, @io_error} when tries > 1 -> retry_io(fun, tries - 1)
      result -> result
    end
  end

  defp step(command, model), do: do_step(command, poll(model))

  defp do_step({:read, r}, model) do
    expected = read(model.reference, r)

    case retry_unfaulted(model, fn -> read(model.conn, r) end) do
      {:error, @io_error} ->
        assert model.faulted?, "a read failed without a fault: #{inspect(r)}"

      # integrity_check reports a page it cannot read as a row, not as an error.
      actual when r == :integrity and model.faulted? ->
        assert match?({:ok, _}, actual)

      actual ->
        assert actual == expected, "read #{inspect(r)} differs from the full restore"
    end

    {:cont, model}
  end

  defp do_step({:write, ops}, model) do
    model =
      Enum.reduce(ops, model, fn op, model ->
        case retry_unfaulted(model, fn -> apply_op(model.conn, op) end) do
          :ok ->
            assert apply_op(model.reference, op) == :ok
            %{model | dirty?: true}

          {:error, @io_error} ->
            assert model.faulted?, "a write failed without a fault: #{inspect(op)}"
            model

          {:error, message} ->
            assert apply_op(model.reference, op) == {:error, message}
            model
        end
      end)

    {:cont, model}
  end

  defp do_step(:sync, model), do: {:cont, sync(model)}

  defp do_step(:checkpoint, model) do
    case Replicator.checkpoint(model.repl, model.conn) do
      {:ok, repl} -> {:cont, %{model | repl: repl}}
      {:error, _} -> {:cont, model}
    end
  end

  defp do_step({:fault, kind, times}, model) do
    MemoryStore.inject(model.cfg, kind: kind, ops: [:get_range], times: times, ms: 5)
    {:cont, %{model | faulted?: true}}
  end

  defp do_step(:heal, model) do
    MemoryStore.clear(model.cfg)
    {:cont, %{model | faulted?: false}}
  end

  defp do_step(:crash, model) do
    model = quiesce(model)
    # Nobody tells the store; the next owner restores what was acknowledged.
    Process.unlink(model.hydrator)
    Process.exit(model.hydrator, :kill)
    Sqlite.close(model.conn)

    path = Path.join(model.dir, "after-crash.db")
    assert {:ok, result} = Replication.restore(model.ctx, model.cell, path, :latest)
    assert result.checksum == model.repl.checksum
    assert dump_file(path) == dump(model.reference)
    {:halt, %{model | conn: model.reference}}
  end

  # A failed sync is retried: the committed transactions stay in the WAL.
  defp sync(model) do
    case Replicator.sync(model.repl, model.ctx) do
      {:ok, repl} -> %{model | repl: repl}
      {:error, _} -> model
    end
  end

  # No faults, everything replicated.
  defp quiesce(model) do
    MemoryStore.clear(model.cfg)

    model = %{model | faulted?: false}

    Enum.reduce_while(1..20, model, fn _, model ->
      model = sync(model)

      case Native.wal_pending(model.path <> "-wal", model.repl.page_size, model.repl.wal_pos) do
        {:ok, false} -> {:halt, model}
        _ -> {:cont, model}
      end
    end)
  end

  defp await_hydrated(%{hydrated?: true} = model), do: model

  defp await_hydrated(model) do
    hydrator = model.hydrator

    receive do
      {:hydrated, ^hydrator} ->
        assert {:ok, repl} = Replicator.hydrated(model.repl)
        %{model | repl: repl, hydrated?: true}

      {:hydration_failed, ^hydrator, reason} ->
        flunk("hydration failed: #{inspect(reason)}")
    after
      20_000 -> flunk("hydration did not finish")
    end
  end

  defp finish(%{conn: conn, reference: conn}), do: :ok

  defp finish(model) do
    model = model |> quiesce() |> await_hydrated()

    # Same reads, now on a complete file.
    for r <- [:aggregate, :everything, :schema, {:table, 1}, :integrity],
        do: assert(read(model.conn, r) == read(model.reference, r))

    assert {:ok, repl} = Replicator.checkpoint(model.repl, model.conn)
    Sqlite.close(model.conn)

    # The complete file is what a full restore of the store gives.
    {:ok, _sums, checksum, _pages} = Native.file_state(model.path, repl.page_size)
    assert checksum == repl.checksum

    full = Path.join(model.dir, "full.db")
    assert {:ok, result} = Replication.restore(model.ctx, model.cell, full, :latest)
    assert result.checksum == repl.checksum
    assert result.txid == repl.txid
    assert dump_file(full) == dump_file(model.path)
    assert dump_file(full) == dump(model.reference)
    {:ok, conn} = Db.open(model.path)
    %{model | conn: conn}
    :ok
  end

  @tag timeout: 900_000
  property "H1: reads during hydration equal reads after a full restore" do
    check all(
            seed <- list_of(op_gen(), min_length: 4, max_length: 14),
            compact? <- boolean(),
            latency <- member_of([0, 3, 10]),
            commands <- list_of(command_gen(), min_length: 3, max_length: 25),
            max_runs: String.to_integer(System.get_env("H1_RUNS", "40"))
          ) do
      run(seed, compact?, latency, commands)
    end
  end
end
