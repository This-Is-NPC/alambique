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
  alias AlambiqueTestkit.{HostileStore, MemoryStore}

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

  defp damage_gen,
    do: one_of([map(integer(0..99), &{:flip, &1}), member_of([:short, :shifted])])

  defp command_gen do
    frequency([
      {6, map(read_gen(), &{:read, &1})},
      {4, map(list_of(op_gen(), min_length: 1, max_length: 3), &{:write, &1})},
      {3, constant(:sync)},
      {2, constant(:checkpoint)},
      {2,
       gen all(
             ops <-
               member_of([
                 [:get_range],
                 [:get_range],
                 [:get_range, :put],
                 [:get],
                 [:head],
                 [:list],
                 [:put]
               ]),
             times <- member_of([3, 40, :infinity])
           ) do
         {:fault, ops, times}
       end},
      {2,
       gen all(kind <- damage_gen(), times <- member_of([1, 2, 5, 50])) do
         {:damage, kind, times}
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

  defp run_sql(conn, sql), do: Db.writable_script(conn, sql)

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

  # integrity_check reports a page it could not fetch as a row, not as an
  # error: that read failed on the store like any other and is told as such.
  defp read_lazy(conn, :integrity) do
    case read(conn, :integrity) do
      {:ok, [[text]]} = ok ->
        if String.contains?(text, "failed to get page"), do: {:error, @io_error}, else: ok

      other ->
        other
    end
  end

  defp read_lazy(conn, r), do: read(conn, r)

  # Run

  defp run(seed, compact?, latency, lie?, damage, commands) do
    Process.flag(:trap_exit, true)
    {_pid, {_, inner}} = start_store()
    hostile = HostileStore.config(inner)
    store = {HostileStore, hostile}
    cfg = inner
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 150)

    try do
      cell = seed_store(ctx, dir, seed, compact?)
      model = open(ctx, cfg, hostile, dir, cell, latency, lie?, damage)

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

  defp open(ctx, cfg, hostile, dir, cell, latency, lie?, damage) do
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
    with {kind, times} <- damage, do: HostileStore.damage(hostile, kind, times)

    %{
      ctx: ctx,
      cfg: cfg,
      hostile: hostile,
      lie?: lie?,
      over: false,
      seen: 0,
      dir: dir,
      cell: cell,
      path: path,
      conn: conn,
      reference: reference,
      hydrator: hydrator,
      hydrated?: false,
      repl: repl,
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
  defp poll(%{over: true} = model), do: model
  defp poll(%{hydrated?: true} = model), do: model

  defp poll(model) do
    hydrator = model.hydrator

    receive do
      {:hydrated, ^hydrator} -> hydrated(model)
      {:hydration_failed, ^hydrator, reason} -> hydration_failed(model, reason)
    after
      0 -> model
    end
  end

  # The pages are complete and match the restored state. A replicator whose
  # own checksum disagrees with the pages refuses to leave the lazy phase.
  defp hydrated(%{lie?: true} = model) do
    wrong = %{model.repl | checksum: Bitwise.bxor(model.repl.checksum, 1)}
    assert Replicator.hydrated(wrong) == {:error, :checksum_mismatch}
    %{model | hydrated?: true, over: true}
  end

  defp hydrated(model) do
    assert {:ok, repl} = Replicator.hydrated(model.repl)
    %{model | repl: repl, hydrated?: true}
  end

  # Only a damaged read may have corrupted the file: the pages fail their
  # checksum, a page does not decode, or a read comes back short.
  defp hydration_failed(model, reason) do
    assert HostileStore.damaged(model.hostile) > 0,
           "hydration failed without a damaged read: #{inspect(reason)}"

    case reason do
      {:checksum, expected, got} -> assert expected != got
      {:invalid, message} when is_binary(message) -> :ok
      {:short_read, key} when is_binary(key) -> :ok
    end

    %{model | over: true}
  end

  # A read or a write that fails on the store is excused by a ranged read the
  # store failed, or by a damaged one, since the last operation.
  defp excused?(model),
    do:
      HostileStore.range_delivered(model.hostile) > model.seen or
        HostileStore.damaged(model.hostile) > 0

  defp seen(model), do: %{model | seen: HostileStore.range_delivered(model.hostile)}

  # Without a fault a read that waits can still run out of time on a loaded
  # machine; it leaves nothing behind, so it is asked again.
  defp retried(model, fun, tries \\ 20) do
    case fun.() do
      {:error, @io_error} = error ->
        if excused?(model) or tries == 1, do: error, else: retried(model, fun, tries - 1)

      result ->
        result
    end
  end

  defp retry_io(fun, tries \\ 20) do
    case fun.() do
      {:error, @io_error} when tries > 1 -> retry_io(fun, tries - 1)
      result -> result
    end
  end

  defp step(command, model) do
    case poll(model) do
      %{over: true} = model -> {:halt, model}
      model -> do_step(command, model)
    end
  end

  defp do_step({:read, r}, model) do
    expected = read(model.reference, r)

    model =
      case retried(model, fn -> read_lazy(model.conn, r) end) do
        {:error, @io_error} ->
          assert excused?(model), "a read failed without a fault: #{inspect(r)}"
          model

        # integrity_check reports a page it cannot read as a row, not as an error.
        actual when r == :integrity ->
          if HostileStore.damaged(model.hostile) > 0 do
            model
          else
            assert match?({:ok, _}, actual)
            if excused?(model), do: model, else: assert_equal(model, actual, expected, r)
          end

        actual ->
          # Pages of a damaged read are served as they came, until the end of
          # hydration checks them.
          if HostileStore.damaged(model.hostile) > 0,
            do: model,
            else: assert_equal(model, actual, expected, r)
      end

    {:cont, seen(model)}
  end

  defp do_step({:write, ops}, model) do
    model =
      Enum.reduce(ops, model, fn op, model ->
        # The write gate holds writes while a damaged page may be in the file.
        if damaged?(model), do: model, else: write(model, op)
      end)

    {:cont, seen(model)}
  end

  defp do_step(:sync, model), do: {:cont, sync(model)}

  defp do_step(:checkpoint, model) do
    case checkpoint(model) do
      {:ok, repl} -> {:cont, %{model | repl: repl}}
      {:error, _} -> {:cont, model}
    end
  end

  defp do_step({:fault, ops, times}, model) do
    HostileStore.fail(model.hostile, ops, times)
    {:cont, model}
  end

  defp do_step({:damage, kind, times}, model) do
    HostileStore.damage(model.hostile, kind, times)
    {:cont, model}
  end

  defp do_step(:heal, model) do
    HostileStore.heal(model.hostile)
    {:cont, model}
  end

  defp do_step(:crash, model) do
    model = quiesce(model)
    # Nobody tells the store; the next owner restores what was acknowledged.
    Process.unlink(model.hydrator)
    Process.exit(model.hydrator, :kill)
    Sqlite.close(model.conn)

    # Requests that fail while it restores fail the restore, not the state.
    HostileStore.fail(model.hostile, [:get, :head, :list], 2)
    before = HostileStore.delivered(model.hostile)
    path = Path.join(model.dir, "after-crash.db")

    case Replication.restore(model.ctx, model.cell, path, :latest) do
      {:ok, _} -> :ok
      {:error, _} -> assert HostileStore.delivered(model.hostile) > before
    end

    HostileStore.heal(model.hostile)
    assert {:ok, result} = Replication.restore(model.ctx, model.cell, path, :latest)
    assert result.checksum == model.repl.checksum
    if !damaged?(model), do: assert(dump_file(path) == dump(model.reference))
    {:halt, %{model | conn: model.reference}}
  end

  defp write(model, op) do
    case retried(model, fn -> apply_op(model.conn, op) end) do
      :ok ->
        applied = apply_op(model.reference, op)
        if HostileStore.damaged(model.hostile) == 0, do: assert(applied == :ok)
        %{model | dirty?: true}

      {:error, @io_error} ->
        assert excused?(model), "a write failed without a fault: #{inspect(op)}"
        model

      # The pages of a damaged read are served as they came.
      {:error, message} ->
        if HostileStore.damaged(model.hostile) == 0,
          do: assert(apply_op(model.reference, op) == {:error, message})

        model
    end
  end

  defp assert_equal(model, actual, expected, r) do
    assert actual == expected, "read #{inspect(r)} differs from the full restore"
    model
  end

  # A failed sync is retried: the committed transactions stay in the WAL.
  # Nothing is acknowledged while a damaged page may be in the file: the write
  # gate holds the writes until hydration has checked the pages.
  defp sync(model) do
    if damaged?(model) do
      model
    else
      case Replicator.encode(model.repl, model.ctx, :batch, []) do
        :none ->
          model

        {:error, _} ->
          model

        {:ok, next, up} ->
          case Replicator.upload(up, model.ctx, []) do
            :ok -> %{model | repl: next}
            _ when next.lazy == nil -> model
            _ -> %{model | over: true}
          end
      end
    end
  end

  defp checkpoint(model) do
    if damaged?(model),
      do: {:error, :damaged},
      else: Replicator.checkpoint(model.repl, model.conn)
  end

  defp damaged?(model), do: not model.hydrated? and HostileStore.damaged(model.hostile) > 0

  # No faults, everything replicated.
  defp quiesce(model) do
    HostileStore.heal(model.hostile)

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
      {:hydrated, ^hydrator} -> hydrated(model)
      {:hydration_failed, ^hydrator, reason} -> hydration_failed(model, reason)
    after
      20_000 -> flunk("hydration did not finish")
    end
  end

  defp finish(%{conn: conn, reference: conn}), do: :ok
  defp finish(%{over: true}), do: :ok

  defp finish(model) do
    HostileStore.heal(model.hostile)
    model = model |> await_hydrated() |> quiesce()
    if model.over, do: :ok, else: complete(model)
  end

  # The open connection answers like the reference. A mismatch tells whether a
  # fresh connection on the same file agrees with the reference: if it does,
  # the open connection holds a page the file no longer has.
  defp assert_same_read(model, r) do
    expected = read(model.reference, r)

    if read(model.conn, r) != expected do
      {:ok, fresh} = Db.open_reader(model.path, 1024)
      agrees? = read(fresh, r) == expected
      Sqlite.close(fresh)

      flunk(
        "read #{inspect(r)} differs from the full restore on the open connection; " <>
          "a fresh connection on the file " <>
          if(agrees?, do: "agrees with it", else: "differs too")
      )
    end
  end

  defp complete(model) do
    # Same reads, now on a complete file.
    for r <- [:aggregate, :everything, :schema, {:table, 1}, :integrity],
        do: assert_same_read(model, r)

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
    Sqlite.close(conn)
  end

  @tag timeout: 900_000
  property "H1: reads during hydration equal reads after a full restore" do
    check all(
            seed <- list_of(op_gen(), min_length: 4, max_length: 14),
            compact? <- boolean(),
            latency <- member_of([0, 3, 10]),
            lie? <- boolean(),
            damage <-
              frequency([
                {1, constant(nil)},
                {2, tuple({damage_gen(), member_of([1, 3, 50])})}
              ]),
            commands <- list_of(command_gen(), min_length: 3, max_length: 25),
            max_runs: String.to_integer(System.get_env("H1_RUNS", "120"))
          ) do
      run(seed, compact?, latency, lie?, damage, commands)
    end
  end
end
