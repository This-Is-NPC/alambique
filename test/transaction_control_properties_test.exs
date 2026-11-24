defmodule Alambique.TransactionControlPropertiesTest do
  @moduledoc """
  Whatever a transaction function runs (transaction control in odd spellings,
  several statements in one text, scripts that try to smuggle a COMMIT or an
  INSERT behind a variable token or a BOM, a NUL byte, `end` as an
  identifier, statements run directly on the connection of the handle, a
  transaction ended behind the library's back, a conflict that rolls back),
  with the store holding the uploads, a write of another caller in flight when
  the function runs, and idle feed cleanups between the steps:

    * I1: committed frames no batch carries imply a write in flight;
    * I2: a read never returns a row the store does not hold, and the row is
      still there after the cell sleeps or is killed and wakes;
    * I3: after a hibernation and after a kill, the cell wakes with the rows
      the store holds;
    * I4: every durable row is in the change feed once.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Feed, Sqlite, Tx}
  alias Alambique.Replication.Replicator
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true
  @moduletag timeout: 600_000

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @all "SELECT n FROM t ORDER BY n"

  @controls [
    "BEGIN",
    "BEGIN IMMEDIATE TRANSACTION",
    "COMMIT",
    "COMMIT TRANSACTION",
    "END",
    "END TRANSACTION",
    "ROLLBACK",
    "ROLLBACK TRANSACTION",
    "ROLLBACK TO s",
    "ROLLBACK TO SAVEPOINT s",
    "SAVEPOINT s",
    "RELEASE s",
    "RELEASE SAVEPOINT s"
  ]
  @idents ["a$case", "écase", "x$end", "b_begin", "ünicode$end", "plain"]
  @bom <<0xEF, 0xBB, 0xBF>>
  # A variable token that the splitter of a migration reads as a quote or a
  # comment, with what closes it for SQLite.
  @variables [{"$a(')", "--'"}, {":a(--)", ""}, {"@a(/*)", "/* x */"}, {"$a::b(')", "--'"}]

  defp control do
    gen all(
          word <- member_of(@controls),
          cased <- member_of([&Function.identity/1, &String.downcase/1, &mixed/1]),
          before <- member_of(["", " ", "/* c */ ", "-- c\n", ";", "; "]),
          after_ <- member_of(["", ";", " ;", " -- c", "/* c */"])
        ) do
      before <> cased.(word) <> after_
    end
  end

  defp mixed(text),
    do: text |> String.graphemes() |> Enum.with_index() |> Enum.map_join(&mix/1)

  defp mix({g, i}), do: if(rem(i, 2) == 0, do: String.upcase(g), else: String.downcase(g))

  defp op do
    one_of([
      map(control(), &{:sql, &1}),
      map(control(), &{:sql, &1}),
      constant(:insert),
      constant(:insert),
      constant(:conflict),
      map(control(), &{:multi, &1}),
      map({member_of(@idents), control()}, &{:smuggle, &1}),
      map({member_of(@variables), member_of(["", "COMMIT; "])}, &{:variable, &1}),
      map(member_of(["COMMIT", "INSERT INTO t VALUES (7000)"]), &{:bom, &1}),
      map(member_of(["", "; DELETE FROM t", "; COMMIT"]), &{:nul, &1}),
      constant(:end_column),
      constant(:trailing_bom),
      map(control(), &{:direct, &1}),
      constant(:lifted)
    ])
  end

  # The function of the write: each op is a call whose result is ignored.
  defp function(ops) do
    fn tx ->
      ops
      |> Enum.with_index(10)
      |> Enum.each(fn {op, n} -> run(tx, op, n) end)

      :ok
    end
  end

  defp run(tx, {:sql, text}, _n), do: Tx.execute(tx, text)
  defp run(tx, :insert, n), do: Tx.execute(tx, "INSERT INTO t VALUES (#{n})")
  defp run(tx, :conflict, _n), do: Tx.execute(tx, "INSERT OR ROLLBACK INTO t VALUES (0)")

  defp run(tx, {:multi, text}, n),
    do: Tx.execute(tx, "INSERT INTO t VALUES (#{n}); #{text}; INSERT INTO t VALUES (#{n + 1000})")

  # The splitter of a migration reads the variable's text as a quote or a
  # comment; SQLite does not.
  defp run(tx, {:variable, {{token, closer}, control}}, n) do
    Tx.script(
      tx,
      "CREATE TABLE s#{n} AS SELECT #{token} AS x; #{control}" <>
        "INSERT INTO t VALUES (#{n + 4000});\n#{closer}"
    )
  end

  defp run(tx, {:bom, text}, n) do
    Tx.script(
      tx,
      "CREATE TABLE b#{n} (x); " <>
        "CREATE TRIGGER bt#{n} AFTER INSERT ON b#{n} BEGIN SELECT 1;#{@bom}END; " <>
        "#{text}; INSERT INTO t VALUES (#{n + 3000})"
    )
  end

  defp run(tx, {:nul, rest}, n), do: Tx.execute(tx, "INSERT INTO t VALUES (#{n})\0#{rest}")
  defp run(tx, :trailing_bom, n), do: Tx.script(tx, "CREATE TABLE e#{n} (x);#{@bom}")

  defp run(tx, :end_column, n) do
    Tx.script(
      tx,
      "CREATE TABLE c#{n} (end, x); " <>
        "CREATE TRIGGER ct#{n} AFTER INSERT ON c#{n} BEGIN UPDATE c#{n} SET x = new.end; SELECT 1; END"
    )
  end

  # A statement run on the connection of the handle, not through it.
  defp run(tx, {:direct, text}, _n), do: Sqlite.execute(tx.conn, text)

  # The transaction ends behind the library's back, as a defect of the library
  # would: the connection refuses it until the flag of the library is lifted.
  defp run(tx, :lifted, n) do
    _ = Tx.execute(tx, "INSERT INTO t VALUES (#{n})")
    :ok = Sqlite.expect_transaction(tx.conn, false)
    Sqlite.execute(tx.conn, "COMMIT")
  end

  defp run(tx, {:smuggle, {ident, text}}, n) do
    Tx.script(
      tx,
      "CREATE TRIGGER z#{n} AFTER INSERT ON t BEGIN SELECT 1 AS #{ident}; END; " <>
        "#{text}; INSERT INTO t VALUES (#{n + 2000}); " <>
        "CREATE TRIGGER y#{n} AFTER INSERT ON t BEGIN SELECT 1; END"
    )
  end

  setup do
    env = setup_store()
    # Started first, so it stops after the instance: the cells release their
    # leases through it.
    gate = start_supervised!(%{id: :gate, start: {GatedStore, :start_link, []}})
    store = GatedStore.wrap(gate, env.store)

    inst =
      start_instance(%{env | store: store},
        migrations: @schema,
        compaction: false,
        feed: [cleanup: :idle, cleanup_after_ms: 50]
      )

    %{env: env, gate: gate, inst: inst, raw: env.store}
  end

  defp ids(rows), do: rows |> List.flatten() |> Enum.reject(&(&1 == 0))

  defp stored(ctx, cell), do: ids(durable_rows(%{ctx.env | store: ctx.raw}, cell, @all))

  defp rows(ctx, cell) do
    case Alambique.query(ctx.inst, cell, @all, [], timeout: 800) do
      {:ok, %{rows: rows}} -> ids(rows)
      _ -> :blocked
    end
  catch
    :exit, _ -> :blocked
  end

  defp wake_rows(ctx, cell) do
    {:ok, %{rows: rows}} = Alambique.query(ctx.inst, cell, @all, [], timeout: 20_000)
    ids(rows)
  end

  defp pid(ctx, cell), do: Cell.whereis(Config.fetch!(ctx.inst), cell)

  defp unreplicated_without_write?(ctx, cell) do
    with pid when is_pid(pid) <- pid(ctx, cell),
         %{repl: repl, pending: pending, pipe: pipe} <- :sys.get_state(pid, 2_000) do
      Replicator.unreplicated?(repl) and pending == [] and pipe.queue == []
    else
      _ -> false
    end
  catch
    :exit, _ -> false
  end

  defp cleaned?(ctx, cell) do
    case pid(ctx, cell) do
      pid when is_pid(pid) ->
        s = :sys.get_state(pid, 2_000)
        s.feed_cleaned == s.feed_published

      _ ->
        true
    end
  catch
    :exit, _ -> true
  end

  defp wipe(ctx, cell) do
    path = Alambique.Cell.Local.db_path(Config.fetch!(ctx.inst), cell)
    File.rm_rf!(Path.dirname(path))
  end

  defp feed_counts(ctx, cell, wanted) do
    feed = Feed.ctx(store: ctx.raw, prefix: "cells-test", settle_ms: 0)

    eventually(fn ->
      {:ok, records, _} = Feed.read(feed, nil, 10_000)
      keys = for r <- records, r.cell == cell, r.table == "t", do: r.key["n"]
      if Enum.all?(wanted, &(&1 in keys)), do: Enum.frequencies(keys)
    end)
  end

  property "a transaction function cannot make a write the cell does not account for", ctx do
    counter = :counters.new(1, [])

    check all(
            ops <- list_of(op(), min_length: 1, max_length: 8),
            sleep? <- boolean(),
            inflight? <- boolean(),
            clean? <- boolean(),
            max_runs: 40
          ) do
      :counters.add(counter, 1, 1)
      cell = "cell-#{:counters.get(counter, 1)}-#{System.unique_integer([:positive])}"
      GatedStore.open(ctx.gate)
      {:ok, _} = Alambique.execute(ctx.inst, cell, "INSERT INTO t VALUES (0)")
      GatedStore.hold(ctx.gate, Regex.compile!(Regex.escape(cell) <> "/0000/"))

      # Another caller's write is in flight when the function runs.
      inflight =
        if inflight? do
          other =
            Task.async(fn -> Alambique.execute(ctx.inst, cell, "INSERT INTO t VALUES (900)") end)

          eventually(fn -> :sys.get_state(pid(ctx, cell)).pipe.queue != [] end)
          other
        end

      task = Task.async(fn -> Alambique.transaction(ctx.inst, cell, function(ops)) end)
      _ = Task.yield(task, 300)

      # I1 and I2 while the puts are held.
      refute unreplicated_without_write?(ctx, cell), "I1: #{inspect(ops)}"
      seen = rows(ctx, cell)
      held = stored(ctx, cell)

      if seen != :blocked,
        do: assert(seen -- held == [], "I2: #{inspect(ops)} saw #{inspect(seen)}")

      # Idle cleanups run between the steps, and must not carry frames of a
      # failed write or lose their feed rows.
      if clean? do
        GatedStore.open(ctx.gate)
        _ = Task.yield(task, 3_000)
        eventually(fn -> cleaned?(ctx, cell) end)
      end

      after_wake =
        if sleep? do
          GatedStore.open(ctx.gate)
          _ = Task.yield(task, 3_000) || Task.shutdown(task, :brutal_kill)
          _ = Alambique.hibernate(ctx.inst, cell)
          eventually(fn -> Alambique.owner(ctx.inst, cell) == :none end)
          wipe(ctx, cell)
          wake_rows(ctx, cell)
        else
          if pid = pid(ctx, cell), do: Process.exit(pid, :kill)
          eventually(fn -> Alambique.owner(ctx.inst, cell) == :none end)
          wipe(ctx, cell)
          GatedStore.open(ctx.gate)
          Task.shutdown(task, 2_000)
          wake_rows(ctx, cell)
        end

      # I3: the wake serves what the store holds; and what a reader saw is still there.
      durable = stored(ctx, cell)
      assert after_wake == durable, "I3: #{inspect(ops)}"
      if seen != :blocked, do: assert(seen -- after_wake == [], "I2 later: #{inspect(ops)}")

      if inflight, do: Task.shutdown(inflight, 2_000)

      # I4: every durable row is in the feed once.
      counts = feed_counts(ctx, cell, durable)
      assert Enum.all?(durable, &(counts[&1] == 1)), "I4: #{inspect(ops)} #{inspect(counts)}"
    end
  end
end
