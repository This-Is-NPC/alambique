defmodule Alambique.TxControlFailedWriteTest do
  @moduledoc """
  What a failed or refused write leaves in the store, in a cut and in the
  feed (design §15): where the splitter ends a statement, a NUL byte, frames
  that no write carries while a batch is in flight or the cell is idle, a
  feed cleanup behind them, a checkpoint with a deadline, and the pragma
  that follows the commit.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Feed, Sqlite, Tx}
  alias Alambique.Cell.{Db, Sql}
  alias Alambique.Replication.{Native, Replicator}
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @all "SELECT n FROM t ORDER BY n"
  # U+00A0 is no space to SQLite; the UTF-8 BOM is.
  @nbsp <<0xC2, 0xA0>>
  @bom <<0xEF, 0xBB, 0xBF>>

  setup ctx do
    env = setup_store()
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    install_commit_behind_back()
    Map.merge(env, %{gate: gate, tag: ctx.test |> Atom.to_string() |> :erlang.phash2()})
  end

  # An awake cell with rows 0 and 1 acknowledged; `gated: true` then holds
  # every level-0 put.
  defp boot(env, opts \\ []) do
    {gated, opts} = Keyword.pop(opts, :gated, false)
    store = GatedStore.wrap(env.gate, env.store)
    cell = "cell-#{env.tag}"

    inst = start_instance(%{env | store: store}, [migrations: @schema, compaction: false] ++ opts)
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (0)")
    if gated, do: GatedStore.hold(env.gate, ~r{/0000/})
    %{env | store: store} |> Map.merge(%{inst: inst, cell: cell, raw: env.store})
  end

  defp pid(c), do: Cell.whereis(Config.fetch!(c.inst), c.cell)
  defp cstate(c), do: :sys.get_state(pid(c))
  defp unreplicated?(c), do: Replicator.unreplicated?(cstate(c).repl)
  defp ins(tx, n), do: Tx.execute(tx, "INSERT INTO t VALUES (#{n})")

  defp store_ids(c),
    do: %{c | store: c.raw} |> durable_rows(c.cell, @all) |> List.flatten()

  defp local_ids(c) do
    {:ok, %{rows: rows}} = Alambique.query(c.inst, c.cell, @all, [], timeout: 20_000)
    List.flatten(rows)
  end

  defp asleep?(c), do: eventually(fn -> Alambique.owner(c.inst, c.cell) == :none end)

  # Rows committed on the cell's own connection, outside any write (the
  # state an undetected transaction end leaves).
  defp orphan(c, n) do
    :ok = in_cell(pid(c), &Db.writable_script(&1.conn, "INSERT INTO t VALUES (#{n})"))
    assert unreplicated?(c)
  end

  # A write function that ends the transaction behind the library's back when
  # it returns.
  defp ending(n) do
    fn tx ->
      {:ok, _} = ins(tx, n)
      commit_behind_back()
    end
  end

  defp feed_keys(c) do
    ctx = Feed.ctx(store: c.raw, prefix: "cells-test", settle_ms: 0)
    {:ok, records, _} = Feed.read(ctx, nil, 1_000)
    for r <- records, r.cell == c.cell, r.table == "t", do: r.key["n"]
  end

  # Waits until the feed shows `last`, then returns the keys.
  defp feed_until(c, last) do
    eventually(fn ->
      keys = feed_keys(c)
      if last in keys, do: keys
    end)
  end

  defp go(c, fun) do
    task = Task.async(fn -> Alambique.transaction(c.inst, c.cell, fun) end)

    case Task.yield(task, 1_500) do
      {:ok, result} -> {task, result}
      nil -> {task, :blocked}
    end
  end

  defp finish(c, {task, _}) do
    GatedStore.open(c.gate)
    Task.shutdown(task, 5_000)
    :ok
  end

  # Runs `script` as `Tx.script`, then a plain write, and lists what is wrong:
  # the script was not refused although it holds a plain INSERT, and a row
  # that is durable has no record in the change feed.
  defp smuggle(env, script) do
    c = boot(env)
    result = Alambique.transaction(c.inst, c.cell, fn tx -> Tx.script(tx, script) end)
    {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (8)")
    keys = feed_until(c, 8)
    stored = store_ids(c)

    [
      if(not match?({:ok, {:error, {:not_ddl, _}}}, result), do: {:not_refused, result}),
      if(7 in stored and 7 not in keys, do: {:durable_row_not_in_feed, stored, keys})
    ]
    |> Enum.reject(&is_nil/1)
  end

  # ----------------------------------------------------------------------
  # the splitter and TCL-style variables
  # ----------------------------------------------------------------------

  describe "variable tokens" do
    test "the bundled SQLite reads $a(') as one variable (not compiled out)" do
      {:ok, conn} = Sqlite.open(":memory:")
      {:ok, %{rows: opts}} = Sqlite.run(conn, "PRAGMA compile_options", [])
      refute Enum.any?(List.flatten(opts), &String.contains?(&1, "OMIT_TCL_VARIABLE"))

      for sql <- ["SELECT $a(')", "SELECT :a(--)", "SELECT @a(/*)"] do
        assert {:ok, %{columns: [column]}} = Sqlite.run(conn, sql, [7])
        assert column == String.replace_prefix(sql, "SELECT ", "")
      end

      Sqlite.close(conn)
    end

    for {name, variable} <- [dollar: "$a(')", colon: ":a(--)", at: "@a(/*)"] do
      test "#{name}: Sql.split agrees with SQLite on where the first statement ends" do
        variable = unquote(variable)
        sql = "CREATE TABLE s AS SELECT #{variable} AS x; INSERT INTO t VALUES (7); SELECT 1"
        assert length(Sql.split(sql)) == 3, inspect(Sql.split(sql))
      end

      test "#{name}: Tx.script does not run an INSERT behind the variable", env do
        variable = unquote(variable)
        # What SQLite reads as plain statements, and the splitter as the
        # inside of a quote or a comment.
        closer = %{"$a(')" => "--'", ":a(--)" => "", "@a(/*)" => "/* x */"}[variable]
        script = "CREATE TABLE s AS SELECT #{variable} AS x; INSERT INTO t VALUES (7);\n#{closer}"

        assert smuggle(env, script) == []
      end
    end

    test "Tx.script: a BOM before END hides an INSERT from the DDL check", env do
      script =
        "CREATE TABLE e (x); " <>
          "CREATE TRIGGER g AFTER INSERT ON e BEGIN SELECT 1;#{@bom}END; INSERT INTO t VALUES (7)"

      assert smuggle(env, script) == []
    end

    test "Tx.script: a script that ends in a BOM is DDL", env do
      c = boot(env)

      assert {:ok, :ok} =
               Alambique.transaction(c.inst, c.cell, fn tx ->
                 Tx.script(tx, "CREATE TABLE s (x);#{@bom}")
               end)
    end

    test "Tx.script: a COMMIT behind the variable is refused and nothing stays", env do
      c = boot(env)

      script = "CREATE TABLE s AS SELECT $a(') AS x; COMMIT; INSERT INTO t VALUES (7); --'"
      result = Alambique.transaction(c.inst, c.cell, fn tx -> Tx.script(tx, script) end)

      assert match?({:ok, {:error, _}}, result) or match?({:error, _}, result),
             inspect(result)

      assert local_ids(c) == [0]
      refute unreplicated?(c)
    end
  end

  # ----------------------------------------------------------------------
  # other splitter inputs, against SQLite's own boundaries
  # ----------------------------------------------------------------------

  defp scratch do
    {:ok, conn} = Sqlite.open(":memory:")
    :ok = Sqlite.execute(conn, "CREATE TABLE t (n); CREATE TABLE e (end, x)")
    conn
  end

  # Statement texts as SQLite's prepare tail cuts them (`run` refuses the
  # tail, which the error carries trimmed; short inputs only).
  defp sqlite_split(conn, text, params \\ 0) do
    text = String.replace(text, ~r/\A[ \t\n\r\f]+|[ \t\n\r\f]+\z/, "")

    result =
      try do
        Sqlite.run(conn, text, List.duplicate(1, params))
      rescue
        e in ArgumentError ->
          [_, n] = Regex.run(~r/expected (\d+) arg/, e.message)
          {:retry, String.to_integer(n)}
      end

    case result do
      {:retry, n} -> sqlite_split(conn, text, n)
      {:error, {:multiple_statements, tail}} -> sqlite_rest(conn, text, tail)
      {:ok, _} -> [text]
      {:error, _} -> throw(:rejected)
    end
  end

  # `:rejected` when SQLite does not accept the script as a whole, or the
  # text is too long for the tail an error carries.
  defp oracle(_conn, text) when byte_size(text) > 98, do: :rejected

  defp oracle(conn, text) do
    {:ok, conn |> sqlite_split(text) |> normalized() |> Enum.reject(&(&1 == ""))}
  catch
    :rejected -> :rejected
  end

  defp sqlite_rest(conn, text, tail) do
    first = binary_part(text, 0, byte_size(text) - byte_size(tail))
    [String.trim(first) | sqlite_split(conn, tail)]
  end

  defp normalized(statements),
    do:
      Enum.map(statements, fn statement ->
        statement
        |> String.replace(~r/--[^\n]*/, "")
        |> String.replace(@bom, "")
        |> String.trim()
        |> String.trim(";")
        |> String.trim()
      end)

  @cases [
    trigger_end_column:
      "CREATE TRIGGER IF NOT EXISTS g1 AFTER INSERT ON e BEGIN UPDATE e SET end = 1; SELECT 2; END; SELECT 3",
    trigger_new_end:
      "CREATE TRIGGER IF NOT EXISTS g2 AFTER INSERT ON e BEGIN UPDATE e SET x = new.end; SELECT 2; END; SELECT 3",
    trigger_end_in_when:
      "CREATE TRIGGER IF NOT EXISTS g3 AFTER INSERT ON e WHEN new.end > 0 BEGIN SELECT 2; SELECT 3; END; SELECT 4",
    nbsp_after_semicolon: "SELECT 1;#{@nbsp}SELECT 2",
    bom_after_semicolon: "SELECT 1;#{@bom}SELECT 2",
    bom_first: "#{@bom}SELECT 1; SELECT 2",
    bom_trailing: "SELECT 1;#{@bom}",
    bom_before_end:
      "CREATE TRIGGER IF NOT EXISTS g6 AFTER INSERT ON e BEGIN SELECT 1;#{@bom}END; SELECT 3",
    bom_before_case_end:
      "CREATE TRIGGER IF NOT EXISTS g7 AFTER INSERT ON e BEGIN SELECT CASE WHEN 1 THEN 2 #{@bom}END; SELECT 3; END; SELECT 4",
    blob_literal: "SELECT x'3b'; SELECT 2",
    quoted_names: ~s|SELECT 1 AS "a;b"; SELECT 2 AS [c;d]; SELECT `e;f`|,
    line_comment_then_text: "SELECT 1 -- a;b\n; SELECT 2",
    block_comment: "SELECT /* ; */ 1; SELECT 2",
    case_expression: "SELECT CASE WHEN 1 THEN 2 END; SELECT 3",
    only_semicolons: ";;; SELECT 1;;; SELECT 2;;",
    vertical_tab: "SELECT 1;\vSELECT 2",
    form_feed: "SELECT 1;\fSELECT 2",
    lone_cr: "SELECT 1;\rSELECT 2",
    dollar_ident: "SELECT 1 AS a$b; SELECT 2",
    number_exponent: "SELECT 1e5; SELECT 2",
    hash_var: "SELECT #a; SELECT 2",
    unicode_ident: "SELECT 1 AS é; SELECT 2",
    nested_comment_like: "SELECT 1 /* /* ; */ ; SELECT 2",
    unterminated_string: "SELECT 'a; SELECT 2",
    escaped_quote: "SELECT 'a''; b'; SELECT 2"
  ]

  describe "Sql.split against SQLite's statement boundaries" do
    for {name, sql} <- @cases do
      test "#{name}" do
        conn = scratch()
        sql = unquote(sql)
        sqlite = oracle(conn, sql)
        ours = sql |> Sql.split() |> normalized()
        Sqlite.close(conn)
        # A script SQLite rejects as a whole has no boundaries to compare.
        if sqlite != :rejected do
          assert {:ok, ours} == sqlite, inspect(%{sql: sql, ours: ours, sqlite: sqlite})
        end
      end
    end

    @pieces [
      "SELECT 1",
      "SELECT 'a;b'",
      "SELECT x'3b'",
      ~s|SELECT "a;b"|,
      "SELECT [a;b]",
      "SELECT 1 -- ;\n",
      "SELECT /* ; */ 2",
      "SELECT $a(')",
      "SELECT :a(--)",
      "SELECT @a(/*)",
      "SELECT CASE WHEN 1 THEN 2 END",
      "CREATE TRIGGER IF NOT EXISTS g4 AFTER INSERT ON e BEGIN SELECT 1; END",
      "CREATE TRIGGER IF NOT EXISTS g5 AFTER INSERT ON e BEGIN UPDATE e SET end=1; END"
    ]
    @separators ["; ", ";", ";\n", ";#{@bom}", " ;", ";\t", ";;", "; -- c\n"]

    property "splits where SQLite does (generated)" do
      check all(
              pieces <- list_of(member_of(@pieces), min_length: 1, max_length: 4),
              seps <- list_of(member_of(@separators), length: 4),
              max_runs: 400
            ) do
        sql =
          pieces
          |> Enum.zip(seps)
          |> Enum.map_join(fn {piece, sep} -> piece <> sep end)

        conn = scratch()
        sqlite = oracle(conn, sql)
        ours = sql |> Sql.split() |> normalized()
        Sqlite.close(conn)
        # A script SQLite rejects as a whole has no boundaries to compare.
        if sqlite != :rejected do
          assert {:ok, ours} == sqlite, inspect(%{sql: sql, ours: ours, sqlite: sqlite})
        end
      end
    end

    test "a valid trigger that names a column `end` is accepted as DDL", env do
      c = boot(env)

      script =
        "CREATE TABLE e (end, x); " <>
          "CREATE TRIGGER g AFTER INSERT ON e BEGIN UPDATE e SET x = new.end; SELECT 1; END"

      assert {:ok, :ok} =
               Alambique.transaction(c.inst, c.cell, fn tx -> Tx.script(tx, script) end)
    end
  end

  # ----------------------------------------------------------------------
  # NUL byte in the text
  # ----------------------------------------------------------------------

  describe "a NUL byte" do
    test "Alambique.execute does not drop the text after it silently", env do
      c = boot(env)

      result =
        Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (5)\0; DELETE FROM t")

      assert match?({:error, _}, result), inspect(%{result: result, rows: local_ids(c)})
      assert local_ids(c) == [0]
    end

    test "Tx.execute inside a transaction does not run the head and drop the rest", env do
      c = boot(env)

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          Tx.execute(tx, "INSERT INTO t VALUES (5)\0; DELETE FROM t")
        end)

      assert match?({:ok, {:error, _}}, result) or match?({:error, _}, result),
             inspect(%{result: result, rows: local_ids(c)})
    end

    test "Alambique.query refuses a second statement behind a NUL", env do
      c = boot(env)
      result = Alambique.query(c.inst, c.cell, "SELECT 1\0; SELECT 2")
      assert match?({:error, _}, result), inspect(result)
    end

    test "Alambique.query refuses a plain second statement", env do
      c = boot(env)

      assert {:error, {:multiple_statements, _}} =
               Alambique.query(c.inst, c.cell, "SELECT 1; SELECT 2")
    end

    test "Sqlite.execute with a NUL does not run a prefix and report :ok" do
      {:ok, conn} = Sqlite.open(":memory:")
      :ok = Sqlite.execute(conn, "CREATE TABLE t (n)")
      result = Sqlite.execute(conn, "INSERT INTO t VALUES (1)\0; INSERT INTO t VALUES (2)")
      {:ok, %{rows: rows}} = Sqlite.run(conn, "SELECT n FROM t ORDER BY n", [])
      Sqlite.close(conn)
      assert match?({:error, _}, result), inspect(%{result: result, rows: rows})
    end

    test "a replica read refuses a second statement, with and without a NUL", env do
      owner = start_instance(env, migrations: @schema, compaction: false)

      reader =
        start_instance(env,
          migrations: @schema,
          compaction: false,
          replicas: [poll_ms: 50, marker_ttl_ms: 200, refresh_timeout_ms: 1_000]
        )

      cell = unique_cell()
      {:ok, _} = Alambique.execute(owner, cell, "INSERT INTO t VALUES (1)")
      :ok = Alambique.replicas(reader, cell, true)

      read = fn sql ->
        Alambique.query(reader, cell, sql, [], consistency: {:bounded, 1_000}, timeout: 5_000)
      end

      Enum.find_value(1..400, fn _ ->
        if match?({:ok, _}, read.(@all)), do: true, else: Process.sleep(25)
      end) || flunk("the replica never answered")

      assert {:error, {:multiple_statements, _}} = read.("SELECT 1; SELECT 2")
      assert {:error, _} = read.("SELECT 1\0; SELECT 2")
    end

    test "Alambique.Query refuses a second statement, with and without a NUL", env do
      owner = start_instance(env, migrations: @schema, compaction: false)
      reader = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      {:ok, _} = Alambique.execute(owner, cell, "INSERT INTO t VALUES (1)")
      :ok = Alambique.hibernate(owner, cell)

      all = fn sql -> Alambique.Query.all(reader, [cell], sql) end
      assert %{rows: [_], errors: []} = all.(@all)
      assert %{rows: [], errors: [{^cell, _}]} = all.("SELECT 1; SELECT 2")
      assert %{rows: [], errors: [{^cell, _}]} = all.("SELECT 1\0; SELECT 2")
    end
  end

  # ----------------------------------------------------------------------
  # a failed write that leaves frames while a batch is in flight
  # ----------------------------------------------------------------------

  describe "the backstop with a batch in flight" do
    for isolated <- [false, true] do
      test ":stop, isolated: #{isolated}: the cell stops and the row of the failed write is gone",
           env do
        c = boot(env, gated: true, sql: [orphan_frames: :stop])
        a = Task.async(fn -> Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (1)") end)
        eventually(fn -> cstate(c).pipe.queue != [] end)

        {task, failed} = run_failing(c, unquote(isolated))
        assert failed == {:error, :transaction_ended}
        assert asleep?(c)

        # What a stop gives the write whose batch was in flight.
        assert {:error, :outcome_unknown} = Task.await(a, 10_000)
        GatedStore.open(c.gate)
        _ = Task.shutdown(task, 5_000)
        {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (3)")

        assert 2 not in store_ids(c)
        assert 2 not in local_ids(c)
        assert local_ids(c) == store_ids(c)
      end

      test ":replicate, isolated: #{isolated}: the failed write is answered after its batch, and reads wait",
           env do
        c = boot(env, gated: true, sql: [orphan_frames: :replicate])
        a = Task.async(fn -> Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (1)") end)
        eventually(fn -> cstate(c).pipe.queue != [] end)

        # Parked as a write: its answer waits for the batch that carries it.
        {task, :blocked} = run_failing(c, unquote(isolated))
        reader = Task.async(fn -> Alambique.query(c.inst, c.cell, @all, [], timeout: 8_000) end)
        eventually(fn -> Process.info(reader.pid, :status) == {:status, :waiting} end)
        _ = cstate(c)

        # Only the batch in flight is acknowledged; the row of the failed write
        # is not in it, so a read must not return it yet.
        [key | _] = GatedStore.held(c.gate)
        :ok = GatedStore.release(c.gate, key)
        stored = store_ids(c)
        early = Task.yield(reader, 700)
        assert early == nil or (match?({:ok, {:ok, _}}, early) and 2 not in rows_of(early))
        assert 2 not in stored

        GatedStore.open(c.gate)
        assert {:ok, _} = Task.await(a, 10_000)
        assert {:error, {:committed, :transaction_ended}} = Task.await(task, 10_000)
        assert 2 in store_ids(c)
        assert 2 in (feed_until(c, 2) |> Enum.filter(&(&1 == 2)))
        assert Enum.count(feed_keys(c), &(&1 == 2)) == 1
        if early == nil, do: assert({:ok, _} = Task.await(reader, 10_000))
      end
    end
  end

  defp rows_of({:ok, {:ok, %{rows: rows}}}), do: List.flatten(rows)

  describe "a statement run directly on the connection of the handle" do
    for control <- ["COMMIT", "END", "ROLLBACK", "BEGIN"] do
      test "#{control} is refused and the write goes on", env do
        c = boot(env)
        control = unquote(control)

        result =
          Alambique.transaction(c.inst, c.cell, fn tx ->
            {:ok, _} = ins(tx, 1)
            refused = Sqlite.execute(tx.conn, control)
            {:ok, _} = ins(tx, 2)
            refused
          end)

        assert result == {:ok, {:error, :not_authorized}}
        assert local_ids(c) == [0, 1, 2]
        assert store_ids(c) == [0, 1, 2]
        refute unreplicated?(c)
      end
    end
  end

  describe "what a write function can reach with the connection of its handle" do
    # Nothing public ends the library's transaction: it needs the token that
    # only `Db.transaction` holds.
    test "every public way to end or restart the transaction is refused", env do
      c = boot(env)

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          conn = tx.conn
          {:ok, _} = ins(tx, 1)
          {:ok, _} = ins(tx, 2)

          attempts =
            for guess <- [0, 1, 0xFFFFFFFFFFFFFFFF, :rand.uniform(0xFFFFFFFFFFFFFFFF)],
                how <- [:commit, :rollback] do
              Sqlite.finish_transaction(conn, guess, how)
            end ++
              [
                Native.sqlite_finish_transaction(conn, 1, true),
                Sqlite.begin_transaction(conn),
                Sqlite.execute(conn, "COMMIT"),
                Sqlite.execute(conn, "END"),
                Sqlite.execute(conn, "ROLLBACK"),
                Sqlite.execute(conn, "BEGIN"),
                Sqlite.script(conn, "COMMIT"),
                Db.writable_script(conn, "COMMIT"),
                Db.transaction(conn, 1, fn _ -> :ok end)
              ]

          Sqlite.interrupt(conn)
          {attempts, Sqlite.checkpoint(conn)}
        end)

      assert {:ok, {attempts, checkpoint}} = result
      assert Enum.all?(attempts, &(&1 == {:error, :not_authorized})), inspect(attempts)
      # A checkpoint with the transaction open copies nothing.
      assert {:error, _} = checkpoint
      assert local_ids(c) == [0, 1, 2]
      assert store_ids(c) == [0, 1, 2]
      refute unreplicated?(c)
    end

    test "closing the connection is refused and the write goes on", env do
      c = boot(env)

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          {:ok, _} = ins(tx, 1)
          Sqlite.close(tx.conn)
        end)

      assert {:ok, {:error, :not_authorized}} = result
      assert local_ids(c) == [0, 1]
      assert store_ids(c) == [0, 1]
    end
  end

  describe "a rollback that fails" do
    test "stops the cell, and the next wake serves what the store holds", env do
      c = boot(env)

      # The handler makes the library's own ROLLBACK fail.
      Alambique.Fault.install(fn
        :db_before_rollback, _ -> {:error, :not_authorized}
        _, _ -> :ok
      end)

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          {:ok, _} = ins(tx, 1)
          raise "boom"
        end)

      Alambique.Fault.uninstall()
      assert {:error, {:rollback_failed, :not_authorized}} = result
      assert asleep?(c)
      assert local_ids(c) == [0]
      assert store_ids(c) == [0]
    end
  end

  describe "an idle cell (the backstop applies)" do
    for isolated <- [false, true] do
      test "isolated: #{isolated}: a failed write stops the cell and its row is gone", env do
        c = boot(env, sql: [orphan_frames: :stop])
        {task, failed} = run_failing(c, unquote(isolated))
        _ = Task.shutdown(task, 5_000)

        assert failed == {:error, :transaction_ended}
        assert asleep?(c)
        assert store_ids(c) == [0]
        assert local_ids(c) == [0]
      end
    end
  end

  # One failing orphan write while the held batch is in flight.
  defp run_failing(c, isolated?) do
    opts = if isolated?, do: [isolated: true], else: []
    task = Task.async(fn -> Alambique.transaction(c.inst, c.cell, ending(2), opts) end)

    case Task.yield(task, 2_000) do
      {:ok, result} -> {task, result}
      nil -> {task, :blocked}
    end
  end

  # ----------------------------------------------------------------------
  # the idle feed cleanup carries orphans
  # ----------------------------------------------------------------------

  defp cleanup_settled?(c) do
    if Alambique.owner(c.inst, c.cell) == :none do
      true
    else
      s = cstate(c)
      s.feed_cleaned == s.feed_published
    end
  catch
    :exit, _ -> true
  end

  describe "idle feed cleanup" do
    for mode <- [:stop, :replicate] do
      test "orphan then idle cleanup then position (#{mode})", env do
        mode = String.to_existing_atom(unquote(Atom.to_string(mode)))

        c =
          boot(env,
            sql: [orphan_frames: mode],
            feed: [cleanup: :idle, cleanup_after_ms: 1_000]
          )

        eventually(fn ->
          s = cstate(c)
          s.feed_published > 0 and s.feed_published == s.feed_sent and s.feed_backlog == 0
        end)

        orphan(c, 5)
        # The cleanup is a batch of its own, or the cell stopped before it.
        eventually(fn -> cleanup_settled?(c) end)
        position = Alambique.position(c.inst, c.cell)

        case mode do
          :stop ->
            assert match?({:error, :orphan_frames}, position) or
                     match?({:ok, %{awake: false}}, position),
                   inspect(position)

            assert asleep?(c)
            refute 5 in store_ids(c)
            assert local_ids(c) == [0]

          :replicate ->
            assert {:ok, _} = position
            assert 5 in store_ids(c)
            assert feed_until(c, 5) |> Enum.count(&(&1 == 5)) == 1
        end

        refute unreplicated?(c)
      end
    end

    test "orphan, idle cleanup, then a kill and wake: no row nothing carried survives (:stop)",
         env do
      c =
        boot(env, sql: [orphan_frames: :stop], feed: [cleanup: :idle, cleanup_after_ms: 1_000])

      eventually(fn ->
        s = cstate(c)
        s.feed_published > 0 and s.feed_published == s.feed_sent and s.feed_backlog == 0
      end)

      orphan(c, 5)
      eventually(fn -> cleanup_settled?(c) end)
      _ = Alambique.position(c.inst, c.cell)
      if Alambique.owner(c.inst, c.cell) != :none, do: Process.exit(pid(c), :kill)
      assert asleep?(c)
      refute 5 in store_ids(c)
      assert local_ids(c) == store_ids(c)
    end
  end

  # ----------------------------------------------------------------------
  # :replicate misattributes an ordinary rollback
  # ----------------------------------------------------------------------

  describe ":replicate and an older orphan" do
    test "a constraint failure is not answered as committed", env do
      c = boot(env, sql: [orphan_frames: :replicate])
      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (1)")
      orphan(c, 5)

      result = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (0)")

      refute match?({:error, {:committed, _}}, result), inspect(result)
      assert match?({:error, _}, result)
      assert 0 in local_ids(c)
    end
  end

  # ----------------------------------------------------------------------
  # the checkpoint deadline under :replicate
  # ----------------------------------------------------------------------

  describe "drain_within" do
    test "checkpoint with an orphan under :replicate answers :busy at its deadline", env do
      c = boot(env, gated: true, sql: [orphan_frames: :replicate])
      orphan(c, 5)

      task =
        Task.async(fn ->
          try do
            Alambique.checkpoint(c.inst, c.cell, deadline_ms: 300, timeout: 8_000)
          catch
            :exit, reason -> {:exit, reason}
          end
        end)

      started = System.monotonic_time(:millisecond)
      result = Task.yield(task, 3_000)
      elapsed = System.monotonic_time(:millisecond) - started
      GatedStore.open(c.gate)
      Task.shutdown(task, 10_000)

      assert result == {:ok, {:error, :busy}} and elapsed < 2_000,
             inspect(%{result: result, elapsed_ms: elapsed})
    end
  end

  # ----------------------------------------------------------------------
  # Native.wal_pending
  # ----------------------------------------------------------------------

  describe "Native.wal_pending" do
    test "a missing WAL file", env do
      c = boot(env)
      s = cstate(c)
      missing = Path.join(env.dir, "no-such-dir/x.db-wal")
      assert {:ok, false} = Native.wal_pending(missing, s.repl.page_size, s.repl.wal_pos, nil)
    end
  end

  # ----------------------------------------------------------------------
  # A write whose transaction was ended behind the library's back
  # ----------------------------------------------------------------------

  describe "a write whose transaction was ended behind the library's back" do
    # A write function that ends its transaction behind the library's back
    # while every put is held: the caller must be answered at once, the cell
    # must have stopped, and nothing that the write made may reach the store,
    # a cut or the feed.
    # The cell is gone right after the answer, not at its next timer.
    defp assert_down(pid) do
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, _},
                     300,
                     "the cell kept serving after the write that ended its transaction"
    end

    defp ended_write(env) do
      c = boot(env, gated: true)
      old = pid(c)
      {_, result} = written = go(c, ending(1))
      {c, old, result, written}
    end

    test "a later acknowledged write does not carry the frames of a failed write", env do
      {c, old, result, written} = ended_write(env)

      assert result == {:error, :transaction_ended}
      assert_down(old)

      GatedStore.open(c.gate)
      assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
      finish(c, written)

      assert store_ids(c) == [0, 2]
      assert local_ids(c) == [0, 2]
      refute unreplicated?(c)
    end

    test "a cut never includes a row of a write that failed", env do
      {c, old, result, written} = ended_write(env)

      assert result == {:error, :transaction_ended}
      assert_down(old)

      # The cut is what the store holds: the state of the cell before the write.
      position = Alambique.position(c.inst, c.cell, timeout: 2_000)
      assert {:ok, %{txid: txid}} = position
      stored = store_ids(c)
      finish(c, written)

      assert stored == [0], inspect(%{position: position, stored: stored})
      {:ok, %{txid: head}} = Alambique.Replication.head(repl_ctx(c.inst, c.cell), c.cell)
      assert txid == head
      assert local_ids(c) == [0]
    end

    test "a row committed outside any write is no part of a cut", env do
      c = boot(env)
      old = pid(c)
      orphan(c, 5)

      # The backstop answers the cut with the error and stops the cell, which
      # holds nothing of the row.
      assert Alambique.position(c.inst, c.cell) == {:error, :orphan_frames}
      assert eventually(fn -> not Process.alive?(old) end, 3_000)
      assert store_ids(c) == [0]
      assert local_ids(c) == [0]
    end

    test "a row of a failed write is not published to the feed", env do
      {c, old, result, written} = ended_write(env)
      assert result == {:error, :transaction_ended}
      assert_down(old)

      next =
        Task.async(fn -> Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)") end)

      GatedStore.open(c.gate)
      assert {:ok, _} = Task.await(next, 10_000)
      keys = feed_until(c, 2)
      finish(c, written)

      assert keys == [0, 2]
      assert store_ids(c) == [0, 2]
    end

    test "a row committed outside any write reaches the feed once under :replicate", env do
      c = boot(env, sql: [orphan_frames: :replicate])
      orphan(c, 5)

      # The cut is answered after the frames are a batch of their own.
      assert {:ok, _} = Alambique.position(c.inst, c.cell)
      assert store_ids(c) == [0, 5]
      keys = feed_until(c, 5)
      assert keys == [0, 5]
    end

    test "a Tx.execute with several statements runs none of them", env do
      c = boot(env)
      old = pid(c)

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          Tx.execute(tx, "INSERT INTO t VALUES (1); COMMIT; INSERT INTO t VALUES (2)")
        end)

      assert {:ok, {:error, {:multiple_statements, _}}} = result
      assert pid(c) == old
      refute unreplicated?(c)
      assert local_ids(c) == [0]
      assert store_ids(c) == [0]
      assert {:ok, _} = Alambique.position(c.inst, c.cell)
    end
  end

  # ----------------------------------------------------------------------
  # COMMIT; PRAGMA query_only = ON
  # ----------------------------------------------------------------------

  describe "the tail pragma" do
    test "a flag pragma takes effect when it is prepared, after the COMMIT ran" do
      {:ok, conn} = Sqlite.open(":memory:")
      :ok = Sqlite.execute(conn, "CREATE TABLE t (n)")
      :ok = Sqlite.execute(conn, "BEGIN IMMEDIATE; INSERT INTO t VALUES (1)")
      assert :ok = Sqlite.execute(conn, "COMMIT; PRAGMA query_only = ON")
      assert {:error, _} = Sqlite.execute(conn, "INSERT INTO t VALUES (2)")
      Sqlite.close(conn)
    end
  end
end
