defmodule Alambique.SqlSafetyTest do
  @moduledoc "What caller SQL may do to a cell: read-only reads, no ATTACH, deadlines, result caps, bad parameters (design §5)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Cell

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @rows "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 400000000) SELECT x FROM c"
  @forever "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 400000000) SELECT count(*) FROM c"

  setup do
    setup_store()
  end

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp pid(inst, cell), do: Cell.whereis(Alambique.Config.fetch!(inst), cell)

  describe "read-only reads" do
    test "a PRAGMA written in call syntax is refused and changes nothing", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)

      for sql <- [
            "PRAGMA recursive_triggers(0)",
            "PRAGMA foreign_keys(0)",
            "PRAGMA ignore_check_constraints(1)",
            "PRAGMA max_page_count(1)",
            "PRAGMA synchronous(0)",
            "PRAGMA locking_mode(exclusive)",
            "PRAGMA cache_size(-4000000)",
            "PRAGMA soft_heap_limit(1)",
            "PRAGMA writable_schema(1)",
            "SAVEPOINT s",
            "RELEASE s",
            "ATTACH ':memory:' AS m",
            "VACUUM INTO '#{ctx.dir}/vacuumed.db'",
            "/**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ /**/ BEGIN"
          ] do
        assert {:error, :read_only} = Alambique.query(inst, cell, sql), sql
      end

      refute File.exists?(Path.join(ctx.dir, "vacuumed.db"))
      assert {:ok, %{rows: [[0]]}} = Alambique.query(inst, cell, "PRAGMA foreign_keys")
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "PRAGMA recursive_triggers")
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "PRAGMA synchronous")
      assert {:ok, %{rows: [[0]]}} = Alambique.query(inst, cell, "PRAGMA soft_heap_limit")
      # A write that needs a new page still works.
      assert {:ok, _} =
               Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (2, zeroblob(100000))")
    end

    test "introspection pragmas with an argument still read", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)

      assert {:ok, %{rows: [[0, "n" | _], [1, "v" | _]]}} =
               Alambique.query(inst, cell, "PRAGMA table_info(t)")

      assert {:ok, %{rows: [[_ | _] | _]}} =
               Alambique.query(inst, cell, "SELECT * FROM pragma_table_info('t')")

      assert {:ok, %{rows: []}} = Alambique.query(inst, cell, "PRAGMA index_list(t)")
    end

    test "a hostile statement prefix costs nothing to refuse", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)
      sql = String.duplicate("/**/ ", 30) <> "SELECT 1"

      {micros, result} = :timer.tc(fn -> Alambique.query(inst, cell, sql) end)
      assert {:ok, %{rows: [[1]]}} = result
      assert micros < 50_000
    end
  end

  test "the DDL check of a transaction statement is linear in the statement" do
    alias Alambique.Feed.Capture
    hostile = String.duplicate("/**/ ", 30) <> "SELECT 1"

    {micros, false} = :timer.tc(fn -> Capture.ddl?(hostile) end)
    assert micros < 50_000
    assert Capture.ddl?("/* a */ -- b\n  CREATE TABLE x (y)")
    assert Capture.ddl?(String.duplicate("/**/ ", 30) <> "drop table x")
    refute Capture.ddl?("/* never closed CREATE TABLE x (y)")
    refute Capture.ddl?("SELECT 'create'")
  end

  describe "ATTACH" do
    test "a write cannot attach another cell's file or create a file", ctx do
      inst = start_instance(ctx, migrations: @schema)
      [a, b] = [unique_cell(), unique_cell()]
      {:ok, _} = Alambique.execute(inst, b, "INSERT INTO t (n, v) VALUES (1, 'SECRET-B')")
      :ok = Alambique.hibernate(inst, b)

      path_b = Cell.Local.db_path(Alambique.Config.fetch!(inst), b)
      assert File.exists?(path_b)

      assert {:error, _} = Alambique.execute(inst, a, "ATTACH DATABASE '#{path_b}' AS o")
      assert {:error, _} = Alambique.query(inst, a, "SELECT v FROM o.t")

      planted = Path.join(ctx.dir, "planted.db")
      assert {:error, _} = Alambique.execute(inst, a, "ATTACH '#{planted}' AS p")

      assert {:error, _} =
               Alambique.transaction(inst, a, fn tx ->
                 with {:error, reason} <- Alambique.Tx.execute(tx, "ATTACH '#{planted}' AS p"),
                      do: Alambique.Tx.rollback(tx, reason)
               end)

      refute File.exists?(planted)
    end
  end

  describe "statement deadline" do
    @tag timeout: 30_000
    test "a runaway read ends with :statement_timeout and the cell keeps serving", ctx do
      inst = start_instance(ctx, migrations: @schema, sql: [statement_timeout_ms: 300])
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)
      before = pid(inst, cell)

      assert {:error, :statement_timeout} = Alambique.query(inst, cell, @forever)

      assert {:error, :statement_timeout} =
               Alambique.query(inst, cell, @forever, [], verify_lease: true)

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT 1", [], timeout: 2_000)
      assert pid(inst, cell) == before
    end

    @tag timeout: 30_000
    test "a runaway write rolls back, the caller gets the error and the cell serves", ctx do
      inst = start_instance(ctx, migrations: @schema, sql: [statement_timeout_ms: 300])
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)
      before = pid(inst, cell)

      slow = "INSERT INTO t (n, v) SELECT x + 100, 'y' FROM (#{@rows})"
      assert {:error, :statement_timeout} = Alambique.execute(inst, cell, slow)

      assert {:error, :statement_timeout} =
               Alambique.transaction(inst, cell, fn tx ->
                 {:ok, _} = Alambique.Tx.execute(tx, "INSERT INTO t (n, v) VALUES (5, 'z')")

                 case Alambique.Tx.execute(tx, slow) do
                   {:error, reason} -> Alambique.Tx.rollback(tx, reason)
                   other -> other
                 end
               end)

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t")
      assert {:ok, _} = put(inst, cell, 2)
      assert pid(inst, cell) == before
      assert [[1], [2]] = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")
    end

    @tag timeout: 30_000
    test "a migration past the script deadline is interrupted and the wake fails", ctx do
      inst =
        start_instance(ctx,
          migrations: [{1, "CREATE TABLE t (n); CREATE TABLE big AS #{@forever}"}],
          sql: [script_timeout_ms: 300]
        )

      cell = unique_cell()

      assert {:error, {:migration, 1, :statement_timeout}} =
               Alambique.query(inst, cell, "SELECT 1")

      assert :none = Alambique.owner(inst, cell)
    end

    @tag timeout: 30_000
    test "a Tx.script past the script deadline is interrupted and rolls back", ctx do
      inst = start_instance(ctx, migrations: @schema, sql: [script_timeout_ms: 300])
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)
      before = pid(inst, cell)

      assert {:error, :statement_timeout} =
               Alambique.transaction(inst, cell, fn tx ->
                 case Alambique.Tx.script(tx, "CREATE TABLE big AS #{@forever}") do
                   {:error, reason} -> Alambique.Tx.rollback(tx, reason)
                   other -> other
                 end
               end)

      assert {:ok, %{rows: []}} =
               Alambique.query(inst, cell, "SELECT name FROM sqlite_master WHERE name = 'big'")

      assert {:ok, _} = put(inst, cell, 2)
      assert pid(inst, cell) == before
    end

    @tag timeout: 30_000
    test "a call lowers the statement deadline but never raises it past the :sql one", ctx do
      inst = start_instance(ctx, migrations: @schema, sql: [statement_timeout_ms: 3_000])
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)
      slow = "INSERT INTO t (n, v) SELECT x + 100, 'y' FROM (#{@rows})"

      assert {:error, :statement_timeout} =
               timed(
                 fn -> Alambique.query(inst, cell, @forever, [], statement_timeout_ms: 200) end,
                 2_000
               )

      assert {:error, :statement_timeout} =
               timed(
                 fn -> Alambique.execute(inst, cell, slow, [], statement_timeout_ms: 200) end,
                 2_000
               )

      assert {:error, :statement_timeout} =
               timed(
                 fn ->
                   Alambique.transaction(
                     inst,
                     cell,
                     fn tx ->
                       case Alambique.Tx.query(tx, @forever) do
                         {:error, reason} -> Alambique.Tx.rollback(tx, reason)
                         other -> other
                       end
                     end,
                     statement_timeout_ms: 200
                   )
                 end,
                 2_000
               )

      # Above the ceiling the ceiling applies.
      assert {:error, :statement_timeout} =
               timed(
                 fn ->
                   Alambique.query(inst, cell, @forever, [],
                     statement_timeout_ms: 600_000,
                     timeout: 20_000
                   )
                 end,
                 10_000
               )

      for bad <- [0, -1, 1.5, "1", nil] do
        assert {:error, :bad_option} =
                 Alambique.query(inst, cell, "SELECT 1", [], statement_timeout_ms: bad)

        assert {:error, :bad_option} =
                 Alambique.execute(inst, cell, "SELECT 1", [], statement_timeout_ms: bad)
      end

      assert {:ok, %{rows: [[1]]}} =
               Alambique.query(inst, cell, "SELECT 1", [], statement_timeout_ms: 100)
    end

    defp timed(fun, limit_ms) do
      {us, result} = :timer.tc(fun)
      assert div(us, 1000) < limit_ms
      result
    end

    test "the options are validated and default from the operation timeout", ctx do
      base = fn extra ->
        Keyword.merge(
          [name: :cfg, store: ctx.store, data_dir: ctx.dir, prefix: "p"],
          extra
        )
      end

      assert %{statement_timeout_ms: 10_000, script_timeout_ms: 60_000, max_result_rows: 100_000} =
               Alambique.Config.build(base.([])).sql

      assert Alambique.Config.build(base.(op_timeout_ms: 2_000)).sql.statement_timeout_ms == 2_000

      for bad <- [
            [statement_timeout_ms: 0],
            [script_timeout_ms: 0],
            [max_result_rows: 0],
            [max_result_bytes: -1],
            [x: 1]
          ] do
        assert_raise ArgumentError, fn -> Alambique.Config.build(base.(sql: bad)) end
      end
    end
  end

  describe "result size" do
    test "a result over the row or byte cap is refused", ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          sql: [max_result_rows: 5, max_result_bytes: 1_000_000]
        )

      cell = unique_cell()
      for n <- 1..6, do: {:ok, _} = put(inst, cell, n)

      assert {:error, :result_too_large} = Alambique.query(inst, cell, "SELECT * FROM t")
      assert {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT * FROM t LIMIT 5")
      assert length(rows) == 5
      assert {:error, :result_too_large} = Alambique.query(inst, cell, "SELECT zeroblob(2000000)")
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT zeroblob(500000)")

      assert {:error, :result_too_large} =
               Alambique.query(inst, cell, "SELECT zeroblob(900000000)")

      assert {:ok, %{rows: [[6]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    end

    test "writes with RETURNING are capped too", ctx do
      inst = start_instance(ctx, migrations: @schema, sql: [max_result_rows: 2])
      cell = unique_cell()

      assert {:error, :result_too_large} =
               Alambique.execute(
                 inst,
                 cell,
                 "INSERT INTO t (n, v) VALUES (1, 'a'), (2, 'b'), (3, 'c') RETURNING n"
               )

      assert {:ok, %{rows: []}} = Alambique.query(inst, cell, "SELECT n FROM t")
    end
  end

  describe "bad parameters" do
    test "a wrong count or type is an error and the cell survives", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      {:ok, _} = put(inst, cell, 1)
      before = pid(inst, cell)

      for params <- [[], [%{}], [{1, 2}], [1, 2]] do
        assert {:error, {:bad_params, _}} = Alambique.query(inst, cell, "SELECT ?", params)

        assert {:error, {:bad_params, _}} =
                 Alambique.query(inst, cell, "SELECT ?", params, verify_lease: true)

        assert {:error, {:bad_params, _}} =
                 Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (?)", params)
      end

      assert pid(inst, cell) == before
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t")
    end
  end
end
