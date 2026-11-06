defmodule Alambique.ReadOnlyPropertiesTest do
  @moduledoc """
  The read path never writes (design §5): whatever the spelling of a
  statement that would write, `Alambique.query/4` refuses it with
  `{:error, :read_only}` and the cell, its session settings and the files
  around it stay as they were. Reads in the same spellings succeed.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  # Statements that would write, as keyword/raw parts: keywords get a random
  # case, raw parts are kept as they are.
  defp writes(dir) do
    vacuum = Path.join(dir, "vacuumed.db")

    one_of(
      Enum.map(
        [
          ["insert into t (n, v) values (100,", {:raw, "'new')"}],
          ["insert or replace into t (n, v) values (1,", {:raw, "'new')"}],
          ["replace into t (n, v) values (1,", {:raw, "'new')"}],
          ["insert into t select n + 1000, v from t"],
          ["update t set v =", {:raw, "'changed'"}],
          ["update t set n = n + 1 where n > 0"],
          ["delete from t"],
          ["delete from t where n = 1"],
          ["create table z (a)"],
          ["create temp table z (a)"],
          ["create index i on t (v)"],
          ["create view w as select 1"],
          ["create trigger tr after insert on t begin select 1; end"],
          ["drop table t"],
          ["drop table if exists t"],
          ["alter table t add column w"],
          ["alter table t rename to u"],
          ["pragma user_version = 9"],
          ["pragma foreign_keys = 0"],
          ["pragma recursive_triggers = 0"],
          ["pragma journal_mode = delete"],
          ["pragma synchronous = off"],
          ["pragma writable_schema = on"],
          ["pragma query_only = off"],
          ["pragma ignore_check_constraints = 1"],
          ["pragma wal_checkpoint(truncate)"],
          ["pragma user_version(9)"],
          ["attach", {:raw, "':memory:'"}, "as m"],
          ["attach database", {:raw, "'#{dir}/attached.db'"}, "as m"],
          ["detach m"],
          ["vacuum"],
          ["vacuum into", {:raw, "'#{vacuum}'"}],
          ["reindex"],
          ["analyze"],
          ["begin"],
          ["begin immediate"],
          ["commit"],
          ["rollback"],
          ["savepoint s"],
          ["release s"]
        ],
        &constant/1
      )
    )
  end

  defp reads do
    one_of(
      Enum.map(
        [
          ["select n, v from t"],
          ["select count(*) from t"],
          ["select", {:raw, "'a;b'"}, "as x"],
          ["select * from sqlite_master"],
          ["select * from (select 1) as s"],
          ["values (1), (2)"],
          ["explain select * from t"],
          ["explain query plan select * from t"],
          ["with c(x) as (select 1) select x from c"],
          ["pragma user_version"],
          ["pragma foreign_keys"],
          ["pragma table_info(t)"],
          ["pragma index_list(t)"],
          ["select * from pragma_table_info(", {:raw, "'t')"}]
        ],
        &constant/1
      )
    )
  end

  defp mixed_case(word) do
    bind(list_of(boolean(), length: String.length(word)), fn flags ->
      constant(
        word
        |> String.graphemes()
        |> Enum.zip(flags)
        |> Enum.map_join(fn
          {c, true} -> String.upcase(c)
          {c, false} -> c
        end)
      )
    end)
  end

  # Joins the parts of a statement with random casing of the keywords.
  defp spell(parts) do
    parts
    |> Enum.map(fn
      {:raw, text} ->
        constant(text)

      words ->
        words
        |> String.split(" ")
        |> Enum.map(&mixed_case/1)
        |> fixed_list()
        |> map(&Enum.join(&1, " "))
    end)
    |> fixed_list()
    |> map(&Enum.join(&1, " "))
  end

  defp blank,
    do: member_of(["", " ", "\n", "\t ", "/* c */", "-- c\n", "/* ; */ ", "/**/ -- x\n"])

  # The statement inside the spellings a caller can give it.
  defp wrapped(statement, cte?) do
    gen all(
          before <- blank(),
          cte <- if(cte?, do: member_of(["", "with c as (select 1) "]), else: constant("")),
          tail <- member_of(["", ";", " ;", "; ", ";\n-- end", " /* done */"]),
          body <- statement
        ) do
      before <> cte <> body <> tail
    end
  end

  defp parts_statement(parts), do: spell(parts)

  # DML takes a WITH clause in front; the other statements do not.
  defp dml?([first | _]) when is_binary(first),
    do: String.starts_with?(first, ["insert", "update", "delete", "replace"])

  defp snapshot(inst, cell) do
    for sql <- [
          "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY 1, 2",
          "SELECT n, v FROM t ORDER BY n",
          "PRAGMA user_version",
          "PRAGMA foreign_keys",
          "PRAGMA recursive_triggers",
          "PRAGMA synchronous",
          "PRAGMA journal_mode",
          "PRAGMA writable_schema",
          "PRAGMA query_only",
          "SELECT name FROM pragma_database_list WHERE name <> 'temp'"
        ] do
      {sql, Alambique.query(inst, cell, sql)}
    end
  end

  defp assert_unchanged(inst, cell, before, sql) do
    for {{query, was}, {query, now}} <- Enum.zip(before, snapshot(inst, cell)) do
      assert now == was, "#{query} changed after #{sql}"
    end
  end

  setup do
    setup_store()
  end

  property "a statement that would write is refused and changes nothing", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()

    for n <- 1..3,
        do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

    before = snapshot(inst, cell)
    files = File.ls!(ctx.dir)
    assert {:ok, %{rows: [[3]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")

    statement =
      bind(writes(ctx.dir), fn parts ->
        parts |> parts_statement() |> then(&wrapped(&1, dml?(parts)))
      end)

    check all(sql <- statement, max_runs: 300) do
      assert Alambique.query(inst, cell, sql) == {:error, :read_only}, sql
      assert_unchanged(inst, cell, before, sql)
      assert File.ls!(ctx.dir) == files
    end
  end

  property "a read is answered, whatever its spelling", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (1, 'x')")

    statement = bind(reads(), fn parts -> parts |> spell() |> then(&wrapped(&1, false)) end)

    check all(sql <- statement, max_runs: 150) do
      assert {:ok, %{rows: rows}} = Alambique.query(inst, cell, sql), sql
      assert is_list(rows)
    end

    assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
  end
end
