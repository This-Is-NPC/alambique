defmodule Alambique.DdlDecisionPropertiesTest do
  @moduledoc """
  `Alambique.Sqlite.ddl?/1` is the decision of the script of the NIF: for any
  prefix of blanks and comments, a statement is schema for the library exactly
  when the script runs it instead of refusing it as `:not_ddl`.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Sqlite

  defp skipped do
    one_of([
      member_of([" ", "\t", "\n", "\r", "\f", "﻿"]),
      map(string([?a..?z, ?\s, ?;, ?', ?*], max_length: 5), &"-- #{&1}\n"),
      map(string([?a..?z, ?\s, ?;, ?', ?-], max_length: 5), &"/* #{&1} */")
    ])
  end

  defp prefix, do: map(list_of(skipped(), max_length: 6), &Enum.join/1)

  defp statement do
    member_of([
      {"CREATE TABLE x (a)", true},
      {"create index i on x (a)", true},
      {"DROP TABLE IF EXISTS x", true},
      {"alter table x add column b", true},
      {"SELECT 1", false},
      {"INSERT INTO x VALUES (1)", false},
      {"WITH c AS (SELECT 1) SELECT * FROM c", false},
      {"PRAGMA user_version", false}
    ])
  end

  property "the Elixir decision is the one the script of the NIF makes" do
    {:ok, conn} = Sqlite.open(":memory:")
    :ok = Sqlite.execute(conn, "CREATE TABLE x (a)")

    check all(prefix <- prefix(), {text, expected} <- statement(), max_runs: 300) do
      sql = prefix <> text
      assert Sqlite.ddl?(sql) == expected, inspect(sql)

      refused? = match?({:error, {:not_ddl, _}}, Sqlite.script(conn, sql))
      assert refused? == not Sqlite.ddl?(sql), inspect(sql)
      # Put the schema back for the next run.
      Sqlite.execute(conn, "DROP TABLE IF EXISTS x; CREATE TABLE x (a)")
    end

    Sqlite.close(conn)
  end
end
