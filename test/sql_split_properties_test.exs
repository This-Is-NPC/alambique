defmodule Alambique.Cell.SqlSplitPropertiesTest do
  @moduledoc """
  `Alambique.Cell.Sql.split/1` cuts a migration script at the semicolons that
  end a statement, and only there: semicolons in quotes, comments and trigger
  bodies stay, nothing else is lost, and no input raises.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Cell.Sql

  # Text without the characters that start a token.
  defp plain(max \\ 8),
    do: string([?a..?z, ?0..?9, ?\s, ?,, ?=, ?(, ?)], min_length: 1, max_length: max)

  defp word, do: string(?a..?z, min_length: 1, max_length: 6)

  # A piece of a statement where a semicolon does not end it.
  defp shielded do
    one_of([
      map(string([?a..?z, ?;, ?\s, ?', ?", ?`, ?[, ?-], max_length: 6), &escape(&1, ?')),
      map(string([?a..?z, ?;, ?\s, ?', ?", ?`, ?[, ?-], max_length: 6), &escape(&1, ?")),
      map(string([?a..?z, ?;, ?\s, ?', ?", ?`, ?[, ?-], max_length: 6), &escape(&1, ?`)),
      map(string([?a..?z, ?;, ?\s, ?', ?", ?`], max_length: 6), &"[#{&1}]"),
      map(string([?a..?z, ?;, ?\s, ?', ?"], max_length: 6), &"/* #{&1} */"),
      map(string([?a..?z, ?;, ?\s, ?', ?"], max_length: 6), &"-- #{&1}\n")
    ])
  end

  # Quotes inside a quoted text are doubled, as SQL spells them.
  defp escape(text, q) do
    quote = <<q>>
    quote <> String.replace(text, quote, quote <> quote) <> quote
  end

  defp tokens, do: list_of(one_of([plain(), shielded()]), max_length: 5)

  # A statement ending in a semicolon, with no other semicolon outside a quote.
  defp statement do
    gen all(first <- word(), rest <- tokens()) do
      Enum.join(["select", first | rest], " ") <> ";"
    end
  end

  defp trigger do
    gen all(
          name <- word(),
          body <- list_of(word(), min_length: 1, max_length: 3),
          case? <- boolean()
        ) do
      inner =
        Enum.map_join(body, " ", fn w ->
          if case?,
            do: "update b set #{w} = case when 1 then 2 end;",
            else: "delete from #{w};"
        end)

      "CREATE TRIGGER #{name} AFTER INSERT ON a BEGIN #{inner} END;"
    end
  end

  defp script do
    list_of(one_of([statement(), trigger()]), min_length: 1, max_length: 5)
  end

  defp gap, do: member_of([" ", "\n", "\n\n", " \t", "  "])

  defp squeeze(text), do: String.replace(text, ~r/\s+/, "")

  property "a script of statements splits into those statements" do
    check all(statements <- script(), gaps <- list_of(gap(), length: 5)) do
      sql = statements |> Enum.zip(gaps) |> Enum.map_join(fn {s, g} -> s <> g end)

      assert Sql.split(sql) == statements
    end
  end

  property "a last statement without a semicolon is kept as written" do
    check all(statements <- script(), last <- word(), gap <- gap()) do
      sql = Enum.join(statements, gap) <> gap <> "select " <> last

      assert Sql.split(sql) == statements ++ ["select " <> last]
    end
  end

  property "splitting loses nothing but blank statements and whitespace around them" do
    check all(
            statements <- script(),
            blanks <- list_of(member_of([";", " ; ", "/* x */;"]), length: 5)
          ) do
      sql = statements |> Enum.zip(blanks) |> Enum.map_join(fn {s, b} -> s <> b end)

      assert squeeze(Enum.join(Sql.split(sql))) == squeeze(Enum.join(statements))
    end
  end

  property "a semicolon is only ever cut at the end of a statement" do
    check all(piece <- shielded(), word <- word()) do
      sql = "select " <> piece <> " from " <> word <> ";"

      assert Sql.split(sql) == [sql]
    end
  end

  property "split never raises and returns trimmed, non-empty statements" do
    alphabet = [?a..?e, ?;, ?\s, ?\n, ?', ?", ?`, ?[, ?], ?-, ?/, ?*, ?(, ?)]

    check all(
            sql <- one_of([string(alphabet, max_length: 40), binary(max_length: 40)]),
            max_runs: 500
          ) do
      for statement <- Sql.split(sql) do
        assert statement != ""
        assert statement == String.trim(statement)
      end
    end
  end
end
