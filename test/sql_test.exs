defmodule Alambique.Cell.SqlTest do
  use ExUnit.Case, async: true

  alias Alambique.Cell.Sql

  test "splits statements and drops empty ones" do
    assert Sql.split("CREATE TABLE a (x); ;\n INSERT INTO a VALUES (1);  -- end\n") ==
             ["CREATE TABLE a (x);", "INSERT INTO a VALUES (1);"]
  end

  test "keeps semicolons inside quotes and comments" do
    sql = "INSERT INTO a VALUES ('x;y', \"p;q\", [r;s]); /* ; */ SELECT 1 -- ;\n;"
    assert [first, second] = Sql.split(sql)
    assert first == "INSERT INTO a VALUES ('x;y', \"p;q\", [r;s]);"
    assert second =~ "SELECT 1"
  end

  test "keeps a trigger body whole, including CASE ... END" do
    sql = """
    CREATE TRIGGER t AFTER INSERT ON a BEGIN
      UPDATE b SET x = CASE WHEN new.y THEN 1 ELSE 2 END;
      DELETE FROM c;
    END;
    INSERT INTO a VALUES (1)
    """

    assert [trigger, "INSERT INTO a VALUES (1)"] = Sql.split(sql)
    assert String.ends_with?(trigger, "END;")
    assert trigger =~ "DELETE FROM c;"
  end

  test "an unterminated last statement is kept" do
    assert Sql.split("SELECT 1") == ["SELECT 1"]
    assert Sql.split("") == []
  end
end
