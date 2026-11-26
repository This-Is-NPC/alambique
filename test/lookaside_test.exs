defmodule Alambique.LookasideTest do
  @moduledoc "The lookaside option of `Alambique.Sqlite.open/2` reaches the native library."
  use ExUnit.Case, async: true

  alias Alambique.Sqlite

  @moduletag :tmp_dir

  test "connections open with any of the settings and run statements", %{tmp_dir: dir} do
    for {setting, i} <- Enum.with_index([nil, {1200, 40}, {512, 16}, {0, 0}, {1200, 0}]) do
      {:ok, conn} = Sqlite.open(Path.join(dir, "l#{i}.db"), lookaside: setting)
      :ok = Sqlite.execute(conn, "CREATE TABLE t (x)")
      :ok = Sqlite.execute(conn, "INSERT INTO t VALUES (1), (2)")
      assert {:ok, %{rows: [[2]]}} = Sqlite.run(conn, "SELECT count(*) FROM t", [])
      :ok = Sqlite.close(conn)
    end
  end

  test "a setting that is not a pair of sizes is an argument error", %{tmp_dir: dir} do
    for bad <- [:big, {1200, :many}, {-1, 4}, [512, 16]] do
      assert_raise ArgumentError, fn -> Sqlite.open(Path.join(dir, "bad.db"), lookaside: bad) end
    end
  end

  test "a slot smaller than SQLite can use gives no lookaside and the connection opens", %{
    tmp_dir: dir
  } do
    {:ok, conn} = Sqlite.open(Path.join(dir, "five.db"), lookaside: {5, 5})
    assert :ok = Sqlite.execute(conn, "CREATE TABLE t (x)")
    :ok = Sqlite.close(conn)
  end
end
