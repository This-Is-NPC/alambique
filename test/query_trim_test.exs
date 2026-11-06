defmodule Alambique.QueryTrimTest do
  @moduledoc "The cache of replicas is trimmed when it grew, and counts replicas being built (design §14)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Query
  alias Alambique.Query.Cache

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]

  setup do
    setup_store()
  end

  test "a run with many rows lists the cache once, not every 256 results", env do
    owner = start_instance(env, migrations: @schema)

    for cell <- 1..4,
        do:
          {:ok, _} = Alambique.execute(owner, "c#{cell}", "INSERT INTO notes (body) VALUES ('x')")

    reader = start_instance(env, migrations: @schema)
    capture([[:alambique, :query, :trim]])

    # 4 cells of 700 rows each: 2 800 results.
    sql =
      "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 700) SELECT i FROM n"

    result = Query.all(reader, ["c1", "c2", "c3", "c4"], sql)

    assert length(result.rows) == 2_800
    # Only the trim at the end of the run.
    assert drain_trims(0) == 1
  end

  defp drain_trims(n) do
    receive do
      {:telemetry, [:alambique, :query, :trim], _, _} -> drain_trims(n + 1)
    after
      0 -> n
    end
  end

  test "a replica being built counts against the bound but is not evicted" do
    dir = Alambique.TestDirs.new("trim")
    on_exit(fn -> File.rm_rf(dir) end)

    done = Path.join(Cache.cell_dir(dir, "a"), "0000000000000001-1.db")
    File.mkdir_p!(Path.dirname(done))
    File.write!(done, String.duplicate("x", 600))
    building = Cache.tmp(dir, "b")
    File.write!(building, String.duplicate("y", 600))

    assert {1, 600} = Cache.trim(dir, 1_000, 600)
    refute File.exists?(done)
    assert File.exists?(building)
  end
end
