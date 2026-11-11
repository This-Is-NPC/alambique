defmodule Alambique.StatementSlotsTest do
  @moduledoc "The node-wide limit on user statements keeps dirty IO schedulers free for commits (design §7)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Sqlite

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @forever "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 400000000) SELECT count(*) FROM c"

  setup do
    setup_store()
  end

  defp reads(inst, cells) do
    for cell <- cells do
      Task.async(fn -> Alambique.query(inst, cell, @forever, [], timeout: 10_000) end)
    end
  end

  defp running(n), do: eventually(fn -> Sqlite.user_statements() >= n end)

  defp seed(inst, cells) do
    for cell <- cells,
        do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (1)")
  end

  @tag timeout: 60_000
  test "a commit on another cell completes while reads fill the dirty IO pool", ctx do
    inst = start_instance(ctx, migrations: @schema, sql: [statement_timeout_ms: 5_000])
    pool = :erlang.system_info(:dirty_io_schedulers)
    cells = for _ <- 1..pool, do: unique_cell()
    other = unique_cell()
    seed(inst, [other | cells])

    tasks = reads(inst, cells)
    running(pool - 2)

    assert {:ok, _} =
             Alambique.execute(inst, other, "INSERT INTO t (n) VALUES (2)", [], timeout: 1_000)

    results = Task.await_many(tasks, 15_000)
    assert Enum.all?(results, &(&1 in [{:error, :busy}, {:error, :statement_timeout}]))
    assert {:error, :busy} in results
    assert Sqlite.user_statements() == 0
  end

  @tag timeout: 60_000
  test "reads over max_concurrent_statements get :busy at once and the cell goes on", ctx do
    inst =
      start_instance(ctx,
        migrations: @schema,
        sql: [statement_timeout_ms: 1_500, max_concurrent_statements: 2]
      )

    cells = for _ <- 1..5, do: unique_cell()
    seed(inst, cells)

    tasks = reads(inst, cells)
    running(2)
    assert Sqlite.user_statements() <= 2

    results = Task.await_many(tasks, 15_000)
    assert Enum.count(results, &(&1 == {:error, :busy})) == 3
    assert Enum.count(results, &(&1 == {:error, :statement_timeout})) == 2
    assert Sqlite.user_statements() == 0
    assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, hd(cells), "SELECT 1")
  end

  @tag timeout: 60_000
  test "a write is not refused because reads hold the slots", ctx do
    inst =
      start_instance(ctx,
        migrations: @schema,
        sql: [statement_timeout_ms: 1_500, max_concurrent_statements: 1]
      )

    [a, b] = [unique_cell(), unique_cell()]
    seed(inst, [a, b])

    [task] = reads(inst, [a])
    running(1)

    assert {:ok, {:ok, %{rows: [[1]]}}} =
             Alambique.transaction(inst, b, &Alambique.Tx.query(&1, "SELECT 1"))

    assert {:ok, _} = Alambique.execute(inst, b, "INSERT INTO t (n) VALUES (2)")

    assert {:error, :statement_timeout} = Task.await(task, 10_000)
  end
end
