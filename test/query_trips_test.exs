defmodule Alambique.QueryTripsTest do
  @moduledoc "A cell of a cross-cell query waits one round trip of the store for its listings, not two (design §14)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Query
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @count "SELECT count(*) FROM notes"
  @cells 20
  @concurrency 4
  @latency_ms 100

  setup do
    setup_store()
  end

  test "up-to-date cells cost one trip each, ceil(cells / concurrency) in sequence", ctx do
    owner = start_instance(ctx, migrations: @schema)
    reader = start_instance(ctx, migrations: @schema)
    cells = for i <- 1..@cells, do: "c-#{i}"

    for cell <- cells,
        do: {:ok, _} = Alambique.execute(owner, cell, "INSERT INTO notes (body) VALUES ('x')")

    opts = [concurrency: @concurrency]
    assert %{errors: [], rows: rows} = Query.all(reader, cells, @count, [], opts)
    assert length(rows) == @cells

    # Warm cache: every cell asks the store for its two listings and reads nothing.
    MemoryStore.inject(ctx.store_config, kind: :latency, ms: @latency_ms, ops: [:list])

    {us, result} = :timer.tc(fn -> Query.all(reader, cells, @count, [], opts) end)

    assert %{errors: [], rows: rows} = result
    assert Enum.all?(rows, &(&1.values == [1]))
    waves = ceil(@cells / @concurrency)
    elapsed_ms = div(us, 1_000)

    # Sequential listings would take 2 * waves trips.
    assert elapsed_ms >= waves * @latency_ms
    assert elapsed_ms < div(3 * waves * @latency_ms, 2), "took #{elapsed_ms} ms"
  end

  @slow "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300000) SELECT max(i) FROM n"

  defp seeded(owner, prefix) do
    cells = for i <- 1..@cells, do: "#{prefix}-#{i}"

    for cell <- cells,
        do: {:ok, _} = Alambique.execute(owner, cell, "INSERT INTO notes (body) VALUES ('x')")

    cells
  end

  test "a query alone never meets :busy from its own cells", ctx do
    owner = start_instance(ctx, migrations: @schema)
    reader = start_instance(ctx, migrations: @schema, sql: [max_concurrent_statements: 1])
    cells = seeded(owner, "s")

    # 16 cells over one slot: their statements overlap unless the query orders them.
    assert %{errors: [], rows: rows} = Query.all(reader, cells, @slow, [], concurrency: 16)
    assert length(rows) == @cells
  end

  test "another caller holding the slots still gets :busy at once", ctx do
    owner = start_instance(ctx, migrations: @schema)
    reader = start_instance(ctx, migrations: @schema, sql: [max_concurrent_statements: 1])
    cells = seeded(owner, "b")
    assert %{errors: []} = Query.all(reader, cells, "SELECT 1")

    # The slot count is VM-wide: a long statement of another caller holds it.
    holder =
      Task.async(fn ->
        Alambique.query(owner, "b-1", String.replace(@slow, "300000", "300000000"))
      end)

    busy = await_busy(reader, cells, System.monotonic_time(:millisecond) + 5_000)
    assert busy != []
    assert Enum.all?(busy, &match?({_, :busy}, &1))
    Task.shutdown(holder, :brutal_kill)
  end

  defp await_busy(reader, cells, limit) do
    {us, %{errors: errors}} = :timer.tc(fn -> Query.all(reader, cells, "SELECT 1") end)

    cond do
      errors != [] ->
        assert div(us, 1_000) < 1_000, "answered :busy after #{div(us, 1_000)} ms"
        errors

      System.monotonic_time(:millisecond) > limit ->
        flunk("the slot was never held")

      true ->
        await_busy(reader, cells, limit)
    end
  end
end
