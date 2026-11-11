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
end
