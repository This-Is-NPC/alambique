defmodule Alambique.QuerySlotsTest do
  @moduledoc """
  A query holds at most `query: [max_statement_share:]` of the statement slots
  (default one half, design §14), so two queries fit together and slots remain
  for cells (design §7).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Query, Sqlite}

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @slow "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300000) SELECT max(i) FROM n"

  setup do
    setup_store()
  end

  defp seeded(ctx, slots, cells, query \\ []) do
    owner = start_instance(ctx, migrations: @schema)

    reader =
      start_instance(ctx,
        migrations: @schema,
        sql: [max_concurrent_statements: slots],
        query: query
      )

    run = System.unique_integer([:positive])
    ids = for i <- 1..cells, do: "c#{run}-#{i}"

    for id <- ids,
        do: {:ok, _} = Alambique.execute(owner, id, "INSERT INTO notes (body) VALUES ('x')")

    # Warm the replicas: the statements are what the queries then overlap on.
    assert %{errors: []} = Query.all(reader, ids, "SELECT 1")
    {reader, ids}
  end

  defp sample(parent, peak) do
    peak = max(peak, Sqlite.user_statements())

    receive do
      :stop -> send(parent, {:peak, peak})
    after
      1 -> sample(parent, peak)
    end
  end

  # Runs `queries` overlapping queries; returns their errors and the most
  # statements the node ran at once.
  defp overlap(reader, ids, queries, opts \\ []) do
    parent = self()
    sampler = spawn_link(fn -> sample(parent, 0) end)

    errors =
      1..queries
      |> Task.async_stream(fn _ -> Query.all(reader, ids, @slow, [], opts).errors end,
        max_concurrency: queries,
        timeout: 60_000
      )
      |> Enum.flat_map(fn {:ok, errors} -> errors end)

    send(sampler, :stop)
    assert_receive {:peak, peak}, 1_000
    {errors, peak}
  end

  test "two overlapping queries both succeed", ctx do
    {reader, ids} = seeded(ctx, 4, 8)
    assert {[], _peak} = overlap(reader, ids, 2)
  end

  test "a share of 1 lets one query take every slot, so a second one gets :busy", ctx do
    {reader, ids} = seeded(ctx, 4, 8, max_statement_share: 1.0)
    assert {errors, 4} = overlap(reader, ids, 2)
    assert errors != [] and Enum.all?(errors, &match?({_, :busy}, &1))
  end

  test "the share of a call overrides the one of the instance", ctx do
    {reader, ids} = seeded(ctx, 8, 8)
    assert {[], peak} = overlap(reader, ids, 1, max_statement_share: 0.25)
    assert peak <= 2
  end

  property "a query holds at most its share of the slots, and queries that fit never get :busy",
           ctx do
    check all(
            slots <- integer(1..8),
            share <- member_of([0.25, 0.5, 0.75, 1.0]),
            queries <- integer(1..3),
            cells <- integer(4..10),
            max_runs: 12
          ) do
      {reader, ids} = seeded(ctx, slots, cells, max_statement_share: share)
      {errors, peak} = overlap(reader, ids, queries)
      places = min(max(floor(slots * share), 1), slots)

      assert peak <= min(queries * places, slots)
      if queries * places <= slots, do: assert(errors == [])
    end
  end
end
