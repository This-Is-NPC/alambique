defmodule Alambique.QueryPropertiesTest do
  @moduledoc """
  A cross-cell query returns, for every cell, what a fresh full restore of the
  cell gives at the TXID the row reports (design §14), whatever the workload,
  hibernations, compactions (snapshots) and earlier queries (and so cache
  states) came before.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Query
  alias Alambique.Query.Row

  @cells ["p-1", "p-2", "p-3"]
  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]

  defp cell_gen, do: member_of(@cells)

  defp write_gen do
    one_of([
      gen(
        all(
          id <- integer(1..8),
          body <- string(:alphanumeric, max_length: 20),
          do: {:put, id, body}
        )
      ),
      map(integer(1..8), &{:del, &1}),
      map(list_of(integer(1..8), min_length: 1, max_length: 4), &{:many, &1})
    ])
  end

  defp command_gen do
    frequency([
      {6, gen(all(cell <- cell_gen(), write <- write_gen(), do: {:write, cell, write}))},
      {2, map(cell_gen(), &{:hibernate, &1})},
      {2, map(cell_gen(), &{:compact, &1})},
      {5, constant(:query)},
      {1, constant(:query_old)}
    ])
  end

  defp apply_write(inst, cell, {:put, id, body}),
    do:
      Alambique.execute(inst, cell, "INSERT OR REPLACE INTO notes (id, body) VALUES (?, ?)", [
        id,
        body
      ])

  defp apply_write(inst, cell, {:del, id}),
    do: Alambique.execute(inst, cell, "DELETE FROM notes WHERE id = ?", [id])

  defp apply_write(inst, cell, {:many, ids}) do
    Alambique.transaction(inst, cell, fn tx ->
      for id <- ids,
          do:
            Alambique.Tx.execute(tx, "INSERT OR REPLACE INTO notes (id, body) VALUES (?, 'm')", [
              id
            ])
    end)
  end

  @sql "SELECT count(*), coalesce(group_concat(id || ':' || body, ';'), '') " <>
         "FROM (SELECT id, body FROM notes ORDER BY id)"

  property "the result equals a full restore of each cell at the reported TXID" do
    base = setup_store()

    check all(commands <- list_of(command_gen(), min_length: 4, max_length: 30), max_runs: 60) do
      {:ok, store} = AlambiqueTestkit.MemoryStore.start_link()
      config = AlambiqueTestkit.MemoryStore.config(store)
      dir = Path.join(base.dir, "run-#{System.unique_integer([:positive])}")
      run = %{store: {AlambiqueTestkit.MemoryStore, config}, store_config: config, dir: dir}
      owner = start_instance(run, migrations: @schema)
      reader = start_instance(run, migrations: @schema)

      try do
        commands
        |> Enum.reduce(MapSet.new(), fn
          {:write, cell, write}, seen ->
            {:ok, _} = apply_write(owner, cell, write)
            MapSet.put(seen, cell)

          {:hibernate, cell}, seen ->
            :ok = Alambique.hibernate(owner, cell)
            seen

          # A snapshot newer than a cached replica sends the refresh to a restore.
          {:compact, cell}, seen ->
            :ok = Alambique.hibernate(owner, cell)

            Alambique.Compaction.run(repl_ctx(owner, cell), cell,
              fanout: 2,
              snapshot_interval_txids: 1
            )

            seen

          :query, seen ->
            verify(run, reader, seen)
            seen

          :query_old, seen ->
            verify(run, reader, seen, at: {:txid, 3})
            seen
        end)
        |> then(&verify(run, reader, &1, owner: owner))
      after
        stop_supervised(owner)
        stop_supervised(reader)
        GenServer.stop(store)
      end
    end
  end

  defp verify(run, reader, cells, opts \\ []) do
    cells = Enum.sort(cells)
    result = Query.all(reader, cells, @sql, [], Keyword.take(opts, [:at]))

    # A point that a cell never reached is an error of that cell.
    for {_cell, reason} <- result.errors, do: assert(match?({:target_not_reached, _}, reason))

    for %Row{cell: cell, txid: txid, values: values} <- result.rows do
      assert [values] == restored(run, cell, txid)

      if owner = opts[:owner],
        do: assert({:ok, %{rows: [^values]}} = Alambique.query(owner, cell, @sql))
    end

    if opts[:at] == nil, do: assert(length(result.rows) == length(cells))
  end

  defp restored(run, cell, txid) do
    repl =
      Alambique.Replication.ctx(
        store: run.store,
        prefix: "cells-test",
        node: "verifier",
        timeout_ms: 5_000
      )

    File.mkdir_p!(run.dir)
    path = Path.join(run.dir, "verify-#{System.unique_integer([:positive])}.db")
    {:ok, %{txid: ^txid}} = Alambique.Replication.restore(repl, cell, path, {:txid, txid})
    {:ok, conn} = Alambique.Cell.Db.open_read_only(path)
    {:ok, %{rows: rows}} = Alambique.Cell.Db.read(conn, @sql, [])
    Alambique.Cell.Db.close(conn)
    File.rm(path)
    rows
  end
end
