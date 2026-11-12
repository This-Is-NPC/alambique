defmodule Alambique.WalPagesTest do
  @moduledoc """
  Pages that one small write puts in its segment (and so in the WAL): the
  table, its index, and what the change feed adds. A write that bookkeeps in
  another page costs 4 KiB more on every commit (docs/how-to-size-storage.md).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.LtxReader

  # The feed adds the page of `_alambique_feed` and the page of its TXID index:
  # the rows of a batch are born, stamped and deleted in the same two pages.
  @feed_pages 2

  setup do
    setup_store()
  end

  defp level0_pages(%{store: {mod, config}}, cell) do
    {:ok, keys, _} = mod.list(config, "cells-test/cells/#{cell}/0000/", nil, 10_000)

    for key <- Enum.sort(keys) do
      {:ok, body, _etag, _meta} = mod.get(config, key)
      {:ok, file} = LtxReader.decode(body)
      Enum.map(file.pages, &elem(&1, 0))
    end
  end

  # Pages of the segments of single-row writes into a table with one index,
  # once the cell is warm (the first write after a wake also writes the
  # migration and the first feed rows).
  defp pages_per_write(ctx, opts) do
    inst =
      start_instance(
        ctx,
        [
          migrations: [
            {1,
             """
             CREATE TABLE ev (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, ref TEXT NOT NULL, body TEXT);
             CREATE INDEX ev_ref ON ev (ref)
             """}
          ]
        ] ++ opts
      )

    cell = unique_cell()
    body = String.duplicate("x", 120)

    for i <- 1..12 do
      {:ok, _} =
        Alambique.execute(inst, cell, "INSERT INTO ev (kind, ref, body) VALUES (?, ?, ?)", [
          "k",
          "ref-#{i}",
          body
        ])
    end

    ctx |> level0_pages(cell) |> Enum.take(-8)
  end

  test "a small write adds at most #{@feed_pages} pages for the change feed", ctx do
    without = pages_per_write(ctx, feed: false)
    assert Enum.all?(without, &(length(&1) == 2)), "table and index: #{inspect(without)}"

    with_feed = pages_per_write(ctx, [])

    assert Enum.all?(with_feed, &(length(&1) <= 2 + @feed_pages)),
           "table, index and feed: #{inspect(with_feed)}"
  end
end
