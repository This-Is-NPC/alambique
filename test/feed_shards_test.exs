defmodule Alambique.FeedShardsTest do
  @moduledoc "The feed of a location is spread over shards by cell (design §19)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{CountingStore, Feed}
  alias Alambique.Feed.Keys

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @shards 4

  setup do
    setup_store()
  end

  defp feed_keys(%{store: {mod, config}}) do
    {:ok, keys, _} = mod.list(config, "cells-test/feed/", nil, 10_000)
    keys
  end

  test "a cell always writes to the shard of its hash", ctx do
    inst = start_instance(ctx, migrations: @schema, feed: [shards: @shards])
    cells = for i <- 1..20, do: "cell-#{i}"

    for cell <- cells, n <- 1..2 do
      assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n])
    end

    eventually(fn -> length(feed_keys(ctx)) == 40 end)

    for key <- feed_keys(ctx) do
      [_, "feed", shard, name] = String.split(key, "/")
      cell = name |> String.split("-", parts: 3) |> List.last()
      assert shard == shard_name(Keys.shard(cell, @shards))
    end

    shards = feed_keys(ctx) |> Enum.map(&(&1 |> String.split("/") |> Enum.at(2))) |> Enum.uniq()
    assert length(shards) == @shards
  end

  test "reads merge the shards and keep the order of every cell", ctx do
    inst = start_instance(ctx, migrations: @schema, feed: [shards: @shards])
    cells = for i <- 1..30, do: "cell-#{i}"

    for n <- 1..3, cell <- cells do
      assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n])
    end

    eventually(fn -> length(feed_keys(ctx)) == 90 end)
    feed = Feed.ctx(store: ctx.store, prefix: "cells-test", shards: @shards, settle_ms: 0)

    {records, _} = read_pages(feed, nil, 7, [])
    assert length(records) == 90

    for cell <- cells do
      assert for(r <- records, r.cell == cell, do: r.values["n"]) == [1, 2, 3]
    end

    times = Enum.map(records, & &1.at)
    assert times == Enum.sort(times)
  end

  test "a read lists one prefix per shard however many cells exist", ctx do
    counting = CountingStore.wrap(ctx.store)
    {_, counts} = counting
    inst = start_instance(%{ctx | store: counting}, migrations: @schema, feed: [shards: @shards])

    for i <- 1..25 do
      assert {:ok, _} = Alambique.execute(inst, "cell-#{i}", "INSERT INTO t VALUES (1)")
    end

    eventually(fn -> length(feed_keys(ctx)) == 25 end)
    feed = Feed.ctx(store: counting, prefix: "cells-test", shards: @shards, settle_ms: 0)

    CountingStore.reset(counts)
    assert {:ok, records, _} = Feed.read(feed, nil, 1000)
    assert length(records) == 25
    assert CountingStore.count(counts, :list) == @shards
    assert CountingStore.count(counts, :get) == 25
  end

  test "a position survives a changed set of locations", ctx do
    inst = start_instance(ctx, migrations: @schema, feed: [shards: @shards])
    assert {:ok, _} = Alambique.execute(inst, "one", "INSERT INTO t VALUES (1)")
    eventually(fn -> length(feed_keys(ctx)) == 1 end)
    feed = Feed.ctx(store: ctx.store, prefix: "cells-test", shards: @shards, settle_ms: 0)

    {:ok, [_], position} = Feed.read(feed, nil, 10)
    assert {:ok, [], ^position} = Feed.read(feed, position, 10)
    assert {:error, :bad_position} = Feed.read(feed, "not json", 10)
  end

  defp read_pages(feed, position, limit, acc) do
    case Feed.read(feed, position, limit) do
      {:ok, [], position} -> {acc, position}
      {:ok, records, next} -> read_pages(feed, next, limit, acc ++ records)
    end
  end

  defp shard_name(shard), do: shard |> Integer.to_string() |> String.pad_leading(3, "0")
end
