defmodule Alambique.FeedPoisonTest do
  @moduledoc "A malformed feed object never stalls the feed (dos-6, input-7)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Feed
  alias Alambique.Feed.{Keys, Record, Rejected}

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)"}]

  setup do
    capture([[:alambique, :feed, :read, :rejected]])
    setup_store()
  end

  defp feed_ctx(%{store: store}), do: Feed.ctx(store: store, prefix: "cells-test", settle_ms: 0)

  defp read_all(ctx, count) do
    eventually(fn ->
      {:ok, records, _} = Feed.read(feed_ctx(ctx), nil, 1000)
      if length(records) >= count, do: records
    end)
  end

  defp put_object(%{store: {mod, config}}, cell, time, body) do
    key = Keys.object("cells-test", Keys.default_shards(), cell, time, 1)
    {:ok, _} = mod.put(config, key, body, :if_none_match, %{})
    key
  end

  test "text that is not UTF-8 reaches the feed with the bad bytes replaced", ctx do
    inst = start_instance(ctx, migrations: @schema)
    {bad, good} = {unique_cell(), unique_cell()}

    {:ok, _} = Alambique.execute(inst, bad, "INSERT INTO t VALUES (1, CAST(x'ff41' AS TEXT))")
    {:ok, _} = Alambique.execute(inst, good, "INSERT INTO t VALUES (2, 'ok')")

    records = read_all(ctx, 2)
    assert %{values: %{"body" => "�A"}} = Enum.find(records, &(&1.cell == bad))
    assert %{values: %{"body" => "ok"}} = Enum.find(records, &(&1.cell == good))
  end

  test "hostile objects become rejected records and the position advances", ctx do
    inst = start_instance(ctx, migrations: @schema)
    now = System.system_time(:millisecond) - 1_000

    bodies = [
      {"junk", "not an object"},
      {"plain", ~s({"cell":"plain","txid":1,"records":[]})},
      {"badjson", Record.pack("not json")},
      {"badop",
       Record.pack(
         ~s({"cell":"badop","txid":1,"records":[{"n":0,"table":"t","op":"bogus","key":{}}]})
       )},
      {"shape", Record.pack(~s({"cell":"shape","txid":1,"records":5}))},
      {"rewind",
       Record.pack(~s({"cell":"rewind","txid":1,"records":[],"rewind":{"from_txid":1}}))},
      {"target",
       Record.pack(
         ~s({"cell":"target","txid":2,"records":[],"rewind":{"from_txid":1,"to_txid":0,"target":{"x":1}}})
       )}
    ]

    keys =
      for {{cell, body}, i} <- Enum.with_index(bodies), do: put_object(ctx, cell, now + i, body)

    good = unique_cell()
    {:ok, _} = Alambique.execute(inst, good, "INSERT INTO t VALUES (3, 'fine')")

    records = read_all(ctx, 8)
    rejected = Enum.filter(records, &match?(%Rejected{}, &1))
    assert Enum.sort(Enum.map(rejected, & &1.key)) == Enum.sort(keys)
    assert [%Record{values: %{"body" => "fine"}}] = Enum.filter(records, &match?(%Record{}, &1))

    for _ <- keys,
        do: assert_receive({:telemetry, [:alambique, :feed, :read, :rejected], _, %{key: _}})

    {:ok, _, position} = Feed.read(feed_ctx(ctx), nil, 1000)
    assert {:ok, [], ^position} = Feed.read(feed_ctx(ctx), position, 1000)
  end

  test "a key with a sign in its time or TXID is foreign" do
    shard = Keys.shard("c", 16)
    prefix = Keys.prefix("p", shard)
    hex = String.duplicate("0", 15) <> "1"

    assert %{time: 1} = Keys.parse("p", shard, prefix <> "0000000000001-#{hex}-c")
    assert nil == Keys.parse("p", shard, prefix <> "+000000000001-#{hex}-c")
    assert nil == Keys.parse("p", shard, prefix <> "0000000000001-+000000000000001-c")
  end
end
