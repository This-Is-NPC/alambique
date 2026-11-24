defmodule Alambique.FeedRewindDecodeTest do
  @moduledoc """
  The rewind marker read back from a feed object (design §13): only what
  `Alambique.Feed.Rewind.encode/1` writes decodes, anything else is
  `{:error, :bad_record}`, and the reader surfaces it as a rejected record
  without stalling the feed.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Feed
  alias Alambique.Feed.{Keys, Record, Rejected, Rewind}

  setup do
    setup_store()
  end

  defp txid, do: integer(0..0xFFFF_FFFF_FFFF)

  defp target,
    do:
      one_of([
        map(integer(0..0xFFFF_FFFF_FFFF), &{:txid, &1}),
        map(integer(-9_999_999_999_999..9_999_999_999_999), &{:timestamp, &1})
      ])

  defp tables, do: list_of(string(:alphanumeric, min_length: 1, max_length: 8), max_length: 4)

  # Values the encoder never writes where a TXID, a time or a table is read.
  defp wrong do
    one_of([
      constant(nil),
      boolean(),
      string(:alphanumeric, max_length: 6),
      float(),
      list_of(integer(), max_length: 2),
      map_of(string(:alphanumeric, max_length: 3), integer(), max_length: 2),
      integer(-1_000..-1)
    ])
  end

  defp object(marker) do
    Record.pack(Jason.encode!(%{cell: "c", txid: 2, records: [], rewind: marker}))
  end

  property "a marker the encoder wrote reads back whole" do
    check all(
            from <- txid(),
            to <- txid(),
            target <- target(),
            kept <- tables(),
            at <- integer(0..9_999_999_999_999)
          ) do
      json = Rewind.encode(%{from: from, to: to, target: target, kept: kept})
      body = Record.encode("c", [{from + 1, [{"", "rewind", "{}", json}]}])

      assert [%Rewind{} = marker] = Record.decode("k", body, at)
      assert {marker.from_txid, marker.to_txid} == {from, to}
      assert {marker.target, marker.kept, marker.at} == {target, kept, at}
    end
  end

  property "a marker with a member of the wrong type or range is a bad record" do
    check all(
            field <- member_of([:from_txid, :to_txid, :txid, :timestamp, :kept]),
            value <- wrong()
          ) do
      marker = %{from_txid: 1, to_txid: 0, target: %{txid: 1}, kept: ["t"]}

      marker =
        case field do
          :txid -> %{marker | target: %{txid: value}}
          :timestamp -> %{marker | target: %{timestamp: value}}
          field -> Map.put(marker, field, value)
        end

      valid? = (field == :timestamp and is_integer(value)) or (field == :kept and value == [])
      body = Map.new(marker, fn {k, v} -> {Atom.to_string(k), json_key(v)} end)

      if valid? do
        assert %Rewind{} = Rewind.decode("c", 2, body, 0)
      else
        assert Rewind.decode("c", 2, body, 0) == {:error, :bad_record}
        assert [%Rejected{reason: :bad_record}] = Record.decode("k", object(marker), 0)
      end
    end
  end

  defp json_key(map) when is_map(map),
    do: Map.new(map, fn {k, v} -> {to_string(k), v} end)

  defp json_key(value), do: value

  property "a list of kept tables with a member that is not a name is a bad record" do
    check all(
            kept <-
              list_of(one_of([string(:alphanumeric, min_length: 1), wrong()]), min_length: 1)
          ) do
      body = %{"from_txid" => 1, "to_txid" => 0, "target" => %{"txid" => 1}, "kept" => kept}

      if Enum.all?(kept, &is_binary/1),
        do: assert(%Rewind{kept: ^kept} = Rewind.decode("c", 2, body, 0)),
        else: assert(Rewind.decode("c", 2, body, 0) == {:error, :bad_record})
    end
  end

  test "a reader returns a damaged marker as a rejected record and goes on", ctx do
    %{store: {mod, config}} = ctx
    inst = start_instance(ctx, migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY)"}])
    now = System.system_time(:millisecond) - 1_000

    damaged =
      Record.pack(
        ~s({"cell":"bad","txid":2,"records":[],"rewind":{"from_txid":"1","to_txid":0,"target":{"txid":1}}})
      )

    key = Keys.object("cells-test", Keys.default_shards(), "bad", now, 2)
    {:ok, _} = mod.put(config, key, damaged, :if_none_match, %{})

    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
    reader = Feed.ctx(store: ctx.store, prefix: "cells-test", settle_ms: 0)

    records =
      eventually(fn ->
        {:ok, records, _} = Feed.read(reader, nil, 100)
        Enum.any?(records, &match?(%{cell: ^cell}, &1)) and records
      end)

    assert [%Rejected{reason: :bad_record, key: ^key}] =
             Enum.filter(records, &match?(%Rejected{}, &1))

    assert Enum.any?(records, &match?(%{cell: ^cell, table: "t"}, &1))
  end
end
