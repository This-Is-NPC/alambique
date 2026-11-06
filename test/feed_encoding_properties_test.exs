defmodule Alambique.FeedEncodingPropertiesTest do
  @moduledoc """
  Invariants of the feed object format (design §13): what `Record.encode/2`
  writes `Record.decode/3` reads back, an object that is not one is rejected
  and never raises, and the keys of the objects sort by time and TXID.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Feed.{Keys, Record, Rejected, Rewind}
  alias Alambique.Replication.Keys, as: ReplKeys

  # The JSON text inside an object body.
  defp unpack(body) do
    {:ok, json} = Record.json(body, Record.default_max_bytes())
    json
  end

  # JSON scalars as SQLite renders them, blobs as `{"$blob": hex}`.
  defp scalar do
    one_of([
      constant(nil),
      boolean(),
      integer(),
      float(),
      string(:printable, max_length: 12),
      map(binary(max_length: 6), &%{"$blob" => Base.encode16(&1, case: :lower)})
    ])
  end

  defp columns do
    map_of(string(:alphanumeric, min_length: 1, max_length: 6), scalar(),
      min_length: 1,
      max_length: 4
    )
  end

  defp change do
    gen all(
          table <- string(:printable, min_length: 1, max_length: 8),
          op <- member_of(["insert", "update", "delete"]),
          key <- columns(),
          values <- one_of([constant(nil), columns()])
        ) do
      {table, op, key, values}
    end
  end

  defp sql_change({table, op, key, values}),
    do: {table, op, Jason.encode!(key), values && Jason.encode!(values)}

  defp cell, do: string(:printable, min_length: 1, max_length: 12)

  defp json do
    tree(
      one_of([constant(nil), boolean(), integer(), string(:printable, max_length: 6)]),
      fn child ->
        one_of([
          list_of(child, max_length: 3),
          map_of(string(:alphanumeric, max_length: 6), child, max_length: 3)
        ])
      end
    )
  end

  describe "Record" do
    property "decode returns what encode wrote, in order, with position and id" do
      check all(
              cell <- cell(),
              txid <- integer(1..1_000_000_000_000),
              at <- non_negative_integer(),
              changes <- list_of(change(), min_length: 1, max_length: 5)
            ) do
        body = Record.encode(cell, [{txid, Enum.map(changes, &sql_change/1)}])
        records = Record.decode("k", body, at)

        assert length(records) == length(changes)

        for {{%Record{} = record, {table, op, key, values}}, n} <-
              records |> Enum.zip(changes) |> Enum.with_index() do
          assert record.cell == cell
          assert record.table == table
          assert record.op == String.to_existing_atom(op)
          assert record.key == key
          assert record.values == values
          assert record.position == {txid, n}
          assert record.id == "#{cell}:#{txid}:#{n}"
          assert record.at == at
        end
      end
    end

    property "a group of batches decodes to the records of its batches, in order" do
      check all(
              cell <- cell(),
              first <- integer(1..1_000_000),
              groups <- list_of(list_of(change(), max_length: 3), min_length: 2, max_length: 5),
              at <- non_negative_integer()
            ) do
        batches =
          groups
          |> Enum.with_index(first)
          |> Enum.map(fn {changes, txid} -> {txid, Enum.map(changes, &sql_change/1)} end)

        singles = Enum.flat_map(batches, &Record.decode("k", Record.encode(cell, [&1]), at))

        assert Record.decode("k", Record.encode(cell, batches), at) == singles
      end
    end

    property "a damaged batch rejects the whole group" do
      check all(
              groups <- list_of(list_of(change(), min_length: 1, max_length: 2), min_length: 2),
              index <- integer(0..10),
              damage <- member_of([:txid, :records, :batch])
            ) do
        batches =
          groups
          |> Enum.with_index(1)
          |> Enum.map(fn {changes, txid} -> {txid, Enum.map(changes, &sql_change/1)} end)

        object = "c" |> Record.encode(batches) |> unpack() |> Jason.decode!()

        damaged =
          List.update_at(object["batches"], rem(index, length(batches)), fn batch ->
            case damage do
              :txid -> Map.put(batch, "txid", "1")
              :records -> Map.put(batch, "records", %{})
              :batch -> "batch"
            end
          end)

        assert [%Rejected{reason: :bad_object}] =
                 Record.decode(
                   "k",
                   Record.pack(Jason.encode!(%{object | "batches" => damaged})),
                   0
                 )
      end
    end

    property "encode always writes valid UTF-8 JSON, replacing bad bytes with U+FFFD" do
      check all(
              cell <- cell(),
              junk <-
                map(
                  list_of(integer(0x20..0xFF), min_length: 1, max_length: 6),
                  &:binary.list_to_bin/1
                ),
              suffix <- string(:alphanumeric, max_length: 4)
            ) do
        # Raw bytes inside a JSON string, as SQLite hands them over unchecked.
        text = String.replace(junk <> suffix, ["\"", "\\"], "x")
        values = ~s({"v":") <> text <> ~s("})
        body = Record.encode(cell, [{1, [{"t", "insert", ~s({"id":1}), values}]}])

        assert String.valid?(unpack(body))
        assert [%Record{values: %{"v" => v}}] = Record.decode("k", body, 0)
        assert String.valid?(v)
      end
    end

    property "an object that is not a feed object is one Rejected, whatever its content" do
      check all(value <- json(), key <- string(:printable, min_length: 1, max_length: 8)) do
        case Record.decode(key, Record.pack(Jason.encode!(value)), 7) do
          [%Rejected{} = rejected] ->
            assert rejected.key == key
            assert rejected.id == "rejected:" <> key
            assert rejected.at == 7
            assert rejected.reason in [:bad_object, :bad_record]

          other ->
            # Only an object with the three required members decodes.
            assert is_map(value) and is_list(value["records"])
            assert other == []
        end
      end
    end

    property "bytes that are not JSON are Rejected with :invalid_json and never raise" do
      check all(bytes <- binary(max_length: 40), match?({:error, _}, Jason.decode(bytes))) do
        assert [%Rejected{reason: :invalid_json, id: "rejected:k"}] =
                 Record.decode("k", Record.pack(bytes), 0)
      end
    end

    property "a body that is not in the object format is Rejected and never raises" do
      check all(bytes <- binary(max_length: 40), not String.starts_with?(bytes, "ALF")) do
        assert [%Rejected{reason: :bad_format}] = Record.decode("k", bytes, 0)
      end
    end

    test "plain JSON is not an object" do
      body = Jason.encode!(%{cell: "c", txid: 1, records: []})
      assert [%Rejected{reason: :bad_format}] = Record.decode("k", body, 0)
    end

    property "one record with an unknown op rejects the whole object" do
      check all(
              changes <- list_of(change(), min_length: 1, max_length: 4),
              bad_op <- string(:alphanumeric, min_length: 1, max_length: 8),
              bad_op not in ["insert", "update", "delete"],
              index <- integer(0..3)
            ) do
        index = rem(index, length(changes))

        object =
          "c"
          |> Record.encode([{1, Enum.map(changes, &sql_change/1)}])
          |> unpack()
          |> Jason.decode!()

        records = List.update_at(object["records"], index, &Map.put(&1, "op", bad_op))
        damaged = Record.pack(Jason.encode!(%{object | "records" => records}))

        assert [%Rejected{reason: {:bad_op, ^bad_op}}] = Record.decode("k", damaged, 0)
      end
    end

    property "a record missing a member is rejected as :bad_record" do
      check all(
              changes <- list_of(change(), min_length: 1, max_length: 3),
              member <- member_of(["n", "table", "op", "key"])
            ) do
        object =
          "c"
          |> Record.encode([{1, Enum.map(changes, &sql_change/1)}])
          |> unpack()
          |> Jason.decode!()

        records = List.update_at(object["records"], 0, &Map.delete(&1, member))
        damaged = Record.pack(Jason.encode!(%{object | "records" => records}))

        assert [%Rejected{reason: :bad_record}] = Record.decode("k", damaged, 0)
      end
    end
  end

  describe "Rewind" do
    defp target do
      one_of([
        map(non_negative_integer(), &{:txid, &1}),
        map(integer(), &{:timestamp, &1})
      ])
    end

    property "a rewind marker decodes to the fields it was encoded from" do
      check all(
              cell <- cell(),
              from <- integer(1..1_000_000),
              to <- integer(0..1_000_000),
              target <- target(),
              kept <- list_of(string(:alphanumeric, min_length: 1, max_length: 6), max_length: 3),
              at <- non_negative_integer()
            ) do
        marker = Rewind.encode(%{from: from, to: to, target: target, kept: kept})
        body = Record.encode(cell, [{from + 1, [{"", "rewind", "{}", marker}]}])

        assert [%Rewind{} = rewind] = Record.decode("k", body, at)
        assert rewind.cell == cell
        assert rewind.from_txid == from
        assert rewind.to_txid == to
        assert rewind.txid == from + 1
        assert rewind.target == target
        assert rewind.kept == kept
        assert rewind.id == "#{cell}:#{from + 1}:rewind"
        assert rewind.at == at
      end
    end

    property "a rewind member that is not a marker rejects the object and never raises" do
      check all(rewind <- json()) do
        body = Record.pack(Jason.encode!(%{cell: "c", txid: 5, records: [], rewind: rewind}))

        assert [%Rejected{reason: :bad_record}] = Record.decode("k", body, 0)
      end
    end

    property "a marker whose target is neither a txid nor a timestamp is rejected" do
      check all(target <- json()) do
        marker = %{from_txid: 1, to_txid: 0, target: target, kept: []}
        body = Record.pack(Jason.encode!(%{cell: "c", txid: 2, records: [], rewind: marker}))

        case Record.decode("k", body, 0) do
          [%Rewind{target: {kind, _}}] ->
            assert kind in [:txid, :timestamp]
            assert is_map(target)

          [%Rejected{reason: :bad_record}] ->
            refute is_map(target) and
                     (is_map_key(target, "txid") or is_map_key(target, "timestamp"))
        end
      end
    end

    property "decode of arbitrary JSON as a marker body never raises" do
      check all(body <- map_of(string(:alphanumeric, max_length: 8), json(), max_length: 4)) do
        assert match?(%Rewind{}, Rewind.decode("c", 2, body, 0)) or
                 Rewind.decode("c", 2, body, 0) == {:error, :bad_record}
      end
    end
  end

  describe "Feed.Keys" do
    defp prefix, do: string(:alphanumeric, min_length: 1, max_length: 8)
    defp time, do: integer(0..9_999_999_999_999)
    defp txid, do: integer(1..0xFFFF_FFFF_FFFF_FFFF)
    defp shards, do: integer(1..1000)

    property "parse reads back the time, TXID and cell of an object key" do
      check all(
              prefix <- prefix(),
              shards <- shards(),
              cell <- cell(),
              time <- time(),
              txid <- txid()
            ) do
        key = Keys.object(prefix, shards, cell, time, txid)
        shard = Keys.shard(cell, shards)

        assert Keys.parse(prefix, shard, key) == %{time: time, txid: txid, cell: cell}
        assert String.starts_with?(key, Keys.prefix(prefix, shard))
      end
    end

    property "the shard of a cell is stable and below the shard count" do
      check all(cell <- cell(), shards <- shards()) do
        shard = Keys.shard(cell, shards)
        assert shard in 0..(shards - 1)
        assert shard == Keys.shard(cell, shards)
        assert shard == :erlang.phash2(cell, shards)
      end
    end

    property "keys of one shard sort by time, then TXID, then cell" do
      check all(
              prefix <- prefix(),
              [c1, c2] <- list_of(string([?a..?c, ?-], min_length: 1, max_length: 3), length: 2),
              [t1, t2] <- list_of(one_of([integer(0..3), integer(8..12)]), length: 2),
              [x1, x2] <- list_of(one_of([integer(1..3), integer(14..18)]), length: 2)
            ) do
        # One shard, so every cell shares the prefix.
        key = fn cell, t, x -> Keys.object(prefix, 1, cell, t, x) end

        assert key.(c1, t1, x1) < key.(c2, t2, x2) == {t1, x1, c1} < {t2, x2, c2}
        assert key.(c1, t1, x1) == key.(c2, t2, x2) == ({t1, x1, c1} == {t2, x2, c2})
      end
    end

    property "after_time sorts after every object of that time and before every later one" do
      check all(
              prefix <- prefix(),
              shards <- shards(),
              cell <- cell(),
              time <- integer(0..9_999_999_999_998),
              txid <- txid(),
              later <- txid()
            ) do
        shard = Keys.shard(cell, shards)
        after_time = Keys.after_time(prefix, shard, time)

        assert Keys.object(prefix, shards, cell, time, txid) < after_time
        assert after_time < Keys.object(prefix, shards, cell, time + 1, later)
      end
    end

    property "an object is the zlib stream of its JSON, behind the magic and the version" do
      check all(
              cell <- cell(),
              groups <- list_of(list_of(change(), max_length: 3), min_length: 1, max_length: 4)
            ) do
        batches =
          groups
          |> Enum.with_index(1)
          |> Enum.map(fn {changes, txid} -> {txid, Enum.map(changes, &sql_change/1)} end)

        packed = Record.encode(cell, batches)
        assert <<"ALF", 1, stream::binary>> = packed
        assert unpack(packed) == :zlib.uncompress(stream)
      end
    end

    test "an object of a format this release does not know is rejected" do
      assert [%Rejected{reason: {:unsupported_format, 2}}] =
               Record.decode("k", <<"ALF", 2, "anything">>, 0)
    end

    test "an object that is damaged or inflates too far is rejected" do
      body =
        Record.encode("c", [
          {1, [{"t", "insert", ~s({"id":1}), ~s({"v":"#{String.duplicate("a", 5_000)}"})}]}
        ])

      assert [%Record{}] = Record.decode("k", body, 0, 10_000)
      assert [%Rejected{reason: :too_large}] = Record.decode("k", body, 0, 4_000)

      damaged = binary_part(body, 0, 20) <> <<255, 255, 255>>
      assert [%Rejected{reason: reason}] = Record.decode("k", damaged, 0)
      assert reason in [:bad_compression, :invalid_json]

      # A stream that ends early is damaged too.
      assert [%Rejected{reason: reason}] = Record.decode("k", binary_part(body, 0, 12), 0)
      assert reason in [:bad_compression, :invalid_json]
    end

    property "parse never raises and accepts only keys the format writes" do
      check all(
              prefix <- prefix(),
              shard <- integer(0..999),
              rest <- string(:printable, max_length: 40)
            ) do
        key = Keys.prefix(prefix, shard) <> rest

        case Keys.parse(prefix, shard, key) do
          nil ->
            :ok

          %{time: time, txid: txid, cell: cell} ->
            assert Keys.prefix(prefix, shard) <>
                     String.pad_leading("#{time}", 13, "0") <>
                     "-" <> ReplKeys.hex(txid) <> "-" <> cell == key
        end

        assert Keys.parse(prefix, shard, rest) == nil

        # TXIDs start at 1, and a key names its cell.
        zero = Keys.prefix(prefix, shard) <> "0000000000000-0000000000000000-cell"
        no_cell = Keys.prefix(prefix, shard) <> "0000000000000-0000000000000001-"
        assert Keys.parse(prefix, shard, zero) == nil
        assert Keys.parse(prefix, shard, no_cell) == nil
      end
    end
  end
end
