defmodule Alambique.KeyPropertiesTest do
  @moduledoc """
  Invariants of the object keys and of the restore targets: a key parses back
  to what built it, keys sort in TXID order, and different inputs never share
  a key.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.{Ops, Schema}
  alias Alambique.Query.Cache
  alias Alambique.Replication.Keys

  defp prefix, do: string(:alphanumeric, min_length: 1, max_length: 8)
  defp cell, do: string([?a..?z, ?0..?9, ?-, ?_, ?.], min_length: 1, max_length: 10)
  defp txid, do: integer(1..0xFFFF_FFFF_FFFF_FFFF)

  # A segment is `min <= max` at a level below the snapshots.
  defp span do
    gen all(a <- txid(), b <- txid()) do
      {min(a, b), max(a, b)}
    end
  end

  describe "Replication.Keys" do
    property "hex is 16 lowercase digits and reads back to the TXID" do
      check all(n <- integer(0..0xFFFF_FFFF_FFFF_FFFF)) do
        hex = Keys.hex(n)

        assert byte_size(hex) == 16
        assert hex =~ ~r/\A[0-9a-f]{16}\z/
        assert String.to_integer(hex, 16) == n
      end
    end

    property "hex sorts as the TXIDs do" do
      check all(a <- txid(), b <- txid()) do
        assert Keys.hex(a) < Keys.hex(b) == a < b
        assert Keys.hex(a) == Keys.hex(b) == (a == b)
      end
    end

    property "parse reads back the level and TXIDs of a segment key" do
      check all(prefix <- prefix(), cell <- cell(), level <- integer(0..8), {min, max} <- span()) do
        key = Keys.segment(prefix, cell, level, min, max)

        assert {[%{key: ^key, level: ^level, min: ^min, max: ^max}], []} =
                 Keys.parse(prefix, cell, [key])

        assert String.starts_with?(key, Keys.level_prefix(prefix, cell, level))
        assert String.starts_with?(key, Keys.files_prefix(prefix, cell))
      end
    end

    property "parse reads back the TXID of a snapshot key" do
      check all(prefix <- prefix(), cell <- cell(), txid <- txid()) do
        key = Keys.snapshot(prefix, cell, txid)

        assert {[], [%{key: ^key, txid: ^txid}]} = Keys.parse(prefix, cell, [key])
        assert String.starts_with?(key, Keys.level_prefix(prefix, cell, Keys.snapshot_level()))
      end
    end

    property "snapshots come back oldest first and segments keep the order given" do
      check all(
              prefix <- prefix(),
              cell <- cell(),
              snapshots <- uniq_list_of(txid(), min_length: 1, max_length: 6),
              spans <- uniq_list_of(span(), min_length: 1, max_length: 6),
              levels <- list_of(integer(0..8), length: 6)
            ) do
        segments =
          spans
          |> Enum.zip(levels)
          |> Enum.map(fn {{min, max}, level} -> Keys.segment(prefix, cell, level, min, max) end)
          |> Enum.uniq()

        snapshot_keys = Enum.map(snapshots, &Keys.snapshot(prefix, cell, &1))

        {parsed_segments, parsed_snapshots} =
          Keys.parse(prefix, cell, Enum.shuffle(snapshot_keys) ++ segments)

        assert Enum.map(parsed_snapshots, & &1.txid) == Enum.sort(snapshots)
        assert Enum.map(parsed_segments, & &1.key) == segments
      end
    end

    property "the keys of one level sort by TXID range" do
      check all(
              prefix <- prefix(),
              cell <- cell(),
              level <- integer(0..8),
              a <- span(),
              b <- span()
            ) do
        key = fn {min, max} -> Keys.segment(prefix, cell, level, min, max) end

        assert key.(a) < key.(b) == a < b
      end
    end

    property "fixed keys and foreign keys are not data keys" do
      check all(prefix <- prefix(), cell <- cell(), junk <- string(:printable, max_length: 30)) do
        fixed = [
          Keys.frozen(prefix, cell),
          Keys.replicas(prefix, cell),
          Keys.trashed(prefix, cell),
          Keys.crashes(prefix, cell),
          Keys.files_prefix(prefix, cell) <> junk,
          junk
        ]

        data = Keys.snapshot(prefix, cell, 7)

        assert Keys.data_keys(prefix, cell, fixed ++ [data]) == [data]
        assert {[], []} == Keys.parse(prefix, cell, Enum.take(fixed, 4))
      end
    end

    property "a file name that breaks the layout is not a data key" do
      check all(
              prefix <- prefix(),
              cell <- cell(),
              {min, max} <- span(),
              level <- integer(0..9)
            ) do
        key = fn level, min, max ->
          Keys.files_prefix(prefix, cell) <>
            String.pad_leading(Integer.to_string(level, 16), 4, "0") <>
            "/" <> Keys.hex(min) <> "-" <> Keys.hex(max) <> ".ltx"
        end

        assert Keys.parse(prefix, cell, [key.(level, max + 1, max)]) == {[], []}
        assert Keys.parse(prefix, cell, [key.(level, 0, max)]) == {[], []}
        assert Keys.parse(prefix, cell, [key.(10, min, max)]) == {[], []}
        assert Keys.parse(prefix, cell, [key.(9, 2 + min, 2 + max)]) == {[], []}

        upper = Keys.files_prefix(prefix, cell) <> "0000/000000000000000A-000000000000000B.ltx"
        assert Keys.parse(prefix, cell, [upper]) == {[], []}
      end
    end

    property "a key of another cell is not a data key of this one" do
      check all(
              prefix <- prefix(),
              [a, b] <- uniq_list_of(cell(), length: 2),
              {min, max} <- span()
            ) do
        assert Keys.parse(prefix, a, [Keys.segment(prefix, b, 0, min, max)]) == {[], []}
      end
    end

    property "parse never raises on any string" do
      check all(
              prefix <- prefix(),
              cell <- cell(),
              keys <- list_of(string(:printable), max_length: 5)
            ) do
        assert {segments, snapshots} = Keys.parse(prefix, cell, keys)
        assert length(segments) + length(snapshots) <= length(keys)
      end
    end
  end

  describe "Schema.key/3" do
    property "different versions or cells never share a key" do
      check all(
              prefix <- prefix(),
              v1 <- integer(0..9_999_999_999),
              v2 <- integer(0..9_999_999_999),
              c1 <- cell(),
              c2 <- cell()
            ) do
        assert Schema.key(prefix, v1, c1) == Schema.key(prefix, v2, c2) ==
                 ({v1, c1} == {v2, c2})
      end
    end

    property "keys sort by version first, so the first key is the oldest version" do
      check all(
              prefix <- prefix(),
              v1 <- integer(0..9_999_999_999),
              v2 <- integer(0..9_999_999_999),
              c1 <- cell(),
              c2 <- cell()
            ) do
        assert Schema.key(prefix, v1, c1) < Schema.key(prefix, v2, c2) ==
                 {v1, c1} < {v2, c2}
      end
    end
  end

  describe "Query.Cache.key/2" do
    property "a cell at a location has one name and no other pair shares it" do
      scope = string([?0..?9, ?A..?F], min_length: 1, max_length: 3)
      # A small alphabet, so that a cell can end like a scope.
      cell = string([?0..?9, ?A..?F, ?@], min_length: 1, max_length: 3)

      check all(
              c1 <- cell,
              c2 <- cell,
              s1 <- scope,
              s2 <- scope
            ) do
        assert Cache.key(c1, s1) == Cache.key(c1, s1)
        assert Cache.key(c1, s1) == Cache.key(c2, s2) == ({c1, s1} == {c2, s2})
      end
    end

    property "the same text split in two places names two different caches" do
      check all(
              text <- string([?0..?9, ?A..?F], min_length: 3, max_length: 6),
              i <- integer(1..(byte_size(text) - 1)),
              step <- integer(1..(byte_size(text) - 2))
            ) do
        j = rem(i - 1 + step, byte_size(text) - 1) + 1
        {c1, s1} = String.split_at(text, i)
        {c2, s2} = String.split_at(text, j)

        refute Cache.key(c1, s1) == Cache.key(c2, s2)
      end
    end
  end

  describe "Ops.parse_target/1" do
    property "txid:N is a TXID target for every non-negative N" do
      check all(n <- non_negative_integer()) do
        assert Ops.parse_target("txid:#{n}") == {:ok, {:txid, n}}
      end
    end

    property "ms:N is a timestamp target for every integer N" do
      check all(n <- integer()) do
        assert Ops.parse_target("ms:#{n}") == {:ok, {:timestamp, n}}
      end
    end

    property "a UTC date-time is the timestamp of the instant it names" do
      check all(ms <- integer(0..4_102_444_800_000)) do
        text = ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

        assert Ops.parse_target(text) == {:ok, {:timestamp, ms}}
      end
    end

    property "latest is the only word that is a target by itself" do
      assert Ops.parse_target("latest") == {:ok, :latest}

      check all(word <- string(:alphanumeric, max_length: 10), word != "latest") do
        assert Ops.parse_target(word) == :error
      end
    end

    property "a negative TXID and a number with trailing text are rejected" do
      check all(
              n <- positive_integer(),
              tail <- string(?a..?z, min_length: 1, max_length: 4)
            ) do
        assert Ops.parse_target("txid:-#{n}") == :error
        assert Ops.parse_target("txid:#{n}#{tail}") == :error
        assert Ops.parse_target("ms:#{n}#{tail}") == :error
        assert Ops.parse_target("txid:") == :error
        assert Ops.parse_target("ms:") == :error
      end
    end

    property "any string is either a target or :error and never raises" do
      check all(text <- string(:printable, max_length: 30)) do
        assert Ops.parse_target(text) == :error or match?({:ok, _}, Ops.parse_target(text))
      end
    end
  end
end
