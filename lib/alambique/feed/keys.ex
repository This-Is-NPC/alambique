defmodule Alambique.Feed.Keys do
  @moduledoc false
  # Object keys of the change feed (design §13, §19): within a location the
  # feed is spread over `shards` prefixes by a hash of the cell id, each one a
  # flat, time-ordered prefix, so listing after a stored key reads only newer
  # objects and a cell always writes to the same shard.

  @default_shards 16

  @doc "Shards per location unless configured otherwise (design §19)."
  @spec default_shards() :: pos_integer
  def default_shards, do: @default_shards

  @doc "Shard of a cell. `:erlang.phash2/2` is stable across releases and nodes."
  @spec shard(String.t(), pos_integer) :: non_neg_integer
  def shard(cell, shards), do: :erlang.phash2(cell, shards)

  @doc "`<prefix>/feed/<shard, 3 digits>/` of one shard."
  @spec prefix(String.t(), non_neg_integer) :: String.t()
  def prefix(prefix, shard),
    do: "#{prefix}/feed/#{shard |> Integer.to_string() |> String.pad_leading(3, "0")}/"

  @doc "`<prefix>/feed/<shard>/<time, 13 digits>-<txid, 16 hex digits>-<cell>`."
  @spec object(String.t(), pos_integer, String.t(), non_neg_integer, pos_integer) :: String.t()
  def object(prefix, shards, cell, time, txid),
    do:
      "#{prefix(prefix, shard(cell, shards))}#{time(time)}-" <>
        "#{Alambique.Replication.Keys.hex(txid)}-#{cell}"

  @doc "A key that sorts after every object of `time` in a shard and before every later one."
  @spec after_time(String.t(), non_neg_integer, integer) :: String.t()
  def after_time(prefix, shard, time), do: prefix(prefix, shard) <> time(time) <> "."

  @doc "Time, TXID and cell of a key of the given shard; `nil` for foreign keys."
  @spec parse(String.t(), non_neg_integer, String.t()) ::
          %{time: non_neg_integer, txid: pos_integer, cell: String.t()} | nil
  def parse(prefix, shard, key) do
    shard_prefix = prefix(prefix, shard)

    with true <- String.starts_with?(key, shard_prefix),
         <<time::binary-size(13), ?-, txid::binary-size(16), ?-, cell::binary>> <-
           String.replace_prefix(key, shard_prefix, ""),
         true <- digits?(time, ?0..?9) and digits?(txid, ~c"0123456789abcdef"),
         {time, ""} <- Integer.parse(time),
         {txid, ""} when txid >= 1 <- Integer.parse(txid, 16),
         true <- cell != "" do
      %{time: time, txid: txid, cell: cell}
    else
      _ -> nil
    end
  end

  # `Integer.parse/2` also takes a sign; keys must not.
  defp digits?(text, allowed),
    do: for(<<c <- text>>, reduce: true, do: (ok -> ok and c in allowed))

  defp time(time), do: time |> max(0) |> Integer.to_string() |> String.pad_leading(13, "0")
end
