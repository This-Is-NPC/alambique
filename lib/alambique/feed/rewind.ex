defmodule Alambique.Feed.Rewind do
  @moduledoc """
  The feed marker of `Alambique.rewind/4` (design §13, §20): the cell's state
  went back, and the changes after `to_txid` were undone.

    * `:cell` - the cell that was rewound
    * `:from_txid` - the last TXID of the state that was replaced
    * `:to_txid` - the TXID of the state that was restored
    * `:txid` - the TXID of the rewind itself, `from_txid + 1`; records of the
      cell after this one describe changes on top of the restored state
    * `:target` - what the caller asked for: `{:txid, n}` or `{:timestamp, ms}`
    * `:kept` - the host tables carried over from the replaced state
    * `:id` - the idempotency key, `"<cell>:<txid>:rewind"`
    * `:at` - publication time in milliseconds, from the clock of the node
      that published

  After this marker the state of the cell is its state as of `to_txid`: what a
  consumer's copy held when it had applied the records up to that TXID. The
  records of the cell with a `position` TXID above `to_txid` and at most
  `from_txid` were undone. They stay on the feed, before this marker. A
  consumer must put its copy back, keeping a history of its own or rebuilding
  the cell from another source, and carry on with the records after the
  marker. `to_txid` may lie in the TXIDs an earlier marker undid, which brings
  those records back (a rewind to a state from before another rewind).
  Records at or below `to_txid` that arrive after the marker are repeats of
  records already delivered, with the same `id`.
  """

  @enforce_keys [:cell, :from_txid, :to_txid, :txid, :target, :kept, :id, :at]
  defstruct [:cell, :from_txid, :to_txid, :txid, :target, :kept, :id, :at]

  @type t :: %__MODULE__{
          cell: String.t(),
          from_txid: non_neg_integer,
          to_txid: non_neg_integer,
          txid: pos_integer,
          target: {:txid, non_neg_integer} | {:timestamp, integer},
          kept: [String.t()],
          id: String.t(),
          at: non_neg_integer
        }

  @doc false
  @spec decode(String.t(), pos_integer, map, non_neg_integer) :: t | {:error, :bad_record}
  def decode(cell, txid, body, at) do
    case from_json(body["target"]) do
      nil ->
        {:error, :bad_record}

      target ->
        %__MODULE__{
          cell: cell,
          from_txid: body["from_txid"],
          to_txid: body["to_txid"],
          txid: txid,
          target: target,
          kept: body["kept"] || [],
          id: "#{cell}:#{txid}:rewind",
          at: at
        }
    end
  end

  defp from_json(%{"txid" => txid}), do: {:txid, txid}
  defp from_json(%{"timestamp" => ms}), do: {:timestamp, ms}
  defp from_json(_), do: nil

  @doc false
  # The JSON of the marker row's `vals` and of the object's "rewind" key.
  @spec encode(map) :: String.t()
  def encode(%{from: from, to: to, target: target, kept: kept}) do
    Jason.encode!(%{
      from_txid: from,
      to_txid: to,
      target: to_json(target),
      kept: kept
    })
  end

  defp to_json({:txid, txid}), do: %{txid: txid}
  defp to_json({:timestamp, ms}), do: %{timestamp: ms}
end
