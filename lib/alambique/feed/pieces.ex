defmodule Alambique.Feed.Pieces do
  @moduledoc false
  # Reads the captured rows of stamped batches (design §13) as pieces of at
  # most `feed.hand_over_rows` rows: whole batches as far as they fit, and the
  # start, the middle or the end of the one the bound cuts. The cell reads
  # with its own connection and the sender, with `hand_over: :sender`, with
  # its reader; both build the same pieces, so the ids and objects of a feed do
  # not depend on who reads.

  alias Alambique.Feed.Capture

  @typedoc """
  The rows of one batch: `partial` when more rows of it follow (the batch is
  complete when a piece that is not partial arrives), `first` when the piece
  starts it, `seq` the position of its last row.
  """
  @type piece :: %{
          txid: pos_integer,
          ts: integer,
          changes: [{String.t(), String.t(), String.t(), String.t() | nil}],
          partial: boolean,
          first: boolean,
          seq: integer
        }

  @doc """
  Reads from `position`, `{txid, seq}` as `Capture.rows_from/4` takes it, up to
  the batch `upto`. `resumed` is the position of a batch whose first pieces were
  read before, or nil. Returns the pieces and whether more rows follow.
  """
  @spec read(term, {pos_integer, integer}, non_neg_integer, pos_integer | nil, term) ::
          {:ok, [piece], boolean} | {:error, term}
  def read(conn, position, upto, bound, resumed) do
    with {:ok, rows} <- Capture.rows_from(conn, position, upto, bound && bound + 1) do
      {rows, beyond} = if bound, do: Enum.split(rows, bound), else: {rows, []}
      {:ok, pieces(rows, beyond, resumed), beyond != []}
    end
  end

  # `partial` when rows of the batch follow the piece (the first of `beyond` is
  # one), `first` when the piece starts it.
  defp pieces(rows, beyond, resumed) do
    pieces =
      for group <- Enum.chunk_by(rows, &hd/1) do
        [txid, ts | _] = hd(group)

        %{
          txid: txid,
          ts: ts,
          changes:
            Enum.map(group, fn [_, _, tbl, op, key, vals, _seq] -> {tbl, op, key, vals} end),
          partial: false,
          first: not match?({^txid, _}, resumed),
          seq: group |> List.last() |> List.last()
        }
      end

    case {List.last(pieces), beyond} do
      {%{txid: txid} = last, [[txid | _] | _]} ->
        List.replace_at(pieces, -1, %{last | partial: true})

      _ ->
        pieces
    end
  end
end
