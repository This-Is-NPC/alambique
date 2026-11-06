defmodule Alambique.Replication.Chain do
  @moduledoc false
  # The tail rule (design §6, R3). A crashed or fenced owner can leave level-0
  # single-TXID files past a gap: pipelined uploads that landed out of order.
  # Every reader ends the chain at its head and ignores them. Anything else
  # past the head stays loud (`{:gap, txid}`, `:epoch_regression`), so lost
  # history is never mistaken for a tail. The rule reads keys only.

  # The most TXIDs one owner has in flight: the cap of `pipeline_depth`.
  @window 16

  @doc "How far past the head a tail may reach."
  @spec window() :: pos_integer
  def window, do: @window

  @doc """
  Whether the files left over after a chain that ends at `head` are a tail:
  level-0 files of one TXID, none further than `window/0` past the head, and
  no snapshot. Nothing left over is a tail too.
  """
  @spec tail?([%{level: integer, min: integer, max: integer}], [map], non_neg_integer) ::
          boolean
  def tail?(segments, snapshots, head) do
    snapshots == [] and
      Enum.all?(segments, &(&1.level == 0 and &1.min == &1.max and &1.max <= head + @window))
  end

  @doc """
  The contiguous run of single-TXID segments after `txid`, up to `limit`
  (`:infinity` for none), and whether files are left over that are not a
  tail: a gap, or TXIDs past the limit that the run cannot reach. `segments`
  are sorted by `min`.
  """
  @spec run([%{level: integer, min: integer, max: integer}], non_neg_integer, term) ::
          {[map], boolean}
  def run(segments, txid, limit), do: run(segments, txid, limit, [])

  defp run([%{min: min, max: min} = seg | rest], txid, limit, acc)
       when min == txid + 1 and (limit == :infinity or min <= limit),
       do: run(rest, min, limit, [seg | acc])

  defp run(rest, txid, limit, acc) do
    reached = limit == :infinity or txid == limit
    scope = if limit == :infinity, do: rest, else: Enum.filter(rest, &(&1.min <= limit))
    {Enum.reverse(acc), not reached or not tail?(scope, [], txid)}
  end

  @doc """
  The files of `epochs`, oldest first as `{file, epoch}`, up to the first
  whose epoch is lower than an earlier one: the chain ends before it.
  """
  @spec live([{term, non_neg_integer}]) :: [term]
  def live(epochs) do
    epochs
    |> Enum.reduce_while({[], 0}, fn {file, epoch}, {acc, highest} ->
      if epoch < highest, do: {:halt, {acc, highest}}, else: {:cont, {[file | acc], epoch}}
    end)
    |> elem(0)
    |> Enum.reverse()
  end
end
