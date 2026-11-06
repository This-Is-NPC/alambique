defmodule Alambique.Compaction.Plan do
  @moduledoc false
  # Pure planning over the listing of one cell (design §9): which chain
  # restore will walk and which aligned groups of files to merge. Nothing here
  # touches the store.

  @type segment :: %{
          key: String.t(),
          level: non_neg_integer,
          min: pos_integer,
          max: pos_integer
        }
  @type snapshot :: %{key: String.t(), txid: pos_integer}
  @type index :: %{by_min: %{pos_integer => [segment]}, keys: MapSet.t(), by_max: map}

  @spec index([segment]) :: index
  def index(segments) do
    %{
      by_min: Enum.group_by(segments, & &1.min),
      by_max: Enum.group_by(segments, & &1.max),
      keys: MapSet.new(segments, &{&1.level, &1.min, &1.max})
    }
  end

  @spec add(index, segment) :: index
  def add(index, seg) do
    %{
      by_min: Map.update(index.by_min, seg.min, [seg], &[seg | &1]),
      by_max: Map.update(index.by_max, seg.max, [seg], &[seg | &1]),
      keys: MapSet.put(index.keys, {seg.level, seg.min, seg.max})
    }
  end

  @doc """
  The files restore would walk from TXID `from` (exclusive) to the end of the
  listing, the way `Restore` picks them: at each step the file that starts at
  the next TXID and reaches furthest, the lower level first on a tie. Epochs
  are not known here, so this is the chain by keys.
  """
  @spec chain(index, non_neg_integer) :: [segment]
  def chain(index, from), do: walk(index, from, [])

  defp walk(index, at, acc) do
    case Map.get(index.by_min, at + 1) do
      nil ->
        Enum.reverse(acc)

      candidates ->
        best = Enum.min_by(candidates, &{-&1.max, &1.level})
        walk(index, best.max, [best | acc])
    end
  end

  @doc "Last TXID of a chain that starts after `from`."
  @spec head([segment], non_neg_integer) :: non_neg_integer
  def head([], from), do: from
  def head(chain, _from), do: List.last(chain).max

  @doc """
  Aligned groups that can be merged into level `level + 1`.

  A group is `fanout` consecutive files of `level` that cover the block
  `[j * span + 1, (j + 1) * span]` of `span = fanout ^ (level + 1)` TXIDs. The
  blocks are fixed by the TXID, so two compactors that see the same files
  plan the same keys and a race is a conditional-write conflict, never two
  overlapping files. Only blocks that start after `from` and end by `head`
  are planned, and a block whose output already exists is left alone.
  """
  @spec groups(index, non_neg_integer, non_neg_integer, pos_integer, non_neg_integer) :: [map]
  def groups(index, from, head, fanout, level) do
    span_in = pow(fanout, level)
    span = span_in * fanout
    first = div(from + span - 1, span)
    last = div(head, span) - 1

    for j <- first..last//1,
        lo = j * span + 1,
        hi = (j + 1) * span,
        not MapSet.member?(index.keys, {level + 1, lo, hi}),
        inputs = inputs(index, level, lo, span_in, fanout),
        inputs != nil do
      %{level: level + 1, min: lo, max: hi, inputs: inputs}
    end
  end

  defp inputs(index, level, lo, span_in, fanout) do
    Enum.reduce_while(0..(fanout - 1), [], fn i, acc ->
      min = lo + i * span_in
      max = min + span_in - 1

      case Enum.find(Map.get(index.by_min, min, []), &(&1.level == level and &1.max == max)) do
        nil -> {:halt, nil}
        seg -> {:cont, [seg | acc]}
      end
    end)
    |> case do
      nil -> nil
      acc -> Enum.reverse(acc)
    end
  end

  @doc "The files that end exactly at `txid`, the lowest level first."
  @spec ending_at(index, non_neg_integer) :: [segment]
  def ending_at(index, txid), do: index.by_max |> Map.get(txid, []) |> Enum.sort_by(& &1.level)

  defp pow(base, exp), do: Integer.pow(base, exp)
end
