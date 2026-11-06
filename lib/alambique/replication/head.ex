defmodule Alambique.Replication.Head do
  @moduledoc false
  # The head of a cell's chain: enough to compare a local database with the
  # store without restoring it. A tail past a gap is not the head (design §6,
  # R3): the chain is followed by keys from the newest snapshot, and the
  # epochs of its last files cut off lower-epoch objects that a fenced owner
  # left right after the chain.

  alias Alambique.Compaction.Plan
  alias Alambique.Replication.{Chain, Native, Objects, Retry}

  @type t :: %{
          txid: pos_integer,
          checksum: integer,
          epoch: non_neg_integer,
          data: non_neg_integer,
          page_size: pos_integer,
          commit: non_neg_integer
        }

  def run(ctx, cell, opts) do
    deadline = Retry.deadline_in(Keyword.get(opts, :timeout_ms, ctx.timeout_ms))

    with {:ok, {segments, snapshots}} <- Objects.list_cell(ctx, cell, deadline) do
      newest = List.last(snapshots)
      chain = segments |> Plan.index() |> Plan.chain(start(segments, newest))
      base = if newest, do: [newest.key], else: []

      case base ++ Enum.map(chain, & &1.key) do
        [] -> {:ok, nil}
        [key] -> describe(ctx, key, deadline)
        keys -> keys |> Enum.take(-(Chain.window() + 1)) |> cut(ctx, deadline)
      end
    end
  end

  # Where the chain starts: after the newest snapshot, else at the oldest file
  # (retention may have cut the early history).
  defp start(_segments, %{txid: txid}), do: txid
  defp start([], nil), do: 0
  defp start(segments, nil), do: Enum.min(Enum.map(segments, & &1.min)) - 1

  # HEADs the epochs of all `keys` but the last (oldest first) while the last
  # is described: its answer carries its own epoch, so it needs no HEAD. The
  # result is the last file before the first epoch regression, which is the
  # last key unless a fenced owner wrote it, and then one more file is read.
  defp cut(keys, ctx, deadline) do
    {earlier, [last]} = Enum.split(keys, -1)
    described = Task.async(fn -> describe(ctx, last, deadline) end)
    epochs = epochs(ctx, earlier, deadline)
    described = Task.await(described, :infinity)

    with {:ok, epochs} <- epochs,
         {:ok, head} <- described do
      live = keys |> Enum.zip(epochs ++ [head.epoch]) |> Chain.live() |> Enum.reverse()

      case live do
        [^last | _] -> {:ok, head}
        [key | _] -> describe(ctx, key, deadline)
      end
    end
  end

  defp epochs(ctx, keys, deadline) do
    keys
    |> Task.async_stream(
      fn key ->
        with {:ok, meta} <- Objects.head(ctx, key, deadline), do: {:ok, Objects.epoch_of(meta)}
      end,
      max_concurrency: length(keys),
      timeout: :infinity,
      ordered: true
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, epoch}}, {:ok, acc} -> {:cont, {:ok, [epoch | acc]}}
      {:ok, {:error, _} = error}, _ -> {:halt, error}
      {:exit, reason}, _ -> {:halt, {:error, {:task_exit, reason}}}
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  # The header and the trailer of the file are all `ltx_info/2` reads. A file
  # that fits in the first range is one request; a larger one is two ranges
  # and the middle is never downloaded.
  @first_range 4096
  # Sizes of the LTX header and trailer (docs/segment-format.md).
  @header_size 100
  @trailer_size 16

  defp describe(ctx, key, deadline) do
    with {:ok, head, total, meta} <- Objects.fetch_range(ctx, key, 0, @first_range, deadline),
         {:ok, edges} <- edges(ctx, key, head, total, deadline),
         {:ok, {_min, max, _ts, commit, _pre, page_size}, post} <-
           wrap(Native.ltx_info(edges, false), key) do
      {:ok,
       %{
         txid: max,
         checksum: post,
         epoch: Objects.epoch_of(meta),
         data: Objects.data_of(meta, max),
         page_size: page_size,
         commit: commit
       }}
    end
  end

  defp edges(_ctx, _key, head, total, _deadline) when byte_size(head) >= total, do: {:ok, head}

  defp edges(ctx, key, head, total, deadline) do
    with {:ok, tail, _total, _meta} <-
           Objects.fetch_range(ctx, key, total - @trailer_size, @trailer_size, deadline),
         do: {:ok, binary_part(head, 0, @header_size) <> tail}
  end

  defp wrap({:ok, _, _} = ok, _key), do: ok
  defp wrap({:error, reason}, key), do: {:error, {:segment, key, reason}}
end
