defmodule Alambique.Replication.Objects do
  @moduledoc false
  # Store access shared by restore, head, compaction and retention: paged
  # listing, and gets, heads, puts and deletes retried until a deadline.

  alias Alambique.Replication.{Keys, Retry}

  @doc "Keys under `prefix`, after `start_after` when given."
  @spec list(map, String.t(), integer, String.t() | nil) :: {:ok, [String.t()]} | {:error, term}
  def list(ctx, prefix, deadline, start_after \\ nil),
    do: list(ctx, prefix, deadline, start_after, [])

  defp list(ctx, prefix, deadline, start_after, acc) do
    case page(ctx, prefix, deadline, start_after, 1000) do
      {:ok, keys, nil} -> {:ok, Enum.reverse([keys | acc]) |> Enum.concat()}
      {:ok, keys, next} -> list(ctx, prefix, deadline, next, [keys | acc])
      {:error, _} = error -> error
    end
  end

  @doc "One page of at most `limit` keys under `prefix` and the cursor of the next, retried."
  @spec page(map, String.t(), integer, String.t() | nil, pos_integer) ::
          {:ok, [String.t()], String.t() | nil} | {:error, term}
  def page(ctx, prefix, deadline, start_after, limit) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.list(config, prefix, start_after, limit) do
        {:ok, keys, next} -> {:ok, keys, next}
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc "Lists the cell once and splits it into segments and snapshots (`Keys.parse/3`)."
  @spec list_cell(map, String.t(), integer) :: {:ok, {[map], [map]}} | {:error, term}
  def list_cell(ctx, cell, deadline) do
    with {:ok, keys} <- list(ctx, Keys.files_prefix(ctx.prefix, cell), deadline),
         do: {:ok, Keys.parse(ctx.prefix, cell, keys)}
  end

  @doc """
  An object and its metadata, retried. A missing object is retried too, in
  case the listing was ahead of the read, unless `ctx.missing_final` is set:
  a reader that lists first (`Alambique.Query`) then knows the file was
  deleted and lists again.
  """
  @spec fetch(map, String.t(), integer) :: {:ok, binary, map} | {:error, term}
  def fetch(ctx, key, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.get(config, key) do
        {:ok, body, _etag, meta} -> {:ok, body, meta}
        {:error, reason} -> retry_or_final(ctx, reason)
      end
    end)
  end

  defp retry_or_final(%{missing_final: true}, :not_found), do: {:error, :not_found}
  defp retry_or_final(_ctx, reason), do: {:retry, reason}

  @doc "Like `fetch/3`, but a missing object is `:none` at once instead of being retried."
  @spec fetch_optional(map, String.t(), integer) :: {:ok, binary, map} | :none | {:error, term}
  def fetch_optional(ctx, key, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.get(config, key) do
        {:ok, body, _etag, meta} -> {:ok, body, meta}
        {:error, :not_found} -> :none
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc "Part of an object, its size and its metadata, retried."
  @spec fetch_range(map, String.t(), non_neg_integer, pos_integer, integer) ::
          {:ok, binary, non_neg_integer, map} | {:error, term}
  def fetch_range(ctx, key, offset, length, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.get_range(config, key, offset, length) do
        {:ok, body, total, meta} -> {:ok, body, total, meta}
        {:error, reason} -> retry_or_final(ctx, reason)
      end
    end)
  end

  @doc "Epoch recorded in the object metadata; a missing or unreadable one counts as 0."
  @spec epoch_of(map) :: non_neg_integer
  def epoch_of(meta) do
    case Integer.parse(Map.get(meta, "epoch", "0")) do
      {epoch, ""} -> epoch
      _ -> 0
    end
  end

  @doc """
  Data TXID recorded in the object metadata (design §6): the newest commit of
  the chain, up to this object, that changed data. An object without a
  readable one counts as `max_txid`: that can
  only flag a fence as data, never hide data.
  """
  @spec data_of(map, non_neg_integer) :: non_neg_integer
  def data_of(meta, max_txid) do
    case int(meta, "data") do
      nil -> max_txid
      data -> data
    end
  end

  @doc "Metadata of an object, retried."
  @spec head(map, String.t(), integer) :: {:ok, map} | {:error, term}
  def head(ctx, key, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.head(config, key) do
        {:ok, _etag, meta} -> {:ok, meta}
        {:error, :not_found} = missing -> missing
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc """
  Writes an object only if the key is free. `:exists` means another writer
  got there first. A failed attempt may have landed: the retry then sees
  `:exists`, which is the right answer for content-addressed keys such as
  compacted files.
  """
  @spec put_new(map, String.t(), iodata, map, integer) :: :ok | :exists | {:error, term}
  def put_new(ctx, key, body, info, deadline),
    do: put(ctx, key, body, :if_none_match, meta(ctx, info), deadline)

  @doc "Writes an object with the given condition and metadata, retried; `:exists` is a failed `:if_none_match`."
  @spec put(map, String.t(), iodata, Alambique.Store.condition(), map, integer) ::
          :ok | :exists | {:error, term}
  def put(ctx, key, body, condition, meta, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.put(config, key, body, condition, meta) do
        {:ok, _etag} -> :ok
        {:error, :precondition_failed} -> :exists
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc "Deletes an object, retried; a missing object counts as deleted."
  @spec delete(map, String.t(), integer) :: :ok | {:error, term}
  def delete(ctx, key, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.delete(config, key) do
        :ok -> :ok
        {:error, :not_found} -> :ok
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc """
  Object metadata for an upload: `epoch`, `data` (the data TXID, design §6)
  and `ts` (the stamp of the newest state the object holds) always, plus the
  same stamp as `litestream-timestamp` (the key of Litestream's replica
  layout), `created` (when the object was written, for retention) when given.
  The writer's node comes from the context.
  """
  @spec meta(map, %{
          required(:epoch) => integer,
          required(:data) => integer,
          optional(atom) => integer
        }) ::
          map
  def meta(ctx, %{epoch: epoch, data: data} = info) do
    base =
      for {key, value} <- [ts: info[:ts], created: info[:created]],
          value != nil,
          into: %{
            "epoch" => Integer.to_string(epoch),
            "data" => Integer.to_string(data),
            "node" => ctx.node
          },
          do: {Atom.to_string(key), Integer.to_string(value)}

    case info[:ts] do
      nil -> base
      ts -> Map.put(base, "litestream-timestamp", litestream_timestamp(ts))
    end
  end

  # Litestream's S3 replica client reads the stamp of a file from this key
  # (RFC 3339) for point-in-time restores.
  defp litestream_timestamp(ms),
    do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

  @doc "An integer metadata value such as `ts`; `nil` when missing or unreadable."
  @spec int(map, String.t()) :: integer | nil
  def int(meta, key) do
    case Integer.parse(Map.get(meta, key, "")) do
      {value, ""} -> value
      _ -> nil
    end
  end
end
