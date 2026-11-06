defmodule Alambique.Feed.Record do
  @moduledoc """
  One row change on the change feed (design §13).

    * `:cell` - the cell the change happened in
    * `:table` - the table
    * `:op` - `:insert`, `:update` or `:delete`
    * `:key` - the primary key as a map of column to value, or `%{"rowid" => n}`
      for a table without one. A blob value is `%{"$blob" => hex}`
    * `:values` - the new row as a map of column to value, `nil` for a delete
    * `:position` - `{txid, n}`: the TXID of the replicated batch and the
      index of the change in it. Positions of one cell grow with commit order
    * `:id` - the idempotency key, `"<cell>:<txid>:<n>"`
    * `:at` - publication time in milliseconds, from the clock of the node
      that published

  Text that is not valid UTF-8 arrives with each bad byte replaced by U+FFFD.

  The body of a feed object is `"ALF"`, the version byte 1 and the JSON
  compressed with zlib. Any other body becomes a `Rejected`.
  """

  alias Alambique.Feed.{Rejected, Rewind}

  @enforce_keys [:cell, :table, :op, :key, :values, :position, :id, :at]
  defstruct [:cell, :table, :op, :key, :values, :position, :id, :at]

  @type t :: %__MODULE__{
          cell: String.t(),
          table: String.t(),
          op: :insert | :update | :delete,
          key: %{optional(String.t()) => term},
          values: %{optional(String.t()) => term} | nil,
          position: {pos_integer, non_neg_integer},
          id: String.t(),
          at: non_neg_integer
        }

  @ops %{"insert" => :insert, "update" => :update, "delete" => :delete}

  @magic "ALF"
  @version 1
  @level 6
  @max_object_bytes 268_435_456

  @doc false
  # The largest object a reader inflates unless told otherwise.
  @spec default_max_bytes() :: pos_integer
  def default_max_bytes, do: @max_object_bytes

  @doc false
  # Total: an object that cannot be decoded becomes one `Rejected` in place of
  # its records. The object of a rewind has no records and a "rewind" key,
  # (design §13). An object of a group of
  # batches holds a "batches" list instead of one "txid" and "records". A
  # object is read as the JSON it holds, up to `max_bytes` once
  # inflated, so a hostile writer cannot exhaust the reader's memory.
  @spec decode(String.t(), binary, non_neg_integer, pos_integer) :: [
          t | Rewind.t() | Rejected.t()
        ]
  def decode(key, body, at, max_bytes \\ @max_object_bytes) do
    case json(body, max_bytes) do
      {:ok, json} -> decode_json(key, json, at)
      {:error, reason} -> [rejected(key, reason, at)]
    end
  end

  @doc false
  # The JSON text of an object body.
  @spec json(binary, pos_integer) :: {:ok, binary} | {:error, term}
  def json(<<@magic, @version, packed::binary>>, max_bytes), do: inflate(packed, max_bytes)
  def json(<<@magic, version, _::binary>>, _max), do: {:error, {:unsupported_format, version}}
  def json(_body, _max), do: {:error, :bad_format}

  defp inflate(packed, max_bytes) do
    z = :zlib.open()

    try do
      :ok = :zlib.inflateInit(z)
      inflate(z, :zlib.safeInflate(z, packed), [], 0, max_bytes)
    catch
      :error, _ -> {:error, :bad_compression}
    after
      :zlib.close(z)
    end
  end

  # A stream that asks for more input and gives no output has ended early.
  defp inflate(z, {:continue, out}, acc, size, max_bytes) do
    length = IO.iodata_length(out)

    cond do
      length == 0 -> {:error, :bad_compression}
      size + length > max_bytes -> {:error, :too_large}
      true -> inflate(z, :zlib.safeInflate(z, []), [acc | out], size + length, max_bytes)
    end
  end

  defp inflate(_z, {:finished, out}, acc, size, max_bytes) do
    if size + IO.iodata_length(out) > max_bytes,
      do: {:error, :too_large},
      else: {:ok, IO.iodata_to_binary([acc | out])}
  end

  defp decode_json(key, body, at) do
    case Jason.decode(body) do
      {:ok, %{"cell" => cell, "txid" => txid, "records" => records} = object}
      when is_binary(cell) and is_integer(txid) and is_list(records) ->
        with {:ok, records} <- records(records, cell, txid, at),
             {:ok, marker} <- marker(object, cell, txid, at) do
          records ++ marker
        else
          {:error, reason} -> [rejected(key, reason, at)]
        end

      {:ok, %{"cell" => cell, "batches" => batches}} when is_binary(cell) and is_list(batches) ->
        case group(batches, cell, at) do
          {:ok, records} -> records
          {:error, reason} -> [rejected(key, reason, at)]
        end

      {:ok, _} ->
        [rejected(key, :bad_object, at)]

      {:error, _} ->
        [rejected(key, :invalid_json, at)]
    end
  end

  defp group(batches, cell, at) do
    Enum.reduce_while(batches, {:ok, []}, fn
      %{"txid" => txid, "records" => records}, {:ok, acc}
      when is_integer(txid) and is_list(records) ->
        case records(records, cell, txid, at) do
          {:ok, records} -> {:cont, {:ok, acc ++ records}}
          {:error, _} = error -> {:halt, error}
        end

      _, _ ->
        {:halt, {:error, :bad_object}}
    end)
  end

  defp rejected(key, reason, at),
    do: %Rejected{key: key, reason: reason, id: "rejected:" <> key, at: at}

  defp marker(
         %{"rewind" => %{"from_txid" => _, "to_txid" => _, "target" => _} = rewind},
         cell,
         txid,
         at
       ) do
    case Rewind.decode(cell, txid, rewind, at) do
      %Rewind{} = marker -> {:ok, [marker]}
      error -> error
    end
  end

  defp marker(%{"rewind" => _}, _cell, _txid, _at), do: {:error, :bad_record}
  defp marker(_object, _cell, _txid, _at), do: {:ok, []}

  defp records(records, cell, txid, at) do
    Enum.reduce_while(records, {:ok, []}, fn raw, {:ok, acc} ->
      case record(raw, cell, txid, at) do
        {:ok, record} -> {:cont, {:ok, [record | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end)
  end

  defp record(%{"n" => n, "table" => table, "op" => op, "key" => key} = r, cell, txid, at)
       when is_integer(n) and is_binary(table) and is_map(key) do
    case @ops do
      %{^op => op} ->
        {:ok,
         %__MODULE__{
           cell: cell,
           table: table,
           op: op,
           key: key,
           values: r["values"],
           position: {txid, n},
           id: "#{cell}:#{txid}:#{n}",
           at: at
         }}

      _ ->
        {:error, {:bad_op, op}}
    end
  end

  defp record(_raw, _cell, _txid, _at), do: {:error, :bad_record}

  @doc false
  # The object body is spliced from the JSON text SQLite produced, so column
  # order is kept and nothing is decoded on the publishing side. SQLite does
  # not check that text is UTF-8, so bytes that are not become U+FFFD: the
  # body is always valid JSON. One batch keeps the single-batch shape; two or
  # more make a group, which never holds a rewind. The JSON is then compressed
  # with zlib at level 6 (design §13).
  @type change :: {String.t(), String.t(), String.t(), String.t() | nil}
  @spec encode(String.t(), [{pos_integer, [change]}, ...]) :: binary
  def encode(cell, batches) do
    body = batches |> iodata(cell) |> IO.iodata_to_binary()
    pack(if String.valid?(body), do: body, else: String.replace_invalid(body))
  end

  @doc false
  # The object body for a JSON text. Tests use it to build damaged objects.
  @spec pack(binary) :: binary
  def pack(json) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, @level)
      IO.iodata_to_binary([@magic, @version, :zlib.deflate(z, json, :finish)])
    after
      :zlib.close(z)
    end
  end

  defp iodata([{txid, [{"", "rewind", _key, rewind}]}], cell) do
    [
      ~s({"cell":),
      Jason.encode!(cell),
      ~s(,"txid":),
      Integer.to_string(txid),
      ~s(,"records":[],"rewind":),
      rewind,
      ?}
    ]
  end

  defp iodata([{txid, changes}], cell) do
    [
      ~s({"cell":),
      Jason.encode!(cell),
      ~s(,"txid":),
      Integer.to_string(txid),
      ~s(,"records":[),
      rows(changes),
      "]}"
    ]
  end

  defp iodata(batches, cell) do
    batches =
      batches
      |> Enum.map(fn {txid, changes} ->
        [~s({"txid":), Integer.to_string(txid), ~s(,"records":[), rows(changes), "]}"]
      end)
      |> Enum.intersperse(?,)

    [~s({"cell":), Jason.encode!(cell), ~s(,"batches":[), batches, "]}"]
  end

  defp rows(changes) do
    changes
    |> Enum.with_index()
    |> Enum.map(fn {{table, op, key, values}, n} ->
      [
        ~s({"n":),
        Integer.to_string(n),
        ~s(,"table":),
        Jason.encode!(table),
        ~s(,"op":"),
        op,
        ~s(","key":),
        key,
        ~s(,"values":),
        values || "null",
        ?}
      ]
    end)
    |> Enum.intersperse(?,)
  end
end
