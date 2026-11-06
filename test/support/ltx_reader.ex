defmodule Alambique.LtxReader do
  @moduledoc """
  An LTX version 3 reader written from the specification only
  (README of superfly/ltx at the commit pinned in docs/segment-format.md).
  It shares no code with the Rust encoder and decoder: it has its own LZ4
  block decompressor and CRC-64/ISO, checks everything the specification
  states about a file, and applies chains of files to an in-memory database.

  Files written by Alambique set `NoChecksum` and keep the pre- and
  post-apply database checksums in header bytes 80..96; `decode/1` reads them
  from there and requires the header and trailer fields to be zero.
  """

  import Bitwise

  alias Alambique.LtxReader.{Crc64, Lz4}

  @header_size 100
  @flag 1 <<< 63
  @no_checksum 2
  @pending_byte 0x40000000

  @type file :: %{
          header: map,
          pages: [{pos_integer, binary}],
          pre_apply: non_neg_integer,
          post_apply: non_neg_integer,
          file_checksum: non_neg_integer
        }

  @doc "Decodes and verifies one file."
  @spec decode(binary) :: {:ok, file} | {:error, term}
  def decode(bin) when is_binary(bin) do
    {:ok, parse!(bin)}
  catch
    {:ltx, reason} -> {:error, reason}
  end

  @doc "Database checksum of a set of pages: the flag bit and the xor of the page checksums."
  @spec database_checksum([{pos_integer, binary}]) :: non_neg_integer
  def database_checksum(pages),
    do: Enum.reduce(pages, @flag, fn {pgno, data}, acc -> bxor(acc, page_sum(pgno, data)) end)

  @doc "An empty database at TXID 0."
  def new_db, do: %{pages: %{}, sums: %{}, checksum: @flag, txid: 0, commit: 0, page_size: nil}

  @doc """
  Applies a decoded file. A file starting at TXID 1 replaces the database;
  any other must start at the next TXID with a pre-apply checksum equal to the
  database checksum. The resulting checksum must equal the post-apply one.
  """
  @spec apply_file(map, file) :: {:ok, map} | {:error, term}
  def apply_file(db, %{header: h} = file) do
    db = if h.min_txid == 1, do: new_db(), else: db

    cond do
      h.min_txid != db.txid + 1 ->
        {:error, {:txid_gap, db.txid, h.min_txid}}

      h.min_txid > 1 and file.pre_apply != db.checksum ->
        {:error, {:pre_apply_mismatch, file.pre_apply, db.checksum}}

      true ->
        pages = Map.merge(db.pages, Map.new(file.pages))
        pages = Map.reject(pages, fn {pgno, _} -> pgno > h.commit end)
        sums = Map.new(pages, fn {pgno, data} -> {pgno, page_sum(pgno, data)} end)
        checksum = Enum.reduce(sums, @flag, fn {_, s}, acc -> bxor(acc, s) end)

        if checksum == file.post_apply do
          {:ok,
           %{
             db
             | pages: pages,
               sums: sums,
               checksum: checksum,
               txid: h.max_txid,
               commit: h.commit,
               page_size: h.page_size
           }}
        else
          {:error, {:post_apply_mismatch, file.post_apply, checksum}}
        end
    end
  end

  @doc "Applies decoded files in order, starting from an empty database."
  @spec apply_chain([file]) :: {:ok, map} | {:error, term}
  def apply_chain(files) do
    Enum.reduce_while(files, {:ok, new_db()}, fn file, {:ok, db} ->
      case apply_file(db, file) do
        {:ok, db} -> {:cont, {:ok, db}}
        error -> {:halt, error}
      end
    end)
  end

  @doc "The database file bytes: pages 1 to `commit`, every one present."
  @spec image(map) :: {:ok, binary} | {:error, term}
  def image(%{commit: 0}), do: {:ok, <<>>}

  def image(%{pages: pages, commit: commit, page_size: size}) do
    lock = div(@pending_byte, size) + 1

    Enum.reduce_while(1..commit, {:ok, []}, fn
      pgno, {:ok, acc} when pgno == lock -> {:cont, {:ok, [:binary.copy(<<0>>, size) | acc]}}
      pgno, {:ok, acc} -> fetch_page(pages, pgno, acc)
    end)
    |> case do
      {:ok, iodata} -> {:ok, iodata |> Enum.reverse() |> IO.iodata_to_binary()}
      error -> error
    end
  end

  defp fetch_page(pages, pgno, acc) do
    case pages do
      %{^pgno => data} -> {:cont, {:ok, [data | acc]}}
      _ -> {:halt, {:error, {:missing_page, pgno}}}
    end
  end

  # Each page checksum carries the flag bit; xor-ing them and setting the flag
  # again is the same as xor-ing the lower 63 bits under a fixed flag.
  defp page_sum(pgno, data), do: band(Crc64.checksum([<<pgno::32>>, data]), @flag - 1)

  # Parsing

  defp parse!(bin) when byte_size(bin) < @header_size + 6 + 8 + 16, do: fail(:too_short)

  defp parse!(bin) do
    <<header_bin::binary-size(@header_size), body::binary>> = bin
    header = parse_header!(header_bin)
    {frames, index_bin} = parse_frames!(body, header, @header_size, 0, [])
    {index_part, <<post::64, file_checksum::64>>} = parse_index!(index_bin, frames)

    expected =
      @flag |||
        Crc64.checksum([header_bin, hashed(frames), <<0::48>>, index_part, <<post::64>>])

    if file_checksum != expected, do: fail({:file_checksum, file_checksum, expected})

    {pre_apply, post_apply} = database_checksums!(header, post)

    %{
      header: Map.delete(header, :reserved),
      pages: for({pgno, _offset, _size, data} <- frames, do: {pgno, data}),
      pre_apply: pre_apply,
      post_apply: post_apply,
      file_checksum: file_checksum
    }
  end

  defp parse_header!(
         <<"LTX1", flags::32, page_size::32, commit::32, min::64, max::64, timestamp::64, pre::64,
           wal_offset::64, wal_size::64, salt1::32, salt2::32, node_id::64,
           reserved::binary-size(20)>>
       ) do
    if flags not in [0, @no_checksum], do: fail({:invalid_flags, flags})

    if page_size < 512 or page_size > 65_536 or (page_size &&& page_size - 1) != 0,
      do: fail({:invalid_page_size, page_size})

    if min < 1 or max < min, do: fail({:invalid_txid_range, min, max})

    %{
      flags: flags,
      page_size: page_size,
      commit: commit,
      min_txid: min,
      max_txid: max,
      timestamp: timestamp,
      pre_apply: pre,
      wal_offset: wal_offset,
      wal_size: wal_size,
      wal_salt1: salt1,
      wal_salt2: salt2,
      node_id: node_id,
      reserved: reserved
    }
  end

  defp parse_header!(_), do: fail(:bad_magic)

  # Page frames up to the zero page header: {frames in order, bytes after it}.
  defp parse_frames!(<<0::32, 0::16, rest::binary>>, _h, _offset, _prev, acc),
    do: {Enum.reverse(acc), rest}

  defp parse_frames!(<<pgno::32, 1::16, size::32, rest::binary>>, h, offset, prev, acc) do
    lock = div(@pending_byte, h.page_size) + 1

    cond do
      pgno == 0 -> fail({:page_zero, offset})
      pgno <= prev -> fail({:page_order, pgno, prev})
      pgno > h.commit -> fail({:page_past_commit, pgno, h.commit})
      pgno == lock -> fail({:lock_page, pgno})
      byte_size(rest) < size -> fail({:truncated_frame, pgno})
      true -> :ok
    end

    <<payload::binary-size(^size), rest::binary>> = rest
    data = Lz4.decompress(payload, h.page_size)
    frame = {pgno, offset, 10 + size, data}
    parse_frames!(rest, h, offset + 10 + size, pgno, [frame | acc])
  end

  defp parse_frames!(<<_pgno::32, flags::16, _::binary>>, _h, _offset, _prev, _acc),
    do: fail({:invalid_page_flags, flags})

  defp parse_frames!(_, _h, _offset, _prev, _acc), do: fail(:truncated_pages)

  # What the file checksum covers per frame: header and size prefix as stored,
  # then the decompressed page.
  defp hashed(frames) do
    for {pgno, _offset, size, data} <- frames, do: [<<pgno::32, 1::16, size - 10::32>>, data]
  end

  # Entries of three varints, a zero page number, then the 8-byte size of all
  # of that. Returns the bytes the file checksum covers and the trailer.
  defp parse_index!(bin, frames) do
    {entries, after_entries} = varint_entries(bin, [])
    consumed = byte_size(bin) - byte_size(after_entries)
    expected = for {pgno, offset, size, _data} <- frames, do: {pgno, offset, size}

    if entries != expected, do: fail(:index_mismatch)

    case after_entries do
      <<size::64, trailer::binary-size(16)>> ->
        if size != consumed, do: fail({:index_size, size, consumed})
        {binary_part(bin, 0, consumed + 8), trailer}

      _ ->
        fail(:bad_trailer_length)
    end
  end

  defp varint_entries(bin, acc) do
    {pgno, rest} = varint(bin)

    if pgno == 0 do
      {Enum.reverse(acc), rest}
    else
      {offset, rest} = varint(rest)
      {size, rest} = varint(rest)
      varint_entries(rest, [{pgno, offset, size} | acc])
    end
  end

  defp varint(bin, shift \\ 0, acc \\ 0)
  defp varint(<<>>, _shift, _acc), do: fail(:truncated_varint)
  defp varint(<<0::1, v::7, rest::binary>>, shift, acc), do: {acc ||| v <<< shift, rest}

  defp varint(<<1::1, v::7, rest::binary>>, shift, acc),
    do: varint(rest, shift + 7, acc ||| v <<< shift)

  defp database_checksums!(%{flags: 0} = h, post) do
    if h.reserved != <<0::160>>, do: fail(:reserved_not_zero)
    if band(post, @flag) == 0, do: fail(:post_apply_format)
    if h.min_txid == 1 and h.pre_apply != 0, do: fail(:snapshot_pre_apply)
    if h.min_txid > 1 and band(h.pre_apply, @flag) == 0, do: fail(:pre_apply_format)
    {h.pre_apply, post}
  end

  # NoChecksum: the fields the specification defines are zero, ours are in the
  # reserved bytes.
  defp database_checksums!(%{flags: @no_checksum} = h, post) do
    if h.pre_apply != 0 or post != 0, do: fail(:no_checksum_not_zero)
    <<pre::64, post::64, rest::binary>> = h.reserved
    if rest != <<0::32>>, do: fail(:reserved_not_zero)
    if band(post, @flag) == 0, do: fail(:post_apply_format)
    if h.min_txid == 1 and pre != 0, do: fail(:snapshot_pre_apply)
    if h.min_txid > 1 and band(pre, @flag) == 0, do: fail(:pre_apply_format)
    {pre, post}
  end

  defp fail(reason), do: throw({:ltx, reason})
end
