defmodule Alambique.LtxExample do
  @moduledoc """
  The two example files of `encoder_golden_test.go` in superfly/ltx
  (Apache-2.0, commit f299ac5b951cdb69ed7a99e957b4ed66cba32560), built from
  the inputs that test states. Upstream has no `.ltx` fixture files at that
  commit, and the only file checksums it pins are those two.

  The file checksum covers each frame's compressed size but not its bytes, and
  upstream's compressor writes 31 bytes for the 512-byte pages of the test.
  The frames here hold a valid LZ4 block of that size, so the files decode
  and have the checksums upstream states.
  """

  import Bitwise

  alias Alambique.LtxReader.Crc64

  @flag 1 <<< 63

  @doc "Checksum upstream's test states for the sparse non-snapshot file."
  def sparse_checksum, do: 0xE27F4C18F0DA2C10

  @doc "Checksum upstream's test states for the snapshot file."
  def snapshot_checksum, do: 0xD926967672F7CA53

  @doc "Page `seed` of the test: byte `i` is `seed + i mod 7`, 512 bytes."
  def page(seed), do: for(i <- 0..511, into: <<>>, do: <<rem(seed + rem(i, 7), 256)>>)

  @doc "Pages 2, 5, 9 and 16 of a 16-page database, TXIDs 2 to 3."
  def sparse, do: build(2, 3, 16, @flag ||| 1, @flag ||| 2, [2, 5, 9, 16])

  @doc "Pages 1 to 6, TXIDs 1 to 4, with the post-apply checksum the test sets."
  def snapshot, do: snapshot(@flag ||| 3)

  @doc "The snapshot with another post-apply checksum, such as the real one of its pages."
  def snapshot(post_apply), do: build(1, 4, 6, 0, post_apply, Enum.to_list(1..6))

  @doc "Pages of the snapshot as `{pgno, data}`."
  def snapshot_pages, do: for(pgno <- 1..6, do: {pgno, page(pgno)})

  # Header, frames, end marker, page index and trailer, laid out as the
  # specification describes.
  defp build(min, max, commit, pre_apply, post_apply, pgnos) do
    header =
      <<"LTX1", 0::32, 512::32, commit::32, min::64, max::64, 1000::64, pre_apply::64, 0::64,
        0::64, 0::32, 0::32, 0::64, 0::160>>

    frames = for pgno <- pgnos, do: {pgno, frame(pgno)}

    {entries, _} =
      Enum.map_reduce(frames, byte_size(header), fn {pgno, f}, offset ->
        {[varint(pgno), varint(offset), varint(byte_size(f))], offset + byte_size(f)}
      end)

    entries = IO.iodata_to_binary([entries, varint(0)])
    index = <<entries::binary, byte_size(entries)::64>>

    hashed =
      for {pgno, f} <- frames,
          do: [binary_part(f, 0, 10), page(pgno)]

    checksum =
      @flag ||| Crc64.checksum([header, hashed, <<0::48>>, index, <<post_apply::64>>])

    IO.iodata_to_binary([
      header,
      Enum.map(frames, &elem(&1, 1)),
      <<0::48>>,
      index,
      <<post_apply::64, checksum::64>>
    ])
  end

  # A 31-byte LZ4 block: 19 literals, a match of 488 bytes at offset 7 (the
  # page repeats every 7 bytes) and the last 5 literals.
  defp frame(pgno) do
    page = page(pgno)
    <<literals::binary-size(19), _::binary-size(488), tail::binary-size(5)>> = page

    block =
      <<0xFF, 4, literals::binary, 7::16-little, 255, 214, 0x50, tail::binary>>

    31 = byte_size(block)
    <<pgno::32, 1::16, 31::32, block::binary>>
  end

  defp varint(n) when n < 128, do: <<n>>
  defp varint(n), do: <<1::1, band(n, 127)::7, varint(n >>> 7)::binary>>
end
