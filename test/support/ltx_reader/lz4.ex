defmodule Alambique.LtxReader.Lz4 do
  @moduledoc """
  LZ4 block decompression after the block format description of the LZ4
  project: a sequence of (token, literals, offset, match) groups, the last of
  which holds literals only.
  """

  @doc "Decompresses `block`, which must yield exactly `size` bytes."
  @spec decompress(binary, non_neg_integer) :: binary
  def decompress(block, size) do
    out = sequence(block, <<>>, size)

    if byte_size(out) != size,
      do: throw({:ltx, {:page_size, byte_size(out), size}}),
      else: out
  end

  defp sequence(<<>>, _out, _size), do: throw({:ltx, :lz4_truncated})

  defp sequence(<<lit_nibble::4, match_nibble::4, rest::binary>>, out, size) do
    {lit_len, rest} = extend(lit_nibble, rest)

    case rest do
      <<literals::binary-size(^lit_len), rest::binary>> ->
        out = <<out::binary, literals::binary>>
        match(rest, match_nibble, out, size)

      _ ->
        throw({:ltx, :lz4_truncated})
    end
  end

  # The last sequence ends after its literals.
  defp match(<<>>, _nibble, out, _size), do: out

  defp match(<<offset::16-little, rest::binary>>, nibble, out, size) do
    {extra, rest} = extend(nibble, rest)
    len = extra + 4
    have = byte_size(out)

    if offset == 0 or offset > have or have + len > size,
      do: throw({:ltx, {:lz4_match, offset, len}})

    sequence(rest, <<out::binary, copy(out, offset, len)::binary>>, size)
  end

  defp match(_, _nibble, _out, _size), do: throw({:ltx, :lz4_truncated})

  # A match may overlap its own output: repeat the last `offset` bytes.
  defp copy(out, offset, len) do
    pattern = binary_part(out, byte_size(out) - offset, offset)
    reps = div(len, offset) + 1
    binary_part(:binary.copy(pattern, reps), 0, len)
  end

  defp extend(15, rest), do: extend_bytes(rest, 15)
  defp extend(n, rest), do: {n, rest}

  defp extend_bytes(<<255, rest::binary>>, acc), do: extend_bytes(rest, acc + 255)
  defp extend_bytes(<<b, rest::binary>>, acc), do: {acc + b, rest}
  defp extend_bytes(<<>>, _acc), do: throw({:ltx, :lz4_truncated})
end
