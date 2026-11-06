defmodule Alambique.LtxReader.Crc64 do
  @moduledoc """
  CRC-64/ISO as the LTX specification names it (Go's `crc64.ISO`): reflected
  polynomial `0xD800000000000000`, initial value and final xor all ones.
  """

  import Bitwise

  @poly 0xD800000000000000
  @mask 0xFFFFFFFFFFFFFFFF

  @table (for i <- 0..255 do
            Enum.reduce(1..8, i, fn _, crc ->
              if (crc &&& 1) == 1, do: bxor(crc >>> 1, @poly), else: crc >>> 1
            end)
          end)
         |> List.to_tuple()

  @doc "Checksum of iodata."
  @spec checksum(iodata) :: non_neg_integer
  def checksum(data), do: update(0, data)

  @doc "Continues `crc` over more iodata."
  @spec update(non_neg_integer, iodata) :: non_neg_integer
  def update(crc, data) do
    data |> IO.iodata_to_binary() |> run(bxor(crc, @mask)) |> bxor(@mask)
  end

  defp run(<<>>, crc), do: crc

  defp run(<<byte, rest::binary>>, crc),
    do: run(rest, bxor(elem(@table, bxor(crc, byte) &&& 0xFF), crc >>> 8))
end
