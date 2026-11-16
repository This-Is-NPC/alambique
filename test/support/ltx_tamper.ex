defmodule Alambique.LtxTamper do
  @moduledoc """
  Corrupts an LTX file so that the page frames still decode, to other pages:
  the last byte of an LZ4 block (always a literal) is flipped.
  """

  @doc "The file with the last literal of every frame flipped, or of the last frame only."
  @spec alter_frames(binary, :all | :last) :: binary
  def alter_frames(<<head::binary-size(100), frames::binary>>, which \\ :all) do
    <<head::binary, alter(frames, which)::binary>>
  end

  defp alter(<<0::48, _::binary>> = rest, _which), do: rest

  defp alter(<<pgno::32, flags::16, n::32, payload::binary-size(n), rest::binary>>, which) do
    last? = match?(<<0::48, _::binary>>, rest)
    payload = if which == :all or last?, do: flip(payload), else: payload
    <<pgno::32, flags::16, n::32, payload::binary, alter(rest, which)::binary>>
  end

  defp flip(payload) do
    m = byte_size(payload) - 1
    <<init::binary-size(^m), last>> = payload
    <<init::binary, Bitwise.bxor(last, 1)>>
  end
end
