defmodule Alambique.BenchText do
  @moduledoc false
  # Text for the benchmarks: from a vocabulary of 3000 words in a skewed order,
  # with line breaks, about as compressible as plain mail text. Deterministic
  # per `id`.

  @doc "A text of exactly `bytes` bytes."
  @spec body(integer, pos_integer) :: binary
  def body(id, bytes) do
    :rand.seed(:exsss, {id, 17, 99})
    vocab = vocabulary()

    Stream.repeatedly(fn ->
      word = elem(vocab, trunc(:math.pow(:rand.uniform(), 2.5) * tuple_size(vocab)))
      if :rand.uniform(12) == 1, do: word <> "\n", else: word <> " "
    end)
    |> Enum.reduce_while({[], 0}, fn word, {acc, size} ->
      if size >= bytes,
        do: {:halt, {acc, size}},
        else: {:cont, {[word | acc], size + byte_size(word)}}
    end)
    |> elem(0)
    |> Enum.reverse()
    |> IO.iodata_to_binary()
    |> binary_part(0, bytes)
  end

  defp vocabulary do
    case :persistent_term.get({__MODULE__, :vocab}, nil) do
      nil ->
        :rand.seed(:exsss, {3, 1, 4})

        vocab =
          List.to_tuple(
            for _ <- 1..3000 do
              for(_ <- 1..(2 + :rand.uniform(7)), into: "", do: <<Enum.random(?a..?z)>>)
            end
          )

        :persistent_term.put({__MODULE__, :vocab}, vocab)
        vocab

      vocab ->
        vocab
    end
  end
end
