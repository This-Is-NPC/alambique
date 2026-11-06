defmodule Alambique.Replication.ListScaleTest do
  @moduledoc "Listing a long chain costs time linear in its keys (design §6)."
  use ExUnit.Case, async: true

  alias Alambique.Replication.Objects

  # A store that lists `total` keys, a page at a time, without storing them.
  defmodule ListStore do
    def list(%{total: total}, "p/", start_after, limit) do
      first = if start_after, do: String.to_integer(start_after) + 1, else: 1
      last = min(first + limit - 1, total)
      keys = for n <- first..last//1, do: Integer.to_string(n) |> String.pad_leading(8, "0")
      {:ok, keys, if(last < total, do: List.last(keys))}
    end
  end

  defp time(total) do
    ctx = %{
      store: {ListStore, %{total: total}},
      timeout_ms: 60_000,
      backoff_ms: 1,
      max_backoff_ms: 4
    }

    deadline = System.monotonic_time(:millisecond) + 60_000
    {us, {:ok, keys}} = :timer.tc(fn -> Objects.list(ctx, "p/", deadline) end)
    assert length(keys) == total
    assert hd(keys) == "00000001"
    assert List.last(keys) == String.pad_leading(Integer.to_string(total), 8, "0")
    us
  end

  test "listing four times the keys takes about four times as long" do
    # Appending each page to the accumulated list is quadratic: about 16 times.
    time(100_000)
    small = time(250_000)
    large = time(1_000_000)

    assert large < small * 10,
           "#{div(small, 1000)} ms for 250 000 keys, #{div(large, 1000)} ms for 1 000 000"
  end
end
