defmodule Alambique.CellRenewJitterTest do
  @moduledoc "Lease renewals of cells woken together are spread (design §7)."
  use ExUnit.Case, async: true

  alias Alambique.Cell

  defp config(ttl, jitter), do: %{ttl_ms: ttl, renew_jitter: jitter}

  test "the interval is a third of the TTL, moved by at most the jitter" do
    intervals = for _ <- 1..2_000, do: Cell.renew_interval(config(30_000, 0.1))

    assert Enum.all?(intervals, &(&1 in 9_000..11_000))
    # Cells woken at the same moment must not renew in the same few ms.
    assert Enum.max(intervals) - Enum.min(intervals) > 1_500
    assert length(Enum.uniq(intervals)) > 500
  end

  test "no jitter is exactly a third" do
    assert Enum.uniq(for _ <- 1..100, do: Cell.renew_interval(config(30_000, 0))) == [10_000]
  end
end
