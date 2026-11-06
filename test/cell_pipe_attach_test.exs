defmodule Alambique.CellPipeAttachTest do
  @moduledoc "Making reads wait for the newest batch costs the same for each read (design §6)."
  use ExUnit.Case, async: true

  alias Alambique.Cell.Pipe

  @batch %{txid: 1, ref: nil, pid: nil, landed: false, items: [], writes: 0}

  defp reductions do
    {:reductions, n} = Process.info(self(), :reductions)
    n
  end

  test "attaching one item at a time is linear in the number of items" do
    pipe = %Pipe{queue: [%{@batch | items: Enum.map(1..256, &{:write, &1, :ok})}]}
    before = reductions()

    pipe =
      Enum.reduce(1..20_000, pipe, fn n, acc -> Pipe.attach(acc, [{:read, n, :ok}]) end)

    assert reductions() - before < 2_000_000
    assert length(Pipe.items(pipe)) == 20_256
  end

  test "attached items are kept with the batch, the newest first" do
    pipe = %Pipe{queue: [%{@batch | items: [:b, :a]}, %{@batch | items: [:d, :c]}]}
    pipe = pipe |> Pipe.attach([:e]) |> Pipe.attach([:g, :f])

    assert [%{items: [:b, :a]}, %{items: [:g, :f, :e, :d, :c]}] = pipe.queue
  end
end
