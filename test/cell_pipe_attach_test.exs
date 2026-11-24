defmodule Alambique.CellPipeAttachTest do
  @moduledoc "Making reads wait for the newest batch with a write costs the same for each read (design §6, §15)."
  use ExUnit.Case, async: true

  alias Alambique.Cell.Pipe

  @batch %{txid: 1, ref: nil, pid: nil, landed: false, items: [], writes: 1}

  defp reductions do
    {:reductions, n} = Process.info(self(), :reductions)
    n
  end

  defp batch(ref, writes, items \\ []), do: %{@batch | ref: ref, writes: writes, items: items}

  test "attaching one item at a time is linear in the number of items" do
    pipe = %Pipe{queue: [%{@batch | items: Enum.map(1..256, &{:write, &1, :ok})}]}
    before = reductions()

    pipe =
      Enum.reduce(1..20_000, pipe, fn n, acc ->
        {:ok, acc} = Pipe.attach_to_write(acc, [{:read, n, :ok}])
        acc
      end)

    assert reductions() - before < 2_000_000
    assert length(Pipe.items(pipe)) == 20_256
  end

  test "attached items are kept with the batch, the newest first" do
    pipe = %Pipe{queue: [batch(:x, 1, [:b, :a]), batch(:y, 1, [:d, :c])]}
    {:ok, pipe} = Pipe.attach_to_write(pipe, [:e])
    {:ok, pipe} = Pipe.attach_to_write(pipe, [:g, :f])

    assert [%{items: [:b, :a]}, %{items: [:g, :f, :e, :d, :c]}] = pipe.queue
  end

  test "the items go to the newest batch with a write; no batch moves or changes" do
    queue = [batch(:a, 2, [:a]), batch(:u, 1, [:u]), batch(:c1, 0), batch(:c2, 0)]

    assert {:ok, %Pipe{queue: attached}} = Pipe.attach_to_write(%Pipe{queue: queue}, [:r])

    assert Enum.map(attached, & &1.ref) == [:a, :u, :c1, :c2]
    assert Enum.map(attached, & &1.items) == [[:a], [:r, :u], [], []]
    assert Enum.map(attached, &Map.delete(&1, :items)) == Enum.map(queue, &Map.delete(&1, :items))
  end

  test "a queue of batches without a write takes no items" do
    assert Pipe.attach_to_write(%Pipe{queue: [batch(:c1, 0), batch(:c2, 0)]}, [:r]) == :none
    assert Pipe.attach_to_write(%Pipe{queue: []}, [:r]) == :none
  end
end
