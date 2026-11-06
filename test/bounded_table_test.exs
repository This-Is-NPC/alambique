defmodule Alambique.BoundedTableTest do
  @moduledoc "The hint, location and memo caches keep most entries at their limit (design §8)."
  use ExUnit.Case, async: true

  alias Alambique.{BoundedTable, Membership}

  defp table, do: :ets.new(:bounded, [:public, :set, read_concurrency: true])

  test "a full table loses a sixteenth of its entries, not all of them" do
    config = %{hints: table(), hint_limit: 160}
    for n <- 1..160, do: Membership.put_hint(config, "cell-#{n}", :a@node)

    Membership.put_hint(config, "cell-161", :a@node)

    assert :ets.info(config.hints, :size) == 160 - 10 + 1
    assert {:ok, :a@node} = Membership.hint(config, "cell-161")
  end

  test "the size never passes the limit" do
    t = table()
    for n <- 1..1_000, do: BoundedTable.put(t, 100, {n, :x})
    assert :ets.info(t, :size) <= 100
  end

  # Counts the `:ets.insert/2` calls that `fun` makes in this process.
  defp inserts(fun) do
    parent = self()

    tracer =
      spawn_link(fn ->
        receive do
          :stop -> send(parent, {:inserts, count_inserts(0)})
        end
      end)

    :erlang.trace_pattern({:ets, :insert, 2}, true, [:global])
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])
    fun.()
    :erlang.trace(self(), false, [:call])
    send(tracer, :stop)
    assert_receive {:inserts, n}
    n
  end

  defp count_inserts(n) do
    receive do
      {:trace, _, :call, {:ets, :insert, _}} -> count_inserts(n + 1)
    after
      50 -> n
    end
  end

  test "an entry that is already stored is not written again" do
    t = table()
    BoundedTable.put(t, 100, {"cell", :a@node})

    assert inserts(fn -> BoundedTable.put(t, 100, {"cell", :a@node}) end) == 0
    assert inserts(fn -> BoundedTable.put(t, 100, {"cell", :b@node}) end) == 1
  end
end
