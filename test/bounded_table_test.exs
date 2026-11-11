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

  # The table belongs to another process and is protected: this process can
  # read it but any write raises, so the check needs no timing.
  defp read_only_table(object) do
    parent = self()

    owner =
      start_supervised!(
        {Task,
         fn ->
           t = :ets.new(:bounded, [:protected, :set])
           :ets.insert(t, object)
           send(parent, {:table, t})
           Process.sleep(:infinity)
         end}
      )

    assert_receive {:table, t}
    {t, owner}
  end

  test "an entry that is already stored is not written again" do
    {t, _owner} = read_only_table({"cell", :a@node})

    assert BoundedTable.put(t, 100, {"cell", :a@node})
    assert_raise ArgumentError, fn -> BoundedTable.put(t, 100, {"cell", :b@node}) end
  end
end
