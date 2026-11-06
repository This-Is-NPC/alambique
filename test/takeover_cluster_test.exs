defmodule Alambique.TakeoverClusterTest do
  @moduledoc "Early takeover and drain across peer nodes (design §5, §8)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster
  @moduletag timeout: 120_000

  # A lease long enough to tell "served at once" from "served after expiry".
  @ttl_ms 8_000

  setup do
    {:ok, cluster} =
      Cluster.start_link(ttl_ms: @ttl_ms, skew_margin_ms: 400, migrations: ClusterFuns.schema())

    %{
      c: cluster,
      inst: Cluster.instance(cluster),
      cell: "cell-#{System.unique_integer([:positive])}"
    }
  end

  defp on(c, name, fun, args),
    do: with({:ok, result} <- Cluster.call(c, name, Alambique, fun, args), do: result)

  defp insert(c, name, inst, cell, n),
    do: on(c, name, :execute, [inst, cell, "INSERT INTO log (n) VALUES (?)", [n]])

  defp rows(c, name, inst, cell, opts \\ []) do
    with {:ok, %{rows: rows}} <-
           on(c, name, :query, [inst, cell, "SELECT n FROM log ORDER BY id", [], opts]),
         do: List.flatten(rows)
  end

  defp name_of(c, node), do: Enum.find(Cluster.names(c), &(Cluster.node(c, &1) == node))

  # The node that owns `cell` after a write, and two other peers.
  defp owner_and_others(c, inst, cell) do
    assert {:ok, _} = insert(c, :n1, inst, cell, 1)
    {:ok, owner} = on(c, :n1, :owner, [inst, cell])
    owner_name = name_of(c, owner)
    {owner_name, Cluster.names(c) -- [owner_name]}
  end

  defp events(c, kind, cell),
    do: for(%{event: ^kind, meta: %{cell: ^cell}} = e <- Cluster.events(c), do: e)

  test "takes a cell from a killed owner before its lease expires", %{
    c: c,
    inst: inst,
    cell: cell
  } do
    {owner, [taker, other]} = owner_and_others(c, inst, cell)
    assert {:ok, _} = insert(c, taker, inst, cell, 2)
    Cluster.kill(c, owner)

    # Without a takeover the cell is unavailable until the lease expires.
    assert {:error, {:unavailable, _, _}} = on(c, other, :query, [inst, cell, "SELECT 1", []])

    started = System.monotonic_time(:millisecond)
    taker_node = Cluster.node(c, taker)
    assert {:ok, ^taker_node} = on(c, taker, :take_over, [inst, cell, :node_down])
    assert System.monotonic_time(:millisecond) - started < div(@ttl_ms, 2)

    assert rows(c, other, inst, cell) == [1, 2]
    assert {:ok, _} = insert(c, other, inst, cell, 3)
    assert rows(c, taker, inst, cell, verify_lease: true) == [1, 2, 3]

    assert [%{meta: %{result: :ok, previous_epoch: 1, epoch: 2, cause: :node_down}}] =
             events(c, [:alambique, :lease, :take_over], cell)
  end

  test "takes a cell from a live owner: it stops soon and routing reaches the new owner", %{
    c: c,
    inst: inst,
    cell: cell
  } do
    {owner, [taker, other]} = owner_and_others(c, inst, cell)
    taker_node = Cluster.node(c, taker)

    assert {:ok, ^taker_node} = on(c, taker, :take_over, [inst, cell, :suspected])
    assert {:ok, _} = insert(c, taker, inst, cell, 2)

    # The previous owner is told, so it stops long before its renewal (a third of the TTL).
    eventually(
      fn -> Enum.any?(events(c, [:alambique, :cell, :stop], cell), &(&1.node == owner)) end,
      div(@ttl_ms, 6)
    )

    [%{meta: %{reason: reason}}] = events(c, [:alambique, :cell, :stop], cell)
    assert reason == :lease_lost

    # Every node reaches the new owner, whichever node was asked first.
    for name <- [owner, taker, other] do
      assert {:ok, ^taker_node} = on(c, name, :owner, [inst, cell])
      assert {:ok, _} = insert(c, name, inst, cell, 10 + length(Cluster.events(c)))
    end

    assert 1 in rows(c, owner, inst, cell, verify_lease: true)
    assert 2 in rows(c, other, inst, cell)
  end

  test "drain hands the cells of a node over at once", %{c: c, inst: inst} do
    cells = for n <- 1..8, do: "drain-#{System.unique_integer([:positive])}-#{n}"
    for cell <- cells, do: assert({:ok, _} = insert(c, :n1, inst, cell, 1))

    owners = Map.new(cells, fn cell -> {cell, on(c, :n1, :owner, [inst, cell])} end)
    {:ok, victim_node} = owners[hd(cells)]
    victim = name_of(c, victim_node)
    mine = for {cell, {:ok, ^victim_node}} <- owners, do: cell
    survivor = hd(Cluster.names(c) -- [victim])

    assert {:ok, %{hibernated: hibernated, errors: []}} = on(c, victim, :drain, [inst])
    assert hibernated == length(mine)
    assert %{cells: [], count: 0} = on(c, victim, :cells, [inst])

    # Served by the others now, without waiting for any lease to expire.
    started = System.monotonic_time(:millisecond)

    for cell <- cells do
      assert rows(c, survivor, inst, cell) == [1]
      assert {:ok, node} = on(c, survivor, :owner, [inst, cell])
      assert node != victim_node
    end

    assert System.monotonic_time(:millisecond) - started < div(@ttl_ms, 2)

    # The drained node forwards what it is asked to the remaining members.
    fresh = "fresh-#{System.unique_integer([:positive])}"
    assert {:ok, served_by} = on(c, victim, :touch, [inst, fresh])
    assert served_by != victim_node
    assert {:ok, _} = insert(c, victim, inst, fresh, 5)
  end
end
