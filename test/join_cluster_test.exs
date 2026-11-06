defmodule Alambique.JoinClusterTest do
  @moduledoc """
  A node started with `join: false` takes no part in routing until
  `Alambique.join/1`, yet serves the calls made on it (design §8).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster
  @moduletag timeout: 120_000

  setup do
    {:ok, cluster} =
      Cluster.start_link(
        names: [:n1, :n2, :n3],
        not_joined: [:n3],
        ttl_ms: 2_000,
        skew_margin_ms: 400,
        migrations: ClusterFuns.schema()
      )

    %{cluster: cluster, inst: Cluster.instance(cluster)}
  end

  defp on(cluster, name, fun, args),
    do: with({:ok, result} <- Cluster.call(cluster, name, Alambique, fun, args), do: result)

  defp cells(prefix, count),
    do: for(_ <- 1..count, do: "#{prefix}-#{System.unique_integer([:positive])}")

  defp woken_on(cluster, name),
    do:
      for(
        %{event: [:alambique, :cell, :wake], node: ^name, meta: %{cell: cell}} <-
          Cluster.events(cluster),
        do: cell
      )

  defp insert(c, from, inst, cell),
    do: on(c, from, :execute, [inst, cell, "INSERT INTO log (n) VALUES (7)"])

  test "peers never choose a node that has not joined, and it serves its own calls", ctx do
    %{cluster: c, inst: inst} = ctx
    members = for name <- [:n1, :n2], do: Cluster.node(c, name)
    cells = cells("peer", 60)

    # Calls from random peers, then from the node that has not joined.
    for cell <- cells do
      from = Enum.random([:n1, :n2, :n3])
      assert {:ok, _} = insert(c, from, inst, cell)
    end

    for cell <- cells, from <- [:n1, :n2, :n3] do
      assert {:ok, %{rows: [[7]]}} = on(c, from, :query, [inst, cell, "SELECT n FROM log"])
      assert {:ok, owner} = on(c, from, :owner, [inst, cell])
      assert owner in members
    end

    assert woken_on(c, :n3) == []
    assert Enum.sort(woken_on(c, :n1) ++ woken_on(c, :n2)) == Enum.sort(cells)
  end

  test "after join/1 peers route cells to the node", ctx do
    %{cluster: c, inst: inst} = ctx
    n3 = Cluster.node(c, :n3)

    assert :ok = on(c, :n3, :join, [inst])
    assert :ok = on(c, :n3, :join, [inst])

    eventually(
      fn ->
        {:ok, config} = Cluster.call(c, :n1, Alambique.Config, :fetch!, [inst])
        {:ok, nodes} = Cluster.call(c, :n1, Alambique.Membership, :nodes, [config])
        n3 in nodes
      end,
      10_000
    )

    cells = cells("joined", 60)
    for cell <- cells, do: assert({:ok, _} = insert(c, Enum.random([:n1, :n2]), inst, cell))

    assert woken_on(c, :n3) != []
    assert Enum.all?(woken_on(c, :n3), &(&1 in cells))
  end
end
