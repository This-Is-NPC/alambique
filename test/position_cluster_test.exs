defmodule Alambique.PositionClusterTest do
  @moduledoc "position/3 and isolated writes routed between peer nodes (design §8, §13)."
  use ExUnit.Case, async: false

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster
  @moduletag timeout: 120_000

  setup do
    {:ok, cluster} = Cluster.start_link(migrations: ClusterFuns.schema())

    %{
      c: cluster,
      inst: Cluster.instance(cluster),
      cell: "cell-#{System.unique_integer([:positive])}"
    }
  end

  defp on(c, name, mod, fun, args),
    do: with({:ok, result} <- Cluster.call(c, name, mod, fun, args), do: result)

  defp insert(c, name, inst, cell, n, opts \\ []),
    do:
      on(c, name, Alambique, :execute, [inst, cell, "INSERT INTO log (n) VALUES (?)", [n], opts])

  test "every node gets the same cut as the owner, awake or asleep", %{
    c: c,
    inst: inst,
    cell: cell
  } do
    assert {:ok, _} = insert(c, :n1, inst, cell, 1)
    assert {:ok, _} = insert(c, :n2, inst, cell, 2, isolated: true)

    cuts =
      for name <- Cluster.names(c),
          do: on(c, name, Alambique, :position, [inst, cell])

    assert [{:ok, %{awake: true, txid: txid}} | rest] = cuts
    assert Enum.all?(rest, &match?({:ok, %{awake: true, txid: ^txid}}, &1))

    {:ok, owner} = on(c, :n1, Alambique, :owner, [inst, cell])
    owner_name = Enum.find(Cluster.names(c), &(Cluster.node(c, &1) == owner))
    assert :ok = on(c, owner_name, Alambique, :hibernate, [inst, cell])

    for name <- Cluster.names(c) do
      assert {:ok, %{awake: false, txid: ^txid}} = on(c, name, Alambique, :position, [inst, cell])
    end
  end

  test "the data TXID survives a change of owner", %{c: c, inst: inst, cell: cell} do
    assert {:ok, _} = insert(c, :n1, inst, cell, 1)
    assert {:ok, %{data: data, txid: txid}} = on(c, :n1, Alambique, :position, [inst, cell])
    assert data == txid

    {:ok, owner} = on(c, :n1, Alambique, :owner, [inst, cell])
    other = Enum.find(Cluster.names(c), &(Cluster.node(c, &1) != owner))
    target = Cluster.node(c, other)

    assert {:ok, ^target} = on(c, :n1, Alambique, :move, [inst, cell, target])
    assert {:ok, %{data: ^data, txid: moved, awake: true}} = position_on(c, :n1, inst, cell)
    assert moved > txid

    assert {:ok, _} = insert(c, :n2, inst, cell, 2)
    assert {:ok, %{data: written, txid: written}} = position_on(c, :n1, inst, cell)
    assert written > moved
  end

  defp position_on(c, name, inst, cell), do: on(c, name, Alambique, :position, [inst, cell])
end
