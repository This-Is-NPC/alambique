defmodule Alambique.CellOptionsRefreshClusterTest do
  @moduledoc "`Alambique.refresh_options/3` across peer nodes: routed to the owner, never waking a cell (design §7, §8)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster

  setup do
    {:ok, cluster} =
      Cluster.start_link(
        ttl_ms: 2_000,
        skew_margin_ms: 400,
        migrations: ClusterFuns.schema(),
        instance_opts: [
          cell_options: {ClusterFuns, :cell_options},
          awake: [cache_kib: 512]
        ]
      )

    eventually(
      fn ->
        Enum.all?(Cluster.names(cluster), fn name ->
          {:ok, config} =
            Cluster.call(cluster, name, Alambique.Config, :fetch!, [Cluster.instance(cluster)])

          {:ok, nodes} = Cluster.call(cluster, name, Alambique.Membership, :nodes, [config])
          length(nodes) == 3
        end)
      end,
      5_000
    )

    %{cluster: cluster, inst: Cluster.instance(cluster)}
  end

  defp on(cluster, name, module, fun, args),
    do: with({:ok, result} <- Cluster.call(cluster, name, module, fun, args), do: result)

  defp cell, do: "cell-#{System.unique_integer([:positive])}"

  test "a refresh from a peer changes the cache of the cell on its owner", %{
    cluster: c,
    inst: inst
  } do
    cell = cell()

    assert {:ok, {_, owner}} =
             on(c, :n1, Alambique, :transaction, [inst, cell, ClusterFuns.append(1)])

    owner_name = Enum.find(Cluster.names(c), &(Cluster.node(c, &1) == owner))
    caller = Enum.find(Cluster.names(c), &(&1 != owner_name))
    assert 512 == -on(c, owner_name, ClusterFuns, :cache_size, [inst, cell])

    for name <- Cluster.names(c),
        do: on(c, name, ClusterFuns, :put_options, [[awake: [cache_kib: 128]]])

    assert {:ok, :override, changed: [:cache_kib]} =
             on(c, caller, Alambique, :refresh_options, [inst, cell])

    assert on(c, owner_name, ClusterFuns, :cache_size, [inst, cell]) == -128
  end

  test "a cell nobody holds is not woken by a peer's refresh", %{cluster: c, inst: inst} do
    asleep = cell()

    for name <- Cluster.names(c) do
      assert {:ok, :asleep} = on(c, name, Alambique, :refresh_options, [inst, asleep])
    end

    woken =
      for %{event: [:alambique, :cell, :wake], meta: %{cell: ^asleep}} <- Cluster.events(c), do: 1

    assert woken == []
  end
end
