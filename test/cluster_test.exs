defmodule Alambique.ClusterTest do
  @moduledoc "Routing, forwarding and moving across peer nodes (design §8)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  setup do
    {:ok, cluster} =
      Cluster.start_link(
        ttl_ms: 2_000,
        skew_margin_ms: 400,
        migrations: ClusterFuns.schema(),
        instance_opts: [hint_limit: 5]
      )

    # A node's view follows the groups a moment after they change.
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

  defp on(cluster, name, fun, args),
    do: with({:ok, result} <- Cluster.call(cluster, name, Alambique, fun, args), do: result)

  defp cell, do: "cell-#{System.unique_integer([:positive])}"

  test "a call on any node reaches the one node that owns the cell", %{cluster: c, inst: inst} do
    cell = cell()

    assert {:ok, {3, owner}} = on(c, :n1, :transaction, [inst, cell, ClusterFuns.append(1)])

    for name <- Cluster.names(c) do
      assert {:ok, %{rows: [[1]]}} = on(c, name, :query, [inst, cell, "SELECT n FROM log"])
      assert {:ok, ^owner} = on(c, name, :owner, [inst, cell])
    end

    wakes =
      for %{event: [:alambique, :cell, :wake], meta: %{cell: ^cell}} = e <- Cluster.events(c),
          do: e.node

    assert [_] = wakes
    assert Cluster.node(c, hd(wakes)) == owner
  end

  test "the cell wakes on its preferred node", %{cluster: c, inst: inst} do
    for _ <- 1..6 do
      cell = cell()
      assert {:ok, _} = on(c, :n2, :execute, [inst, cell, "INSERT INTO log (n) VALUES (1)"])

      {:ok, preferred} =
        Cluster.call(c, :n3, Alambique.Membership, :preferred, [config(c, :n3), cell])

      assert {:ok, ^preferred} = on(c, :n1, :owner, [inst, cell])
    end
  end

  defp config(c, name) do
    {:ok, config} = Cluster.call(c, name, Alambique.Config, :fetch!, [Cluster.instance(c)])
    config
  end

  test "moving a cell hibernates it on the owner and wakes it on the target", %{
    cluster: c,
    inst: inst
  } do
    cell = cell()
    assert {:ok, _} = on(c, :n1, :execute, [inst, cell, "INSERT INTO log (n) VALUES (1)"])
    {:ok, owner} = on(c, :n1, :owner, [inst, cell])

    [target | _] =
      for name <- Cluster.names(c), Cluster.node(c, name) != owner, do: Cluster.node(c, name)

    assert {:ok, ^target} = on(c, :n1, :move, [inst, cell, target])
    assert {:ok, ^target} = on(c, :n2, :owner, [inst, cell])
    assert {:ok, %{rows: [[1]]}} = on(c, :n3, :query, [inst, cell, "SELECT n FROM log"])
    assert {:ok, ^target} = on(c, :n1, :move, [inst, cell, target])
    assert {:error, :unknown_node} = on(c, :n1, :move, [inst, cell, :nobody@nowhere])
  end

  test "the owner-hint cache stays bounded", %{cluster: c, inst: inst} do
    for _ <- 1..30, do: assert({:ok, _} = on(c, :n1, :query, [inst, cell(), "SELECT 1"]))
    {:ok, size} = Cluster.call(c, :n1, :ets, :info, [Module.concat(inst, Hints), :size])
    assert size <= 5
  end

  test "the cell is served again from another node after its owner dies", %{
    cluster: c,
    inst: inst
  } do
    cell = cell()
    assert {:ok, _} = on(c, :n1, :execute, [inst, cell, "INSERT INTO log (n) VALUES (1)"])
    {:ok, owner} = on(c, :n1, :owner, [inst, cell])
    dead = Enum.find(Cluster.names(c), &(Cluster.node(c, &1) == owner))
    survivor = Enum.find(Cluster.names(c), &(&1 != dead))
    Cluster.kill(c, dead)

    # The lease has to run out first (ttl 2000 ms plus the skew margin).
    eventually(
      fn ->
        match?(
          {:ok, _},
          on(c, survivor, :execute, [inst, cell, "INSERT INTO log (n) VALUES (2)"])
        )
      end,
      10_000
    )

    assert {:ok, %{rows: [[1], [2]]}} =
             on(c, survivor, :query, [inst, cell, "SELECT n FROM log ORDER BY id"])

    assert {:ok, new_owner} = on(c, survivor, :owner, [inst, cell])
    assert new_owner != owner
  end

  # A node that is not the preferred one for `cell`, and the preferred one.
  defp other_than_preferred(c, cell) do
    {:ok, preferred} =
      Cluster.call(c, :n1, Alambique.Membership, :preferred, [config(c, :n1), cell])

    name = Enum.find(Cluster.names(c), &(Cluster.node(c, &1) != preferred))
    {name, preferred}
  end

  @tag :cluster
  test "touch from a node that is not the preferred one wakes the cell on the preferred one", %{
    cluster: c,
    inst: inst
  } do
    cell = cell()
    {from, preferred} = other_than_preferred(c, cell)

    assert {:ok, ^preferred} = on(c, from, :touch, [inst, cell])
    assert {:ok, ^preferred} = on(c, :n1, :owner, [inst, cell])

    assert {:ok, %{cells: [^cell]}} =
             Cluster.call(c, name_of(c, preferred), Alambique, :cells, [inst])
  end

  @tag :cluster
  test "touch returns the owner of a cell that is awake elsewhere", %{cluster: c, inst: inst} do
    cell = cell()
    {from, preferred} = other_than_preferred(c, cell)

    [target | _] =
      for n <- Cluster.names(c), Cluster.node(c, n) not in [preferred], do: Cluster.node(c, n)

    assert {:ok, ^preferred} = on(c, from, :touch, [inst, cell])
    assert {:ok, ^target} = on(c, :n1, :move, [inst, cell, target])

    for name <- Cluster.names(c), do: assert({:ok, ^target} = on(c, name, :touch, [inst, cell]))
  end

  @tag :cluster
  test "a lease answer fills the hint, which then answers without the store", %{
    cluster: c,
    inst: inst
  } do
    cell = cell()
    {from, preferred} = other_than_preferred(c, cell)
    owner = name_of(c, preferred)
    assert {:ok, ^preferred} = on(c, owner, :touch, [inst, cell])

    hint = fn -> Cluster.call(c, from, Alambique.Membership, :hint, [config(c, from), cell]) end
    assert {:ok, :error} = hint.()
    assert {:ok, ^preferred} = on(c, from, :owner, [inst, cell, [hint: true]])
    assert {:ok, {:ok, ^preferred}} = hint.()
    assert {:ok, ^preferred} = on(c, from, :owner, [inst, cell, [hint: true]])
  end

  defp name_of(c, node), do: Enum.find(Cluster.names(c), &(Cluster.node(c, &1) == node))
end
