defmodule Alambique.ForeignStoreClusterTest do
  @moduledoc """
  Two connected clusters run an instance with the same name over different
  stores. Nodes of one store must never take part in routing for the other:
  no cell of one store wakes in the other (design §8).
  """
  use ExUnit.Case, async: false

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster
  @moduletag timeout: 120_000

  setup do
    opts = [ttl_ms: 2_000, skew_margin_ms: 400, migrations: ClusterFuns.schema()]
    {:ok, ours} = Cluster.start_link([names: [:n1, :n2]] ++ opts)
    {:ok, foreign} = Cluster.start_link([names: [:f1]] ++ opts)

    for name <- Cluster.names(ours) do
      true = :erpc.call(Cluster.node(foreign, :f1), Node, :connect, [Cluster.node(ours, name)])
    end

    %{ours: ours, foreign: foreign, inst: Cluster.instance(ours)}
  end

  defp on(cluster, name, fun, args),
    do: with({:ok, result} <- Cluster.call(cluster, name, Alambique, fun, args), do: result)

  defp woken(cluster),
    do:
      for(
        %{event: [:alambique, :cell, :wake], meta: %{cell: cell}} <- Cluster.events(cluster),
        do: cell
      )

  test "no cell wakes in the store of the other cluster", ctx do
    %{ours: ours, foreign: foreign, inst: inst} = ctx

    cells =
      for i <- 1..40 do
        cell = "foreign-#{i}-#{System.unique_integer([:positive])}"
        assert {:ok, _} = on(ours, :n1, :execute, [inst, cell, "INSERT INTO log (n) VALUES (7)"])
        cell
      end

    for cell <- cells, name <- Cluster.names(ours) do
      # Correct rows or an explicit error, never a silent empty answer.
      case on(ours, name, :query, [inst, cell, "SELECT n FROM log"]) do
        {:ok, %{rows: rows}} -> assert rows == [[7]], "#{cell} read on #{name}: #{inspect(rows)}"
        {:error, _} -> :ok
      end
    end

    assert woken(foreign) == []
    assert cells -- woken(ours) == []
  end

  test "a stale hint to the other cluster is an explicit error and is dropped", ctx do
    %{ours: ours, foreign: foreign, inst: inst} = ctx
    cell = "hinted-#{System.unique_integer([:positive])}"
    assert {:ok, _} = on(ours, :n1, :execute, [inst, cell, "INSERT INTO log (n) VALUES (7)"])

    # The hint is read on a node that does not hold the cell.
    {:ok, owner} = on(ours, :n1, :owner, [inst, cell])
    caller = Enum.find(Cluster.names(ours), &(Cluster.node(ours, &1) != owner))
    {:ok, config} = Cluster.call(ours, caller, Alambique.Config, :fetch!, [inst])
    stranger = Cluster.node(foreign, :f1)

    assert {:ok, true} =
             Cluster.call(ours, caller, Alambique.Membership, :put_hint, [config, cell, stranger])

    assert {:error, {:foreign_store, ^stranger}} =
             on(ours, caller, :query, [inst, cell, "SELECT n FROM log"])

    assert {:ok, %{rows: [[7]]}} = on(ours, caller, :query, [inst, cell, "SELECT n FROM log"])
    refute cell in woken(foreign)
  end
end
