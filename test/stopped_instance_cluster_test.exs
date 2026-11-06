defmodule Alambique.StoppedInstanceClusterTest do
  @moduledoc """
  A node whose instance stopped while its VM is up refuses forwarded calls as
  a draining node does (design §8): the call did not run.
  """
  use ExUnit.Case, async: false

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster
  @moduletag timeout: 120_000

  setup do
    {:ok, cluster} = Cluster.start_link(migrations: ClusterFuns.schema())
    %{cluster: cluster, inst: Cluster.instance(cluster)}
  end

  defp on(cluster, name, module, fun, args),
    do: with({:ok, result} <- Cluster.call(cluster, name, module, fun, args), do: result)

  # A cell id whose preferred node is `name`.
  defp cell_for(c, name, inst) do
    node = Cluster.node(c, name)
    config = on(c, :n1, Alambique.Config, :fetch!, [inst])

    Enum.find_value(1..1_000, fn i ->
      cell = "cell-#{i}-#{System.unique_integer([:positive])}"
      on(c, :n1, Alambique.Membership, :preferred, [config, cell]) == node && cell
    end)
  end

  test "a direct remote call answers draining", %{cluster: c, inst: inst} do
    cell = cell_for(c, :n2, inst)
    on(c, :n2, Alambique.Config, :delete, [inst])

    assert {nil, {:error, :draining}} =
             on(c, :n1, :erpc, :call, [
               Cluster.node(c, :n2),
               Alambique.Router,
               :remote,
               [inst, cell, {:query, "SELECT 1", []}, 5_000, 1]
             ])
  end

  test "a routed call to a stopped instance is refused, never forward_failed", %{
    cluster: c,
    inst: inst
  } do
    cell = cell_for(c, :n2, inst)
    on(c, :n2, Alambique.Config, :delete, [inst])

    for {fun, sql} <- [execute: "CREATE TABLE IF NOT EXISTS t (n)", query: "SELECT 1"] do
      case on(c, :n1, Alambique, fun, [inst, cell, sql]) do
        {:error, :draining} -> :ok
        {:ok, _} -> :ok
        other -> flunk("#{fun} answered #{inspect(other)}")
      end
    end
  end
end
