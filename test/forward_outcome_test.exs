defmodule Alambique.ForwardOutcomeTest do
  @moduledoc """
  A forwarded call whose connection fails after it was sent (design §8). The
  target serves the call and then drops its links, so the caller sees
  `noconnection` for a call that ran.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.{Cluster, CrashPoints}

  @moduletag :cluster
  @moduletag timeout: 120_000
  @insert "INSERT INTO log (n) VALUES (1)"

  setup do
    {:ok, cluster} =
      Cluster.start_link(ttl_ms: 2_000, skew_margin_ms: 400, migrations: ClusterFuns.schema())

    %{cluster: cluster, inst: Cluster.instance(cluster)}
  end

  defp on(cluster, name, module, fun, args),
    do: with({:ok, result} <- Cluster.call(cluster, name, module, fun, args), do: result)

  defp config(c, name) do
    {:ok, config} = Cluster.call(c, name, Alambique.Config, :fetch!, [Cluster.instance(c)])
    config
  end

  # A cell id whose preferred node is `name`.
  defp cell_for(c, name) do
    node = Cluster.node(c, name)

    Enum.find_value(1..1_000, fn i ->
      cell = "cell-#{i}-#{System.unique_integer([:positive])}"
      on(c, :n1, Alambique.Membership, :preferred, [config(c, :n1), cell]) == node && cell
    end)
  end

  defp hint(c, at, cell, name),
    do: on(c, at, Alambique.Membership, :put_hint, [config(c, at), cell, Cluster.node(c, name)])

  # The sender notices a lost connection late, when the peer is reachable again.
  defp late_notice(c) do
    for name <- Cluster.names(c) do
      {:ok, :ok} =
        Cluster.call(c, name, CrashPoints, :arm, [
          :router_forward_lost,
          ClusterFuns.late_notice(300),
          [times: :infinity]
        ])
    end
  end

  # The next served call of `kinds` on `name` ends by dropping its links.
  defp drop_after_serve(c, name, kinds) do
    late_notice(c)

    {:ok, :ok} =
      Cluster.call(c, name, CrashPoints, :arm, [
        :router_after_serve,
        ClusterFuns.drop_links([node()]),
        [times: 1, when: ClusterFuns.serving(kinds)]
      ])
  end

  defp rows(c, inst, cell) do
    eventually(
      fn ->
        Enum.find_value([:n1, :n2, :n3], fn name ->
          case on(c, name, Alambique, :query, [inst, cell, "SELECT n FROM log"]) do
            {:ok, %{rows: rows}} -> List.flatten(rows)
            _ -> nil
          end
        end)
      end,
      10_000
    )
  end

  test "a write lost after the first hop is not run again", %{cluster: c, inst: inst} do
    cell = cell_for(c, :n2)
    drop_after_serve(c, :n2, [:execute])

    reply = on(c, :n1, Alambique, :execute, [inst, cell, @insert])
    assert rows(c, inst, cell) == [1]
    assert reply == {:error, :outcome_unknown}
  end

  test "a write lost after a free lease sent it to the preferred node is not run again", %{
    cluster: c,
    inst: inst
  } do
    cell = cell_for(c, :n3)
    hint(c, :n1, cell, :n2)
    drop_after_serve(c, :n3, [:execute])

    reply = on(c, :n1, Alambique, :execute, [inst, cell, @insert])
    assert rows(c, inst, cell) == [1]
    assert reply == {:error, :outcome_unknown}
  end

  test "a write lost after it was sent to a live owner is not run again", %{
    cluster: c,
    inst: inst
  } do
    cell = cell_for(c, :n3)

    assert {:ok, _} =
             on(c, :n3, Alambique, :execute, [inst, cell, "INSERT INTO log (n) VALUES (0)"])

    hint(c, :n1, cell, :n2)
    drop_after_serve(c, :n3, [:execute])

    reply = on(c, :n1, Alambique, :execute, [inst, cell, @insert])
    assert rows(c, inst, cell) == [0, 1]
    assert reply == {:error, :outcome_unknown}
  end

  test "a transaction lost after it was sent is not run again", %{cluster: c, inst: inst} do
    cell = cell_for(c, :n2)
    drop_after_serve(c, :n2, [:transaction])

    reply = on(c, :n1, Alambique, :transaction, [inst, cell, ClusterFuns.append(1)])
    assert rows(c, inst, cell) == [1]
    assert reply == {:error, :outcome_unknown}
  end

  test "a read lost after it was sent is routed again", %{cluster: c, inst: inst} do
    cell = cell_for(c, :n2)
    assert {:ok, _} = on(c, :n2, Alambique, :execute, [inst, cell, @insert])
    drop_after_serve(c, :n2, [:query])

    assert {:ok, %{rows: [[1]]}} =
             on(c, :n1, Alambique, :query, [inst, cell, "SELECT n FROM log"])
  end

  test "an idempotent write lost after it was sent is routed again", %{cluster: c, inst: inst} do
    cell = cell_for(c, :n2)
    drop_after_serve(c, :n2, [:execute])
    sql = "INSERT INTO log (id, n) VALUES (1, 1) ON CONFLICT DO NOTHING"

    assert {:ok, _} = on(c, :n1, Alambique, :execute, [inst, cell, sql, [], [idempotent: true]])
    assert rows(c, inst, cell) == [1]
  end

  test "a call that was not sent because the node cannot be reached is routed again", %{
    cluster: c,
    inst: inst
  } do
    cell = cell_for(c, :n2)
    :ok = Cluster.partition_nodes(c, :n1, :n2)

    assert {:ok, _} = on(c, :n1, Alambique, :execute, [inst, cell, @insert])
    assert rows(c, inst, cell) == [1]
  end
end
