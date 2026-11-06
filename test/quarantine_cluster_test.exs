defmodule Alambique.QuarantineClusterTest do
  @moduledoc "A cell that crashes the VM is frozen after a few crashes (design §10)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster
  @moduletag timeout: 120_000

  setup do
    {:ok, cluster} =
      Cluster.start_link(
        ttl_ms: 2_000,
        skew_margin_ms: 400,
        migrations: ClusterFuns.schema(),
        instance_opts: [quarantine: [threshold: 3, window_ms: 600_000]]
      )

    %{c: cluster, inst: Cluster.instance(cluster)}
  end

  defp on(c, name, fun, args), do: on(c, name, Alambique, fun, args)

  defp on(c, name, module, fun, args),
    do: with({:ok, result} <- Cluster.call(c, name, module, fun, args), do: result)

  defp events(c, kind, cell),
    do: for(%{event: ^kind, meta: %{cell: ^cell}} = e <- Cluster.events(c), do: e)

  defp insert(c, name, inst, cell, n),
    do: on(c, name, :execute, [inst, cell, "INSERT INTO log (n) VALUES (?)", [n]])

  # The VM of `name` dies while the cell runs the poison transaction.
  defp crash(c, name, inst, cell) do
    assert {:ok, _} = on(c, name, :take_over, [inst, cell, :poison_round])
    _ = Cluster.call(c, name, Alambique, :transaction, [inst, cell, ClusterFuns.poison()])
    Cluster.kill(c, name)
    :ok = Cluster.restart(c, name)
  end

  test "the third crash of a cell, on three nodes, freezes it", %{c: c, inst: inst} do
    poison = "poison-#{System.unique_integer([:positive])}"
    healthy = "healthy-#{System.unique_integer([:positive])}"
    assert {:ok, _} = insert(c, :n1, inst, poison, 1)
    assert {:ok, _} = insert(c, :n1, inst, healthy, 1)

    for {name, round} <- Enum.with_index([:n1, :n2, :n3], 1) do
      crash(c, name, inst, poison)

      eventually(
        fn -> length(events(c, [:alambique, :cell, :crash_recorded], poison)) == round end,
        5_000
      )

      assert {:ok, %{total: ^round}} = on(c, :n1, :crashes, [inst, poison])
      assert events(c, [:alambique, :cell, :quarantined], poison) |> length() == div(round, 3)
    end

    # Each node recorded the crash it had, so three nodes, one crash each.
    nodes = for %{node: node} <- events(c, [:alambique, :cell, :crash_recorded], poison), do: node
    assert Enum.sort(nodes) == [:n1, :n2, :n3]

    assert [%{meta: %{threshold: 3}, measurements: %{count: 3}, node: :n3}] =
             events(c, [:alambique, :cell, :quarantined], poison)

    assert {:ok, true} = on(c, :n1, :frozen?, [inst, poison])

    # The lease of the last crashed owner has to expire first.
    for name <- Cluster.names(c) do
      eventually(fn -> on(c, name, :touch, [inst, poison]) == {:error, :frozen} end, 8_000)
      assert {:error, :frozen} = insert(c, name, inst, poison, 2)
    end

    # The other cell never noticed.
    for name <- Cluster.names(c), do: assert({:ok, _} = insert(c, name, inst, healthy, 2))

    # An operator looks, then lets it wake: its data is intact.
    assert :ok = on(c, :n1, :unfreeze, [inst, poison])
    assert {:ok, nil} = on(c, :n1, :crashes, [inst, poison])
    assert {:ok, %{rows: [[1]]}} = on(c, :n2, :query, [inst, poison, "SELECT n FROM log", []])
  end

  test "a node that dies while its cells are idle blames none of them", %{c: c, inst: inst} do
    cells = for n <- 1..4, do: "idle-#{System.unique_integer([:positive])}-#{n}"
    for cell <- cells, do: assert({:ok, _} = insert(c, :n1, inst, cell, 1))

    for _ <- 1..3 do
      # A cell answers its caller before its flush ends; kill between callbacks.
      eventually(fn -> on(c, :n1, ClusterFuns, :busy_markers, [inst]) == 0 end, 5_000)
      Cluster.kill(c, :n1)
      :ok = Cluster.restart(c, :n1)
    end

    for cell <- cells do
      assert {:ok, nil} = on(c, :n2, :crashes, [inst, cell])
      assert events(c, [:alambique, :cell, :crash_recorded], cell) == []
      assert {:ok, false} = on(c, :n2, :frozen?, [inst, cell])
    end
  end
end
