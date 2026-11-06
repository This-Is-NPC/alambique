defmodule Alambique.OpsClusterTest do
  @moduledoc "The mix tasks that talk to a running cluster of peer nodes (design §10)."
  use ExUnit.Case, async: false

  @moduletag :cluster

  import Alambique.CellHelpers, only: [eventually: 1]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  setup do
    {:ok, cluster} =
      Cluster.start_link(
        names: [:n1, :n2],
        ttl_ms: 2_000,
        skew_margin_ms: 400,
        migrations: ClusterFuns.schema()
      )

    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    {_, config} = Cluster.store(cluster)
    old = Application.get_env(:alambique, :ops)

    Application.put_env(:alambique, :ops,
      store: {AlambiqueTestkit.MemoryStore, config},
      prefix: "cluster"
    )

    on_exit(fn ->
      Mix.shell(shell)

      if old,
        do: Application.put_env(:alambique, :ops, old),
        else: Application.delete_env(:alambique, :ops)
    end)

    %{cluster: cluster, inst: Cluster.instance(cluster)}
  end

  defp on(cluster, name, fun, args),
    do: with({:ok, result} <- Cluster.call(cluster, name, Alambique, fun, args), do: result)

  defp cookie, do: Atom.to_string(Node.get_cookie())

  defp output do
    receive do
      {:mix_shell, :info, [line]} -> [line | output()]
    after
      0 -> []
    end
  end

  test "move hands a cell to another node", %{cluster: c, inst: inst} do
    cell = "move-#{System.unique_integer([:positive])}"
    assert {:ok, _} = on(c, :n1, :execute, [inst, cell, "INSERT INTO log (n) VALUES (1)"])
    {:ok, owner} = on(c, :n1, :owner, [inst, cell])
    other = Enum.find([Cluster.node(c, :n1), Cluster.node(c, :n2)], &(&1 != owner))

    Mix.Task.rerun("alambique.move", [
      cell,
      Atom.to_string(other),
      "--instance",
      Atom.to_string(inst),
      "--cookie",
      cookie()
    ])

    assert ["Moved " <> _] = output()
    assert {:ok, ^other} = on(c, :n1, :owner, [inst, cell])
    assert {:ok, %{rows: [[1]]}} = on(c, :n2, :query, [inst, cell, "SELECT n FROM log"])
  end

  test "freeze with --connect hibernates the cell now", %{cluster: c, inst: inst} do
    cell = "freeze-#{System.unique_integer([:positive])}"
    assert {:ok, _} = on(c, :n1, :execute, [inst, cell, "INSERT INTO log (n) VALUES (1)"])
    {:ok, owner} = on(c, :n1, :owner, [inst, cell])

    Mix.Task.rerun("alambique.freeze", [
      cell,
      "--instance",
      Atom.to_string(inst),
      "--connect",
      Atom.to_string(owner),
      "--cookie",
      cookie()
    ])

    assert Enum.any?(output(), &(&1 =~ "is asleep"))
    eventually(fn -> on(c, :n1, :owner, [inst, cell]) == :none end)
    assert {:error, :frozen} = on(c, :n2, :query, [inst, cell, "SELECT n FROM log"])
  end

  test "move to a node that does not run the instance is an error", %{inst: inst} do
    assert_raise Mix.Error, ~r/could not (move|connect)/, fn ->
      Mix.Task.rerun("alambique.move", [
        "c1",
        "ghost@127.0.0.1",
        "--instance",
        Atom.to_string(inst),
        "--cookie",
        cookie()
      ])
    end
  end
end
