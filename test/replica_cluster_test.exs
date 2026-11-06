defmodule Alambique.ReplicaClusterTest do
  @moduledoc """
  Read replicas on peer nodes (design §21): the owner answers the reads that
  have no replica yet, and a replica goes on answering when the owner is gone.
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
        names: [:n1, :n2],
        migrations: ClusterFuns.schema(),
        instance_opts: [replicas: [poll_ms: 50, marker_ttl_ms: 200]]
      )

    %{
      c: cluster,
      inst: Cluster.instance(cluster),
      cell: "cell-#{System.unique_integer([:positive])}"
    }
  end

  defp on(c, name, mod, fun, args),
    do: with({:ok, result} <- Cluster.call(c, name, mod, fun, args), do: result)

  defp read(c, name, inst, cell, ms) do
    on(c, name, Alambique, :query, [
      inst,
      cell,
      "SELECT n FROM log ORDER BY n",
      [],
      [consistency: {:bounded, ms}, timeout: 5_000]
    ])
  end

  defp rows({:ok, %{rows: rows}}), do: List.flatten(rows)

  defp files(c, name, inst) do
    config = on(c, name, Alambique.Config, :fetch!, [inst])
    dir = on(c, name, Alambique.Replica, :dir, [config])
    on(c, name, Path, :wildcard, [Path.join(dir, "*/replica.db")])
  end

  test "reads are answered by the owner, then by a replica, and survive the owner", t do
    %{c: c, inst: inst, cell: cell} = t
    insert = [inst, cell, "INSERT INTO log (n) VALUES (?)", [1]]
    assert {:ok, _} = on(c, :n1, Alambique, :execute, insert)
    {:ok, owner} = on(c, :n1, Alambique, :owner, [inst, cell])
    owner_name = Enum.find(Cluster.names(c), &(Cluster.node(c, &1) == owner))
    reader = Enum.find(Cluster.names(c), &(&1 != owner_name))

    # No switch: the owner answers, and no replica is built.
    assert [1] = rows(read(c, reader, inst, cell, 500))
    assert files(c, reader, inst) == []

    # The reader reports who answers: the owner until the replica is ready.
    handler = &Alambique.CellHelpers.relay/4

    :ok =
      on(c, reader, :telemetry, :attach, [
        "replica-read",
        [:alambique, :replica, :read],
        handler,
        self()
      ])

    assert :ok = on(c, reader, Alambique, :replicas, [inst, cell, true])

    answered_by_replica? = fn ->
      assert [1] = rows(read(c, reader, inst, cell, 500))

      receive do
        {:telemetry, _, _, %{source: :replica}} -> true
      after
        0 -> false
      end
    end

    assert eventually(answered_by_replica?, 10_000)
    assert [_] = files(c, reader, inst)

    # A write is in the next read whose bound it is older than.
    assert {:ok, _} =
             on(c, owner_name, Alambique, :execute, [inst, cell, "INSERT INTO log (n) VALUES (2)"])

    Process.sleep(5)
    assert [1, 2] = rows(read(c, reader, inst, cell, 1))

    # The owner stops without handing the cell over: the replica still answers.
    Cluster.kill(c, owner_name)
    assert [1, 2] = rows(read(c, reader, inst, cell, 60_000))

    assert :ok = on(c, reader, Alambique, :replicas, [inst, cell, false])
    assert eventually(fn -> files(c, reader, inst) == [] end, 10_000)
  end
end
