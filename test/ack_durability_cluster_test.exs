defmodule Alambique.AckDurabilityClusterTest do
  @moduledoc """
  No acknowledged write is lost when the owner dies right after the
  acknowledgement and another node takes the lease (design §6).

  Each round starts concurrent writers against a cell from a node that is not
  its owner, then kills the owner at once: either the whole VM halts at the
  crash point `:cell_after_ack` (after the callers were answered), or the test
  kills the node with `Cluster.kill/2` as soon as it sees the first
  acknowledgement. A survivor takes the lease; every acknowledged write must be
  in the cell, and nothing may appear twice.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.{Cluster, CrashPoints}

  @moduletag :cluster
  @moduletag timeout: 300_000

  @ttl_ms 2_000
  @skew_ms 400
  @writers 6
  @rounds 4
  @call_timeout_ms 3_000

  # A fresh cluster per round: a restarted node would rejoin with stale owner
  # hints from the round before.
  defp with_cluster(fun) do
    {:ok, cluster} =
      Cluster.start_link(
        ttl_ms: @ttl_ms,
        skew_margin_ms: @skew_ms,
        op_timeout_ms: 1_500,
        migrations: ClusterFuns.schema()
      )

    try do
      inst = Cluster.instance(cluster)
      fun.(cluster, inst)
    after
      Process.unlink(cluster)
      Cluster.stop(cluster)
    end
  end

  defp call(cluster, name, fun, args, timeout \\ @call_timeout_ms + 2_000),
    do: Cluster.call(cluster, name, Alambique, fun, args, timeout)

  defp insert(cluster, name, inst, cell, n) do
    args = [inst, cell, "INSERT INTO log (n) VALUES (?)", [n], [timeout: @call_timeout_ms]]
    match?({:ok, {:ok, _}}, call(cluster, name, :execute, args))
  end

  defp owner_name(cluster, inst, cell) do
    {:ok, {:ok, owner}} = call(cluster, :n1, :owner, [inst, cell])
    Enum.find(Cluster.names(cluster), &(Cluster.node(cluster, &1) == owner))
  end

  defp rows(cluster, name, inst, cell) do
    case call(cluster, name, :query, [
           inst,
           cell,
           "SELECT n FROM log ORDER BY id",
           [],
           [timeout: @call_timeout_ms]
         ]) do
      {:ok, {:ok, %{rows: rows}}} -> List.flatten(rows)
      _ -> nil
    end
  end

  # Concurrent writers on `caller`; `after_first_ack` runs in the test process
  # when the first acknowledgement arrives. Returns the acknowledged values.
  defp write_burst(cluster, caller, inst, cell, first, after_first_ack) do
    parent = self()
    ref = make_ref()

    tasks =
      for n <- first..(first + @writers - 1) do
        Task.async(fn ->
          if insert(cluster, caller, inst, cell, n) do
            send(parent, {ref, :acked})
            n
          end
        end)
      end

    receive do
      {^ref, :acked} -> after_first_ack.()
    after
      @call_timeout_ms + 2_000 -> :no_ack
    end

    tasks |> Task.await_many(@call_timeout_ms + 5_000) |> Enum.reject(&is_nil/1)
  end

  defp round(cluster, inst, kind, index) do
    cell = "ack-#{kind}-#{index}"
    assert insert(cluster, :n1, inst, cell, 0)
    owner = owner_name(cluster, inst, cell)
    caller = Enum.find(Cluster.names(cluster), &(&1 != owner))
    survivor = Enum.find(Cluster.names(cluster), &(&1 not in [owner, caller]))

    after_first_ack =
      case kind do
        :crash_point ->
          assert {:ok, :ok} =
                   Cluster.call(cluster, owner, CrashPoints, :arm, [
                     :cell_after_ack,
                     &ClusterFuns.halt/2
                   ])

          fn -> :ok end

        :kill ->
          fn -> Cluster.kill(cluster, owner) end
      end

    acked = write_burst(cluster, caller, inst, cell, 1, after_first_ack)
    assert acked != [], "no write was acknowledged in round #{kind} #{index}"

    # The owner is gone (or about to be): wait for the lease to run out and
    # for a survivor to serve the cell.
    stored =
      eventually(
        fn ->
          rows(cluster, survivor, inst, cell)
        end,
        @ttl_ms + @skew_ms + 10_000
      )

    assert [0 | rest] = stored
    assert acked -- rest == [], "#{kind}: lost acknowledged writes #{inspect(acked -- rest)}"
    assert rest == Enum.uniq(rest), "#{kind}: a write appears twice: #{inspect(rest)}"

    assert {:ok, {:ok, new_owner}} = call(cluster, survivor, :owner, [inst, cell])
    assert new_owner != Cluster.node(cluster, owner)

    # The cell keeps accepting writes under the new owner.
    assert eventually(fn -> insert(cluster, survivor, inst, cell, 100) end, 10_000)
    assert List.last(rows(cluster, survivor, inst, cell)) == 100

    acked
  end

  for kind <- [:crash_point, :kill] do
    test "no acknowledged write is lost when the owner dies after the ack (#{kind})" do
      acked =
        for index <- 1..@rounds,
            do: with_cluster(&round(&1, &2, unquote(kind), index))

      assert Enum.all?(acked, &(&1 != []))
    end
  end
end
