defmodule Alambique.FailoverPropertiesTest do
  @moduledoc """
  Failover properties on a cluster of peer nodes (design §5 to §8).

  Random scenarios mix writes from random nodes with hibernations and moves,
  node kills and restarts (also right after an acknowledgement, killing the
  owner), store faults and timeouts, node-to-store and node-to-node partitions, and clock skew within the margin. After the cluster
  is healed:

    * every write that was acknowledged is in its cell, in the order it was
      acknowledged, and no write appears twice;
    * acknowledged batches never overlap in epochs: a higher epoch never
      acknowledges before a lower one, and no epoch acknowledges from two nodes;
    * every cell is served again within a bound set by the lease expiry.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.Cluster

  @moduletag :cluster
  @moduletag timeout: 900_000

  @names [:n1, :n2, :n3]
  @cells ["c0", "c1", "c2"]
  @ttl_ms 2_000
  @skew_ms 400
  @call_timeout_ms 3_000
  # Lease expiry plus margin, a wake and generous slack for a loaded machine.
  @available_within_ms @ttl_ms + @skew_ms + 8_000

  defp op do
    node = member_of(@names)
    cell = member_of(@cells)

    frequency([
      {10, tuple({constant(:write), node, cell})},
      {2, tuple({constant(:hibernate), node, cell})},
      {2, tuple({constant(:move), node, cell, node})},
      {2, tuple({constant(:write_kill), node, cell})},
      {3, tuple({constant(:kill), node})},
      {3, tuple({constant(:restart), node})},
      {2,
       tuple(
         {constant(:store_fault), node, member_of([:error, :timeout, :lost_ack, :raise, :exit]),
          integer(1..4)}
       )},
      {1, tuple({constant(:partition_store), node})},
      {1, tuple({constant(:heal_store), node})},
      {1, tuple({constant(:partition_nodes), member_of([{:n1, :n2}, {:n1, :n3}, {:n2, :n3}])})},
      {1, tuple({constant(:heal_nodes), member_of([{:n1, :n2}, {:n1, :n3}, {:n2, :n3}])})},
      {1, tuple({constant(:skew), node, integer(-div(@skew_ms, 2)..div(@skew_ms, 2))})},
      {4, tuple({constant(:pass), integer(100..1500)})}
    ])
  end

  property "acknowledged writes survive failures and epochs never overlap" do
    check all(
            ops <- resize(list_of(op(), min_length: 20, max_length: 45), 45),
            max_runs: 20,
            max_shrinking_steps: 0
          ) do
      {:ok, cluster} =
        Cluster.start_link(
          names: @names,
          ttl_ms: @ttl_ms,
          skew_margin_ms: @skew_ms,
          op_timeout_ms: 1_500,
          migrations: ClusterFuns.schema()
        )

      try do
        run = Enum.reduce(ops, new_run(), &step(cluster, &1, &2))
        settle(cluster, run)
        verify(cluster, run)
      after
        Process.unlink(cluster)
        Cluster.stop(cluster)
      end
    end
  end

  # Scenario

  defp new_run, do: %{seq: 0, acked: %{}, attempted: %{}, dead: MapSet.new()}

  defp step(cluster, {:write, node, cell}, run) do
    n = run.seq + 1
    inst = Cluster.instance(cluster)
    args = [inst, cell, "INSERT INTO log (n) VALUES (?)", [n], [timeout: @call_timeout_ms]]

    acked? =
      match?(
        {:ok, {:ok, _}},
        Cluster.call(cluster, node, Alambique, :execute, args, @call_timeout_ms + 2_000)
      )

    run = %{run | seq: n, attempted: Map.update(run.attempted, cell, [n], &(&1 ++ [n]))}
    if acked?, do: %{run | acked: Map.update(run.acked, cell, [n], &(&1 ++ [n]))}, else: run
  end

  # A write whose acknowledgement is followed at once by the death of the
  # node that owns the cell.
  defp step(cluster, {:write_kill, node, cell}, run) do
    before = run.seq
    run = step(cluster, {:write, node, cell}, run)

    if List.last(Map.get(run.acked, cell, [])) == before + 1 do
      kill_owner(cluster, node, cell, run)
    else
      run
    end
  end

  defp step(cluster, {:hibernate, node, cell}, run) do
    args = [Cluster.instance(cluster), cell, [timeout: @call_timeout_ms]]
    Cluster.call(cluster, node, Alambique, :hibernate, args, @call_timeout_ms + 2_000)
    run
  end

  defp step(cluster, {:move, node, cell, target}, run) do
    args = [
      Cluster.instance(cluster),
      cell,
      Cluster.node(cluster, target),
      [timeout: @call_timeout_ms]
    ]

    Cluster.call(cluster, node, Alambique, :move, args, @call_timeout_ms + 2_000)
    run
  end

  defp step(cluster, {:kill, node}, run) do
    unless node in run.dead, do: Cluster.kill(cluster, node)
    %{run | dead: MapSet.put(run.dead, node)}
  end

  defp step(cluster, {:restart, node}, run) do
    if node in run.dead, do: Cluster.restart(cluster, node)
    %{run | dead: MapSet.delete(run.dead, node)}
  end

  defp step(cluster, {:store_fault, node, kind, times}, run) do
    Cluster.inject(cluster, node, kind: kind, times: times, ms: 700, ops: [:put, :get, :list])
    run
  end

  defp step(cluster, {:partition_store, node}, run),
    do: tap(run, fn _ -> Cluster.partition_store(cluster, node) end)

  defp step(cluster, {:heal_store, node}, run),
    do: tap(run, fn _ -> Cluster.heal_store(cluster, node) end)

  defp step(cluster, {:partition_nodes, {a, b}}, run),
    do:
      tap(run, fn _ -> if both_alive?(run, a, b), do: Cluster.partition_nodes(cluster, a, b) end)

  defp step(cluster, {:heal_nodes, {a, b}}, run),
    do: tap(run, fn _ -> if both_alive?(run, a, b), do: Cluster.heal_nodes(cluster, a, b) end)

  defp step(cluster, {:skew, node, ms}, run),
    do: tap(run, fn _ -> Cluster.set_clock_offset(cluster, node, ms) end)

  defp step(_cluster, {:pass, ms}, run), do: tap(run, fn _ -> Process.sleep(ms) end)

  defp kill_owner(cluster, via, cell, run) do
    args = [Cluster.instance(cluster), cell]

    with {:ok, {:ok, owner}} <-
           Cluster.call(cluster, via, Alambique, :owner, args, @call_timeout_ms + 2_000),
         name when name != nil <- Enum.find(@names, &(Cluster.node(cluster, &1) == owner)),
         false <- name in run.dead do
      Cluster.kill(cluster, name)
      %{run | dead: MapSet.put(run.dead, name)}
    else
      _ -> run
    end
  end

  defp both_alive?(run, a, b), do: a not in run.dead and b not in run.dead

  defp settle(cluster, run) do
    Cluster.heal_all(cluster)
    for node <- run.dead, do: Cluster.restart(cluster, node)
  end

  # Checks

  defp verify(cluster, run) do
    inst = Cluster.instance(cluster)
    started = System.monotonic_time(:millisecond)

    rows =
      for cell <- @cells, into: %{} do
        {cell, read_when_available(cluster, inst, cell)}
      end

    waited = System.monotonic_time(:millisecond) - started
    assert waited <= @available_within_ms, "cells took #{waited} ms to come back"

    for cell <- @cells do
      acked = Map.get(run.acked, cell, [])
      attempted = Map.get(run.attempted, cell, [])
      stored = rows[cell]

      assert stored == Enum.uniq(stored), "cell #{cell} has duplicate writes: #{inspect(stored)}"
      assert stored -- attempted == [], "cell #{cell} has writes nobody issued"

      assert acked -- stored == [],
             "cell #{cell} lost acknowledged writes #{inspect(acked -- stored)}"

      assert Enum.filter(stored, &(&1 in acked)) == acked,
             "cell #{cell} reordered acknowledged writes"

      for node <- @names do
        assert {:ok, {:ok, %{rows: seen}}} = query(cluster, node, inst, cell)
        assert List.flatten(seen) == stored, "node #{node} reads a different #{cell}"
      end
    end

    check_epochs(Cluster.events(cluster))
  end

  defp query(cluster, node, inst, cell) do
    Cluster.call(
      cluster,
      node,
      Alambique,
      :query,
      [inst, cell, "SELECT n FROM log ORDER BY id", [], [timeout: @call_timeout_ms]],
      @call_timeout_ms + 2_000
    )
  end

  defp read_when_available(cluster, inst, cell) do
    eventually(
      fn ->
        Enum.find_value(@names, fn node ->
          case query(cluster, node, inst, cell) do
            {:ok, {:ok, %{rows: rows}}} -> List.flatten(rows)
            _ -> nil
          end
        end)
      end,
      @available_within_ms
    )
  end

  defp check_epochs(events) do
    commits =
      for %{event: [:alambique, :cell, :commit], meta: %{result: :ok} = meta} = e <- events,
          do: %{cell: meta.cell, txid: meta.txid, epoch: meta.epoch, node: e.node, at: e.at}

    for {cell, batches} <- Enum.group_by(commits, & &1.cell) do
      assert batches |> Enum.map(& &1.txid) |> Enum.uniq() |> length() == length(batches),
             "cell #{cell}: one TXID acknowledged twice: #{inspect(batches)}"

      by_epoch = Enum.group_by(batches, & &1.epoch, & &1.node)

      for {epoch, nodes} <- by_epoch do
        assert nodes |> Enum.uniq() |> length() == 1,
               "cell #{cell}: epoch #{epoch} acknowledged by #{inspect(Enum.uniq(nodes))}"
      end

      by_time = Enum.sort_by(batches, & &1.at)
      epochs = Enum.map(by_time, & &1.epoch)

      assert epochs == Enum.sort(epochs),
             "cell #{cell}: epochs overlap in time: #{inspect(by_time)}"

      by_txid = Enum.sort_by(batches, & &1.txid)
      epochs = Enum.map(by_txid, & &1.epoch)

      assert epochs == Enum.sort(epochs),
             "cell #{cell}: epochs regress along TXIDs: #{inspect(by_txid)}"
    end
  end
end
