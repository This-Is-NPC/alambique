defmodule Alambique.ForwardOutcomePropertiesTest do
  @moduledoc """
  Forwarded writes whose connection fails after they were sent (design §8).

  Random scenarios send writes with unique client ids from random nodes while
  served writes drop the links of their target (so the sender sees
  `noconnection` for a call that ran) and peers are partitioned and healed.
  After the cluster is healed:

    * every acknowledged write is in its cell exactly once;
    * a write that was not marked `idempotent: true` is never in its cell
      twice, also when it was answered with an error;
    * a write answered with an error that says it did not run is not in the
      cell; one answered `:outcome_unknown` is there at most once;
    * an `idempotent: true` write that was acknowledged is there at least once
      and may be there more often.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.{Cluster, CrashPoints}

  @moduletag :cluster
  @moduletag timeout: 900_000

  @names [:n1, :n2, :n3]
  @cells ["c0", "c1", "c2"]
  @pairs [{:n1, :n2}, {:n1, :n3}, {:n2, :n3}]
  @ttl_ms 2_000
  @skew_ms 400
  @call_timeout_ms 3_000
  @available_within_ms @ttl_ms + @skew_ms + 8_000
  # Replies that say the write did not run.
  @not_run [:unavailable, :owned, :forward_failed, :draining]

  defp op do
    node = member_of(@names)

    frequency([
      {12, tuple({constant(:write), node, member_of(@cells), boolean()})},
      {4, tuple({constant(:drop), node, member_of([:execute, :transaction])})},
      {1, tuple({constant(:partition_nodes), member_of(@pairs)})},
      {2, tuple({constant(:heal_nodes), member_of(@pairs)})},
      {2, constant(:reconnect)},
      {3, tuple({constant(:pass), integer(50..800)})}
    ])
  end

  property "a write that may have run is neither repeated nor reported as not run" do
    check all(
            ops <- resize(list_of(op(), min_length: 15, max_length: 40), 40),
            max_runs: 15,
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
        late_notice(cluster)
        run = Enum.reduce(ops, %{seq: 0, writes: %{}}, &step(cluster, &1, &2))
        settle(cluster)
        verify(cluster, run)
      after
        Process.unlink(cluster)
        Cluster.stop(cluster)
      end
    end
  end

  # The sender notices a lost connection when the target is reachable again.
  defp late_notice(cluster) do
    for name <- @names do
      arm(cluster, name, :router_forward_lost, ClusterFuns.late_notice(300), times: :infinity)
    end
  end

  defp arm(cluster, name, point, action, opts) do
    {:ok, :ok} = Cluster.call(cluster, name, CrashPoints, :arm, [point, action, opts])
  end

  defp step(cluster, {:write, node, cell, idempotent?}, run) do
    n = run.seq + 1
    inst = Cluster.instance(cluster)
    sql = "INSERT INTO log (n) VALUES (?)"
    opts = [timeout: @call_timeout_ms, idempotent: idempotent?]

    reply =
      case Cluster.call(
             cluster,
             node,
             Alambique,
             :execute,
             [inst, cell, sql, [n], opts],
             @call_timeout_ms + 2_000
           ) do
        {:ok, reply} -> reply
        {:error, reason} -> {:error, {:call_failed, reason}}
      end

    write = %{cell: cell, idempotent?: idempotent?, reply: classify(reply)}
    %{run | seq: n, writes: Map.put(run.writes, n, write)}
  end

  # The next served write on `target` ends by dropping the target's links.
  defp step(cluster, {:drop, target, kind}, run) do
    arm(cluster, target, :router_after_serve, ClusterFuns.drop_links([node()]),
      times: 1,
      when: ClusterFuns.serving([kind])
    )

    run
  end

  defp step(cluster, {:partition_nodes, {a, b}}, run),
    do: tap(run, fn _ -> Cluster.partition_nodes(cluster, a, b) end)

  defp step(cluster, {:heal_nodes, {a, b}}, run),
    do: tap(run, fn _ -> Cluster.heal_nodes(cluster, a, b) end)

  defp step(cluster, :reconnect, run), do: tap(run, fn _ -> reconnect(cluster) end)
  defp step(_cluster, {:pass, ms}, run), do: tap(run, fn _ -> Process.sleep(ms) end)

  defp classify({:ok, _}), do: :acked
  defp classify({:error, :outcome_unknown}), do: :unknown
  defp classify({:error, {reason, _, _}}) when reason in @not_run, do: :not_run
  defp classify({:error, {reason, _}}) when reason in @not_run, do: :not_run
  defp classify({:error, reason}) when reason in @not_run, do: :not_run
  defp classify(_), do: :other

  # Links dropped by the faults are not restored by themselves in this cluster.
  defp reconnect(cluster) do
    for {a, b} <- @pairs, do: Cluster.call(cluster, a, Node, :connect, [Cluster.node(cluster, b)])
  end

  defp settle(cluster) do
    Cluster.heal_all(cluster)
    reconnect(cluster)
  end

  # Checks

  defp verify(cluster, run) do
    inst = Cluster.instance(cluster)

    stored =
      for cell <- @cells, into: %{}, do: {cell, read_when_available(cluster, inst, cell)}

    for {n, write} <- run.writes do
      count = Enum.count(stored[write.cell], &(&1 == n))
      context = "write #{n} (#{inspect(write)}) is stored #{count} times"

      cond do
        write.idempotent? and write.reply == :acked -> assert count >= 1, context
        write.idempotent? -> :ok
        write.reply == :acked -> assert count == 1, context
        write.reply == :not_run -> assert count == 0, context
        true -> assert count <= 1, context
      end
    end

    issued = Map.keys(run.writes)

    for {cell, rows} <- stored do
      assert Enum.uniq(rows) -- issued == [],
             "cell #{cell} has writes nobody issued: #{inspect(rows)}"
    end
  end

  defp read_when_available(cluster, inst, cell) do
    eventually(
      fn ->
        Enum.find_value(@names, fn node ->
          args = [inst, cell, "SELECT n FROM log ORDER BY id", [], [timeout: @call_timeout_ms]]

          case Cluster.call(cluster, node, Alambique, :query, args, @call_timeout_ms + 2_000) do
            {:ok, {:ok, %{rows: rows}}} -> List.flatten(rows)
            _ -> nil
          end
        end)
      end,
      @available_within_ms
    )
  end
end
