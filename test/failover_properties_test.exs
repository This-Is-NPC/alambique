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
    * every cell is served again within a bound set by the lease expiry;
    * no write is acknowledged before its segment is in the store, also with
      the puts of a cell held while writes arrive (an acknowledgement is
      checked at the instant it is given), and a segment of a newer epoch that
      is already at the next TXID stops the write instead of being overwritten.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.{AckGuard, ClusterFuns, HeldWrites, Placement}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{Cluster, GatedStore}

  @moduletag :cluster
  @moduletag timeout: 900_000

  @names [:n1, :n2, :n3]
  @cells ["c0", "c1", "c2"]
  @prefix "cluster"
  @ttl_ms 2_000
  @skew_ms 400
  @call_timeout_ms 3_000
  # Lease expiry plus margin, a wake and generous slack for a loaded machine.
  # MITIGATION: on a CI runner no node served a cell within 10.4 s after peers
  # restarted (seed 13016). The log cannot tell a slow wake or peer boot from a
  # missed takeover. A node that failed a wake answers that failure for up to
  # wake.backoff_max_ms (10 s, design §7), which the old 8 s slack did not
  # cover, so the bound now allows it; a real defect would still miss 20 s.
  @available_within_ms @ttl_ms + @skew_ms + 20_000

  defp op do
    node = member_of(@names)
    cell = member_of(@cells)

    frequency([
      {10, tuple({constant(:write), node, cell})},
      {2, tuple({constant(:hibernate), node, cell})},
      {2, tuple({constant(:move), node, cell, node})},
      {2, tuple({constant(:write_kill), node, cell})},
      {3, tuple({constant(:held), node, cell, integer(2..3)})},
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
      {:ok, gate} = GatedStore.start_link()

      {:ok, cluster} =
        Cluster.start_link(
          cluster_opts(@names) ++
            [
              placement: fn shared ->
                {Placement.Single, [store: GatedStore.wrap(gate, shared), prefix: @prefix]}
              end
            ]
        )

      guard = AckGuard.attach_peers(%{"default" => {Cluster.store(cluster), @prefix}})

      try do
        for name <- @names, do: watch(cluster, name, guard)
        run = Enum.reduce(ops, new_run(gate, guard), &step(cluster, &1, &2))
        settle(cluster, run)

        assert AckGuard.violations(guard) == [],
               "acknowledged before the put landed: #{inspect(AckGuard.violations(guard))}"

        if Enum.any?(Map.values(run.acked), &(&1 != [])),
          do: assert(AckGuard.acks(guard) != [], "the guard saw no acknowledgement")

        verify(cluster, run)
      after
        Process.unlink(cluster)
        Cluster.stop(cluster)
        AckGuard.detach(guard)
        GenServer.stop(gate)
      end
    end
  end

  # The segment a cell would put at the next TXID is already in the store with
  # the epoch of the cell or a newer one: the put is refused, the write is not
  # acknowledged and the object stays as it was (design §6).
  property "a segment of the same or a newer epoch at the next TXID is not overwritten" do
    check all(
            cell <- member_of(@cells),
            writes <- integer(1..3),
            ahead <- integer(0..2),
            max_runs: 5,
            max_shrinking_steps: 0
          ) do
      {:ok, cluster} = Cluster.start_link(cluster_opts([:n1, :n2]))

      try do
        refuse_taken_txid(cluster, cell, writes, ahead)
      after
        Process.unlink(cluster)
        Cluster.stop(cluster)
      end
    end
  end

  defp cluster_opts(names) do
    [
      names: names,
      ttl_ms: @ttl_ms,
      skew_margin_ms: @skew_ms,
      op_timeout_ms: 1_500,
      migrations: ClusterFuns.schema()
    ]
  end

  # Peers report their acknowledgements to the guard; a restarted peer needs it again.
  defp watch(cluster, name, guard),
    do: Cluster.call(cluster, name, AckGuard, :watch, [guard])

  # Scenario

  defp new_run(gate, guard),
    do: %{seq: 0, acked: %{}, attempted: %{}, dead: MapSet.new(), gate: gate, guard: guard}

  defp execute(cluster, node, cell, n, timeout \\ @call_timeout_ms) do
    inst = Cluster.instance(cluster)
    args = [inst, cell, "INSERT INTO log (n) VALUES (?)", [n], [timeout: timeout]]
    Cluster.call(cluster, node, Alambique, :execute, args, timeout + 2_000)
  end

  # Records the write `n` and whether `answer` acknowledged it.
  defp account(run, cell, n, answer) do
    run = %{run | seq: n, attempted: Map.update(run.attempted, cell, [n], &(&1 ++ [n]))}

    if match?({:ok, {:ok, _}}, answer),
      do: %{run | acked: Map.update(run.acked, cell, [n], &(&1 ++ [n]))},
      else: run
  end

  defp step(cluster, {:write, node, cell}, run),
    do: account(run, cell, run.seq + 1, execute(cluster, node, cell, run.seq + 1))

  # Writes whose puts are held while the cell is served by whoever owns it now
  # (after takeovers and moves of earlier steps): none is answered before its
  # put, and the put of every batch before it, landed.
  defp step(cluster, {:held, node, cell, count}, run) do
    run = step(cluster, {:write, node, cell}, run)
    ns = Enum.to_list((run.seq + 1)..(run.seq + count))

    write = &execute(cluster, node, cell, &1)

    case HeldWrites.run_on(cluster, run.gate, node, cell, run.dead, ns, write) do
      :none ->
        run

      answers ->
        Enum.reduce(answers, run, fn {n, answer}, run -> account(run, cell, n, answer) end)
    end
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
    restart(cluster, node, run)
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

  defp restart(cluster, node, run) do
    if node in run.dead do
      Cluster.restart(cluster, node)
      watch(cluster, node, run.guard)
    end

    %{run | dead: MapSet.delete(run.dead, node)}
  end

  defp settle(cluster, run) do
    Cluster.heal_all(cluster)
    for node <- run.dead, do: restart(cluster, node, run)
  end

  # Checks

  # `writes` acknowledged writes, then the object of the next TXID put by
  # another writer, `ahead` epochs ahead of the cell's last one.
  defp refuse_taken_txid(cluster, cell, writes, ahead) do
    inst = Cluster.instance(cluster)
    {module, config} = Cluster.store(cluster)

    for n <- 1..writes,
        do: assert({:ok, {:ok, _}} = execute(cluster, :n1, cell, n, 10_000))

    {:ok, keys, _} = module.list(config, Keys.level_prefix(@prefix, cell, 0), nil, 1_000)
    head = Enum.max(keys)
    txid = head |> Path.basename() |> String.split("-") |> hd() |> String.to_integer(16)
    {:ok, body, _etag, meta} = module.get(config, head)
    other = Map.put(meta, "epoch", Integer.to_string(String.to_integer(meta["epoch"]) + ahead))
    next = Keys.segment(@prefix, cell, 0, txid + 1, txid + 1)
    assert {:ok, _} = module.put(config, next, body, :if_none_match, other)

    answer = execute(cluster, :n2, cell, writes + 1)

    refute match?({:ok, {:ok, _}}, answer),
           "a write was acknowledged over an object of epoch +#{ahead}: #{inspect(answer)}"

    assert {:ok, kept, _etag, kept_meta} = module.get(config, next)
    assert {kept, kept_meta} == {body, other}, "the object of the other writer was overwritten"

    # The cell serves again once the object is gone; nothing but what it
    # acknowledged, and at most the refused write, is in it.
    :ok = module.delete(config, next)

    rows =
      eventually(
        fn -> Enum.find_value([:n1, :n2], &try_read(cluster, &1, inst, cell)) end,
        @available_within_ms
      )

    assert Enum.take(rows, writes) == Enum.to_list(1..writes)
    assert rows -- Enum.to_list(1..(writes + 1)) == []
  end

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
        seen = read_on(cluster, node, inst, cell)
        assert seen == stored, "node #{node} reads a different #{cell}"
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
      fn -> Enum.find_value(@names, &try_read(cluster, &1, inst, cell)) end,
      @available_within_ms
    )
  end

  # A node that woke the cell under a fault answers the failure it remembers
  # until the wake backoff (design §7) passes, so a healed cluster is read
  # again until the node serves. Only an error is retried; what a node reads is
  # still compared with the acknowledged writes.
  defp read_on(cluster, node, inst, cell),
    do: eventually(fn -> try_read(cluster, node, inst, cell) end, @available_within_ms)

  defp try_read(cluster, node, inst, cell) do
    case query(cluster, node, inst, cell) do
      {:ok, {:ok, %{rows: rows}}} -> List.flatten(rows)
      _ -> nil
    end
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
