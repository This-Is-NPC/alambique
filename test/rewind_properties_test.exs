defmodule Alambique.RewindPropertiesTest do
  @moduledoc """
  Properties of `Alambique.rewind/4` on a cluster of peer nodes (design §20).

  Random scenarios mix writes, positions read with their rows, rewinds to an
  earlier position (also of an earlier timeline, which undoes a rewind),
  rewinds that crash at a named point, hibernations, node kills and restarts
  and store faults. After every rewind and after the cluster is healed:

    * a rewind that returned `:ok` left exactly the rows of its target;
    * one that failed or crashed left the rows it started from or exactly the
      rows of its target, never anything in between;
    * a sampler that reads the cell while a rewind runs only ever sees one of
      those two states;
    * no write acknowledged after a rewind is lost or reordered;
    * the feed, replayed with the undone records of each rewind removed and
      deduplicated by id, holds the rows of the cell (F1).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.{ClusterFuns, Feed}
  alias Alambique.Feed.Rewind
  alias AlambiqueTestkit.{Cluster, CrashPoints}

  @moduletag :cluster
  @moduletag timeout: 900_000

  @names [:n1, :n2, :n3]
  @cells ["c0", "c1"]
  @ttl_ms 2_000
  @skew_ms 400
  @call_timeout_ms 3_000
  @rewind_timeout_ms 12_000
  @available_within_ms @ttl_ms + @skew_ms + 15_000
  @crash_points [
    :rewind_after_acquire,
    :rewind_after_head,
    :rewind_before_put,
    :rewind_after_put
  ]

  defp op do
    node = member_of(@names)
    cell = member_of(@cells)

    frequency([
      {10, tuple({constant(:write), node, cell})},
      {5, tuple({constant(:snap), node, cell})},
      {8, tuple({constant(:rewind), node, cell, integer(0..20)})},
      {3, tuple({constant(:crash_point), node, member_of(@crash_points)})},
      {2, tuple({constant(:hibernate), node, cell})},
      {2, tuple({constant(:kill), node})},
      {3, tuple({constant(:restart), node})},
      {2,
       tuple(
         {constant(:store_fault), node, member_of([:error, :timeout, :lost_ack]), integer(1..3)}
       )},
      {3, tuple({constant(:pass), integer(100..800)})}
    ])
  end

  property "a rewind installs exactly the target, atomically, and keeps what comes after" do
    check all(
            ops <- resize(list_of(op(), min_length: 20, max_length: 40), 40),
            max_runs: 10,
            max_shrinking_steps: 0
          ) do
      {:ok, cluster} =
        Cluster.start_link(
          names: @names,
          ttl_ms: @ttl_ms,
          skew_margin_ms: @skew_ms,
          op_timeout_ms: 1_500,
          migrations: ClusterFuns.schema(),
          instance_opts: [feed: [window_ms: 500], rewind: [timeout_ms: 10_000, poll_ms: 50]]
        )

      try do
        run = Enum.reduce(ops, new_run(), &step(cluster, &1, &2))
        Cluster.heal_all(cluster)
        for node <- run.dead, do: Cluster.restart(cluster, node)
        verify(cluster, run)
      after
        Process.unlink(cluster)
        Cluster.stop(cluster)
      end
    end
  end

  # Scenario. `base` is the state a cell was last known to have, `since` what
  # was written after it: `{n, acknowledged?}`.
  defp new_run do
    cells = for cell <- @cells, into: %{}, do: {cell, %{base: nil, since: [], snaps: []}}
    %{seq: 0, dead: MapSet.new(), cells: cells, rewinds: 0}
  end

  defp step(cluster, {:write, node, cell}, run) do
    n = run.seq + 1
    inst = Cluster.instance(cluster)
    args = [inst, cell, "INSERT INTO log (n) VALUES (?)", [n], [timeout: @call_timeout_ms]]

    acked? =
      match?(
        {:ok, {:ok, _}},
        Cluster.call(cluster, node, Alambique, :execute, args, @call_timeout_ms + 2_000)
      )

    run = %{run | seq: n}
    update_in(run.cells[cell].since, &(&1 ++ [{n, acked?}]))
  end

  # A position and the rows it covers, to rewind to later.
  defp step(cluster, {:snap, node, cell}, run) do
    inst = Cluster.instance(cluster)
    args = [inst, cell, [timeout: @call_timeout_ms]]

    with {:ok, {:ok, %{txid: txid}}} <-
           Cluster.call(cluster, node, Alambique, :position, args, @call_timeout_ms + 2_000),
         {:ok, rows} <- rows(cluster, node, inst, cell) do
      update_in(run.cells[cell].snaps, &[{txid, rows} | &1])
    else
      _ -> run
    end
  end

  defp step(cluster, {:rewind, node, cell, k}, run) do
    case run.cells[cell].snaps do
      [] -> run
      snaps -> rewind(cluster, node, cell, Enum.at(snaps, rem(k, length(snaps))), run)
    end
  end

  defp step(cluster, {:crash_point, node, point}, run) do
    unless node in run.dead,
      do: Cluster.call(cluster, node, CrashPoints, :arm, [point, :kill, [times: 1]])

    run
  end

  defp step(cluster, {:hibernate, node, cell}, run) do
    args = [Cluster.instance(cluster), cell, [timeout: @call_timeout_ms]]
    Cluster.call(cluster, node, Alambique, :hibernate, args, @call_timeout_ms + 2_000)
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
    unless node in run.dead,
      do:
        Cluster.inject(cluster, node, kind: kind, times: times, ms: 700, ops: [:put, :get, :list])

    run
  end

  defp step(_cluster, {:pass, ms}, run), do: tap(run, fn _ -> Process.sleep(ms) end)

  # The caller must be alive: a rewind on a dead node is not tried.
  defp rewind(cluster, node, cell, {txid, target_rows}, run) do
    if node in run.dead do
      run
    else
      inst = Cluster.instance(cluster)
      Cluster.heal_all(cluster)
      pre = read_when_available(cluster, inst, cell)

      sampler = sampler(cluster, inst, cell)
      args = [inst, cell, {:txid, txid}, [timeout: @rewind_timeout_ms]]
      outcome = Cluster.call(cluster, node, Alambique, :rewind, args, @rewind_timeout_ms + 3_000)
      samples = stop_sampler(sampler)

      now = read_when_available(cluster, inst, cell)

      for rows <- samples do
        assert rows in [pre, target_rows],
               "cell #{cell}: a reader saw #{inspect(rows)}, neither #{inspect(pre)} nor #{inspect(target_rows)}"
      end

      case outcome do
        {:ok, {:ok, %{to: ^txid}}} ->
          assert now == target_rows,
                 "cell #{cell}: rewound to #{inspect(now)}, expected #{inspect(target_rows)}"

        _ ->
          assert now in [pre, target_rows],
                 "cell #{cell}: #{inspect(outcome)} left #{inspect(now)}, neither #{inspect(pre)} nor #{inspect(target_rows)}"
      end

      run = %{run | rewinds: run.rewinds + 1}
      put_in(run.cells[cell], %{run.cells[cell] | base: now, since: []})
    end
  end

  # Reads the cell all the time from all nodes while a rewind runs.
  defp sampler(cluster, inst, cell) do
    parent = self()

    pid =
      spawn_link(fn ->
        sample_loop(cluster, inst, cell, [], parent)
      end)

    pid
  end

  defp sample_loop(cluster, inst, cell, acc, parent) do
    receive do
      :stop -> send(parent, {:samples, self(), acc})
    after
      15 ->
        node = Enum.random(@names)

        acc =
          case rows(cluster, node, inst, cell) do
            {:ok, rows} -> [rows | acc]
            _ -> acc
          end

        sample_loop(cluster, inst, cell, acc, parent)
    end
  end

  defp stop_sampler(pid) do
    send(pid, :stop)

    receive do
      {:samples, ^pid, samples} -> Enum.uniq(samples)
    after
      15_000 -> []
    end
  end

  defp rows(cluster, node, inst, cell) do
    args = [inst, cell, "SELECT n FROM log ORDER BY id", [], [timeout: @call_timeout_ms]]

    case Cluster.call(cluster, node, Alambique, :query, args, @call_timeout_ms + 2_000) do
      {:ok, {:ok, %{rows: rows}}} -> {:ok, List.flatten(rows)}
      _ -> :error
    end
  end

  defp read_when_available(cluster, inst, cell) do
    eventually(
      fn ->
        Enum.find_value(@names, fn node ->
          with {:ok, rows} <- rows(cluster, node, inst, cell), do: rows, else: (_ -> nil)
        end)
      end,
      @available_within_ms
    )
  end

  # Checks

  defp verify(cluster, run) do
    inst = Cluster.instance(cluster)

    stored =
      for cell <- @cells, into: %{} do
        rows = read_when_available(cluster, inst, cell)
        check_cell(cell, rows, run.cells[cell])
        {cell, rows}
      end

    check_feed(cluster, inst, stored)
  end

  # What was written after the last known state is there if it was
  # acknowledged, in order; the rest of the state is that last known state.
  defp check_cell(cell, rows, %{base: nil, since: since}), do: check_cell(cell, rows, [], since)

  defp check_cell(cell, rows, %{base: base, since: since}),
    do: check_cell(cell, rows, base, since)

  defp check_cell(cell, rows, base, since) do
    assert Enum.take(rows, length(base)) == base,
           "cell #{cell}: #{inspect(rows)} does not start with #{inspect(base)}"

    rest = Enum.drop(rows, length(base))
    attempted = Enum.map(since, &elem(&1, 0))
    acked = for {n, true} <- since, do: n

    assert rest == Enum.uniq(rest), "cell #{cell}: duplicate writes #{inspect(rest)}"
    assert rest -- attempted == [], "cell #{cell}: writes nobody issued #{inspect(rest)}"
    assert acked -- rest == [], "cell #{cell} lost acknowledged writes #{inspect(acked -- rest)}"
    assert Enum.filter(rest, &(&1 in acked)) == acked, "cell #{cell} reordered writes"
  end

  # F1 with rewinds: replay the feed in order, drop what each marker undoes,
  # and compare with the table.
  defp check_feed(cluster, inst, stored) do
    {store, config} = Cluster.store(cluster)
    ctx = Feed.ctx(store: {store, config}, prefix: "cluster", settle_ms: 0)

    replayed = fn -> for cell <- @cells, into: %{}, do: {cell, replay(read_all(ctx), cell)} end

    try do
      eventually(fn -> replayed.() == stored end, @available_within_ms)
    rescue
      RuntimeError -> :timeout
    end

    all = replayed.()

    for cell <- @cells do
      assert all[cell] == stored[cell],
             "cell #{cell}: feed replays to #{inspect(all[cell])}, table holds #{inspect(stored[cell])}"
    end

    _ = inst
    :ok
  end

  # A marker puts the cell back to what it was when the records up to its
  # `to_txid` had been applied, which also revives what an earlier marker
  # undid (a rewind to a TXID of an older timeline).
  defp replay(stream, cell) do
    {live, _hist, _seen} =
      stream
      |> Enum.filter(&(&1.cell == cell))
      |> Enum.reduce({[], %{}, MapSet.new()}, fn item, {live, hist, seen} ->
        if item.id in seen do
          {live, hist, seen}
        else
          live =
            case item do
              %Rewind{to_txid: to} -> state_at(hist, to)
              record -> live ++ [{record.position, record.values["n"]}]
            end

          txid = if match?(%Rewind{}, item), do: item.txid, else: elem(item.position, 0)
          {live, Map.put(hist, txid, live), MapSet.put(seen, item.id)}
        end
      end)

    live |> Enum.sort_by(&elem(&1, 0)) |> Enum.map(&elem(&1, 1))
  end

  defp state_at(hist, to) do
    case for({txid, _} <- hist, txid <= to, do: txid) do
      [] -> []
      txids -> Map.fetch!(hist, Enum.max(txids))
    end
  end

  defp read_all(ctx, position \\ nil, acc \\ []) do
    {:ok, records, next} = Feed.read(ctx, position, 100)
    if records == [], do: acc, else: read_all(ctx, next, acc ++ records)
  end
end
