defmodule Alambique.UploadRetryPropertiesTest do
  @moduledoc """
  Upload failures and the lease (design §6). A stream of writes meets random
  store failures: each group of concurrent writes starts after a number of
  segment puts that fail (up to more than a few retries), a group may instead
  hold its puts and release them out of order, and the stream may end with a
  lease that runs out while a put keeps failing, waits for its retry or is in
  flight. Properties:

    * U1: the cell stops if and only if its lease was lost;
    * U2: the segments in the store are TXIDs 1..N without a gap, each one
      once, and a restore gives the rows of the acknowledged writes and no
      other;
    * U3: the rows are in commit order: a write of a later group has a later
      rowid than every write of an earlier one;
    * U4: while the lease holds, every write is acknowledged;
    * U5: no write is answered before its segment is in the store, and none
      while its put is held, nor while an earlier put is;
    * U6: once the lease ended, a failed put is not retried and a retry that
      was waiting does not put again: the callers get `:outcome_unknown`.

  Each property runs at pipeline depth 1 as the control and at depth 3.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AckGuard, Cell, Config, CountingStore, HeldWrites}
  alias AlambiqueTestkit.{GatedStore, ManualClock, MemoryStore}

  @moduletag timeout: 600_000
  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER)"}]
  @insert "INSERT INTO t (n) VALUES (?)"
  # Long enough that no renewal renews a lease the test ended.
  @ttl 300_000

  setup do
    env = setup_store()
    Map.put(env, :counting, CountingStore.wrap(env.store))
  end

  defp scenario do
    group =
      frequency([
        {4, tuple({integer(0..8), integer(1..3), constant(false)})},
        {1, tuple({constant(0), constant(2), constant(true)})}
      ])

    {list_of(group, min_length: 1, max_length: 6),
     member_of([:none, :none, :backoff, :in_flight])}
  end

  for depth <- [1, 3] do
    property "retries keep the chain whole and the order, depth #{depth}", env do
      check all(
              {groups, loss} <- scenario(),
              max_runs: 15,
              max_shrinking_steps: 0
            ) do
        run(env, unquote(depth), groups, loss)
      end
    end
  end

  defp run(env, depth, groups, loss) do
    tag = System.unique_integer([:positive])
    world = ManualClock.new(1_800_000_000_000)
    cell = "retry#{tag}"
    {_, config} = env.store
    {:ok, gate} = GatedStore.start_link()
    counting = env.counting
    CountingStore.reset(elem(counting, 1))
    lose? = loss != :none
    # A retry waits long enough for the lease to end before it starts.
    backoff = if loss == :backoff, do: 400, else: 5
    max_fails = if loss == :backoff, do: 1, else: 8

    inst =
      start_instance(%{env | store: GatedStore.wrap(gate, counting)},
        name: :"retry#{tag}",
        migrations: @schema,
        compaction: false,
        lease_ttl_ms: @ttl,
        skew_margin_ms: 500,
        op_timeout_ms: 300,
        retry: [max_attempts: 1],
        breaker: [failures: 1_000_000],
        upload: [retry_backoff_ms: backoff, retry_backoff_max_ms: max(backoff, 20)],
        pipeline_depth: depth,
        clock: {ManualClock, ManualClock.config(world, 0)}
      )

    capture([[:alambique, :cell, :upload_retry], [:alambique, :cell, :stop]])
    guard = AckGuard.attach(env.store, "cells-test", &(&1 == cell))

    try do
      {:ok, _} = Alambique.execute(inst, cell, @insert, [0])
      pid = Cell.whereis(Config.fetch!(inst), cell)
      lane = %{inst: inst, cell: cell, config: config, gate: gate, pid: pid, depth: depth}

      acked =
        groups
        |> Enum.with_index(1)
        |> Enum.flat_map(fn
          {{_fails, _size, true}, index} -> write_held(lane, index)
          {{fails, size, false}, index} -> write_group(lane, index, min(fails, max_fails), size)
        end)

      assert AckGuard.violations(guard) == [],
             "U5: answered before the put landed: #{inspect(AckGuard.violations(guard))}"

      case loss do
        :none -> :ok
        :backoff -> lose_in_backoff(lane, world, counting)
        :in_flight -> lose_in_flight(lane, world)
      end

      stopped? = receive_stop(cell)
      assert stopped? == lose?, "U1: stopped #{stopped?}, lease lost #{lose?}"

      if lose?,
        do: eventually(fn -> not Process.alive?(pid) end),
        else: assert(Process.alive?(pid))

      assert Enum.all?(acked, &match?({:ok, _}, elem(&1, 1))), "U4: #{inspect(acked)}"

      ids = txids(env, cell)
      assert ids == Enum.to_list(1..length(ids)//1), "U2: segments #{inspect(ids)}"

      rows = durable_rows(env, cell, "SELECT id, n FROM t ORDER BY id")
      ns = Enum.map(rows, fn [_id, n] -> n end)
      want = [0 | for({n, {:ok, _}} <- acked, do: n)]

      assert Enum.sort(ns) == Enum.sort(want),
             "U2: rows #{inspect(ns)}, acknowledged #{inspect(want)}"

      groups_in_order = Enum.map(ns, &div(&1, 10))
      assert groups_in_order == Enum.sort(groups_in_order), "U3: #{inspect(ns)}"
    after
      AckGuard.detach(guard)
      GenServer.stop(gate)
      MemoryStore.clear(config)
      stop_supervised(inst)
    end
  end

  # `size` concurrent writes after `fails` failing segment puts. Returns
  # `{n, answer}` for each, with `n` of the group `index` between index * 10
  # and index * 10 + 9.
  defp write_group(lane, index, fails, size) do
    fault =
      if fails > 0,
        do:
          MemoryStore.inject(lane.config,
            kind: :error,
            ops: [:put],
            key: ~r{/0000/},
            times: fails,
            reason: {:http, 503, ""}
          )

    tasks = for k <- 1..size, do: write_async(lane, index * 10 + k)
    answers = for {n, task} <- tasks, do: {n, Task.await(task, 15_000)}
    if fault, do: MemoryStore.remove(lane.config, fault)
    answers
  end

  defp write_async(lane, n) do
    {n,
     Task.async(fn ->
       Alambique.execute(lane.inst, lane.cell, @insert, [n], timeout: 10_000)
     end)}
  end

  # Two writes whose puts are held (one at depth 1). The later put lands
  # first: nobody is answered until the earlier one has.
  defp write_held(lane, index) do
    HeldWrites.run(
      lane.gate,
      lane.cell,
      lane.pid,
      lane.depth,
      [index * 10 + 1, index * 10 + 2],
      fn n ->
        Alambique.execute(lane.inst, lane.cell, @insert, [n], timeout: 10_000)
      end
    )
  end

  # A put keeps failing and the lease ends during the wait for its retry: the
  # retry does not put again.
  defp lose_in_backoff(lane, world, counting) do
    fault =
      MemoryStore.inject(lane.config,
        kind: :error,
        ops: [:put],
        key: ~r{/0000/},
        reason: {:http, 503, ""}
      )

    drain([:alambique, :cell, :upload_retry], lane.cell)
    task = write_last(lane)
    {_, _} = await_event([:alambique, :cell, :upload_retry], lane.cell)
    puts = segment_puts(counting)
    ManualClock.advance(world, @ttl + 1_000)
    assert {:error, :outcome_unknown} = Task.await(task, 15_000)
    assert segment_puts(counting) == puts, "U6: a put after the lease ended"
    MemoryStore.remove(lane.config, fault)
  end

  # The lease ends while a put is in flight, then the put fails.
  defp lose_in_flight(lane, world) do
    GatedStore.hold(lane.gate, ~r{/0000/})
    drain([:alambique, :cell, :upload_retry], lane.cell)
    task = write_last(lane)
    eventually(fn -> GatedStore.held(lane.gate) != [] end)
    [key] = GatedStore.held(lane.gate)
    ManualClock.advance(world, @ttl + 1_000)
    GatedStore.fail(lane.gate, key)
    assert {:error, :outcome_unknown} = Task.await(task, 15_000)

    assert drain([:alambique, :cell, :upload_retry], lane.cell) == [],
           "U6: a retry after the lease ended"

    GatedStore.open(lane.gate)
  end

  defp write_last(lane) do
    Task.async(fn ->
      Alambique.execute(lane.inst, lane.cell, @insert, [999], timeout: 10_000)
    end)
  end

  defp segment_puts({_, config}) do
    config |> CountingStore.keys(:put) |> Enum.count(&String.contains?(&1, "/0000/"))
  end

  defp receive_stop(cell) do
    receive do
      {:telemetry, [:alambique, :cell, :stop], _, %{cell: ^cell}} -> true
    after
      200 -> false
    end
  end

  defp txids(env, cell) do
    {mod, config} = env.store
    {:ok, keys, _} = mod.list(config, "cells-test/cells/#{cell}/", nil, 1_000)

    for key <- keys, String.contains?(key, "/0000/") do
      key |> Path.basename() |> String.split("-") |> hd() |> String.to_integer(16)
    end
    |> Enum.sort()
  end
end
