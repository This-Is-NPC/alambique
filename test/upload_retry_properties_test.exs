defmodule Alambique.UploadRetryPropertiesTest do
  @moduledoc """
  Upload failures and the lease (design §6). A stream of writes meets random
  store failures: each group of concurrent writes starts after a number of
  segment puts that fail, and the stream may end with a lease that runs out
  while a put keeps failing. Properties:

    * U1: the cell stops if and only if its lease was lost;
    * U2: the segments in the store are TXIDs 1..N without a gap, each one
      once, and a restore gives the rows of the acknowledged writes and no
      other;
    * U3: the rows are in commit order: a write of a later group has a later
      rowid than every write of an earlier one;
    * U4: while the lease holds, every write is acknowledged.

  Each property runs at pipeline depth 1 as the control and at depth 3.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config}
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @moduletag timeout: 600_000
  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER)"}]
  @insert "INSERT INTO t (n) VALUES (?)"
  @ttl 20_000

  setup do
    setup_store()
  end

  defp scenario,
    do: {list_of(tuple({integer(0..3), integer(1..3)}), min_length: 1, max_length: 6), boolean()}

  for depth <- [1, 3] do
    property "retries keep the chain whole and the order, depth #{depth}", env do
      check all(
              {groups, lose?} <- scenario(),
              max_runs: 15,
              max_shrinking_steps: 0
            ) do
        run(env, unquote(depth), groups, lose?)
      end
    end
  end

  defp run(env, depth, groups, lose?) do
    tag = System.unique_integer([:positive])
    world = ManualClock.new(1_800_000_000_000)
    cell = "retry#{tag}"
    {_, config} = env.store

    inst =
      start_instance(env,
        name: :"retry#{tag}",
        migrations: @schema,
        compaction: false,
        lease_ttl_ms: @ttl,
        skew_margin_ms: 500,
        op_timeout_ms: 300,
        retry: [max_attempts: 1],
        upload: [retry_backoff_ms: 5, retry_backoff_max_ms: 20],
        pipeline_depth: depth,
        clock: {ManualClock, ManualClock.config(world, 0)}
      )

    capture([[:alambique, :cell, :upload_retry], [:alambique, :cell, :stop]])

    try do
      {:ok, _} = Alambique.execute(inst, cell, @insert, [0])
      pid = Cell.whereis(Config.fetch!(inst), cell)

      acked =
        groups
        |> Enum.with_index(1)
        |> Enum.flat_map(fn {{fails, size}, index} ->
          write_group(inst, cell, config, index, fails, size)
        end)

      if lose?, do: lose_lease(inst, cell, config, world)

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
      MemoryStore.clear(config)
      stop_supervised(inst)
    end
  end

  # `size` concurrent writes after `fails` failing segment puts. Returns
  # `{n, answer}` for each, with `n` of the group `index` between index * 10
  # and index * 10 + 9.
  defp write_group(inst, cell, config, index, fails, size) do
    fault =
      if fails > 0,
        do:
          MemoryStore.inject(config,
            kind: :error,
            ops: [:put],
            key: ~r{/0000/},
            times: fails,
            reason: {:http, 503, ""}
          )

    tasks =
      for k <- 1..size do
        n = index * 10 + k
        {n, Task.async(fn -> Alambique.execute(inst, cell, @insert, [n], timeout: 10_000) end)}
      end

    answers = for {n, task} <- tasks, do: {n, Task.await(task, 15_000)}
    if fault, do: MemoryStore.remove(config, fault)
    answers
  end

  # A put that keeps failing and a lease that runs out meanwhile.
  defp lose_lease(inst, cell, config, world) do
    fault =
      MemoryStore.inject(config,
        kind: :error,
        ops: [:put],
        key: ~r{/0000/},
        reason: {:http, 503, ""}
      )

    drain([:alambique, :cell, :upload_retry], cell)
    task = Task.async(fn -> Alambique.execute(inst, cell, @insert, [999], timeout: 10_000) end)
    {_, _} = await_event([:alambique, :cell, :upload_retry], cell)
    ManualClock.advance(world, @ttl + 1_000)
    assert {:error, :outcome_unknown} = Task.await(task, 15_000)
    MemoryStore.remove(config, fault)
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
