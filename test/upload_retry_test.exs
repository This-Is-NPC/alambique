defmodule Alambique.UploadRetryTest do
  @moduledoc """
  A batch whose upload fails for a store reason is kept and uploaded again
  while the cell holds its lease (design §6). Only the loss of the lease stops
  the cell. The retry puts the same key and bytes, so it cannot duplicate or
  reorder a batch.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config}
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"
  @ttl 20_000
  @reason {:http, 503, ""}

  setup do
    env = setup_store()
    world = ManualClock.new(1_800_000_000_000)

    inst =
      start_instance(env,
        migrations: @schema,
        compaction: false,
        lease_ttl_ms: @ttl,
        skew_margin_ms: 500,
        op_timeout_ms: 300,
        retry: [max_attempts: 1],
        upload: [retry_backoff_ms: 20, retry_backoff_max_ms: 100],
        pipeline_depth: 2,
        clock: {ManualClock, ManualClock.config(world, 0)}
      )

    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert, [0])
    capture([[:alambique, :cell, :upload_retry], [:alambique, :cell, :stop]])
    %{env: env, inst: inst, cell: cell, world: world}
  end

  # Every segment put fails until `heal/2`.
  defp break(env) do
    {_, config} = env.store

    MemoryStore.inject(config,
      kind: :error,
      ops: [:put],
      key: ~r{/0000/},
      reason: @reason
    )
  end

  defp heal(env, fault), do: MemoryStore.remove(elem(env.store, 1), fault)

  defp write(inst, cell, n, timeout \\ 10_000),
    do: Task.async(fn -> Alambique.execute(inst, cell, @insert, [n], timeout: timeout) end)

  defp txids(env, cell) do
    {mod, config} = env.store
    {:ok, keys, _} = mod.list(config, "cells-test/cells/#{cell}/", nil, 1_000)

    for key <- keys, String.contains?(key, "/0000/") do
      key |> Path.basename() |> String.split("-") |> hd() |> String.to_integer(16)
    end
  end

  defp rows(env, cell),
    do: env |> durable_rows(cell, "SELECT n FROM t ORDER BY n") |> List.flatten()

  test "a write whose uploads fail for longer than one deadline is acknowledged", w do
    pid = Cell.whereis(Config.fetch!(w.inst), w.cell)
    fault = break(w.env)

    first = write(w.inst, w.cell, 1)
    {_, meta} = await_event([:alambique, :cell, :upload_retry], w.cell)
    assert meta.reason == @reason
    assert Task.yield(first, 0) == nil
    assert Process.alive?(pid)

    second = write(w.inst, w.cell, 2)
    heal(w.env, fault)

    assert {:ok, _} = Task.await(first)
    assert {:ok, _} = Task.await(second)
    assert Cell.whereis(Config.fetch!(w.inst), w.cell) == pid
    refute_received {:telemetry, [:alambique, :cell, :stop], _, _}

    ids = txids(w.env, w.cell)
    assert ids == Enum.to_list(1..length(ids))
    assert rows(w.env, w.cell) == [0, 1, 2]
  end

  test "a lost lease ends the retry and the cell", w do
    fault = break(w.env)
    first = write(w.inst, w.cell, 1)
    await_event([:alambique, :cell, :upload_retry], w.cell)

    ManualClock.advance(w.world, @ttl + 1_000)
    assert {:error, :outcome_unknown} = Task.await(first)

    {_, meta} = await_event([:alambique, :cell, :stop], w.cell)
    assert {:sync_failed, _} = meta.reason

    heal(w.env, fault)
    assert rows(w.env, w.cell) == [0]
  end

  test "a caller that stops waiting finds its write durable once the store recovers", w do
    fault = break(w.env)
    assert {:error, :timeout} = Alambique.execute(w.inst, w.cell, @insert, [1], timeout: 200)
    heal(w.env, fault)

    eventually(fn -> rows(w.env, w.cell) == [0, 1] end)
  end

  test "callers beyond max_pending get :busy and the rest are acknowledged", w do
    inst =
      start_instance(w.env,
        migrations: @schema,
        compaction: false,
        op_timeout_ms: 300,
        retry: [max_attempts: 1],
        upload: [retry_backoff_ms: 20, retry_backoff_max_ms: 100],
        awake: [max_pending: 3]
      )

    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert, [0])
    fault = break(w.env)

    tasks = for n <- 1..8, do: write(inst, cell, n)
    await_event([:alambique, :cell, :upload_retry], cell)

    first = Task.yield_many(tasks, 200)
    early = for {_, {:ok, answer}} <- first, do: answer
    assert early != [] and Enum.all?(early, &(&1 == {:error, :busy}))

    heal(w.env, fault)
    pending = for {task, nil} <- first, do: task
    late = for {_, {:ok, answer}} <- Task.yield_many(pending, 5_000), do: answer
    assert length(late) == length(pending)
    assert Enum.all?(late, &match?({:ok, _}, &1))
  end
end
