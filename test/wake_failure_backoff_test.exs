defmodule Alambique.WakeFailureBackoffTest do
  @moduledoc """
  A wake that fails for a reason a retry reproduces is remembered for
  `wake.failure_backoff_ms`, so repeated calls do not each pay a restore
  (design §7).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Cell.Local
  alias Alambique.CountingStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    env = setup_store()
    counting = CountingStore.wrap(env.store)
    {_, counts} = counting
    Map.merge(env, %{store: counting, counts: counts, inner: env.store})
  end

  # Leaves a cell whose only segment is garbage and whose local file is gone,
  # so that every wake restores and fails.
  defp break(env, inst, cell) do
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (1)")
    :ok = Alambique.hibernate(inst, cell)

    config = Alambique.Config.fetch!(inst)
    File.rm_rf!(Path.dirname(Local.db_path(config, cell)))

    {mod, cfg} = env.inner
    {:ok, [key | _], _} = mod.list(cfg, "cells-test/cells/#{cell}/", nil, 100)
    {:ok, _} = mod.put(cfg, key, "garbage", :none, %{})
    :ok
  end

  # Each call still reads the lease (one get); a wake lists the cell and reads
  # its objects.
  defp cost(env),
    do: CountingStore.count(env.counts, :get) + 10 * CountingStore.count(env.counts, :list)

  test "a cell that cannot be restored is not restored again by every call", env do
    inst = start_instance(env, migrations: @schema, compaction: false)
    cell = unique_cell()
    break(env, inst, cell)

    CountingStore.reset(env.counts)
    assert {:error, reason} = Alambique.query(inst, cell, "SELECT 1")
    one = cost(env)
    assert one > 0

    for _ <- 1..49, do: assert({:error, ^reason} = Alambique.query(inst, cell, "SELECT 1"))
    # Only the lease reads of the 49 calls are left.
    assert cost(env) <= one + 49
  end

  test "the wake is tried again once the backoff passed", env do
    inst =
      start_instance(env, migrations: @schema, compaction: false, wake: [failure_backoff_ms: 100])

    cell = unique_cell()
    break(env, inst, cell)

    CountingStore.reset(env.counts)
    assert {:error, _} = Alambique.query(inst, cell, "SELECT 1")
    one = cost(env)
    Process.sleep(150)
    assert {:error, _} = Alambique.query(inst, cell, "SELECT 1")
    assert cost(env) >= 2 * one
  end

  test "another cell is not affected", env do
    inst = start_instance(env, migrations: @schema, compaction: false)
    broken = unique_cell()
    break(env, inst, broken)
    assert {:error, _} = Alambique.query(inst, broken, "SELECT 1")

    assert {:ok, _} = Alambique.query(inst, unique_cell(), "SELECT 1")
  end
end
