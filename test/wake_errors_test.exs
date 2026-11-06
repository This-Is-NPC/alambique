defmodule Alambique.WakeErrorsTest do
  @moduledoc "What a caller sees when the store fails during a wake, and what the wake leaves behind."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Config
  alias AlambiqueTestkit.MemoryStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @commit [:alambique, :cell, :commit]

  setup do
    capture([@commit])
    env = setup_store()
    inst = start_instance(env, migrations: @schema, compaction: false, op_timeout_ms: 300)
    Map.merge(env, %{inst: inst, cell: unique_cell(), dir: Config.fetch!(inst).data_dir})
  end

  test "an unreadable lease fails the call and leaves no cell or marker", env do
    MemoryStore.inject(env.store_config, kind: :error, ops: [:get, :put], key: ~r/lease/)

    assert {:error, reason} =
             Alambique.execute(env.inst, env.cell, "INSERT INTO t (n) VALUES (2)")

    assert reason == {:lease_unreadable, :injected}
    assert Alambique.Cell.whereis(Config.fetch!(env.inst), env.cell) == nil
    eventually(fn -> markers(env.dir) == [] end)
  end

  test "migrations that cannot be uploaded fail the wake and report the commit", env do
    # The fence of a new cell is TXID 1; the migrations follow from TXID 2.
    MemoryStore.inject(env.store_config, kind: :error, ops: [:put], key: ~r/0{15}2-.*\.ltx$/)

    assert {:error, reason} =
             Alambique.execute(env.inst, env.cell, "INSERT INTO t (n) VALUES (2)")

    assert reason == {:migration_sync, :injected}

    assert {_, %{result: :error, reason: :injected}} = await_event(@commit, env.cell)
    assert Alambique.Cell.whereis(Config.fetch!(env.inst), env.cell) == nil
  end

  test "a fence that cannot be uploaded fails the wake of a cell with history", env do
    {:ok, _} = Alambique.execute(env.inst, env.cell, "INSERT INTO t (n) VALUES (1)")
    :ok = Alambique.hibernate(env.inst, env.cell)
    MemoryStore.inject(env.store_config, kind: :error, ops: [:put], key: ~r/\.ltx$/)

    assert {:error, reason} =
             Alambique.execute(env.inst, env.cell, "INSERT INTO t (n) VALUES (2)")

    assert reason == {:fence, :injected}
  end
end
