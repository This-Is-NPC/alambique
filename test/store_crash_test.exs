defmodule Alambique.StoreCrashTest do
  @moduledoc """
  A store that raises or exits is a failed request for the cell: it retries
  the renewal while the lease is valid and stops with `:lease_expired` after.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers
  import ExUnit.CaptureLog

  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER)"}]

  setup do
    capture([[:alambique, :lease, :renew], [:alambique, :cell, :stop]])
    setup_store()
  end

  for kind <- [:raise, :exit] do
    test "a store that #{kind}s during renewal and freeze check does not crash the cell", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

      log =
        capture_log(fn ->
          MemoryStore.inject(ctx.store_config, kind: unquote(kind))

          # Renewals fail while the lease is valid: the cell keeps serving.
          await_event([:alambique, :lease, :renew], cell, 5_000, &(&1.result == :error))
          assert Process.alive?(pid)
          assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t")

          # The lease runs out: the cell stops by itself.
          assert {_, %{reason: :lease_expired}} = await_event([:alambique, :cell, :stop], cell)
          eventually(fn -> not Process.alive?(pid) end)
        end)

      refute log =~ "terminating"
    end
  end

  for kind <- [:raise, :exit] do
    test "a store that #{kind}s during a commit answers an error", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")

      log =
        capture_log(fn ->
          MemoryStore.inject(ctx.store_config, kind: unquote(kind), ops: [:put])

          # The write cannot be stored: the caller gets an error and the cell stops.
          assert {:error, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (2)")
          assert {_, %{reason: {:sync_failed, _}}} = await_event([:alambique, :cell, :stop], cell)
        end)

      refute log =~ "terminating"
    end
  end
end
