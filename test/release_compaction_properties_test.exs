defmodule Alambique.ReleaseCompactionPropertiesTest do
  @moduledoc """
  Compaction on release (design §9) is bounded by its deadline whatever the
  tail, and a deadline that cuts it loses no acknowledged write: the lease is
  handed over after at most the deadline plus the merge in flight, and the
  store restores every write.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.SlowStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  # The latency makes the whole compaction of the tail take several times the
  # longest deadline; one merge in flight and the release take well under it.
  @latency_ms 20
  @slack_ms 150

  setup do
    setup_store()
  end

  property "the release ends within its deadline and every acknowledged write is durable", ctx do
    check all(
            writes <- integer(48..56),
            deadline_ms <- integer(20..60),
            max_runs: 6
          ) do
      inst =
        start_instance(%{ctx | store: SlowStore.wrap(ctx.store, @latency_ms)},
          migrations: @schema,
          retention: false,
          compaction: [
            interval_ms: 3_600_000,
            max_age_ms: 3_600_000,
            release_deadline_ms: deadline_ms,
            release_min_tail: 8
          ]
        )

      cell = unique_cell()

      for n <- 1..writes do
        {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])
      end

      started = System.monotonic_time(:millisecond)
      assert :ok = Alambique.hibernate(inst, cell)
      spent = System.monotonic_time(:millisecond) - started

      assert spent <= deadline_ms + @slack_ms,
             "released after #{spent} ms, deadline #{deadline_ms} ms"

      assert durable_rows(ctx, cell, "SELECT count(*) FROM t") == [[writes]]
      stop_supervised(inst)
    end
  end
end
