defmodule Alambique.WakeCostTest do
  @moduledoc """
  `[:alambique, :cell, :wake]` reports the time the wake spent in store
  requests, summed, and the reductions the cell process ran (design §7).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.SlowStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @latency_ms 20

  setup do
    setup_store()
  end

  test "a wake sums the time of its store requests and counts its reductions", ctx do
    inst =
      start_instance(%{ctx | store: SlowStore.wrap(ctx.store, @latency_ms)}, migrations: @schema)

    capture([[:alambique, :cell, :wake]])
    cell = unique_cell()
    assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (1)", [])

    {wake, %{result: :ok}} = await_event([:alambique, :cell, :wake], cell)
    latency = System.convert_time_unit(@latency_ms, :millisecond, :native)

    # Every request took at least the latency, and the sum counts each of them
    # even when they overlap.
    assert wake.trips > 1
    assert wake.store_time >= wake.trips * latency
    assert wake.reductions > 0
  end
end
