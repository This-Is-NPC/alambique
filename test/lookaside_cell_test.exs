defmodule Alambique.LookasideCellTest do
  @moduledoc "The instance's lookaside keys reach the connection of each awake cell."
  # The memory counter is node-wide, so the test runs alone.
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.NativeStats

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @cells 20

  setup do
    setup_store()
  end

  # Bytes SQLite holds more after `@cells` cells wake.
  defp held_by_cells(ctx, awake) do
    inst = start_instance(ctx, migrations: @schema, awake: awake)
    before = NativeStats.read().sqlite_memory_used

    for _ <- 1..@cells do
      assert {:ok, _} = Alambique.execute(inst, unique_cell(), "INSERT INTO t VALUES (1)")
    end

    NativeStats.read().sqlite_memory_used - before
  end

  test "a cell without lookaside holds the memory of its slots less", ctx do
    default = held_by_cells(ctx, [])
    none = held_by_cells(ctx, lookaside_bytes: 0, lookaside_count: 0)
    small = held_by_cells(ctx, lookaside_bytes: 512, lookaside_count: 16)

    # SQLite's default is 1200 bytes by 40 slots on every connection.
    assert default - none >= @cells * 40_000
    assert default - small >= @cells * 30_000
    assert small - none >= @cells * 6_000
  end
end
