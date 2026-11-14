defmodule Alambique.CompactorDispatchTest do
  @moduledoc """
  A cell at the chain cap gets a pass as soon as a slot is free, however long
  the passes already running take (design §9, "Chain cap").
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias AlambiqueTestkit.CrashPoints

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @compaction [
    interval_ms: 600_000,
    fanout: 2,
    max_chain_segments: 4,
    concurrency: 1,
    urgent_concurrency: 3,
    max_merges: 1_000,
    snapshot_interval_txids: 1_000
  ]

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # Every pass stops once, after its first merge, until the test releases it.
  defp gate_passes do
    test = self()

    CrashPoints.arm(
      :compact_between_merges,
      fn _point, %{cell: cell} ->
        unless Process.put(:gated, true) do
          send(test, {:gated, cell, self()})
          receive do: (:release -> :ok)
        end
      end,
      times: :infinity
    )
  end

  defp write(inst, cell, count) do
    for n <- 1..count do
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])
    end
  end

  test "cells at the cap start passes up to the slots while others are slow", ctx do
    gate_passes()
    inst = start_instance(ctx, migrations: @schema, compaction: @compaction, retention: false)
    cells = for _ <- 1..5, do: unique_cell()
    {first, [last]} = Enum.split(cells, 4)

    # One cell at a time: each reaches the cap while the earlier passes still run.
    gated =
      for {cell, i} <- Enum.with_index(first, 1) do
        write(inst, cell, 4)

        assert_receive {:gated, ^cell, pid},
                       2_000,
                       "no pass started for cell #{i} of 4 at the cap"

        pid
      end

    # The slots (1 + 3) are full: the fifth cell waits.
    write(inst, last, 4)
    refute_receive {:gated, ^last, _}, 300

    # A slot that frees goes to the cell that waits, with no tick.
    send(hd(gated), :release)
    assert_receive {:gated, ^last, last_pid}, 2_000

    for pid <- [last_pid | tl(gated)], do: send(pid, :release)
  end
end
