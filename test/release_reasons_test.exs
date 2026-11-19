defmodule Alambique.ReleaseReasonsTest do
  @moduledoc """
  Which hibernations compact the tail first: the reasons of
  `compaction: [release_on:]` (design §9, "Compaction on release").
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Compaction.Plan
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @rows 60
  @idle_compactor [interval_ms: 3_600_000, max_age_ms: 3_600_000]
  @event [:alambique, :cell, :hibernate]
  @all [:request, :shutdown, :evict, :idle, :frozen]

  setup do
    capture([@event])
    setup_store()
  end

  defp writer(ctx, compaction, extra \\ []) do
    start_instance(
      ctx,
      Keyword.merge(
        [
          migrations: @schema,
          retention: false,
          # A release compacts only for what the lease has left, and a frozen
          # cell is found at a renewal, a third of the ttl after the last one.
          lease_ttl_ms: 3_000,
          skew_margin_ms: 100,
          compaction: @idle_compactor ++ [release_min_tail: 8] ++ compaction
        ],
        extra
      )
    )
  end

  defp fill(inst, cell) do
    for n <- 1..@rows,
        do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])
  end

  # What a restore walks: the level-0 files after the newest snapshot.
  defp tail_length(ctx, cell) do
    prefix = Keys.files_prefix("cells-test", cell)
    {:ok, keys, nil} = MemoryStore.list(ctx.store_config, prefix, nil, 100_000)
    {segments, snapshots} = Keys.parse("cells-test", cell, keys)
    from = snapshots |> List.last() |> then(&((&1 && &1.txid) || 0))
    segments |> Plan.index() |> Plan.chain(from) |> Enum.count(&(&1.level == 0))
  end

  # An instance that releases `cell` for `reason`, given the `release_on` list
  # (`nil`: the default).
  defp instance_for(reason, ctx, on) do
    compaction = if on, do: [release_on: on], else: []

    case reason do
      :idle -> writer(ctx, compaction, idle_timeout_ms: 300)
      # Room for one awake cell: waking another hibernates this one.
      :evict -> writer(ctx, compaction, awake: [max_cells: 1])
      _ -> writer(ctx, compaction)
    end
  end

  # Makes the instance release `cell` for `reason`.
  defp release(:request, _ctx, inst, cell) do
    fill(inst, cell)
    assert :ok = Alambique.hibernate(inst, cell)
  end

  defp release(:idle, _ctx, inst, cell), do: fill(inst, cell)

  # The supervisor stops the cell without a drain, as it does for the cells a
  # stopping instance did not drain in time.
  defp release(:shutdown, _ctx, inst, cell) do
    fill(inst, cell)
    pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)
    :ok = DynamicSupervisor.terminate_child(Module.concat(inst, CellSupervisor), pid)
  end

  defp release(:evict, _ctx, inst, cell) do
    fill(inst, cell)
    assert {:ok, _} = Alambique.execute(inst, unique_cell(), "SELECT 1", [])
  end

  # What `mix alambique.freeze` does: the cell finds the marker at its next renewal.
  defp release(:frozen, ctx, inst, cell) do
    fill(inst, cell)

    {:ok, _} =
      MemoryStore.put(ctx.store_config, Keys.frozen("cells-test", cell), "{}", :none, %{})

    inst
  end

  for reason <- [:request, :shutdown, :idle, :evict, :frozen] do
    describe "a hibernation for #{inspect(reason)}" do
      test "compacts the tail when the reason is in release_on", ctx do
        reason = unquote(reason)
        inst = instance_for(reason, ctx, [reason])
        cell = unique_cell()
        release(reason, ctx, inst, cell)

        assert {%{tail: tail}, %{reason: ^reason, compaction: :done}} =
                 await_event(@event, cell, 10_000)

        assert tail < 8
        assert tail == tail_length(ctx, cell)
      end

      test "leaves the chain alone when the reason is left out", ctx do
        reason = unquote(reason)
        inst = instance_for(reason, ctx, @all -- [reason])
        cell = unique_cell()
        release(reason, ctx, inst, cell)

        assert {%{tail: 0}, %{reason: ^reason, compaction: :none}} =
                 await_event(@event, cell, 10_000)

        assert tail_length(ctx, cell) >= @rows
      end
    end
  end

  test "by default idle and eviction compact and a frozen cell does not", ctx do
    for reason <- [:shutdown, :idle, :evict, :frozen] do
      inst = instance_for(reason, ctx, nil)
      cell = unique_cell()
      release(reason, ctx, inst, cell)
      assert {_, %{reason: ^reason, compaction: compaction}} = await_event(@event, cell, 10_000)
      assert compaction == if(reason == :frozen, do: :none, else: :done)
    end
  end
end
