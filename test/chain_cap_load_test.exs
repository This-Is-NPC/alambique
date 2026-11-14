defmodule Alambique.ChainCapLoadTest do
  @moduledoc """
  The chain a restore walks stays near `max_chain_segments` while busy cells
  commit continuously over a store with latency (design §9, "Chain cap").
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Compaction.Plan
  alias Alambique.Replication.Keys
  alias Alambique.SlowStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @cap 48
  @cells 12
  @commits 400

  setup do
    setup_store()
  end

  # What a restore walks: the files from the newest snapshot, the way it picks them.
  defp walked(ctx, cell) do
    prefix = Keys.files_prefix("cells-test", cell)
    {:ok, keys, nil} = AlambiqueTestkit.MemoryStore.list(ctx.store_config, prefix, nil, 100_000)
    {segments, snapshots} = Keys.parse("cells-test", cell, keys)
    from = snapshots |> List.last() |> then(&((&1 && &1.txid) || 0))
    segments |> Plan.index() |> Plan.chain(from) |> length()
  end

  defp sample(ctx, cells, max) do
    receive do
      {:stop, from} -> send(from, {:max, max})
    after
      50 -> sample(ctx, cells, Enum.max([max | Enum.map(cells, &walked(ctx, &1))]))
    end
  end

  # The 25 ms round trip of the field report, 12 cells that commit back to
  # back (about 30 commits per second each, against the ~8 of the report,
  # which also kept a few cells much hotter than the rest).
  test "no chain walks much past the cap under continuous commits", ctx do
    store = SlowStore.wrap(ctx.store, 25)

    inst =
      start_instance(%{ctx | store: store},
        migrations: @schema,
        retention: false,
        compaction: [max_chain_segments: @cap]
      )

    cells = for _ <- 1..@cells, do: unique_cell()
    me = self()
    sampler = spawn_link(fn -> sample(ctx, cells, 0) end)

    writers =
      for {cell, i} <- Enum.with_index(cells) do
        Task.async(fn ->
          for n <- 1..@commits do
            {:ok, _} =
              Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [
                i * 1_000_000 + n
              ])
          end
        end)
      end

    Task.await_many(writers, 120_000)
    send(sampler, {:stop, me})

    assert_receive {:max, max}, 5_000
    assert max > 0
    assert max <= 2 * @cap, "a restore would walk #{max} files, cap #{@cap}"
  end
end
