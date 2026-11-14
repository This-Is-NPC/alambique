defmodule Alambique.RestoreRequestsPropertiesTest do
  @moduledoc """
  A restore reads each file of the chain it walks once, plus one more request
  for each file larger than its read window (design §6.3). A lazy restore that
  finds the chain too small to be worth it reads the files it already has
  instead of asking the store again. The budget holds the heads of the chain.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.ReplicationHelpers

  alias Alambique.CountingStore
  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Native, Replicator}
  alias AlambiqueTestkit.MemoryStore

  @edge 8_192
  @cell "cell"
  @prefix "repl-test"

  # A cell with one file per row size.
  defp write_chain(store, dir, rows) do
    ctx = ctx(store, new_world(), timeout_ms: 600_000)
    {conn, state} = new_replicator(dir, "src.db")

    Enum.reduce(Enum.with_index(rows, 1), state, fn {n, k}, state ->
      run_op(conn, {:put, k, n})
      {:ok, state} = Replicator.sync(state, ctx)
      state
    end)

    Alambique.Sqlite.close(conn)
  end

  # Sizes of the files the store holds for the cell.
  defp sizes(config) do
    {:ok, keys, nil} = MemoryStore.list(config, Keys.files_prefix(@prefix, @cell), nil, 100_000)

    for key <- keys do
      {:ok, body, _etag, _meta} = MemoryStore.get(config, key)
      byte_size(body)
    end
  end

  property "requests for a chain are its files plus those larger than the read window" do
    check all(
            rows <- list_of(member_of([10, 3_000, 20_000]), min_length: 1, max_length: 14),
            budget <- member_of([262_144, 67_108_864]),
            concurrency <- member_of([1, 4, 16]),
            min_bytes <- member_of([nil, 0, 4_194_304]),
            max_runs: 30
          ) do
      {_pid, {MemoryStore, config} = inner} = start_store()
      dir = tmp_dir()
      write_chain(inner, dir, rows)
      sizes = sizes(config)

      table = :ets.new(:requests, [:public, :duplicate_bag])
      counting = {CountingStore, %{inner: inner, table: table}}

      ctx =
        Map.merge(ctx(counting, new_world(), timeout_ms: 600_000), %{
          edge_bytes: @edge,
          max_inflight_bytes: budget,
          fetch_concurrency: concurrency
        })

      opts = if min_bytes, do: [lazy: %{min_bytes: min_bytes, wait_ms: 5_000}], else: []
      dest = Path.join(dir, "dest.db")
      assert {:ok, result} = Replication.restore(ctx, @cell, dest, :latest, opts)
      if result[:lazy], do: Native.lazy_release(result.lazy)

      requests = length(:ets.lookup(table, :get)) + length(:ets.lookup(table, :get_range))
      window = min(@edge, budget)
      large = Enum.count(sizes, &(&1 > window))

      assert result.segments == length(sizes)

      assert requests <= length(sizes) + large,
             "#{requests} requests for #{length(sizes)} files, #{large} larger than #{window} " <>
               "(min_bytes #{inspect(min_bytes)}, concurrency #{concurrency}, budget #{budget})"
    end
  end
end
