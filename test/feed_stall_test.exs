defmodule Alambique.FeedStallTest do
  @moduledoc "Feed publication stays off the commit path (perf-1, dos-10)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Feed
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY)"}]

  setup do
    capture([[:alambique, :feed, :publish]])
    setup_store()
  end

  defp feed_ctx(%{store: store}), do: Feed.ctx(store: store, prefix: "cells-test", settle_ms: 0)

  defp ids(ctx) do
    {:ok, records, _} = Feed.read(feed_ctx(ctx), nil, 1000)
    Enum.map(records, & &1.key["id"])
  end

  defp insert(inst, cell, n) do
    {us, {:ok, _}} =
      :timer.tc(fn -> Alambique.execute(inst, cell, "INSERT INTO t VALUES (#{n})") end)

    div(us, 1000)
  end

  defp fast!(inst, cell, n, limit_ms) do
    ms = insert(inst, cell, n)
    assert ms < limit_ms, "commit #{n} took #{ms} ms"
  end

  test "slow feed puts do not slow the commits", ctx do
    inst = start_instance(ctx, migrations: @schema)
    {_, config} = ctx.store
    MemoryStore.inject(config, kind: :latency, ms: 300, ops: [:put], key: ~r{/feed/})
    cell = unique_cell()
    insert(inst, cell, 0)

    for n <- 1..5, do: fast!(inst, cell, n, 150)

    eventually(fn -> ids(ctx) == Enum.to_list(0..5) end, 10_000)
  end

  test "a feed prefix that always fails never blocks the cell, and is published on recovery",
       ctx do
    inst =
      start_instance(ctx,
        migrations: @schema,
        op_timeout_ms: 1_000,
        feed: [publish_retry_ms: 20, publish_retry_max_ms: 100]
      )

    {_, config} = ctx.store

    fault =
      MemoryStore.inject(config,
        kind: :error,
        reason: {:http, 403, ""},
        ops: [:put],
        key: ~r{/feed/}
      )

    cell = unique_cell()

    for n <- 1..5, do: fast!(inst, cell, n, 400)

    assert {_, %{result: :error, reason: {:http, 403, ""}}} =
             await_event([:alambique, :feed, :publish], cell)

    assert ids(ctx) == []
    MemoryStore.remove(config, fault)
    eventually(fn -> ids(ctx) == Enum.to_list(1..5) end, 10_000)
  end

  test "the backlog handed to the publisher is bounded and the rest follows", ctx do
    inst =
      start_instance(ctx,
        migrations: @schema,
        feed: [max_backlog_batches: 2, publish_retry_ms: 20, publish_retry_max_ms: 100]
      )

    {_, config} = ctx.store
    fault = MemoryStore.inject(config, kind: :error, ops: [:put], key: ~r{/feed/})
    cell = unique_cell()
    for n <- 1..6, do: insert(inst, cell, n)

    pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)
    assert %{feed_backlog: 2, feed_capped: true} = :sys.get_state(pid)

    MemoryStore.remove(config, fault)
    eventually(fn -> ids(ctx) == Enum.to_list(1..6) end, 10_000)
  end
end
