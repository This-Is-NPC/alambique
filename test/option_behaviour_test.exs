defmodule Alambique.OptionBehaviourTest do
  @moduledoc "Shows that the operational options reach the code that uses them."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [setup_store: 0, start_instance: 2]

  alias Alambique.{Config, Placement, Query, RecordingStore, Schema}
  alias Alambique.Query.Cache
  alias Alambique.Replication.Retry

  setup do
    env = setup_store()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  defp base(ctx, opts) do
    Keyword.merge(
      [
        name: :option_behaviour,
        store: ctx.store,
        prefix: "option-test",
        data_dir: Path.join(ctx.dir, "option-behaviour"),
        node_id: "option-behaviour",
        lease_ttl_ms: 3_000,
        skew_margin_ms: 500,
        idle_timeout_ms: 60_000,
        op_timeout_ms: 2_000
      ],
      opts
    )
  end

  defp lists(acc \\ []) do
    receive do
      {:store_list, prefix, limit} -> lists([{prefix, limit} | acc])
    after
      0 -> acc
    end
  end

  test "a running instance carries the option groups", ctx do
    inst =
      start_instance(ctx,
        routing: [hops: 1, attempts: 2, slack_ms: 10],
        wake: [fence_attempts: 2],
        relocate: [fence_attempts: 2, timeout_ms: 30_000, poll_ms: 5],
        schema: [check_page: 10, listing_timeout_ms: 1_000],
        hydrator: [poll_ms: 10],
        query: [page_size: 10, stale_tmp_seconds: 5],
        feed: [publish_retry_ms: 5],
        compaction: [batch_factor: 2],
        quarantine: [concurrency: 2]
      )

    config = Config.fetch!(inst)
    assert config.routing == %{hops: 1, attempts: 2, slack_ms: 10}
    assert config.wake == %{fence_attempts: 2, failure_backoff_ms: 5_000}
    assert config.relocate == %{fence_attempts: 2, timeout_ms: 30_000, poll_ms: 5}
    assert config.schema == %{check_page: 10, listing_timeout_ms: 1_000}
    assert config.hydrator == %{poll_ms: 10}
    assert config.query.page_size == 10
    assert config.query.stale_tmp_seconds == 5
    assert config.feed.publish_retry_ms == 5
    assert config.compaction.batch_factor == 2
    assert config.quarantine.concurrency == 2
  end

  test "the query page size bounds the listing requests", ctx do
    store = RecordingStore.wrap(ctx.store, self())
    inst = start_instance(ctx, store: store, query: [page_size: 1])

    for i <- 1..3, do: assert({:ok, _} = Alambique.touch(inst, "page-#{i}"))

    _ = lists()
    assert length(Enum.to_list(Query.cells(inst))) == 3

    calls = lists()

    assert Enum.any?(calls, fn {prefix, limit} ->
             String.ends_with?(prefix, "/cells/") and limit == 1
           end)
  end

  test "the schema check page size bounds the per-version listing", ctx do
    store = RecordingStore.wrap(ctx.store, self())

    config =
      Config.build(
        base(ctx,
          store: store,
          migrations: {Alambique.OptionFuns, :migrations},
          schema: [check_page: 2]
        )
      )

    {:ok, [location | _]} = Placement.locations(config)

    for i <- 1..5 do
      assert :ok =
               Schema.put(config, location, "schema-#{i}", 1, Retry.deadline_in(1_000))
    end

    _ = lists()
    assert {:ok, _report} = Schema.check(config)

    calls = lists()

    assert Enum.any?(calls, fn {prefix, limit} ->
             String.contains?(prefix, "/schema/0000000001/") and limit == 2
           end)
  end

  test "temporary replica files use the configured age", ctx do
    dir = Path.join(ctx.dir, "cache")
    {:ok, path} = Cache.tmp(dir, "cell")
    File.write!(path, "x")

    old =
      :calendar.universal_time()
      |> :calendar.datetime_to_gregorian_seconds()
      |> Kernel.-(100)
      |> :calendar.gregorian_seconds_to_datetime()

    File.touch!(path, old)

    assert {0, _} = Cache.trim(dir, 1_000_000, 600)
    assert File.exists?(path)
    assert {1, _} = Cache.trim(dir, 1_000_000, 0)
    refute File.exists?(path)
  end
end
