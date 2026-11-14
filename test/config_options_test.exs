defmodule Alambique.ConfigOptionsTest do
  @moduledoc "Defaults and validation of the operational option groups."
  use ExUnit.Case, async: true

  import Alambique.CellHelpers, only: [setup_store: 0]

  setup do
    env = setup_store()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  defp base(ctx, opts \\ []) do
    Keyword.merge(
      [
        name: :config_options,
        store: ctx.store,
        prefix: "config-options",
        data_dir: Path.join(ctx.dir, "config-options"),
        node_id: "config-options",
        lease_ttl_ms: 3_000,
        skew_margin_ms: 500,
        idle_timeout_ms: 60_000,
        op_timeout_ms: 2_000
      ],
      opts
    )
  end

  test "awake.max_wake_queue is four times max_wakes unless given, and is a positive integer",
       ctx do
    assert Alambique.Config.build(base(ctx, awake: [max_wakes: 7])).awake.max_wake_queue == 28

    for bad <- [0, -1, :many] do
      assert_raise ArgumentError, ~r/max_wake_queue/, fn ->
        Alambique.Config.build(base(ctx, awake: [max_wake_queue: bad]))
      end
    end
  end

  test "the operational groups have documented defaults", ctx do
    config = Alambique.Config.build(base(ctx))

    assert config.routing == %{hops: 2, attempts: 3, slack_ms: 1_000}

    assert config.wake == %{
             fence_attempts: 5,
             failure_backoff_ms: 5_000,
             backoff_ms: 200,
             backoff_max_ms: 10_000,
             backoff_factor: 2,
             backoff_jitter: 0.2
           }

    assert config.wal == %{
             checkpoint_bytes: 67_108_864,
             checkpoint_deadline_ms: 10_000,
             budget_interval_ms: 1_000,
             max_bytes: nil
           }

    assert config.awake == %{
             max_cells: 20_000,
             max_wakes: 256,
             max_pending: 10_000,
             cache_kib: 512,
             wake_queue_ms: 500,
             max_wake_queue: 1_024,
             max_bytes: nil
           }

    assert config.relocate == %{fence_attempts: 5, timeout_ms: 60_000, poll_ms: 100}

    assert config.rewind == %{
             fence_attempts: 5,
             timeout_ms: 60_000,
             poll_ms: 100,
             max_bytes: 268_435_456
           }

    assert config.schema == %{check_page: 1_000, listing_timeout_ms: 10_000}

    assert config.restore == %{
             edge_bytes: 65_536,
             fetch_concurrency: 16,
             max_inflight_bytes: 67_108_864
           }

    assert config.hydrator == %{poll_ms: 100}
    assert config.retry == %{max_attempts: 10}
    assert config.upload == %{retry_backoff_ms: 500, retry_backoff_max_ms: 5_000}
    assert config.breaker == %{failures: 5, cooldown_ms: 5_000, probe_timeout_ms: 10_000}
    assert config.query.page_size == 1_000
    assert config.query.stale_tmp_seconds == 600
    assert config.query.restore_attempts == 3
    assert config.feed.publish_retry_ms == 1_000
    assert config.compaction.urgent_concurrency == 4
    assert config.compaction.max_chain_segments == 256
    assert config.quarantine.concurrency == 8
  end

  test "query concurrency defaults to 16, whatever the schedulers", ctx do
    assert Alambique.Config.build(base(ctx)).query.concurrency == 16
    assert Alambique.Config.build(base(ctx, query: [concurrency: 64])).query.concurrency == 64
  end

  test "query max_statement_share defaults to 0.5 and is above 0 up to 1", ctx do
    assert Alambique.Config.build(base(ctx)).query.max_statement_share == 0.5

    config = Alambique.Config.build(base(ctx, query: [max_statement_share: 1]))
    assert config.query.max_statement_share == 1

    for bad <- [0, 0.0, -0.5, 1.5, "0.5", nil] do
      assert_raise ArgumentError, ~r/max_statement_share/, fn ->
        Alambique.Config.build(base(ctx, query: [max_statement_share: bad]))
      end
    end
  end

  test "lease_renew_jitter defaults to 0.1 and is a fraction up to 0.5", ctx do
    assert Alambique.Config.build(base(ctx)).renew_jitter == 0.1
    assert Alambique.Config.build(base(ctx, lease_renew_jitter: 0)).renew_jitter == 0

    for bad <- [-0.1, 0.6, "0.1", nil] do
      assert_raise ArgumentError, ~r/lease_renew_jitter/, fn ->
        Alambique.Config.build(base(ctx, lease_renew_jitter: bad))
      end
    end
  end

  test "join defaults to true and must be a boolean", ctx do
    assert Alambique.Config.build(base(ctx)).join
    refute Alambique.Config.build(base(ctx, join: false)).join

    assert_raise ArgumentError, ~r/:join must be true or false/, fn ->
      Alambique.Config.build(base(ctx, join: :later))
    end
  end

  test "pipeline_depth is 8 by default and stays within the pipeline window", ctx do
    assert Alambique.Config.build(base(ctx)).pipeline_depth == 8
    assert Alambique.Config.build(base(ctx, pipeline_depth: 16)).pipeline_depth == 16

    for bad <- [0, 17, 2.5, nil] do
      assert_raise ArgumentError, ~r/:pipeline_depth/, fn ->
        Alambique.Config.build(base(ctx, pipeline_depth: bad))
      end
    end
  end

  test "a group accepts values and derives the listing deadline from op_timeout_ms", ctx do
    config =
      Alambique.Config.build(
        base(ctx,
          op_timeout_ms: 1_000,
          routing: [hops: 1, attempts: 1, slack_ms: 20],
          wake: [
            fence_attempts: 2,
            failure_backoff_ms: 50,
            backoff_ms: 20,
            backoff_max_ms: 400,
            backoff_factor: 3,
            backoff_jitter: 0.5
          ],
          wal: [checkpoint_bytes: 1_000],
          awake: [
            max_cells: 7,
            max_wakes: 3,
            max_pending: 9,
            cache_kib: 64,
            wake_queue_ms: 40,
            max_wake_queue: 5,
            max_bytes: 5_000_000
          ],
          relocate: [fence_attempts: 2, timeout_ms: 30_000, poll_ms: 5],
          rewind: [fence_attempts: 3, timeout_ms: 20_000, poll_ms: 7, max_bytes: nil],
          schema: [check_page: 10],
          restore: [edge_bytes: 1_024, fetch_concurrency: 3, max_inflight_bytes: 5_000],
          hydrator: [poll_ms: 10],
          retry: [max_attempts: 3],
          upload: [retry_backoff_ms: 11, retry_backoff_max_ms: 12],
          breaker: [failures: 2, cooldown_ms: 7, probe_timeout_ms: 9],
          query: [page_size: 10, stale_tmp_seconds: 5, restore_attempts: 7],
          feed: [publish_retry_ms: 5],
          compaction: [moved_scan_page: 2],
          quarantine: [concurrency: 2]
        )
      )

    assert config.routing == %{hops: 1, attempts: 1, slack_ms: 20}

    assert config.wake == %{
             fence_attempts: 2,
             failure_backoff_ms: 50,
             backoff_ms: 20,
             backoff_max_ms: 400,
             backoff_factor: 3,
             backoff_jitter: 0.5
           }

    assert config.wal == %{
             checkpoint_bytes: 1_000,
             checkpoint_deadline_ms: 10_000,
             budget_interval_ms: 1_000,
             max_bytes: nil
           }

    assert config.awake == %{
             max_cells: 7,
             max_wakes: 3,
             max_pending: 9,
             cache_kib: 64,
             wake_queue_ms: 40,
             max_wake_queue: 5,
             max_bytes: 5_000_000
           }

    assert config.relocate == %{fence_attempts: 2, timeout_ms: 30_000, poll_ms: 5}

    assert config.rewind == %{
             fence_attempts: 3,
             timeout_ms: 20_000,
             poll_ms: 7,
             max_bytes: nil
           }

    assert config.schema == %{check_page: 10, listing_timeout_ms: 5_000}
    assert config.restore == %{edge_bytes: 1_024, fetch_concurrency: 3, max_inflight_bytes: 5_000}
    assert config.hydrator == %{poll_ms: 10}
    assert config.retry == %{max_attempts: 3}
    assert config.upload == %{retry_backoff_ms: 11, retry_backoff_max_ms: 12}
    assert config.breaker == %{failures: 2, cooldown_ms: 7, probe_timeout_ms: 9}
    assert config.query.page_size == 10
    assert config.query.stale_tmp_seconds == 5
    assert config.query.restore_attempts == 7
    assert config.feed.publish_retry_ms == 5
    assert config.compaction.moved_scan_page == 2
    assert config.quarantine.concurrency == 2
  end

  test "every group rejects an unknown key and a non-positive value", ctx do
    bad = [
      {[routing: [bogus: 1]], ~r/unknown :routing/},
      {[wake: [fence_attempts: 0]], ~r/:wake :fence_attempts/},
      {[relocate: [timeout_ms: -1]], ~r/:relocate :timeout_ms/},
      {[rewind: [fence_attempts: 0]], ~r/:rewind :fence_attempts/},
      {[rewind: [max_bytes: 0]], ~r/:rewind :max_bytes/},
      {[schema: [check_page: 0]], ~r/:schema :check_page/},
      {[schema: [listing_timeout_ms: 0]], ~r/:schema :listing_timeout_ms/},
      {[restore: [edge_bytes: 0]], ~r/:restore :edge_bytes/},
      {[restore: [fetch_concurrency: 0]], ~r/:restore :fetch_concurrency/},
      {[restore: [max_inflight_bytes: 0]], ~r/:restore :max_inflight_bytes/},
      {[hydrator: [poll_ms: 0]], ~r/:hydrator :poll_ms/},
      {[retry: [max_attempts: 0]], ~r/:retry :max_attempts/},
      {[upload: [retry_backoff_ms: 0]], ~r/:upload :retry_backoff_ms/},
      {[upload: [retry_backoff_ms: 9, retry_backoff_max_ms: 8]],
       ~r/retry_backoff_max_ms must be at least/},
      {[breaker: [failures: 0]], ~r/:breaker :failures/},
      {[breaker: [cooldown_ms: 0]], ~r/:breaker :cooldown_ms/},
      {[breaker: [probe_timeout_ms: 0]], ~r/:breaker :probe_timeout_ms/},
      {[query: [page_size: 0]], ~r/:page_size/},
      {[query: [restore_attempts: 0]], ~r/:restore_attempts/},
      {[feed: [publish_retry_ms: 0]], ~r/:publish_retry_ms/},
      {[feed: [internal_prefixes: "_a_"]], ~r/internal_prefixes/},
      {[feed: [internal_prefixes: [""]]], ~r/internal_prefixes/},
      {[feed: [internal_prefixes: [:a]]], ~r/internal_prefixes/},
      {[feed: [tables: [{"a", true}]]], ~r/tables must be a map/},
      {[feed: [tables: %{a: true}]], ~r/names must be non-empty strings/},
      {[feed: [tables: %{"a" => :yes}]], ~r/must be true, false/},
      {[feed: [tables: %{"a" => [omit: "body"]}]], ~r/omit must be a list/},
      {[feed: [tables: %{"a" => [only: [:body]]}]], ~r/only must be a list/},
      {[feed: [tables: %{"a" => [omit: ["b"], only: ["c"]]}]], ~r/must be true, false/},
      {[feed: [other_tables: :some]], ~r/other_tables/},
      {[feed: [compression: 6]], ~r/compression/},
      {[feed: [max_object_bytes: 0]], ~r/max_object_bytes/},
      {[compaction: [urgent_concurrency: -1]], ~r/:urgent_concurrency/},
      {[compaction: [max_chain_segments: 0]], ~r/:max_chain_segments/},
      {[compaction: [fanout: 8, max_chain_segments: 7]],
       ~r/:max_chain_segments must be at least/},
      {[compaction: [delete_superseded: 1]], ~r/:compaction :delete_superseded/},
      {[compaction: [snapshot_ratio: 0]], ~r/:snapshot_ratio/},
      {[compaction: [snapshot_ratio: "1"]], ~r/:snapshot_ratio/},
      {[compaction: [snapshot_keep_ms: -1]], ~r/:snapshot_keep_ms/},
      {[compaction: [snapshot_min_bytes: 0]], ~r/:snapshot_min_bytes/},
      {[feed: [retention_ms: 0]], ~r/retention_ms/},
      {[quarantine: [concurrency: 0]], ~r/:quarantine :concurrency/},
      {[replicas: :on], ~r/:replicas must be false or a keyword list/},
      {[replicas: [colour: 1]], ~r/unknown :replicas options \[:colour\]/},
      {[replicas: [max_cells: 0]], ~r/:replicas :max_cells/},
      {[replicas: [max_bytes: -1]], ~r/:replicas :max_bytes/},
      {[replicas: [cache_kib: 0]], ~r/:replicas :cache_kib/},
      {[replicas: [poll_ms: 0]], ~r/:replicas :poll_ms/},
      {[replicas: [idle_ms: 0]], ~r/:replicas :idle_ms/},
      {[replicas: [poll_max_ms: 0]], ~r/:replicas :poll_max_ms/},
      {[replicas: [marker_ttl_ms: 0]], ~r/:replicas :marker_ttl_ms/},
      {[replicas: [refresh_timeout_ms: 1.5]], ~r/:replicas :refresh_timeout_ms/},
      {[replicas: [on_stale: :never]], ~r/:replicas :on_stale/},
      {[replicas: [drift_ppm: -1]], ~r/:replicas :drift_ppm/}
    ]

    for {opts, message} <- bad do
      assert_raise ArgumentError, message, fn ->
        Alambique.Config.build(base(ctx, opts))
      end
    end
  end

  test "the feed keeps its objects for seven days unless told otherwise", ctx do
    assert Alambique.Config.build(base(ctx)).feed.retention_ms == 604_800_000
    assert Alambique.Config.build(base(ctx, feed: [retention_ms: nil])).feed.retention_ms == nil
    assert Alambique.Config.build(base(ctx, feed: [retention_ms: 5])).feed.retention_ms == 5
  end

  test "the feed captures every table whole unless told otherwise", ctx do
    feed = Alambique.Config.build(base(ctx)).feed
    assert feed.tables == %{}
    assert feed.other_tables == :capture

    feed =
      Alambique.Config.build(
        base(ctx,
          feed: [
            tables: %{
              "m" => [omit: ["z", "body", "z"]],
              "k" => [only: ["b", "a"]],
              "p" => false,
              "q" => true
            },
            other_tables: :skip
          ]
        )
      ).feed

    assert feed.tables == %{
             "m" => {:omit, ["body", "z"]},
             "k" => {:only, ["a", "b"]},
             "p" => :skip,
             "q" => :capture
           }

    assert feed.other_tables == :skip
  end

  test "feed objects inflate to at most 256 MiB", ctx do
    assert Alambique.Config.build(base(ctx)).feed.max_object_bytes == 268_435_456
  end

  test "feed: false captures no table and keeps the host's prefixes for rewinds", ctx do
    on = Alambique.Config.build(base(ctx, feed: [internal_prefixes: ["jobs_"]]))
    assert on.feed_capture
    assert on.feed.internal_prefixes == ["jobs_"]
    assert on.feed_host_prefixes == ["jobs_"]

    off = Alambique.Config.build(base(ctx, feed: false))
    refute off.feed_capture
    assert off.feed.retention_ms == nil
    assert Alambique.Feed.Capture.internal?("notes", off.feed.internal_prefixes)
    assert off.feed_host_prefixes == []
  end

  test "snapshots follow the bytes written, with documented defaults", ctx do
    c = Alambique.Config.build(base(ctx)).compaction
    assert c.snapshot_ratio == 2.0
    assert c.snapshot_min_bytes == 4_194_304
    assert c.snapshot_keep_ms == 259_200_000
    assert c.snapshot_interval_ms == 604_800_000

    c =
      Alambique.Config.build(base(ctx, compaction: [snapshot_ratio: 2, snapshot_keep_ms: 0])).compaction

    assert {c.snapshot_ratio, c.snapshot_keep_ms} == {2, 0}
  end

  test "compaction deletes superseded files by default", ctx do
    assert Alambique.Config.build(base(ctx)).compaction.delete_superseded
    config = Alambique.Config.build(base(ctx, compaction: [delete_superseded: false]))
    refute config.compaction.delete_superseded
  end

  test "replicas are off by default and have documented defaults", ctx do
    assert Alambique.Config.build(base(ctx)).replicas == nil
    refute Alambique.Config.build(base(ctx, replicas: false)).replicas

    assert Alambique.Config.build(base(ctx, replicas: [])).replicas == %{
             max_cells: 32,
             max_bytes: 1_073_741_824,
             cache_kib: 4_096,
             poll_ms: 250,
             poll_max_ms: 5_000,
             idle_ms: 600_000,
             marker_ttl_ms: 5_000,
             on_stale: :refresh,
             refresh_timeout_ms: 2_000,
             drift_ppm: 200
           }

    replicas = [on_stale: :owner, drift_ppm: 0, max_cells: 1]
    config = Alambique.Config.build(base(ctx, replicas: replicas))
    assert %{on_stale: :owner, drift_ppm: 0, max_cells: 1, poll_ms: 250} = config.replicas
  end

  test "registry is off by default and derives its timings from the lease", ctx do
    assert Alambique.Config.build(base(ctx)).nodes == nil

    config = Alambique.Config.build(base(ctx, registry: [advertise: "h:1"]))

    assert config.nodes == %{
             advertise: "h:1",
             ttl_ms: 3_000,
             renew_ms: 1_000,
             refresh_ms: 1_000,
             meta: %{},
             page_size: 100,
             sweep_ms: 3_600_000
           }

    config = Alambique.Config.build(base(ctx, registry: [advertise: "h:1", renew_ms: 400]))
    assert %{renew_ms: 400, refresh_ms: 400} = config.nodes
  end

  test "registry options are validated", ctx do
    bad = fn opts, message ->
      assert_raise ArgumentError, message, fn ->
        Alambique.Config.build(base(ctx, registry: opts))
      end
    end

    bad.([], ~r/:advertise/)
    bad.([advertise: ""], ~r/:advertise/)
    bad.([advertise: "h", meta: [1]], ~r/:meta/)
    bad.([advertise: "h", renew_ms: 3_000], ~r/:renew_ms must be less than :ttl_ms/)
    bad.([advertise: "h", ttl_ms: 1_000], ~r/:ttl_ms must be greater than twice/)
    bad.([advertise: "h", refresh_ms: 0], ~r/:refresh_ms/)
    bad.([advertise: "h", other: 1], ~r/unknown/)
    bad.(true, ~r/keyword list/)
  end
end
