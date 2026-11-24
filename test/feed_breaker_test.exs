defmodule Alambique.FeedBreakerTest do
  @moduledoc """
  The feed has a circuit breaker state of its own (design §3, §13). A store
  that fails only feed puts opens that state and refuses no write, a store that
  fails only segment puts opens the default state and leaves the feed one
  closed, and a store that fails both opens both and recovers.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Feed, Tx}
  alias Alambique.Store.{Breaker, Metered}
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @cell "feed-breaker-cell"

  setup do: setup_store()

  defp start(ctx, prefix, extra \\ []) do
    start_instance(
      ctx,
      [
        prefix: prefix,
        migrations: @schema,
        lease_ttl_ms: 1_000,
        skew_margin_ms: 200,
        idle_timeout_ms: 60_000,
        retry: [max_attempts: 3],
        feed: [
          cleanup: :idle_or_write,
          cleanup_after_ms: 40,
          cleanup_rows: 2,
          hand_over_rows: 2,
          hand_over: :cell,
          publish_retry_ms: 10,
          publish_retry_max_ms: 40,
          stop_publish_ms: 200,
          window_ms: 60_000
        ]
      ] ++ extra
    )
  end

  defp write(inst, id, timeout) do
    Alambique.transaction(
      inst,
      @cell,
      fn tx ->
        {:ok, _} = Tx.execute(tx, "INSERT INTO notes VALUES (?, 'x')", [id])
        :ok
      end,
      timeout: timeout
    )
  end

  defp status(inst), do: Alambique.breaker_status(inst)["default"]
  defp default_state(inst), do: status(inst).state
  defp feed_state(inst), do: status(inst).feed.state

  # Every put whose key matches `key` fails, as a store that answers 503.
  defp fail(ctx, key) do
    MemoryStore.inject(ctx.store_config,
      kind: :error,
      reason: {:http, 503, ""},
      ops: [:put],
      key: key
    )
  end

  defp feed_key(prefix), do: ~r{#{prefix}/feed/}
  defp segment_key(prefix), do: ~r{#{prefix}/cells/[^/]+/0000/}

  # The ids of the rows of the cell on the feed, in the order it gives them.
  defp feed_ids(ctx, prefix) do
    feed = Feed.ctx(store: ctx.store, prefix: prefix, settle_ms: 0)
    {:ok, records, _} = Feed.read(feed, nil, 10_000)
    for %{cell: @cell} = record <- records, do: record.key["id"]
  end

  test "a store that fails only feed puts refuses no write", ctx do
    inst = start(ctx, "fb-feed")
    assert {:ok, :ok} = write(inst, 1, 15_000)

    fail(ctx, feed_key("fb-feed"))
    assert {:ok, :ok} = write(inst, 2, 15_000)
    eventually(fn -> feed_state(inst) != :closed end)

    for id <- 3..6 do
      {us, answer} = :timer.tc(fn -> write(inst, id, 15_000) end)
      assert answer == {:ok, :ok}
      assert us < 2_000_000, "write #{id} took #{div(us, 1_000)} ms"
    end

    assert default_state(inst) == :closed
    assert feed_state(inst) != :closed

    MemoryStore.clear(ctx.store_config)
    assert {:ok, :ok} = write(inst, 7, 15_000)

    eventually(fn -> feed_ids(ctx, "fb-feed") == Enum.to_list(1..7) end, 30_000)
    eventually(fn -> feed_state(inst) == :closed end)
  end

  test "a store that fails both opens both states and they close when it answers", ctx do
    inst = start(ctx, "fb-all", breaker: [failures: 3, cooldown_ms: 500])
    assert {:ok, :ok} = write(inst, 1, 15_000)

    # The feed fails first, so that the sender has an object to put; then the
    # segments fail too.
    fail(ctx, feed_key("fb-all"))
    assert {:ok, :ok} = write(inst, 2, 15_000)
    eventually(fn -> feed_state(inst) != :closed end)

    fail(ctx, segment_key("fb-all"))
    assert {:error, :timeout} = write(inst, 3, 1_500)
    eventually(fn -> default_state(inst) != :closed end)
    assert feed_state(inst) != :closed

    MemoryStore.clear(ctx.store_config)
    assert {:ok, :ok} = write(inst, 4, 15_000)

    eventually(fn -> default_state(inst) == :closed and feed_state(inst) == :closed end, 30_000)

    # The write that timed out may or may not have landed.
    eventually(
      fn ->
        ids = feed_ids(ctx, "fb-all")
        ids -- [3] == [1, 2, 4] and ids == Enum.sort(ids)
      end,
      30_000
    )
  end

  test "a store that fails only segment puts opens the default state alone", ctx do
    capture([[:alambique, :store, :breaker, :open]])
    inst = start(ctx, "fb-seg", breaker: [failures: 3, cooldown_ms: 500])
    assert {:ok, :ok} = write(inst, 1, 15_000)
    eventually(fn -> feed_ids(ctx, "fb-seg") == [1] end)

    fail(ctx, segment_key("fb-seg"))
    assert {:error, :timeout} = write(inst, 2, 1_500)
    eventually(fn -> default_state(inst) != :closed end)

    assert_receive {:telemetry, [:alambique, :store, :breaker, :open], _,
                    %{class: :default, instance: ^inst, location: "default"}}

    assert feed_state(inst) == :closed
    refute_received {:telemetry, [:alambique, :store, :breaker, :open], _, %{class: :feed}}

    MemoryStore.clear(ctx.store_config)
    assert {:ok, :ok} = write(inst, 3, 15_000)

    eventually(fn -> default_state(inst) == :closed end, 30_000)
    assert feed_state(inst) == :closed

    # The write that timed out may or may not have landed.
    eventually(fn -> feed_ids(ctx, "fb-seg") -- [2] == [1, 3] end, 30_000)
  end

  test "breaker_status/1 reports the feed state apart and the events name their class", ctx do
    capture([
      [:alambique, :store, :breaker, :open],
      [:alambique, :store, :breaker, :close],
      [:alambique, :store, :failure]
    ])

    inst = start(ctx, "fb-status", breaker: [failures: 3, cooldown_ms: 300])

    all = Alambique.breaker_status(inst)
    assert Map.keys(all) == ["default"]
    default = all["default"]
    assert %{state: :closed, failures: 0, since_ms: since, feed: feed} = default
    assert %{state: :closed, failures: 0, since_ms: feed_since} = feed
    assert is_integer(since) and is_integer(feed_since)
    assert default |> Map.keys() |> Enum.sort() == [:failures, :feed, :since_ms, :state]
    assert feed |> Map.keys() |> Enum.sort() == [:failures, :since_ms, :state]

    assert {:ok, :ok} = write(inst, 1, 15_000)
    fail(ctx, feed_key("fb-status"))
    assert {:ok, :ok} = write(inst, 2, 15_000)

    assert_receive {:telemetry, [:alambique, :store, :breaker, :open], %{cooldown_ms: 300},
                    %{class: :feed, instance: ^inst, location: "default", reason: :failures}},
                   10_000

    assert_receive {:telemetry, [:alambique, :store, :failure], _,
                    %{class: :feed, operation: :put, counted: true, instance: ^inst}}

    assert %{state: :closed, feed: %{state: open}} = status(inst)
    assert open != :closed

    MemoryStore.clear(ctx.store_config)

    assert_receive {:telemetry, [:alambique, :store, :breaker, :close], _,
                    %{class: :feed, instance: ^inst, location: "default"}},
                   10_000

    refute_received {:telemetry, _, _, %{class: :default}}
  end

  test "scope/2 puts the feed state under a metered store and leaves other stores alone" do
    settings = %{
      failures: 1,
      cooldown_ms: 1_000,
      probe_timeout_ms: 5_000,
      detail_limit: 200,
      log_interval_ms: 10_000
    }

    default = Breaker.new(settings, %{class: :default})
    feed = Breaker.new(settings, %{class: :feed})
    guard = {Alambique.Store.Guard, :none}
    guarded = {Breaker, %{store: guard, breaker: default, feed: feed}}
    metered = Metered.wrap(guarded, Metered.new())

    assert {Breaker, %{store: ^guard, breaker: ^feed}} = Breaker.scope(guarded, :feed)

    assert {Metered, %{store: {Breaker, %{breaker: ^feed}}}} = Breaker.scope(metered, :feed)

    assert Breaker.scope(guard, :feed) == guard

    by_hand = {Breaker, %{store: guard, breaker: default}}
    assert Breaker.scope(by_hand, :feed) == by_hand
  end
end
