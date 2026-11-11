defmodule Alambique.ConfigPropertiesTest do
  @moduledoc """
  `Alambique.Config.build/1` accepts every option set inside the documented
  bounds, keeps what it was given and normalizes it idempotently; a value
  outside the bounds raises an `ArgumentError` that names the option.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Cell.LocalCache
  alias Alambique.Config
  alias Alambique.Replication.Chain

  # Options that take any positive integer, by group (`nil`: top level). The
  # options with a relation to another one are checked on their own below.
  @positive [
    {nil, :idle_timeout_ms},
    {nil, :call_timeout_ms},
    {nil, :hint_limit},
    {nil, :migration_window},
    {:routing, :hops},
    {:routing, :attempts},
    {:routing, :slack_ms},
    {:wake, :fence_attempts},
    {:wake, :failure_backoff_ms},
    {:wal, :checkpoint_bytes},
    {:awake, :max_cells},
    {:awake, :max_wakes},
    {:awake, :max_pending},
    {:relocate, :fence_attempts},
    {:relocate, :timeout_ms},
    {:relocate, :poll_ms},
    {:rewind, :fence_attempts},
    {:rewind, :timeout_ms},
    {:rewind, :poll_ms},
    {:rewind, :max_bytes},
    {:schema, :check_page},
    {:schema, :listing_timeout_ms},
    {:restore, :edge_bytes},
    {:restore, :fetch_concurrency},
    {:restore, :prefetch_bytes},
    {:hydrator, :poll_ms},
    {:sql, :statement_timeout_ms},
    {:sql, :script_timeout_ms},
    {:sql, :max_result_rows},
    {:sql, :max_result_bytes},
    {:drain, :concurrency},
    {:drain, :timeout_ms},
    {:drain, :rounds},
    {:drain, :page_size},
    {:drain, :shutdown_ms},
    {:quarantine, :threshold},
    {:quarantine, :window_ms},
    {:quarantine, :startup_timeout_ms},
    {:quarantine, :retry_ms},
    {:quarantine, :concurrency},
    {:list_cells, :timeout_ms},
    {:query, :cache_bytes},
    {:query, :concurrency},
    {:query, :timeout_ms},
    {:query, :page_size},
    {:query, :stale_tmp_seconds},
    {:query, :restore_attempts},
    {:feed, :window_ms},
    {:feed, :prune_interval_ms},
    {:feed, :max_backlog_batches},
    {:replicas, :max_cells},
    {:replicas, :max_bytes},
    {:replicas, :cache_kib},
    {:replicas, :poll_ms},
    {:replicas, :idle_ms},
    {:replicas, :marker_ttl_ms},
    {:replicas, :refresh_timeout_ms},
    {:compaction, :interval_ms},
    {:compaction, :concurrency},
    {:compaction, :queue_limit},
    {:compaction, :max_age_ms},
    {:compaction, :retry_ms},
    {:compaction, :batch_factor},
    {:sweeper, :interval_ms},
    {:sweeper, :concurrency},
    {:sweeper, :rate},
    {:sweeper, :page_size},
    {:sweeper, :max_backoff_ms}
  ]

  # `nil` is a valid value of these: the default, or no limit.
  @nil_ok [
    {:rewind, :max_bytes},
    {:schema, :listing_timeout_ms},
    {:list_cells, :timeout_ms},
    {:query, :concurrency}
  ]

  # Groups that are `false` unless given.
  @off [:replicas, :compaction, :sweeper]

  # The map of a group in the built configuration, to feed back as a keyword.
  @grouped @positive |> Enum.map(&elem(&1, 0)) |> Enum.reject(&is_nil/1) |> Enum.uniq()

  defp base(opts \\ []) do
    Keyword.merge(
      [
        name: :config_properties,
        store: {AlambiqueTestkit.MemoryStore, nil},
        data_dir: "/tmp/config-properties",
        node_id: "config-properties",
        lease_ttl_ms: 30_000,
        skew_margin_ms: 2_000,
        op_timeout_ms: 10_000
      ],
      opts
    )
  end

  defp value, do: integer(1..1_000_000_000)

  # A random subset of the options as `[{group | nil, key, value}]`.
  defp option_set do
    @positive
    |> Enum.map(fn {group, key} = path ->
      gen all(
            use? <- boolean(),
            value <- if(path in @nil_ok, do: one_of([value(), constant(nil)]), else: value())
          ) do
        if use?, do: [{group, key, value}], else: []
      end
    end)
    |> fixed_list()
    |> map(&List.flatten/1)
  end

  defp to_opts(set) do
    {top, grouped} = Enum.split_with(set, fn {group, _, _} -> is_nil(group) end)

    groups =
      grouped
      |> Enum.group_by(&elem(&1, 0), fn {_, key, value} -> {key, value} end)
      |> Enum.to_list()

    base(Enum.map(top, fn {_, key, value} -> {key, value} end) ++ groups)
  end

  property "every option set inside the bounds is accepted and keeps its values" do
    check all(set <- option_set(), max_runs: 200) do
      config = set |> to_opts() |> Config.build()

      for {group, key, value} <- set, value != nil do
        got = if group, do: Map.fetch!(config, group)[key], else: Map.fetch!(config, key)
        assert got == value, "#{inspect(group)} #{inspect(key)}"
      end
    end
  end

  property "building from the built groups gives the same configuration" do
    check all(set <- option_set(), max_runs: 200) do
      config = set |> to_opts() |> Config.build()

      again =
        base(
          for group <- @grouped, value = Map.fetch!(config, group), value != nil do
            {group, Map.to_list(value)}
          end ++
            [
              idle_timeout_ms: config.idle_timeout_ms,
              call_timeout_ms: config.call_timeout_ms,
              hint_limit: config.hint_limit,
              migration_window: config.migration_window
            ]
        )

      # The incarnation is drawn at random by every build.
      assert Map.delete(Config.build(again), :incarnation) == Map.delete(config, :incarnation)
    end
  end

  property "an option left out has the same value whatever else is set" do
    defaults = Config.build(base())

    check all(set <- option_set(), max_runs: 100) do
      given = MapSet.new(set, fn {group, key, _} -> {group, key} end)
      config = set |> to_opts() |> Config.build()

      for {group, key} <- @positive, {group, key} not in given, group not in @off do
        got = if group, do: Map.fetch!(config, group)[key], else: Map.fetch!(config, key)
        want = if group, do: Map.fetch!(defaults, group)[key], else: Map.fetch!(defaults, key)
        assert got != nil and got == want, "#{inspect(group)} #{inspect(key)}"
      end
    end
  end

  property "a value that is not a positive integer is rejected, naming the option" do
    bad =
      one_of([
        one_of([constant(0), constant(-1), integer(-1_000..0)]),
        float(),
        string(:alphanumeric, max_length: 5),
        member_of([:soon, [], %{}, {1}])
      ])

    check all({group, key} <- member_of(@positive), bad <- bad, max_runs: 300) do
      # Options that are `false` by default need their group switched on.
      set = [{group, key, bad}]

      error = assert_raise ArgumentError, fn -> set |> to_opts() |> Config.build() end
      assert error.message =~ "invalid Alambique options"
      assert error.message =~ inspect(key)
      if group in @off, do: assert(error.message =~ inspect(group))
    end
  end

  property "nil is refused by the options that have no default for it" do
    check all({group, key} <- member_of(@positive -- @nil_ok)) do
      error =
        assert_raise ArgumentError, fn -> Config.build(to_opts([{group, key, nil}])) end

      assert error.message =~ inspect(key)
    end
  end

  property "a key that no group knows is rejected, naming it" do
    check all(
            group <- member_of(@grouped),
            key <- string(:alphanumeric, min_length: 3, max_length: 8),
            atom = String.to_atom("zz_" <> key)
          ) do
      error = assert_raise ArgumentError, fn -> Config.build(base([{group, [{atom, 1}]}])) end
      assert error.message =~ "unknown"
      assert error.message =~ inspect(atom)
    end
  end

  describe "bounds that relate options" do
    property "lease_renew_jitter is a number from 0 to 0.5" do
      check all(
              ok <- one_of([float(min: 0.0, max: 0.5), member_of([0, 0.0, 0.5])]),
              low <- float(max: -1.0e-9),
              high <- float(min: 0.5000001)
            ) do
        assert Config.build(base(lease_renew_jitter: ok)).renew_jitter == ok

        for bad <- [low, high] do
          error =
            assert_raise ArgumentError, fn -> Config.build(base(lease_renew_jitter: bad)) end

          assert error.message =~ "lease_renew_jitter"
        end
      end
    end

    property "lease_ttl_ms must exceed twice skew_margin_ms" do
      check all(skew <- integer(0..20), around <- integer(-2..2)) do
        ttl = max(2 * skew + around, 1)
        opts = base(lease_ttl_ms: ttl, skew_margin_ms: skew)

        if ttl > 2 * skew do
          assert %{ttl_ms: ^ttl, skew_margin_ms: ^skew} = Config.build(opts)
        else
          error = assert_raise ArgumentError, fn -> Config.build(opts) end
          assert error.message =~ "lease_ttl_ms"
        end
      end
    end

    property "pipeline_depth is a positive integer up to the pipeline window" do
      window = Chain.window()

      check all(depth <- integer(-5..(window + 20))) do
        if depth in 1..window do
          assert Config.build(base(pipeline_depth: depth)).pipeline_depth == depth
        else
          error = assert_raise ArgumentError, fn -> Config.build(base(pipeline_depth: depth)) end
          assert error.message =~ "pipeline_depth"
        end
      end
    end

    property "feed object_batches is 1 to max_backlog_batches and linger_ms is not negative" do
      check all(
              batches <- integer(-1..8),
              backlog <- integer(1..6),
              linger <- integer(-2..3)
            ) do
        opts = base(feed: [object_batches: batches, max_backlog_batches: backlog])

        if batches in 1..backlog do
          assert Config.build(opts).feed.object_batches == batches
        else
          error = assert_raise ArgumentError, fn -> Config.build(opts) end
          assert error.message =~ "object_batches"
        end

        linger_opts = base(feed: [linger_ms: linger])

        if linger >= 0 do
          assert Config.build(linger_opts).feed.linger_ms == linger
        else
          error = assert_raise ArgumentError, fn -> Config.build(linger_opts) end
          assert error.message =~ "linger_ms"
        end
      end
    end

    property "feed stop_publish_ms is not negative" do
      check all(wait <- integer(-2..3)) do
        opts = base(feed: [stop_publish_ms: wait])

        if wait >= 0 do
          assert Config.build(opts).feed.stop_publish_ms == wait
        else
          error = assert_raise ArgumentError, fn -> Config.build(opts) end
          assert error.message =~ "stop_publish_ms"
        end
      end

      assert Config.build(base([])).feed.stop_publish_ms == 5_000

      error =
        assert_raise ArgumentError, fn -> Config.build(base(feed: [stop_publish_ms: 1.5])) end

      assert error.message =~ "stop_publish_ms"
    end

    test "feed groups nothing by default" do
      assert %{object_batches: 1, linger_ms: 0} = Config.build(base([])).feed
    end

    property "feed shards are 1 to 1000 and the retry ceiling is at least the retry" do
      check all(
              shards <- one_of([integer(-2..3), integer(998..1_003)]),
              retry <- integer(1..6),
              ceiling <- integer(1..6)
            ) do
        shard_opts = base(feed: [shards: shards])

        if shards in 1..1000 do
          assert Config.build(shard_opts).feed.shards == shards
        else
          error = assert_raise ArgumentError, fn -> Config.build(shard_opts) end
          assert error.message =~ "shards"
        end

        retry_opts = base(feed: [publish_retry_ms: retry, publish_retry_max_ms: ceiling])

        if ceiling >= retry do
          feed = Config.build(retry_opts).feed
          assert {feed.publish_retry_ms, feed.publish_retry_max_ms} == {retry, ceiling}
        else
          error = assert_raise ArgumentError, fn -> Config.build(retry_opts) end
          assert error.message =~ "publish_retry_max_ms"
        end
      end
    end

    property "a list_cells page never exceeds the limit" do
      check all(page <- integer(1..8), limit <- integer(1..8)) do
        opts = base(list_cells: [page_size: page, max_limit: limit])

        if page <= limit do
          assert %{page_size: ^page, max_limit: ^limit} = Config.build(opts).list_cells
        else
          error = assert_raise ArgumentError, fn -> Config.build(opts) end
          assert error.message =~ "page_size"
        end
      end
    end

    property "compaction fanout is at least 2 and max_level at most 8" do
      check all(fanout <- integer(-1..6), level <- integer(0..12)) do
        opts = base(compaction: [fanout: fanout, max_level: level])

        bad_fanout? = fanout < 2
        bad_level? = level < 1 or level > 8

        if bad_fanout? or bad_level? do
          expected =
            Enum.join(
              Enum.filter([bad_fanout? && "fanout", bad_level? && "max_level"], & &1),
              "|"
            )

          assert_raise ArgumentError, Regex.compile!(expected), fn -> Config.build(opts) end
        else
          assert %{fanout: ^fanout, max_level: ^level} = Config.build(opts).compaction
        end
      end
    end

    property "local_cache options are positive integers and false switches it off" do
      keys = Keyword.keys(LocalCache.defaults())

      check all(key <- member_of(keys), good <- value(), bad <- integer(-100..0)) do
        assert Config.build(base(local_cache: [{key, good}])).local_cache[key] == good
        assert Config.build(base(local_cache: false)).local_cache == nil

        error =
          assert_raise ArgumentError, fn -> Config.build(base(local_cache: [{key, bad}])) end

        assert error.message =~ inspect(key)
      end
    end
  end

  describe "validate_migrations/1" do
    property "versions 1..n in any order are accepted and returned sorted" do
      check all(n <- integer(0..8), keys <- list_of(integer(), length: n)) do
        versions = keys |> Enum.zip(1..n//1) |> Enum.sort() |> Enum.map(&elem(&1, 1))
        list = Enum.map(versions, &{&1, "SELECT #{&1}"})

        assert {:ok, sorted} = Config.validate_migrations(list)
        assert Enum.map(sorted, &elem(&1, 0)) == Enum.to_list(1..n//1)
      end
    end

    property "a missing version below the last one is a gap" do
      check all(n <- integer(2..8), missing <- integer(1..(n - 1))) do
        list = for v <- 1..n, v != missing, do: {v, "SELECT 1"}

        assert {:error, message} = Config.validate_migrations(list)
        assert message =~ "contiguous"
      end
    end

    property "a repeated version is refused" do
      check all(n <- integer(1..8), repeated <- integer(1..n)) do
        list = Enum.map(1..n, &{&1, "SELECT 1"}) ++ [{repeated, "SELECT 2"}]

        assert {:error, message} = Config.validate_migrations(list)
        assert message =~ "duplicate"
      end
    end

    property "a version that is not positive is refused" do
      check all(n <- integer(0..5), bad <- integer(-5..0)) do
        list = Enum.map(1..n//1, &{&1, "SELECT 1"}) ++ [{bad, "SELECT 1"}]

        assert {:error, message} = Config.validate_migrations(list)
        assert message =~ "positive"
      end
    end

    property "an entry that is not {version, sql | fun/1}, or not a list, is refused" do
      check all(
              junk <-
                one_of([
                  constant({1, 5}),
                  constant({1, fn _, _ -> :ok end}),
                  constant({"1", "SELECT 1"}),
                  constant({1.0, "SELECT 1"}),
                  constant(:migrations),
                  integer()
                ])
            ) do
        assert {:error, message} = Config.validate_migrations([junk])
        error = assert_raise ArgumentError, fn -> Config.build(base(migrations: [junk])) end
        assert error.message =~ message
        assert {:error, ":migrations must be a list"} = Config.validate_migrations(junk)
      end
    end

    property "functions of one argument are valid migrations" do
      check all(n <- integer(1..5)) do
        list = Enum.map(1..n, &{&1, fn _tx -> :ok end})
        assert {:ok, ^list} = Config.validate_migrations(list)
      end
    end
  end
end
