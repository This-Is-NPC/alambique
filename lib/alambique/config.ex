defmodule Alambique.Config do
  @moduledoc false
  # Validated options of one instance. The result is kept in `:persistent_term`
  # under the instance name, so every call reads it without a message.

  alias Alambique.Cell.LocalCache
  alias Alambique.{Compaction, Lease, Location, Replication, Retention, Scrub, Trash}

  @defaults [
    join: true,
    clock: {Alambique.Clock.System, nil},
    lease_ttl_ms: 30_000,
    lease_renew_jitter: 0.1,
    skew_margin_ms: 2_000,
    idle_timeout_ms: 300_000,
    op_timeout_ms: 10_000,
    call_timeout_ms: 15_000,
    hint_limit: 10_000,
    migrations: [],
    migration_window: 1,
    moved_grace_ms: 600_000,
    deletion_grace_ms: Trash.default_grace_ms(),
    trash: [],
    scrub: false,
    sweeper: false,
    local_cache: [],
    after_ack: nil,
    pipeline_depth: 8,
    feed: [],
    compaction: [],
    retention: [],
    query: [],
    sql: [],
    routing: [],
    wake: [],
    wal: [],
    awake: [],
    relocate: [],
    rewind: [],
    schema: [],
    restore: [],
    hydrator: [],
    lazy_restore: [],
    drain: [],
    list_cells: [],
    quarantine: [],
    replicas: false
  ]

  @routing_defaults [hops: 2, attempts: 3, slack_ms: 1_000]
  @wake_defaults [fence_attempts: 5, failure_backoff_ms: 5_000]
  @wal_defaults [checkpoint_bytes: 67_108_864]
  @awake_defaults [max_cells: 20_000, max_wakes: 256, max_pending: 10_000, cache_kib: 512]
  @relocate_defaults [fence_attempts: 5, timeout_ms: 60_000, poll_ms: 100]
  @rewind_defaults [fence_attempts: 5, timeout_ms: 60_000, poll_ms: 100, max_bytes: 268_435_456]
  @schema_defaults [check_page: 1_000, listing_timeout_ms: nil]
  @restore_defaults [edge_bytes: 65_536, fetch_concurrency: 16, prefetch_bytes: 67_108_864]
  @hydrator_defaults [poll_ms: 100]

  @compaction_defaults [
    interval_ms: 5_000,
    concurrency: 2,
    queue_limit: 10_000,
    max_age_ms: 300_000,
    retry_ms: 30_000,
    batch_factor: 4,
    hold_bytes: 33_554_432,
    hold_object_bytes: 1_048_576
  ]

  @sweeper_defaults [
    interval_ms: 60_000,
    concurrency: 2,
    rate: 20,
    page_size: 100,
    max_backoff_ms: 600_000
  ]

  @quarantine_defaults [
    threshold: 3,
    window_ms: 3_600_000,
    startup_timeout_ms: 5_000,
    retry_ms: 30_000,
    concurrency: 8
  ]

  @list_cells_defaults [page_size: 100, max_limit: 1_000, timeout_ms: nil]

  @feed_defaults [
    window_ms: 10_000,
    retention_ms: 604_800_000,
    prune_interval_ms: 3_600_000,
    publish_retry_ms: 1_000,
    publish_retry_max_ms: 30_000,
    max_backlog_batches: 256,
    object_batches: 1,
    linger_ms: 0,
    stop_publish_ms: 5_000,
    shards: Alambique.Feed.Keys.default_shards(),
    internal_prefixes: [],
    tables: %{},
    other_tables: :capture,
    max_object_bytes: Alambique.Feed.Record.default_max_bytes()
  ]

  @drain_defaults [
    concurrency: 64,
    timeout_ms: 15_000,
    deadline_ms: 60_000,
    rounds: 3,
    page_size: 1_000,
    shutdown_ms: 75_000
  ]

  @lazy_defaults [
    min_bytes: 4_194_304,
    concurrency: 4,
    batch_bytes: 4_194_304,
    miss_pages: 16
  ]

  @query_defaults [
    cache_bytes: 1_073_741_824,
    concurrency: nil,
    timeout_ms: 30_000,
    page_size: 1_000,
    stale_tmp_seconds: 600,
    restore_attempts: 3,
    reflink: true
  ]

  # `statement_timeout_ms` defaults to `op_timeout_ms`.
  @sql_defaults [
    script_timeout_ms: 60_000,
    max_result_rows: 100_000,
    max_result_bytes: 67_108_864
  ]

  @replicas_defaults [
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
  ]

  @known [:name, :store, :prefix, :placement, :data_dir, :node_id | Keyword.keys(@defaults)]

  @type t :: %{
          name: atom,
          node_id: String.t(),
          incarnation: String.t(),
          join: boolean,
          placement: {module, term},
          data_dir: Path.t(),
          clock: {module, term},
          ttl_ms: pos_integer,
          renew_jitter: float,
          skew_margin_ms: non_neg_integer,
          idle_timeout_ms: pos_integer,
          op_timeout_ms: pos_integer,
          call_timeout_ms: pos_integer,
          hint_limit: pos_integer,
          migrations:
            [{pos_integer, String.t() | (Alambique.Tx.t() -> any)}] | {module, atom, list},
          schema_version: non_neg_integer | nil,
          migration_window: pos_integer,
          moved_grace_ms: non_neg_integer,
          deletion_grace_ms: non_neg_integer,
          trash: map,
          scrub: map | nil,
          sweeper: map | nil,
          local_cache: %{max_bytes: pos_integer, interval_ms: pos_integer} | nil,
          after_ack: (String.t(), pos_integer -> any) | nil,
          pipeline_depth: pos_integer,
          feed_capture: boolean,
          feed_host_prefixes: [String.t()],
          feed: %{
            window_ms: pos_integer,
            retention_ms: pos_integer | nil,
            prune_interval_ms: pos_integer,
            publish_retry_ms: pos_integer,
            publish_retry_max_ms: pos_integer,
            max_backlog_batches: pos_integer,
            object_batches: pos_integer,
            linger_ms: non_neg_integer,
            stop_publish_ms: non_neg_integer,
            shards: pos_integer,
            internal_prefixes: [String.t()],
            tables: %{String.t() => :capture | :skip | {:omit | :only, [String.t()]}},
            other_tables: :capture | :skip,
            max_object_bytes: pos_integer
          },
          compaction: map | nil,
          retention: keyword | nil,
          compactor: atom,
          cleanups: atom,
          holds: atom,
          lazy_restore: map | nil,
          drain: map,
          quarantine: map | nil,
          list_cells: %{
            page_size: pos_integer,
            max_limit: pos_integer,
            timeout_ms: pos_integer
          },
          query: %{
            cache_bytes: pos_integer,
            concurrency: pos_integer,
            timeout_ms: pos_integer,
            page_size: pos_integer,
            stale_tmp_seconds: pos_integer,
            restore_attempts: pos_integer,
            reflink: boolean
          },
          sql: %{
            statement_timeout_ms: pos_integer,
            script_timeout_ms: pos_integer,
            max_result_rows: pos_integer,
            max_result_bytes: pos_integer,
            max_concurrent_statements: pos_integer
          },
          replicas:
            %{
              max_cells: pos_integer,
              max_bytes: pos_integer,
              cache_kib: pos_integer,
              poll_ms: pos_integer,
              poll_max_ms: pos_integer,
              idle_ms: pos_integer,
              marker_ttl_ms: pos_integer,
              on_stale: :refresh | :owner,
              refresh_timeout_ms: pos_integer,
              drift_ppm: non_neg_integer
            }
            | nil,
          routing: %{hops: pos_integer, attempts: pos_integer, slack_ms: pos_integer},
          wake: %{fence_attempts: pos_integer, failure_backoff_ms: pos_integer},
          wal: %{checkpoint_bytes: pos_integer},
          awake: %{
            max_cells: pos_integer,
            max_wakes: pos_integer,
            max_pending: pos_integer,
            cache_kib: pos_integer,
            max_bytes: pos_integer | nil
          },
          relocate: %{fence_attempts: pos_integer, timeout_ms: pos_integer, poll_ms: pos_integer},
          rewind: %{
            fence_attempts: pos_integer,
            timeout_ms: pos_integer,
            poll_ms: pos_integer,
            max_bytes: pos_integer | nil
          },
          schema: %{check_page: pos_integer, listing_timeout_ms: pos_integer},
          restore: %{
            edge_bytes: pos_integer,
            fetch_concurrency: pos_integer,
            prefetch_bytes: pos_integer
          },
          hydrator: %{poll_ms: pos_integer},
          supervisor: atom,
          registry: atom,
          live: atom,
          scope: atom,
          store_identity: String.t(),
          hints: atom,
          drain_status: atom,
          admission: atom,
          locations: atom,
          query_supervisor: atom,
          inflight: atom,
          replica_registry: atom,
          replica_supervisor: atom,
          replica_manager: atom,
          replica_memo: atom
        }

  @doc "Validates `opts` and builds the configuration; raises `ArgumentError` with the reason."
  @spec build(keyword) :: t
  def build(opts) do
    unless Keyword.keyword?(opts), do: bad!("options must be a keyword list")

    case Keyword.keys(opts) -- @known do
      [] -> :ok
      unknown -> bad!("unknown options #{inspect(unknown)}")
    end

    opts = Keyword.merge(@defaults, opts)
    name = fetch_atom!(opts, :name)

    placement = placement!(opts)

    unless match?({mod, _} when is_atom(mod), opts[:clock]),
      do: bad!(":clock must be {module, config}")

    data_dir = required!(opts, :data_dir)
    unless is_binary(data_dir), do: bad!(":data_dir must be a path string")

    ttl = pos_int!(opts, :lease_ttl_ms)
    skew = opts[:skew_margin_ms]

    unless is_integer(skew) and skew >= 0,
      do: bad!(":skew_margin_ms must be a non-negative integer")

    unless ttl > 2 * skew, do: bad!(":lease_ttl_ms must be greater than twice :skew_margin_ms")

    node_id = Keyword.get_lazy(opts, :node_id, fn -> Atom.to_string(node()) end)
    unless is_binary(node_id) and node_id != "", do: bad!(":node_id must be a non-empty string")

    after_ack = opts[:after_ack]

    unless after_ack == nil or is_function(after_ack, 2),
      do: bad!(":after_ack must be a function of (cell, txid)")

    op_timeout = pos_int!(opts, :op_timeout_ms)
    feed = feed!(opts[:feed])
    query = query!(opts[:query])
    migrations = migrations!(opts[:migrations])

    %{
      name: name,
      node_id: node_id,
      incarnation: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false),
      join: join!(opts),
      placement: placement,
      data_dir: data_dir,
      clock: opts[:clock],
      ttl_ms: ttl,
      renew_jitter: jitter!(opts),
      skew_margin_ms: skew,
      idle_timeout_ms: pos_int!(opts, :idle_timeout_ms),
      op_timeout_ms: op_timeout,
      call_timeout_ms: pos_int!(opts, :call_timeout_ms),
      hint_limit: pos_int!(opts, :hint_limit),
      migrations: migrations,
      schema_version: if(is_list(migrations), do: length(migrations)),
      migration_window: pos_int!(opts, :migration_window),
      moved_grace_ms: moved_grace!(opts),
      deletion_grace_ms: deletion_grace!(opts),
      trash: trash!(opts[:trash]),
      scrub: scrub!(opts[:scrub]),
      sweeper: sweeper!(opts[:sweeper]),
      local_cache: local_cache!(opts[:local_cache]),
      after_ack: after_ack,
      pipeline_depth: pipeline_depth!(opts),
      feed: feed,
      feed_capture: opts[:feed] != false,
      feed_host_prefixes: if(opts[:feed] == false, do: [], else: feed.internal_prefixes),
      compaction: compaction!(opts[:compaction]),
      retention: retention!(opts[:retention]),
      query: query,
      sql: sql(opts[:sql], op_timeout),
      routing: options!(:routing, opts[:routing], @routing_defaults),
      wake: options!(:wake, opts[:wake], @wake_defaults),
      wal: options!(:wal, opts[:wal], @wal_defaults),
      awake: awake!(opts[:awake]),
      relocate: options!(:relocate, opts[:relocate], @relocate_defaults),
      rewind: rewind!(opts[:rewind]),
      schema: schema!(opts[:schema], op_timeout),
      restore: options!(:restore, opts[:restore], @restore_defaults),
      hydrator: options!(:hydrator, opts[:hydrator], @hydrator_defaults),
      lazy_restore: lazy_restore!(opts[:lazy_restore]),
      drain: drain!(opts[:drain]),
      quarantine: quarantine!(opts[:quarantine]),
      replicas: replicas!(opts[:replicas]),
      list_cells: list_cells!(opts[:list_cells], op_timeout),
      supervisor: Module.concat(name, CellSupervisor),
      registry: Module.concat(name, Registry),
      live: Module.concat(name, Live),
      scope: Module.concat(name, Scope),
      store_identity: store_identity(placement),
      hints: Module.concat(name, Hints),
      drain_status: Module.concat(name, DrainStatus),
      admission: Module.concat(name, Admission),
      locations: Module.concat(name, Locations),
      compactor: Module.concat(name, Compactor),
      cleanups: Module.concat(name, Cleanups),
      holds: Module.concat(name, Holds),
      query_supervisor: Module.concat(name, QuerySupervisor),
      inflight: Module.concat(name, Inflight),
      replica_registry: Module.concat(name, ReplicaRegistry),
      replica_supervisor: Module.concat(name, ReplicaSupervisor),
      replica_manager: Module.concat(name, Replicas),
      replica_memo: Module.concat(name, ReplicaMemo)
    }
  end

  # Names where the data lives, not how it is reached: the sorted locations of
  # the placement with the location of their stores (design §8). Equal on every
  # node of one deployment whatever its credentials or timeouts, and hashed so
  # that nothing else travels with it.
  @doc false
  @spec store_identity({module, term}) :: String.t()
  def store_identity({mod, opts}) do
    locations =
      case mod.locations(opts) do
        {:ok, [_ | _] = list} -> list
        other -> bad!(":placement locations must be a non-empty list, got #{inspect(other)}")
      end

    named =
      locations
      |> Enum.map(&{&1.id, &1.prefix, Alambique.Store.location(&1.store)})
      |> Enum.sort()

    :sha256
    |> :crypto.hash(:erlang.term_to_binary(named, [:deterministic]))
    |> binary_part(0, 8)
    |> Base.encode16(case: :lower)
  end

  @doc "Defaults of the `:sweeper` option."
  @spec sweeper_defaults() :: keyword
  def sweeper_defaults, do: @sweeper_defaults

  @doc "Configuration of a running instance, or `:error`."
  @spec fetch(atom) :: {:ok, t} | :error
  def fetch(name) do
    case :persistent_term.get({__MODULE__, name}, nil) do
      nil -> :error
      config -> {:ok, config}
    end
  end

  @doc "Configuration of a running instance."
  @spec fetch!(atom) :: t
  def fetch!(name) do
    case fetch(name) do
      {:ok, config} -> config
      :error -> raise ArgumentError, "no Alambique instance named #{inspect(name)} is running"
    end
  end

  @doc false
  def put(config), do: :persistent_term.put({__MODULE__, config.name}, config)

  @doc false
  def delete(name), do: :persistent_term.erase({__MODULE__, name})

  # Every store call of the cell, the compactor and the router goes through a
  # context built here, so the store is guarded once (design §3).
  defp guarded(%Location{store: store}), do: {Alambique.Store.Guard, store}

  @doc "Lease context for the cell data in `location`."
  @spec lease_ctx(t, Location.t()) :: Lease.ctx()
  def lease_ctx(config, %Location{} = location) do
    Lease.ctx(
      store: guarded(location),
      clock: config.clock,
      prefix: location.prefix,
      ttl_ms: config.ttl_ms,
      skew_margin_ms: config.skew_margin_ms,
      incarnation: config.incarnation
    )
  end

  @doc "Replication context for the cell data in `location`."
  @spec repl_ctx(t, Location.t()) :: Replication.ctx()
  def repl_ctx(config, %Location{} = location) do
    Replication.ctx(
      store: guarded(location),
      prefix: location.prefix,
      node: config.node_id,
      clock: config.clock,
      timeout_ms: config.op_timeout_ms,
      edge_bytes: config.restore.edge_bytes,
      fetch_concurrency: config.restore.fetch_concurrency,
      prefetch_bytes: config.restore.prefetch_bytes
    )
  end

  # `:placement` names the module; `:store` and `:prefix` are the shorthand for
  # `Alambique.Placement.Single`.
  defp placement!(opts) do
    placement =
      case {opts[:placement], opts[:store]} do
        {nil, nil} ->
          bad!(":store or :placement is required")

        {nil, store} ->
          {Alambique.Placement.Single,
           [
             store: Alambique.Store.conceal(store),
             prefix: Keyword.get(opts, :prefix, "alambique")
           ]}

        {placement, nil} ->
          if Keyword.has_key?(opts, :prefix), do: bad!(":prefix belongs to the placement options")
          placement

        {_, _} ->
          bad!(":placement replaces :store and :prefix, give one of them")
      end

    check_placement!(placement)
  end

  defp check_placement!({mod, _} = placement) when is_atom(mod) and mod != nil do
    unless Code.ensure_loaded?(mod) and function_exported?(mod, :locate, 2) and
             function_exported?(mod, :locations, 1),
           do: bad!(":placement module #{inspect(mod)} must implement Alambique.Placement")

    with true <- function_exported?(mod, :validate_options, 1),
         {:error, message} <- mod.validate_options(elem(placement, 1)) do
      bad!(message)
    end

    placement
  end

  defp check_placement!(_), do: bad!(":placement must be {module, opts}")

  # `feed: false` captures no table: the instance writes and publishes nothing
  # for the feed, and `Alambique.Feed` reads an empty one. The empty prefix
  # makes every table internal to the capture. `:feed_host_prefixes` stays the
  # host's own list, which rewinds carry over.
  defp feed!(false) do
    %{feed!([]) | retention_ms: nil, internal_prefixes: [""]}
  end

  defp feed!(feed) do
    unless Keyword.keyword?(feed), do: bad!(":feed must be a keyword list")

    case Keyword.keys(feed) -- Keyword.keys(@feed_defaults) do
      [] -> :ok
      unknown -> bad!("unknown :feed options #{inspect(unknown)}")
    end

    feed = Keyword.merge(@feed_defaults, feed)
    retention = feed[:retention_ms]

    unless retention == nil or (is_integer(retention) and retention > 0),
      do: bad!(":feed retention_ms must be nil or a positive integer")

    if pos_int!(feed, :publish_retry_max_ms) < pos_int!(feed, :publish_retry_ms),
      do: bad!(":feed publish_retry_max_ms must be at least publish_retry_ms")

    if pos_int!(feed, :object_batches) > pos_int!(feed, :max_backlog_batches),
      do: bad!(":feed object_batches must be at most max_backlog_batches")

    linger = feed[:linger_ms]

    unless is_integer(linger) and linger >= 0,
      do: bad!(":feed linger_ms must be a non-negative integer")

    stop_publish = feed[:stop_publish_ms]

    unless is_integer(stop_publish) and stop_publish >= 0,
      do: bad!(":feed stop_publish_ms must be a non-negative integer")

    %{
      window_ms: pos_int!(feed, :window_ms),
      retention_ms: retention,
      prune_interval_ms: pos_int!(feed, :prune_interval_ms),
      publish_retry_ms: pos_int!(feed, :publish_retry_ms),
      publish_retry_max_ms: pos_int!(feed, :publish_retry_max_ms),
      max_backlog_batches: pos_int!(feed, :max_backlog_batches),
      object_batches: feed[:object_batches],
      linger_ms: linger,
      stop_publish_ms: stop_publish,
      shards: shards!(feed),
      internal_prefixes: internal_prefixes!(feed[:internal_prefixes]),
      tables: tables!(feed[:tables]),
      other_tables: other_tables!(feed[:other_tables]),
      max_object_bytes: pos_int!(feed, :max_object_bytes)
    }
  end

  defp other_tables!(value) when value in [:capture, :skip], do: value
  defp other_tables!(_), do: bad!(":feed other_tables must be :capture or :skip")

  # `true` and `false` capture the table whole or not at all; `[omit: columns]`
  # captures every column but those, `[only: columns]` the key and those.
  defp tables!(tables) when is_map(tables) do
    Map.new(tables, fn {name, spec} ->
      unless name_ok?(name), do: bad!(":feed tables names must be non-empty strings")
      {name, table_spec!(name, spec)}
    end)
  end

  defp tables!(_), do: bad!(":feed tables must be a map of table name to its selection")

  defp table_spec!(_name, true), do: :capture
  defp table_spec!(_name, false), do: :skip

  defp table_spec!(name, [{kind, columns}]) when kind in [:omit, :only] do
    unless is_list(columns) and Enum.all?(columns, &name_ok?/1),
      do: bad!(":feed tables #{inspect(name)}: #{kind} must be a list of column names")

    {kind, columns |> Enum.uniq() |> Enum.sort()}
  end

  defp table_spec!(name, _spec),
    do:
      bad!(
        ":feed tables #{inspect(name)} must be true, false, [omit: columns] or [only: columns]"
      )

  defp name_ok?(name), do: is_binary(name) and name != "" and String.valid?(name)

  defp internal_prefixes!(prefixes) do
    unless is_list(prefixes) and
             Enum.all?(prefixes, &(is_binary(&1) and &1 != "" and String.valid?(&1))),
           do: bad!(":feed internal_prefixes must be a list of non-empty strings")

    Enum.uniq(prefixes)
  end

  defp shards!(feed) do
    shards = pos_int!(feed, :shards)
    if shards > 1000, do: bad!(":feed shards must be at most 1000")
    shards
  end

  defp compaction!(false), do: nil

  defp compaction!(opts) do
    known = Keyword.keys(@compaction_defaults) ++ Keyword.keys(Compaction.defaults())
    opts = checked_keyword!(:compaction, opts, known)
    merged = Keyword.merge(@compaction_defaults ++ Compaction.defaults(), opts)

    unless is_boolean(merged[:delete_superseded]),
      do: bad!(":compaction :delete_superseded must be true or false")

    ratio = merged[:snapshot_ratio]

    unless is_number(ratio) and ratio > 0,
      do: bad!(":compaction :snapshot_ratio must be a number above 0")

    keep = merged[:snapshot_keep_ms]

    unless is_integer(keep) and keep >= 0,
      do: bad!(":compaction :snapshot_keep_ms must be a non-negative integer")

    for {key, value} <- merged,
        key not in [
          :hold_bytes,
          :hold_object_bytes,
          :delete_superseded,
          :snapshot_ratio,
          :snapshot_keep_ms,
          :pitr_window_ms
        ],
        not (is_integer(value) and value > 0),
        do: bad!(":compaction #{inspect(key)} must be a positive integer")

    for key <- [:hold_bytes, :hold_object_bytes],
        not (is_integer(merged[key]) and merged[key] >= 0),
        do: bad!(":compaction #{inspect(key)} must be a non-negative integer")

    if merged[:fanout] < 2, do: bad!(":compaction :fanout must be at least 2")

    if merged[:max_level] > 8,
      do: bad!(":compaction :max_level must be at most 8 (snapshots are kept at level 9)")

    Map.new(merged)
  end

  defp moved_grace!(opts) do
    value = opts[:moved_grace_ms]

    unless is_integer(value) and value >= 0,
      do: bad!(":moved_grace_ms must be a non-negative integer")

    value
  end

  defp deletion_grace!(opts) do
    value = opts[:deletion_grace_ms]

    unless is_integer(value) and value >= 0,
      do: bad!(":deletion_grace_ms must be a non-negative integer")

    value
  end

  defp trash!(opts) do
    opts = checked_keyword!(:trash, opts, Keyword.keys(Trash.defaults()))
    {reaper, merged} = opts |> then(&Keyword.merge(Trash.defaults(), &1)) |> Keyword.pop(:reaper)

    unless is_boolean(reaper), do: bad!(":trash :reaper must be true or false")

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":trash #{inspect(key)} must be a positive integer")

    merged |> Map.new() |> Map.put(:reaper, reaper)
  end

  defp scrub!(false), do: nil

  defp scrub!(opts) do
    opts = checked_keyword!(:scrub, opts, Keyword.keys(Scrub.defaults()))
    merged = Keyword.merge(Scrub.defaults(), opts)
    {check, merged} = Keyword.pop(merged, :check)
    {prefix, merged} = Keyword.pop(merged, :prefix)

    unless check in [:integrity_check, :quick_check],
      do: bad!(":scrub :check must be :integrity_check or :quick_check")

    unless is_binary(prefix), do: bad!(":scrub :prefix must be a string")

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":scrub #{inspect(key)} must be a positive integer")

    merged |> Map.new() |> Map.merge(%{check: check, prefix: prefix})
  end

  defp local_cache!(false), do: nil
  defp local_cache!(opts), do: options!(:local_cache, opts, LocalCache.defaults())

  defp sweeper!(false), do: nil

  defp sweeper!(opts) do
    opts = checked_keyword!(:sweeper, opts, Keyword.keys(@sweeper_defaults))
    merged = Keyword.merge(@sweeper_defaults, opts)

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":sweeper #{inspect(key)} must be a positive integer")

    Map.new(merged)
  end

  defp retention!(false), do: nil

  defp retention!(opts) do
    opts = checked_keyword!(:retention, opts, Keyword.keys(Retention.defaults()))
    merged = Keyword.merge(Retention.defaults(), opts)

    for {key, value} <- merged,
        not (is_integer(value) and value >= 0),
        do: bad!(":retention #{inspect(key)} must be a non-negative integer")

    merged
  end

  defp drain!(opts) do
    opts = checked_keyword!(:drain, opts, Keyword.keys(@drain_defaults))
    merged = Keyword.merge(@drain_defaults, opts)

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":drain #{inspect(key)} must be a positive integer")

    Map.new(merged)
  end

  # `:timeout_ms` defaults to five store operations, as the other listings.
  defp list_cells!(opts, op_timeout) do
    opts = checked_keyword!(:list_cells, opts, Keyword.keys(@list_cells_defaults))
    merged = Keyword.merge(@list_cells_defaults, opts)

    for {key, value} <- merged,
        not (is_integer(value) and value > 0) and not (key == :timeout_ms and is_nil(value)),
        do: bad!(":list_cells #{inspect(key)} must be a positive integer")

    if merged[:page_size] > merged[:max_limit],
      do: bad!(":list_cells :page_size must not exceed :max_limit")

    merged |> Keyword.update!(:timeout_ms, &(&1 || 5 * op_timeout)) |> Map.new()
  end

  defp quarantine!(false), do: nil

  defp quarantine!(opts) do
    opts = checked_keyword!(:quarantine, opts, Keyword.keys(@quarantine_defaults))
    merged = Keyword.merge(@quarantine_defaults, opts)

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":quarantine #{inspect(key)} must be a positive integer")

    Map.new(merged)
  end

  defp replicas!(false), do: nil

  defp replicas!(opts) do
    opts = checked_keyword!(:replicas, opts, Keyword.keys(@replicas_defaults))
    merged = Keyword.merge(@replicas_defaults, opts)
    {on_stale, merged} = Keyword.pop(merged, :on_stale)
    {drift, merged} = Keyword.pop(merged, :drift_ppm)

    unless on_stale in [:refresh, :owner],
      do: bad!(":replicas :on_stale must be :refresh or :owner")

    unless is_integer(drift) and drift >= 0,
      do: bad!(":replicas :drift_ppm must be a non-negative integer")

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":replicas #{inspect(key)} must be a positive integer")

    merged |> Map.new() |> Map.merge(%{on_stale: on_stale, drift_ppm: drift})
  end

  defp lazy_restore!(false), do: nil

  defp lazy_restore!(opts) do
    opts = checked_keyword!(:lazy_restore, opts, Keyword.keys(@lazy_defaults))
    merged = Keyword.merge(@lazy_defaults, opts)

    for {key, value} <- merged,
        not (is_integer(value) and (value > 0 or (key == :min_bytes and value == 0))),
        do: bad!(":lazy_restore #{inspect(key)} must be a positive integer")

    Map.new(merged)
  end

  defp checked_keyword!(name, opts, known) do
    unless Keyword.keyword?(opts), do: bad!(":#{name} must be false or a keyword list")

    case Keyword.keys(opts) -- known do
      [] -> opts
      unknown -> bad!("unknown #{inspect(name)} options #{inspect(unknown)}")
    end
  end

  defp query!(query) do
    unless Keyword.keyword?(query), do: bad!(":query must be a keyword list")

    case Keyword.keys(query) -- Keyword.keys(@query_defaults) do
      [] -> :ok
      unknown -> bad!("unknown :query options #{inspect(unknown)}")
    end

    query = Keyword.merge(@query_defaults, query)
    query = Keyword.update!(query, :concurrency, &(&1 || default_query_concurrency()))

    %{
      cache_bytes: pos_int!(query, :cache_bytes),
      concurrency: pos_int!(query, :concurrency),
      timeout_ms: pos_int!(query, :timeout_ms),
      page_size: pos_int!(query, :page_size),
      stale_tmp_seconds: pos_int!(query, :stale_tmp_seconds),
      restore_attempts: pos_int!(query, :restore_attempts),
      reflink: bool!(query, :reflink)
    }
  end

  # Statements of callers run on the dirty IO schedulers, which the commits of
  # the node share; the default leaves two for them, like the query default.
  defp sql(opts, op_timeout) do
    options!(
      :sql,
      opts,
      [statement_timeout_ms: op_timeout, max_concurrent_statements: default_statement_slots()] ++
        @sql_defaults
    )
  end

  defp default_statement_slots, do: max(:erlang.system_info(:dirty_io_schedulers) - 2, 1)

  # Every SQLite call of a query runs on a dirty IO scheduler. Leaving two of
  # them free keeps the commits of the cells on this node from queueing behind
  # a cross-cell query.
  defp default_query_concurrency do
    max(min(System.schedulers_online(), :erlang.system_info(:dirty_io_schedulers) - 2), 1)
  end

  defp options!(name, opts, defaults) do
    opts = checked_keyword!(name, opts, Keyword.keys(defaults))
    merged = Keyword.merge(defaults, opts)

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":#{name} #{inspect(key)} must be a positive integer")

    Map.new(merged)
  end

  # `max_bytes: nil` (the default) leaves the memory of the awake cells
  # unbounded.
  defp awake!(opts) do
    opts = checked_keyword!(:awake, opts, [:max_bytes | Keyword.keys(@awake_defaults)])
    {max_bytes, rest} = Keyword.pop(opts, :max_bytes)

    unless is_nil(max_bytes) or (is_integer(max_bytes) and max_bytes > 0),
      do: bad!(":awake :max_bytes must be nil or a positive integer")

    options!(:awake, rest, @awake_defaults) |> Map.put(:max_bytes, max_bytes)
  end

  # `max_bytes: nil` turns the size limit of a rewind off.
  defp rewind!(opts) do
    opts = checked_keyword!(:rewind, opts, Keyword.keys(@rewind_defaults))

    case Keyword.pop(opts, :max_bytes, :default) do
      {nil, rest} ->
        Map.put(
          options!(:rewind, rest, Keyword.delete(@rewind_defaults, :max_bytes)),
          :max_bytes,
          nil
        )

      {:default, rest} ->
        options!(:rewind, rest, @rewind_defaults)

      {max, rest} ->
        options!(:rewind, [max_bytes: max] ++ rest, @rewind_defaults)
    end
  end

  # `:listing_timeout_ms` defaults to five store operations, as `:list_cells`.
  defp schema!(opts, op_timeout) do
    opts = checked_keyword!(:schema, opts, Keyword.keys(@schema_defaults))

    merged =
      opts
      |> then(&Keyword.merge(@schema_defaults, &1))
      |> Keyword.update!(:listing_timeout_ms, &(&1 || 5 * op_timeout))

    for {key, value} <- merged,
        not (is_integer(value) and value > 0),
        do: bad!(":schema #{inspect(key)} must be a positive integer")

    Map.new(merged)
  end

  defp migrations!(module) when is_atom(module) and module not in [nil, []] do
    unless Code.ensure_loaded?(module) and function_exported?(module, :migrations, 0),
      do: bad!(":migrations module #{inspect(module)} must export migrations/0")

    migrations!(module.migrations())
  end

  # Per-cell migrations have one internal form, `{module, function, extra_args}`.
  defp migrations!({module, function}) when is_atom(module) and is_atom(function),
    do: migrations!({module, function, []})

  defp migrations!({module, function, args} = per_cell)
       when is_atom(module) and is_atom(function) and is_list(args) do
    arity = 1 + length(args)

    unless Code.ensure_loaded?(module) and function_exported?(module, function, arity),
      do: bad!(":migrations #{inspect(module)}.#{function}/#{arity} must be exported")

    per_cell
  end

  defp migrations!(list) when is_list(list) do
    case validate_migrations(list) do
      {:ok, sorted} -> sorted
      {:error, message} -> bad!(message)
    end
  end

  defp migrations!(_),
    do: bad!(":migrations must be a module, a {module, function[, extra_args]} or a list")

  @doc """
  Checks a list of migrations: `{version, sql | fun/1}` with unique, positive
  versions contiguous from 1 (`[]` is allowed). Returns it sorted by version.
  """
  @spec validate_migrations(term) :: {:ok, [{pos_integer, term}]} | {:error, String.t()}
  def validate_migrations(list) when is_list(list) do
    valid? =
      Enum.all?(list, fn
        {v, sql} when is_integer(v) and v > 0 and is_binary(sql) -> true
        {v, fun} when is_integer(v) and v > 0 and is_function(fun, 1) -> true
        _ -> false
      end)

    # An entry that is not a tuple has no version to read.
    entries = if valid?, do: list, else: []
    versions = Enum.map(entries, &elem(&1, 0))
    sorted = Enum.sort_by(entries, &elem(&1, 0))

    cond do
      not valid? ->
        {:error, ":migrations must be {version, sql | fun/1} with positive integer versions"}

      versions != Enum.uniq(versions) ->
        {:error, ":migrations has a duplicate version"}

      Enum.map(sorted, &elem(&1, 0)) != Enum.to_list(1..length(sorted)//1) ->
        {:error, ":migrations must have contiguous versions starting at 1"}

      true ->
        {:ok, sorted}
    end
  end

  def validate_migrations(_), do: {:error, ":migrations must be a list"}

  @doc """
  The migrations of `cell`: the instance's list, or the result of
  `apply(module, function, [cell | extra_args])` for a per-cell instance,
  validated like the list. A failure is `{:error, {:bad_migrations, message}}`.
  """
  @spec migrations_for(t, String.t()) ::
          {:ok, [{pos_integer, String.t() | (Alambique.Tx.t() -> any)}]}
          | {:error, {:bad_migrations, String.t()}}
  def migrations_for(%{migrations: list}, _cell) when is_list(list), do: {:ok, list}

  def migrations_for(%{migrations: {module, function, args}}, cell) do
    with {:ok, list} <- call_migrations(module, function, [cell | args]),
         {:ok, _sorted} = ok <- validate_migrations(list) do
      ok
    else
      {:error, message} when is_binary(message) ->
        arity = 1 + length(args)

        {:error,
         {:bad_migrations, "#{inspect(module)}.#{function}/#{arity} for #{cell}: #{message}"}}
    end
  end

  defp call_migrations(module, function, args) do
    {:ok, apply(module, function, args)}
  catch
    kind, reason -> {:error, "failed: " <> Exception.format_banner(kind, reason)}
  end

  defp required!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> bad!("#{inspect(key)} is required")
    end
  end

  defp fetch_atom!(opts, key) do
    value = Keyword.get(opts, key, Alambique)
    unless is_atom(value) and value != nil, do: bad!("#{inspect(key)} must be an atom")
    value
  end

  defp join!(opts) do
    join = opts[:join]
    unless is_boolean(join), do: bad!(":join must be true or false")
    join
  end

  defp jitter!(opts) do
    jitter = opts[:lease_renew_jitter]

    unless is_number(jitter) and jitter >= 0 and jitter <= 0.5,
      do: bad!(":lease_renew_jitter must be a number from 0 to 0.5")

    jitter
  end

  defp pos_int!(opts, key) do
    value = opts[key]
    unless is_integer(value) and value > 0, do: bad!("#{inspect(key)} must be a positive integer")
    value
  end

  defp bool!(opts, key) do
    value = opts[key]
    unless is_boolean(value), do: bad!("#{inspect(key)} must be true or false")
    value
  end

  defp pipeline_depth!(opts) do
    depth = pos_int!(opts, :pipeline_depth)
    max = Replication.Chain.window()
    if depth > max, do: bad!(":pipeline_depth must be at most #{max}")
    depth
  end

  defp bad!(message), do: raise(ArgumentError, "invalid Alambique options: #{message}")
end
