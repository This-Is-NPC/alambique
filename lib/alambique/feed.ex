defmodule Alambique.Feed do
  @moduledoc """
  Reads the change feed (design §13): every row change committed in any cell,
  published without waking the cell.

  The feed of a location is spread over `shards` prefixes by a hash of the
  cell id (design §19). A feed position is an opaque string that marks how far
  a consumer has read in every location and shard. `nil` is the beginning.
  Store the position that `read/3` returns after processing the records, and
  pass it back to continue:

      ctx = Alambique.Feed.ctx(MyApp.Cells)
      {:ok, records, position} = Alambique.Feed.read(ctx, saved_position, 100)

  Delivery is at least once: an owner change or a lost acknowledgement can
  publish the same batch twice. `Alambique.Feed.Record.id` is the idempotency
  key. Records of one cell come in commit order, also across a relocation of
  the cell to another location; across cells they come in publication time
  order. A reader only sees objects older than `:settle_ms`,
  so an object that is still being written cannot appear behind the position.

  A cell that was rewound (`Alambique.rewind/4`) puts an
  `Alambique.Feed.Rewind` among the records: read its documentation before
  you consume a feed in which cells can be rewound.

  An object that cannot be decoded (a faulty or hostile writer of the store)
  comes as one `Alambique.Feed.Rejected` in place of its records, and the
  position moves past it, so it never stops a consumer.

  Feed objects are removed after the retention of the instance
  (`Alambique` option `:feed`); a consumer that was away longer starts at the
  oldest object left.
  """

  alias Alambique.{Location, Placement, Store, Telemetry}
  alias Alambique.Feed.{Keys, Record, Rejected, Rewind}
  alias Alambique.Replication.Retry

  @typedoc "An opaque string that marks how far every shard of every location was read."
  @type position :: String.t() | nil

  @typedoc """
    * `:locations` - the `Alambique.Location`s to read
    * `:shards` - feed shards per location; the value of the publishers
    * `:clock` - `{module, config}` implementing `Alambique.Clock`
    * `:settle_ms` - objects younger than this are not read yet
    * `:max_object_bytes` - the most a compressed object may inflate to; a
      larger one is rejected
    * `:timeout_ms` - the longest one store request of the consumer may take; it
      is killed after that and answers `{:error, :timeout}`
  """
  @type ctx :: %{
          locations: [Location.t()],
          shards: pos_integer,
          clock: {module, term},
          settle_ms: non_neg_integer,
          max_object_bytes: pos_integer,
          timeout_ms: pos_integer
        }

  @doc """
  Builds the context of a consumer.

  With the name of a running instance it takes the locations of its placement,
  the shards and the clock from it, and sets `:settle_ms` to what a publisher
  needs to finish an object: the late-publication window, one store operation
  and the clock skew margin on both sides. The locations are those of the
  moment of the call: build the context again when the placement gains one,
  and keep the position, which starts a new location at its beginning.

  With a keyword list it takes the keys of `t:ctx/0` (`:clock` defaults to the
  system clock, `:shards` to the default of the instances, `:settle_ms` to
  30 000, `:max_object_bytes` to 268 435 456 and `:timeout_ms` to 10 000), for a consumer outside the instance. `:locations` may be one
  location or a list; `store: {module, config}, prefix: "..."` stands for the
  single location of `Alambique.Placement.Single`.
  """
  @spec ctx(atom | keyword) :: ctx
  def ctx(instance) when is_atom(instance), do: from_config(Alambique.Config.fetch!(instance))

  def ctx(opts) when is_list(opts) do
    opts = Map.new(opts)

    locations =
      case opts do
        %{locations: locations} ->
          List.wrap(locations)

        %{store: store, prefix: prefix} ->
          [%Location{id: "default", store: store, prefix: prefix}]

        _ ->
          []
      end

    ctx =
      %{
        clock: {Alambique.Clock.System, nil},
        shards: Keys.default_shards(),
        settle_ms: 30_000,
        max_object_bytes: Record.default_max_bytes(),
        timeout_ms: 10_000
      }
      |> Map.merge(Map.take(opts, [:clock, :shards, :settle_ms, :max_object_bytes, :timeout_ms]))
      |> Map.put(:locations, Enum.map(locations, &guarded/1))

    valid? =
      locations != [] and Enum.all?(locations, &Location.valid?/1) and
        match?(
          %{clock: {c, _}, shards: n, settle_ms: s, max_object_bytes: m, timeout_ms: t}
          when is_atom(c) and is_integer(n) and n > 0 and is_integer(s) and s >= 0 and
                 is_integer(m) and m > 0 and is_integer(t) and t > 0,
          ctx
        )

    unless valid?, do: raise(ArgumentError, "invalid feed context: #{inspect(opts)}")
    ctx
  end

  # A raw store given to `ctx/1` may raise or exit; the feed returns it as
  # `{:error, {:store_crashed, ...}}` like the instance's stores do.
  defp guarded(%Location{store: {Store.Guard, _} = store} = location),
    do: %{location | store: store}

  defp guarded(%Location{store: store} = location), do: %{location | store: {Store.Guard, store}}

  @doc false
  @spec from_config(map) :: ctx
  def from_config(config) do
    case Placement.locations(config) do
      {:ok, locations} ->
        ctx(
          locations: locations,
          shards: config.feed.shards,
          clock: config.clock,
          settle_ms: config.feed.window_ms + config.op_timeout_ms + 2 * config.skew_margin_ms,
          max_object_bytes: config.feed.max_object_bytes,
          timeout_ms: config.op_timeout_ms
        )

      {:error, reason} ->
        raise ArgumentError, "could not list the locations: #{inspect(reason)}"
    end
  end

  @doc "The position that has every object published at or after `time_ms` (Unix milliseconds) after it."
  @spec position_at(ctx, non_neg_integer) :: position
  def position_at(ctx, time_ms) do
    for {id, location, shard} <- lists(ctx), into: %{} do
      {id, Keys.after_time(location.prefix, shard, time_ms - 1)}
    end
    |> encode()
  end

  @doc """
  Reads the records of up to `limit` feed objects after `position`.

  Returns the records and the position to continue from, which equals
  `position` when nothing new has settled. One call lists each shard of each
  location once, however many cells exist, and gets one object per batch. The
  objects come in the order of their keys (time, TXID, cell) across all lists.
  """
  @spec read(ctx, position, pos_integer) ::
          {:ok, [Record.t() | Rewind.t() | Rejected.t()], position} | {:error, term}
  def read(ctx, position, limit \\ 100) when is_integer(limit) and limit > 0 do
    case read_objects(ctx, position, limit) do
      {:ok, objects, next} -> {:ok, Enum.flat_map(objects, &elem(&1, 1)), next}
      {:error, _} = error -> error
    end
  end

  @doc false
  # Like `read/3`, with the records of each object kept together under the
  # position right after the object, for the Broadway producer, which
  # acknowledges per object.
  @spec read_objects(ctx, position, pos_integer) ::
          {:ok, [{String.t(), [Record.t() | Rewind.t() | Rejected.t()]}], position}
          | {:error, term}
  def read_objects(ctx, position, limit) do
    started = System.monotonic_time()
    result = fetch(ctx, position, limit)

    {count, meta} =
      case result do
        {:ok, objects, _next} ->
          {objects |> Enum.map(&length(elem(&1, 1))) |> Enum.sum(),
           %{result: :ok, objects: length(objects)}}

        {:error, reason} ->
          {0, %{result: :error, reason: reason, objects: 0}}
      end

    Telemetry.span_stop([:feed, :read], started, %{records: count}, meta)
    result
  end

  # Every shard of every location, with the name its position uses.
  defp lists(ctx) do
    for location <- ctx.locations, shard <- 0..(ctx.shards - 1) do
      {"#{location.id}/#{shard}", location, shard}
    end
  end

  defp decode(nil), do: {:ok, %{}}

  defp decode(position) when is_binary(position) do
    case Jason.decode(position) do
      {:ok, %{} = map} -> {:ok, map}
      _ -> {:error, :bad_position}
    end
  end

  defp decode(_), do: {:error, :bad_position}

  defp encode(map) when map_size(map) == 0, do: nil
  defp encode(map), do: Jason.encode!(map)

  defp fetch(ctx, position, limit) do
    {clock, clock_config} = ctx.clock
    cutoff = clock.now_ms(clock_config) - ctx.settle_ms

    with {:ok, positions} <- decode(position),
         {:ok, pages} <- list_pages(ctx, positions, limit, cutoff) do
      pages
      |> merge(limit)
      |> load(positions, ctx, [])
    end
  end

  # One listing per list, in parallel. A page yields the settled objects and
  # `bound`: when the page was full and settled to its end, objects of other
  # lists after its last one may come after objects the list still holds.
  defp list_pages(ctx, positions, limit, cutoff) do
    lists(ctx)
    |> Task.async_stream(&list_page(&1, positions, limit, cutoff, ctx.timeout_ms),
      max_concurrency: 16,
      timeout: :infinity,
      ordered: true
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, page}}, {:ok, acc} -> {:cont, {:ok, [page | acc]}}
      {:ok, {:error, _} = error}, _ -> {:halt, error}
      {:exit, reason}, _ -> {:halt, {:error, {:exit, reason}}}
    end)
    |> then(fn
      {:ok, pages} -> {:ok, Enum.reverse(pages)}
      error -> error
    end)
  end

  defp list_page({id, location, shard}, positions, limit, cutoff, timeout) do
    {store, config} = location.store
    prefix = Keys.prefix(location.prefix, shard)

    with {:ok, keys, _next} <-
           Retry.once(timeout, fn -> store.list(config, prefix, positions[id], limit) end) do
      parsed =
        keys
        |> Enum.map(&{&1, Keys.parse(location.prefix, shard, &1)})
        |> Enum.reject(fn {_key, parsed} -> parsed == nil end)

      ready = Enum.take_while(parsed, fn {_key, p} -> p.time <= cutoff end)

      bound =
        with true <- length(keys) == limit and length(ready) == length(parsed),
             {_key, last} when last != nil <- List.last(parsed) do
          sort_key(last)
        else
          _ -> nil
        end

      items =
        for {key, p} <- ready,
            do: %{list: id, store: store, config: config, key: key, parsed: p, sort: sort_key(p)}

      {:ok, %{items: items, bound: bound}}
    end
  end

  defp sort_key(%{time: time, txid: txid, cell: cell}), do: {time, txid, cell}

  defp merge(pages, limit) do
    bound = pages |> Enum.map(& &1.bound) |> Enum.reject(&is_nil/1) |> Enum.min(fn -> nil end)

    pages
    |> Enum.flat_map(& &1.items)
    |> Enum.sort_by(& &1.sort)
    |> Enum.take_while(&(bound == nil or &1.sort <= bound))
    |> Enum.take(limit)
  end

  @doc """
  Deletes feed objects published before `now - retention_ms` and returns how
  many were deleted. Instances run it on a timer when `feed: [retention_ms:]`
  is set; it is safe to run on several nodes at once.
  """
  @spec prune(ctx, pos_integer) :: {:ok, non_neg_integer} | {:error, term}
  def prune(ctx, retention_ms) when is_integer(retention_ms) and retention_ms > 0 do
    started = System.monotonic_time()
    {clock, clock_config} = ctx.clock
    before = clock.now_ms(clock_config) - retention_ms

    result =
      Enum.reduce_while(lists(ctx), {:ok, 0}, fn {_id, location, shard}, {:ok, total} ->
        case prune_page(location, shard, before, nil, 0, ctx.timeout_ms) do
          {:ok, count} -> {:cont, {:ok, total + count}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    meta =
      case result do
        {:ok, _} -> %{result: :ok}
        {:error, reason} -> %{result: :error, reason: reason}
      end

    deleted = with {:ok, count} <- result, do: count, else: (_ -> 0)
    Telemetry.span_stop([:feed, :prune], started, %{deleted: deleted}, meta)
    result
  end

  defp prune_page(location, shard, before, start, deleted, timeout) do
    {store, config} = location.store

    with {:ok, keys, next} <-
           Retry.once(timeout, fn ->
             store.list(config, Keys.prefix(location.prefix, shard), start, 1000)
           end) do
      expired =
        keys
        |> Enum.map(&{&1, Keys.parse(location.prefix, shard, &1)})
        |> Enum.take_while(fn {_key, parsed} -> parsed == nil or parsed.time < before end)

      with {:ok, count} <- delete_all(store, config, expired, 0, timeout) do
        if length(expired) == length(keys) and next != nil,
          do: prune_page(location, shard, before, next, deleted + count, timeout),
          else: {:ok, deleted + count}
      end
    end
  end

  defp delete_all(_store, _config, [], count, _timeout), do: {:ok, count}

  defp delete_all(store, config, [{key, _} | rest], count, timeout) do
    case Retry.once(timeout, fn -> store.delete(config, key) end) do
      :ok -> delete_all(store, config, rest, count + 1, timeout)
      {:error, _} = error -> error
    end
  end

  # `positions` is the map of what is consumed so far.
  defp load([], positions, _ctx, acc), do: done(acc, positions)

  defp load([item | rest], positions, ctx, acc) do
    case Retry.once(ctx.timeout_ms, fn -> item.store.get(item.config, item.key) end) do
      {:ok, body, _etag, _meta} ->
        records = Record.decode(item.key, body, item.parsed.time, ctx.max_object_bytes)
        reject_event(records)
        positions = Map.put(positions, item.list, item.key)
        load(rest, positions, ctx, [{encode(positions), records} | acc])

      # Removed by retention between the listing and the read.
      {:error, :not_found} ->
        load(rest, Map.put(positions, item.list, item.key), ctx, acc)

      {:error, reason} ->
        if acc == [], do: {:error, reason}, else: done(acc, positions_of(acc))
    end
  end

  defp reject_event([%Rejected{key: key, reason: reason}]),
    do: Telemetry.emit([:feed, :read, :rejected], %{key: key, reason: reason})

  defp reject_event(_records), do: :ok

  defp done(acc, positions), do: {:ok, Enum.reverse(acc), encode(positions)}

  # After a failed read, continue from the last object that was loaded.
  defp positions_of([{position, _} | _]), do: elem(decode(position), 1)
end
