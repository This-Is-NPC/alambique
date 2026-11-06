defmodule Alambique.Schema do
  @moduledoc """
  The schema version index and the deploy check (design §17).

  Every cell that has woken has an empty object at
  `<prefix>/schema/<version>/<cell>` in its location, where `<version>` is the
  cell's `PRAGMA user_version` padded to ten digits so that lexicographic order
  is numeric order. The oldest live version of a location is the first key of
  its `schema/` listing, and every version is found with one request each, so
  neither needs a request per cell.

  Every instance keeps the index, cells at version 0 included. A cell that
  has not woken since the index exists has no entry until it does.
  """

  alias Alambique.{Config, Fault, Location, Placement, Telemetry}
  alias Alambique.Replication.{Objects, Retry}

  @width 10

  @type report :: %{
          code_version: non_neg_integer | nil,
          oldest_readable: non_neg_integer | nil,
          locations: [location_report]
        }

  # `too_old_cells` is present only when the migrations are chosen per cell.
  @type location_report :: %{
          required(:location) => String.t(),
          required(:versions) => [pos_integer],
          required(:too_old) => boolean,
          optional(:too_old_cells) => [String.t()]
        }

  # Rule

  @doc """
  Whether code whose schema version is `n` serves a cell at `version`
  (`:serve`) or must refuse it (`:too_new`). Older cells are migrated by the
  wake; code without migrations (`n` = 0) does not check.
  """
  @spec window(Config.t(), non_neg_integer, non_neg_integer) :: :serve | :too_new
  def window(_config, 0, _version), do: :serve

  def window(%{migration_window: w}, n, version),
    do: if(version > n + w, do: :too_new, else: :serve)

  @doc "The oldest version that code whose schema version is `n` accepts in a deploy."
  @spec oldest_readable(Config.t(), non_neg_integer) :: pos_integer
  def oldest_readable(%{migration_window: w}, n), do: max(n - w, 1)

  # Keys

  @doc "Prefix under which the index of the location lives."
  @spec prefix(String.t()) :: String.t()
  def prefix(location_prefix), do: "#{location_prefix}/schema/"

  @doc "Key of the entry of `cell` at `version`."
  @spec key(String.t(), non_neg_integer, String.t()) :: String.t()
  def key(location_prefix, version, cell),
    do: "#{prefix(location_prefix)}#{pad(version)}/#{cell}"

  defp pad(version), do: version |> Integer.to_string() |> String.pad_leading(@width, "0")

  # Sorts after every key `schema/<version>/...` and before `schema/<version + 1>/`,
  # because "0" follows "/".
  defp after_version(location_prefix, version), do: "#{prefix(location_prefix)}#{pad(version)}0"

  defp parse_version(location_prefix, key) do
    with <<digits::binary-size(@width), ?/, _cell::binary>> <-
           String.replace_prefix(key, prefix(location_prefix), ""),
         {version, ""} when version >= 0 <- Integer.parse(digits) do
      version
    else
      _ -> nil
    end
  end

  # Index

  @doc """
  Records that `cell` is at `version`. Idempotent. The caller keeps the entry
  of the previous version until this one is written (`drop/5`), so the index
  never shows the cell nowhere. `ctx` is the replication context to use, the
  one of the location by default.
  """
  @spec put(Config.t(), Location.t(), String.t(), non_neg_integer, integer, map | nil) ::
          :ok | {:error, term}
  def put(config, location, cell, version, deadline, ctx \\ nil) do
    started = System.monotonic_time()
    ctx = ctx || Config.repl_ctx(config, location)
    {store, store_config} = ctx.store
    key = key(location.prefix, version, cell)
    Fault.point(:schema_before_put, %{cell: cell, version: version})

    result =
      Retry.run(ctx, deadline, fn ->
        case store.put(store_config, key, "", :none, %{}) do
          {:ok, _etag} -> :ok
          {:error, reason} -> {:retry, reason}
        end
      end)

    Fault.point(:schema_after_put, %{cell: cell, version: version})
    emit(started, :put, location, cell, version, result)
    result
  end

  @doc "Removes the entry of `cell` at `version`; a missing entry counts as removed."
  @spec drop(Config.t(), Location.t(), String.t(), non_neg_integer, integer, map | nil) ::
          :ok | {:error, term}
  def drop(config, location, cell, version, deadline, ctx \\ nil) do
    started = System.monotonic_time()
    ctx = ctx || Config.repl_ctx(config, location)
    result = Objects.delete(ctx, key(location.prefix, version, cell), deadline)
    emit(started, :delete, location, cell, version, result)
    result
  end

  defp emit(started, op, location, cell, version, result) do
    meta = %{cell: cell, location: location.id, version: version, op: op}

    meta =
      case result do
        :ok -> Map.put(meta, :result, :ok)
        {:error, reason} -> Map.merge(meta, %{result: :error, reason: reason})
      end

    Telemetry.span_stop([:schema, :index], started, %{}, meta)
  end

  # Listing

  @doc """
  The versions that have entries at `location`, ascending and version 0
  included, with one listing request per version and one more at the end.
  """
  @spec versions(Config.t(), Location.t(), integer) :: {:ok, [non_neg_integer]} | {:error, term}
  def versions(config, location, deadline),
    do: versions(Config.repl_ctx(config, location), location, deadline, nil, [])

  defp versions(ctx, location, deadline, start_after, acc) do
    case list(ctx, prefix(location.prefix), start_after, 1, deadline) do
      {:ok, [], _next} ->
        {:ok, Enum.reverse(acc)}

      {:ok, [key | _], _next} ->
        case parse_version(location.prefix, key) do
          nil ->
            {:error, {:bad_schema_key, key}}

          version ->
            versions(ctx, location, deadline, after_version(location.prefix, version), [
              version | acc
            ])
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  The versions of `versions/3` that may hold stale entries or cells below
  their namespace: version 0 is included, because a cell that migrated from it
  can leave a stale entry that the sweeper must remove. A cell whose namespace
  is still at version 0 is not woken by the sweeper.
  """
  @spec outdated_versions(Config.t(), Location.t(), integer) ::
          {:ok, [non_neg_integer]} | {:error, term}
  def outdated_versions(config, location, deadline),
    do: versions(config, location, deadline)

  @doc """
  One page of the cells at `version`: up to `limit` cell ids after `cursor`
  (a cell id or `nil`), and the cursor of the next page or `nil`.
  """
  @spec page(Config.t(), Location.t(), pos_integer, String.t() | nil, pos_integer, integer) ::
          {:ok, [String.t()], String.t() | nil} | {:error, term}
  def page(config, location, version, cursor, limit, deadline) do
    ctx = Config.repl_ctx(config, location)
    base = key(location.prefix, version, "")
    start_after = cursor && base <> cursor

    with {:ok, keys, next} <- list(ctx, base, start_after, limit, deadline) do
      cells = Enum.map(keys, &String.replace_prefix(&1, base, ""))
      {:ok, cells, next && List.last(cells)}
    end
  end

  @doc """
  One page of the cells of `location` whose id starts with `prefix`, in byte
  order and each once, up to `limit` ids after `after_id` (exclusive, or
  `nil`), and the last id of the page when more may follow, `nil` otherwise.

  It surveys the versions (`versions/3`), then asks each version for the
  `limit + 1` ids that follow, so a page costs two requests per live version
  plus one, each returning at most `limit + 1` keys, whatever the number of
  cells. The version lists are merged and a cell that briefly has entries at
  two versions is listed once.
  """
  @spec cells(
          Config.t(),
          Location.t(),
          String.t(),
          String.t() | nil,
          pos_integer,
          integer
        ) :: {:ok, [String.t()], String.t() | nil} | {:error, term}
  def cells(config, location, prefix, after_id, limit, deadline) do
    case resume(prefix, after_id) do
      :done ->
        {:ok, [], nil}

      {:ok, from} ->
        ctx = Config.repl_ctx(config, location)

        with {:ok, versions} <- versions(config, location, deadline),
             {:ok, lists} <-
               map_ok(
                 versions,
                 &version_cells(ctx, location, &1, prefix, from, limit + 1, deadline)
               ) do
          merged = lists |> Enum.concat() |> Enum.sort() |> Enum.dedup()
          page = Enum.take(merged, limit)
          {:ok, page, if(length(merged) > limit, do: List.last(page))}
        end
    end
  end

  # Where the listing of ids starting with `prefix` resumes: from the start
  # when `after_id` sorts before them, after it when it is one of them, and
  # nowhere when it sorts after all of them.
  defp resume(_prefix, nil), do: {:ok, nil}

  defp resume(prefix, after_id) do
    cond do
      String.starts_with?(after_id, prefix) -> {:ok, after_id}
      after_id < prefix -> {:ok, nil}
      true -> :done
    end
  end

  defp version_cells(ctx, location, version, prefix, from, limit, deadline) do
    base = key(location.prefix, version, "")

    with {:ok, keys, _next} <- list(ctx, base <> prefix, from && base <> from, limit, deadline) do
      {:ok, Enum.map(keys, &String.replace_prefix(&1, base, ""))}
    end
  end

  defp list(ctx, prefix, start_after, limit, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.list(config, prefix, start_after, limit) do
        {:ok, keys, next} -> {:ok, keys, next}
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  # Deploy check

  @doc """
  Checks that code with the given configuration can read every live cell: no
  location may hold a cell older than `oldest_readable/2`. Needs only the
  store, so it runs before the new code is deployed (design §17).

  With migrations chosen per cell, the check lists every cell of every version
  and calls the function once per cell, because each cell has its own
  version to compare with; `:timeout_ms` then bounds each request.

  Returns `{:ok, report}`, `{:error, {:too_old, report}}` when a location is
  too old, or `{:error, reason}` when a location cannot be listed.
  """
  @spec check(Config.t(), keyword) :: {:ok, report} | {:error, term}
  def check(config, opts \\ []) do
    timeout = Keyword.get(opts, :timeout_ms, config.schema.listing_timeout_ms)

    with {:ok, locations} <- Placement.locations(config),
         {:ok, found} <- survey(config, locations, timeout),
         {:ok, report} <- report(config, found, timeout) do
      if Enum.any?(report.locations, & &1.too_old),
        do: {:error, {:too_old, report}},
        else: {:ok, report}
    end
  end

  defp report(%{migrations: {_module, _function, _args}} = config, found, timeout) do
    found
    |> map_ok(fn {location, versions} ->
      with {:ok, too_old} <- too_old_cells(config, location, versions, timeout) do
        {:ok,
         %{
           location: location.id,
           versions: versions,
           too_old: too_old != [],
           too_old_cells: too_old
         }}
      end
    end)
    |> with_ok(&%{code_version: nil, oldest_readable: nil, locations: &1})
  end

  defp report(config, found, _timeout) do
    readable = oldest_readable(config, config.schema_version)

    {:ok,
     %{
       code_version: config.schema_version,
       oldest_readable: readable,
       locations:
         for {location, versions} <- found do
           %{location: location.id, versions: versions, too_old: older?(versions, readable)}
         end
     }}
  end

  defp older?([oldest | _], readable), do: oldest < readable
  defp older?([], _readable), do: false

  defp too_old_cells(config, location, versions, timeout) do
    versions
    |> map_ok(&version_too_old(config, location, &1, nil, timeout, []))
    |> with_ok(&Enum.sort(List.flatten(&1)))
  end

  defp version_too_old(config, location, version, cursor, timeout, acc) do
    case page(
           config,
           location,
           version,
           cursor,
           config.schema.check_page,
           Retry.deadline_in(timeout)
         ) do
      {:ok, cells, next} ->
        with {:ok, old} <- map_ok(cells, &cell_too_old(config, version, &1)) do
          acc = [Enum.reject(old, &is_nil/1) | acc]

          if next,
            do: version_too_old(config, location, version, next, timeout, acc),
            else: {:ok, acc}
        end

      {:error, reason} ->
        {:error, {:location, location.id, reason}}
    end
  end

  defp cell_too_old(config, version, cell) do
    case Config.migrations_for(config, cell) do
      {:ok, list} ->
        n = length(list)
        {:ok, if(n > 0 and version < oldest_readable(config, n), do: cell)}

      {:error, reason} ->
        {:error, {:cell, cell, reason}}
    end
  end

  defp survey(config, locations, timeout) do
    map_ok(locations, fn location ->
      case outdated_versions(config, location, Retry.deadline_in(timeout)) do
        {:ok, versions} -> {:ok, {location, versions}}
        {:error, reason} -> {:error, {:location, location.id, reason}}
      end
    end)
  end

  defp map_ok(items, fun) do
    items
    |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
      case fun.(item) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp with_ok({:ok, value}, fun), do: {:ok, fun.(value)}
  defp with_ok(error, _fun), do: error
end
