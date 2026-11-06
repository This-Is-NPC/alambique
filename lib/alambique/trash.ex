defmodule Alambique.Trash do
  @moduledoc """
  Delayed deletions (design §9.1).

  Retention and the cleanup of a relocated cell do not delete objects. They
  record the keys in a *manifest*, an object under `<location prefix>/trash/`
  whose key starts with the time it is due, and a background reaper deletes
  the objects and then the manifest once that time has passed. Until then the
  objects are still in the store and restores see them as before.

  The instance option `:deletion_grace_ms` (default 86 400 000, one day) sets
  how long a key waits; `0` deletes at once, through the same code. The
  reaper is configured with `:trash`: a keyword list with

    * `:reaper` - run the reaper on this node (default `true`)
    * `:interval_ms` - time between rounds (default 300 000)
    * `:page_size` - manifests listed per request (default 100)
    * `:concurrency` - objects deleted at once (default 8)
    * `:rate` - objects deleted per second (default 200)
    * `:timeout_ms` - time budget of one manifest (default 120 000)
    * `:batch_keys` - keys per manifest (default 1 000)

  Every node runs a reaper. Two reapers may take the same manifest; deleting
  twice is harmless.

  ## Operating

  `pending/2` lists the manifests; `cancel/3` deletes one, so its objects are
  kept (retention marks them again at its next run unless it is off);
  `reap/2` runs one round now. Telemetry is described in `docs/telemetry.md`,
  section "Trash events".
  """

  require Logger

  alias Alambique.{Config, Fault, Placement, Telemetry}
  alias Alambique.Replication.{Keys, Objects, Retry}

  @defaults [
    reaper: true,
    interval_ms: 300_000,
    page_size: 100,
    concurrency: 8,
    rate: 200,
    timeout_ms: 120_000,
    batch_keys: 1_000
  ]

  @default_grace_ms 86_400_000

  @type entry :: %{key: String.t(), due_ms: integer, cell: String.t() | nil}

  @doc "Defaults of the `:trash` option."
  @spec defaults() :: keyword
  def defaults, do: @defaults

  @doc "Default of `:deletion_grace_ms`."
  @spec default_grace_ms() :: pos_integer
  def default_grace_ms, do: @default_grace_ms

  @doc "Prefix of the manifests of a location."
  @spec prefix(String.t()) :: String.t()
  def prefix(location_prefix), do: "#{location_prefix}/trash/"

  @doc false
  def children(%{trash: %{reaper: false}}), do: []

  def children(config) do
    [
      %{
        id: Alambique.Trash.Reaper,
        start:
          {Alambique.Periodic, :start_link,
           ["trash reaper", config.trash.interval_ms, fn -> reap(config.name) end]}
      }
    ]
  end

  # Marking

  @doc """
  Deletes `keys` of `cell` after the grace period. Options: `:grace_ms`
  (default `default_grace_ms/0`; `0` deletes now), `:batch_keys` and
  `:concurrency` (defaults of `defaults/0`). Returns the number of keys.

  Idempotent: marking a key again only adds a manifest that finds nothing
  to delete. A crash leaves the objects in place, with or without a
  manifest; it never leaves a manifest for objects that were not meant to
  go. Only segment and snapshot keys are marked: the fixed keys of a cell
  (`frozen`, `trashed`, `crashes`) belong to its current residency and are
  never deleted through a manifest.
  """
  @spec mark(Alambique.Replication.ctx(), String.t(), [String.t()], keyword, integer) ::
          {:ok, non_neg_integer} | {:error, term}
  def mark(ctx, cell, keys, opts, deadline) do
    case Keys.data_keys(ctx.prefix, cell, keys) do
      [] -> {:ok, 0}
      keys -> do_mark(ctx, cell, keys, opts, deadline)
    end
  end

  defp do_mark(ctx, cell, keys, opts, deadline) do
    started = System.monotonic_time()
    grace = Keyword.get(opts, :grace_ms, @default_grace_ms)

    settings =
      Map.merge(Map.new(@defaults), Map.new(Keyword.take(opts, [:batch_keys, :concurrency])))

    result =
      if grace == 0 do
        delete_all(ctx, cell, keys, settings.concurrency, nil, deadline)
      else
        due = now(ctx) + grace

        keys
        |> Enum.chunk_every(settings.batch_keys)
        |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, n} ->
          case put_manifest(ctx, cell, chunk, due, deadline) do
            :ok -> {:cont, {:ok, n + length(chunk)}}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end

    emit_mark(started, cell, grace, result)
    result
  end

  defp put_manifest(ctx, cell, keys, due, deadline) do
    id = 8 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
    key = "#{prefix(ctx.prefix)}#{pad(due)}/#{cell}/#{id}"
    body = Jason.encode!(%{cell: cell, keys: keys})

    Fault.point(:trash_before_mark, %{cell: cell, key: key})
    result = Objects.put(ctx, key, body, :if_none_match, %{}, deadline)
    Fault.point(:trash_after_mark, %{cell: cell, key: key})

    # The id is random: a key that exists is the earlier try of this call.
    if result == :exists, do: :ok, else: result
  end

  defp emit_mark(started, cell, grace, result) do
    {objects, meta} =
      case result do
        {:ok, n} -> {n, %{result: :ok}}
        {:error, reason} -> {0, %{result: :error, reason: reason}}
      end

    Telemetry.span_stop(
      [:trash, :mark],
      started,
      %{objects: objects},
      Map.merge(meta, %{cell: cell, grace_ms: grace})
    )
  end

  # Deleting. The one place where objects of cells are deleted.

  # Keys go in order, so lower levels go before the merged files built from
  # them and a crash never leaves a merged file to be rebuilt (design §9.1).
  defp delete_all(ctx, cell, keys, concurrency, rate, deadline) do
    # A manifest could name a fixed key such as `frozen`; deleting it
    # could hit the marker of a new residency.
    keys = Keys.data_keys(ctx.prefix, cell, keys)

    keys
    |> Enum.sort()
    |> Enum.chunk_every(concurrency)
    |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, n} ->
      started = System.monotonic_time(:millisecond)

      case delete_chunk(ctx, cell, chunk, concurrency, deadline) do
        :ok ->
          pace(rate, length(chunk), started)
          {:cont, {:ok, n + length(chunk)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp delete_chunk(ctx, cell, keys, concurrency, deadline) do
    keys
    |> Task.async_stream(
      fn key ->
        Fault.point(:trash_before_delete, %{cell: cell, key: key})
        result = Objects.delete(ctx, key, deadline)
        Fault.point(:trash_after_delete, %{cell: cell, key: key})
        result
      end,
      max_concurrency: concurrency,
      timeout: :infinity
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, {:error, _} = error}, _ -> {:halt, error}
      {:exit, reason}, _ -> {:halt, {:error, {:task_exit, reason}}}
    end)
  end

  # Holds the deletions to `rate` objects per second.
  defp pace(nil, _count, _started), do: :ok

  defp pace(rate, count, started) do
    wait = div(count * 1000, rate) - (System.monotonic_time(:millisecond) - started)
    if wait > 0, do: Process.sleep(wait)
  end

  # Reaping

  @type summary :: %{
          manifests: non_neg_integer,
          objects: non_neg_integer,
          errors: non_neg_integer
        }

  @doc """
  Runs one reaper round now over every location: deletes the objects of the
  manifests that are due, then the manifests. Options override the
  instance's `:trash` settings: `:page_size`, `:concurrency`, `:rate` and
  `:timeout_ms`.

  Returns `{:ok, %{manifests, objects, errors}}`: manifests finished, objects
  deleted and failures (logged; a failed manifest stays for the next round).
  """
  @spec reap(atom, keyword) :: {:ok, summary} | {:error, term}
  def reap(instance, opts \\ []) do
    config = Config.fetch!(instance)
    started = System.monotonic_time()

    settings =
      Map.merge(
        config.trash,
        Map.new(Keyword.take(opts, [:page_size, :concurrency, :rate, :timeout_ms]))
      )

    result =
      with {:ok, locations} <- Placement.locations(config) do
        {:ok,
         Enum.reduce(locations, %{manifests: 0, objects: 0, errors: 0}, fn location, acc ->
           reap_location(Config.repl_ctx(config, location), settings, location.id, acc)
         end)}
      end

    summary =
      case result do
        {:ok, summary} -> summary
        {:error, _} -> %{manifests: 0, objects: 0, errors: 1}
      end

    Telemetry.span_stop([:trash, :reap], started, summary, %{
      instance: config.name,
      result: if(match?({:ok, _}, result), do: :ok, else: :error)
    })

    result
  end

  @doc false
  def reap_ctx(ctx, settings, label \\ "location"),
    do: reap_location(ctx, settings, label, %{manifests: 0, objects: 0, errors: 0})

  defp reap_location(ctx, settings, location, acc) do
    ctx = %{ctx | timeout_ms: settings.timeout_ms}
    now = now(ctx)
    reap_page(ctx, settings, location, now, nil, acc)
  end

  defp reap_page(ctx, settings, location, now, cursor, acc) do
    deadline = Retry.deadline_in(settings.timeout_ms)

    case Objects.page(ctx, prefix(ctx.prefix), deadline, cursor, settings.page_size) do
      {:ok, keys, next} ->
        {due, rest} = Enum.split_while(keys, &(due_ms(ctx, &1) <= now))
        acc = Enum.reduce(due, acc, &reap_manifest(ctx, settings, location, &1, &2))

        if rest == [] and next != nil,
          do: reap_page(ctx, settings, location, now, next, acc),
          else: acc

      {:error, reason} ->
        Logger.warning("Alambique trash could not list #{location}: #{inspect(reason)}")
        %{acc | errors: acc.errors + 1}
    end
  end

  # A key that is not a manifest of this layout sorts as never due.
  defp due_ms(ctx, key) do
    case String.replace_prefix(key, prefix(ctx.prefix), "") do
      <<due::binary-size(16), ?/, _::binary>> ->
        case Integer.parse(due) do
          {ms, ""} -> ms
          _ -> :infinity
        end

      _ ->
        :infinity
    end
  end

  defp reap_manifest(ctx, settings, location, manifest, acc) do
    deadline = Retry.deadline_in(settings.timeout_ms)

    case reap_one(ctx, settings, manifest, deadline) do
      {:ok, n} ->
        %{acc | manifests: acc.manifests + 1, objects: acc.objects + n}

      {:error, reason} ->
        Logger.warning(
          "Alambique trash could not process #{manifest} at #{location}: #{inspect(reason)}"
        )

        %{acc | errors: acc.errors + 1}
    end
  end

  defp reap_one(ctx, settings, manifest, deadline) do
    case fetch_manifest(ctx, manifest, deadline) do
      :gone ->
        {:ok, 0}

      {:ok, cell, keys} ->
        with {:ok, n} <-
               delete_all(ctx, cell, keys, settings.concurrency, settings.rate, deadline),
             _ = Fault.point(:trash_before_entry_delete, %{cell: cell, key: manifest}),
             :ok <- Objects.delete(ctx, manifest, deadline) do
          {:ok, n}
        end

      {:error, _} = error ->
        error
    end
  end

  # Only keys of the cell the manifest names can go: whatever else is in the
  # object, a damaged or foreign manifest never deletes outside the cell.
  defp fetch_manifest(ctx, manifest, deadline) do
    case Objects.fetch_optional(ctx, manifest, deadline) do
      {:ok, body, _meta} -> decode(ctx, body)
      :none -> :gone
      {:error, _} = error -> error
    end
  end

  defp decode(ctx, body) do
    with {:ok, %{"cell" => cell, "keys" => keys}} when is_binary(cell) and is_list(keys) <-
           Jason.decode(body),
         files = Keys.files_prefix(ctx.prefix, cell),
         true <- Enum.all?(keys, &(is_binary(&1) and String.starts_with?(&1, files))) do
      {:ok, cell, keys}
    else
      _ -> {:error, :bad_manifest}
    end
  end

  # Operators

  @doc """
  Lists the manifests of a location, soonest due first: `{:ok, %{entries,
  next}}`, with `entries` as `%{key, due_ms, cell}`. Options: `:location`
  (as for `Alambique.list_cells/2`), `:after` (the `next` of the previous
  page) and `:limit` (default: the `:trash` `:page_size`).
  """
  @spec pending(atom, keyword) ::
          {:ok, %{entries: [entry], next: String.t() | nil}} | {:error, term}
  def pending(instance, opts \\ []) do
    config = Config.fetch!(instance)
    opts = Keyword.validate!(opts, location: nil, after: nil, limit: config.trash.page_size)

    with {:ok, location} <- Placement.listed_location(config, opts[:location]),
         ctx = Config.repl_ctx(config, location),
         deadline = Retry.deadline_in(config.trash.timeout_ms),
         {:ok, keys, next} <-
           Objects.page(ctx, prefix(ctx.prefix), deadline, opts[:after], opts[:limit]) do
      {:ok, %{entries: Enum.map(keys, &entry(ctx, &1)), next: next}}
    end
  end

  defp entry(ctx, key) do
    cell =
      case String.split(String.replace_prefix(key, prefix(ctx.prefix), ""), "/") do
        [_due, cell, _id] -> cell
        _ -> nil
      end

    %{key: key, due_ms: due_ms(ctx, key), cell: cell}
  end

  @doc """
  Cancels a pending deletion: deletes the manifest `key` (from `pending/2`)
  so the reaper never touches its objects. Option: `:location`.
  """
  @spec cancel(atom, String.t(), keyword) :: :ok | {:error, term}
  def cancel(instance, key, opts \\ []) do
    config = Config.fetch!(instance)
    opts = Keyword.validate!(opts, location: nil)

    with {:ok, location} <- Placement.listed_location(config, opts[:location]),
         ctx = Config.repl_ctx(config, location),
         true <- String.starts_with?(key, prefix(ctx.prefix)) or {:error, :not_a_manifest} do
      Objects.delete(ctx, key, Retry.deadline_in(config.trash.timeout_ms))
    end
  end

  defp pad(ms), do: ms |> Integer.to_string() |> String.pad_leading(16, "0")

  defp now(%{clock: {mod, config}}), do: mod.now_ms(config)
end
