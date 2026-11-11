defmodule Alambique.Relocate.Cleanup do
  @moduledoc false
  # Deleting what a cell left at a location it moved away from (design §18).
  #
  # Segments and snapshots under the cell's prefix are handed to
  # `Alambique.Trash`; the fixed keys (`frozen`, `trashed`, `crashes`) go at
  # once, because a manifest is keyed by TXID and could not tell an old fixed
  # key from the marker of a returning cell. The lease stays with `moved_to`,
  # so stale nodes keep refusing to wake the cell there. The data objects go
  # `deletion_grace_ms` later, or at once when it is 0. The location must not
  # be the cell's current one, its lease must carry `moved_to`, and
  # `moved_at_ms + grace` must have passed: a restore that started before the
  # move finished within the grace, so no reader loses a file it listed.
  #
  # The pending marker (`record/3`) is removed last, so a cleanup that stopped
  # anywhere is found again by the compactors, of this node or of another one.
  # Several nodes may run the same cleanup: the claim above lets only the last
  # claimant stamp, and every step is idempotent.
  #
  # A cleanup claims the lease object (`Lease.claim_cleanup/2`) before it
  # marks and stamps `cleaned_at_ms` after. A cell may move back to a location
  # with that stamp once it is older than `gap/1`, so a cleanup that was slow,
  # or that lost its claim to a later one, has ended (it stops at its
  # deadline, `budget/1`) before new objects appear. The marked data objects
  # may still be there: the returning snapshot has a TXID above every TXID
  # the cell wrote there before, so it covers them (relocate checks the bound
  # after the restore), and manifests of the old objects cannot hit the new
  # ones because keys carry the TXIDs. The fixed keys were deleted at once.

  alias Alambique.{Config, Fault, Lease, Location, Placement, Schema, Telemetry, Trash}
  alias Alambique.Replication.{Keys, Objects, Retry}

  @type summary ::
          %{result: :cleaned, objects: non_neg_integer}
          | %{result: :waiting, ready_at_ms: integer}

  @doc "Time budget of one cleanup."
  @spec budget(Config.t()) :: pos_integer
  def budget(config), do: 6 * config.op_timeout_ms

  @doc "How old the `cleaned_at_ms` stamp must be before a cell may move back."
  @spec gap(Config.t()) :: pos_integer
  def gap(config), do: 8 * config.op_timeout_ms

  @doc """
  Records, at the location a cell is about to leave, that its cleanup is
  pending. It is written before the move is marked, so a crash anywhere after
  leaves the record; the compactor of any node finds it (`pending/4`). The
  body is the deadline of the move (now plus its `timeout_ms`, on the
  instance's clock; `drop_stray/3`).
  """
  @spec record(Config.t(), Location.t(), String.t(), pos_integer) :: :ok | {:error, term}
  def record(config, source, cell, timeout_ms) do
    ctx = Config.repl_ctx(config, source)
    {store, store_config} = ctx.store
    key = Keys.moved(ctx.prefix, cell)

    Retry.run(ctx, Retry.deadline_in(config.op_timeout_ms), fn ->
      case store.put(
             store_config,
             key,
             Integer.to_string(now_ms(config) + timeout_ms),
             :none,
             %{}
           ) do
        {:ok, _etag} -> :ok
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc """
  Removes the marker of `cell` at `from` when no move can still own it: the
  cleanup found the cell live there or not moved, and the deadline the marker
  names, plus the skew margin, has passed. Before it the marker may belong to a
  move between the marker and the mark.
  """
  @spec drop_stray(Config.t(), String.t(), String.t()) :: :ok | :young | {:error, term}
  def drop_stray(config, cell, location_id) do
    with {:ok, source} <- Placement.location(config, location_id) do
      ctx = Config.repl_ctx(config, source)
      key = Keys.moved(ctx.prefix, cell)
      deadline = Retry.deadline_in(config.op_timeout_ms)

      case Objects.fetch_optional(ctx, key, deadline) do
        {:ok, body, _meta} ->
          due = with {ms, ""} <- Integer.parse(body), do: ms + config.skew_margin_ms

          if not is_integer(due) or now_ms(config) >= due,
            do: Objects.delete(ctx, key, deadline),
            else: :young

        :none ->
          :ok

        {:error, _} = error ->
          error
      end
    end
  end

  defp now_ms(%{clock: {clock, clock_config}}), do: clock.now_ms(clock_config)

  @doc """
  One page of the cells with a pending cleanup at `location`, after the cell
  `cursor`: `{:ok, cells, next}`, `next` being `nil` after the last page.
  """
  @spec pending(Config.t(), Location.t(), String.t() | nil, pos_integer) ::
          {:ok, [String.t()], String.t() | nil} | {:error, term}
  def pending(config, location, cursor, limit) do
    ctx = Config.repl_ctx(config, location)
    marker_prefix = Keys.moved_prefix(ctx.prefix)
    after_key = cursor && marker_prefix <> cursor
    deadline = Retry.deadline_in(config.op_timeout_ms)

    with {:ok, keys, next} <- Objects.page(ctx, marker_prefix, deadline, after_key, limit) do
      cells = Enum.map(keys, &String.replace_prefix(&1, marker_prefix, ""))
      {:ok, cells, next && String.replace_prefix(next, marker_prefix, "")}
    end
  end

  @doc """
  One cleanup of `cell` at `from` (a location or its id). Options: `:grace_ms`
  (default: the instance's `moved_grace_ms`).
  """
  @spec run(Config.t(), String.t(), Location.t() | String.t(), keyword) ::
          {:ok, summary} | {:error, term}
  def run(config, cell, from, opts \\ []) do
    started = System.monotonic_time()
    grace = Keyword.get(opts, :grace_ms, config.moved_grace_ms)

    with :ok <- Placement.validate_id(config, cell),
         {:ok, source} <- source(config, from) do
      result = clean(config, cell, source, grace)
      emit(started, cell, source, result)
      result
    end
  end

  defp source(config, id) when is_binary(id), do: Placement.location(config, id)
  defp source(_config, %Location{} = location), do: {:ok, location}
  defp source(_config, _), do: {:error, :bad_location}

  defp clean(config, cell, source, grace) do
    lease_ctx = Config.lease_ctx(config, source)
    ctx = Config.repl_ctx(config, source)
    deadline = Retry.deadline_in(budget(config))

    with {:ok, live} <- Placement.locate(config, cell, fresh: true),
         :ok <- if(live.id == source.id, do: {:error, :live_location}, else: :ok),
         {:ok, lease} <- moved_lease(lease_ctx, cell),
         :ok <- due(config, lease, grace),
         {:ok, lease} <- Lease.claim_cleanup(lease_ctx, lease),
         {:ok, count} <- delete_objects(config, ctx, cell, deadline),
         :ok <- drop_index(config, source, cell, deadline),
         _ = Fault.point(:cleanup_before_mark, %{cell: cell, location: source.id}),
         :ok <- Lease.mark_cleaned(lease_ctx, lease),
         :ok <- Objects.delete(ctx, Keys.moved(ctx.prefix, cell), deadline) do
      {:ok, %{result: :cleaned, objects: count}}
    else
      {:waiting, ready_at} -> {:ok, %{result: :waiting, ready_at_ms: ready_at}}
      {:error, _} = error -> error
    end
  end

  defp moved_lease(lease_ctx, cell) do
    case Lease.read(lease_ctx, cell) do
      {:ok, %{moved_to: to} = lease} when to != nil -> {:ok, lease}
      {:ok, _} -> {:error, :not_moved}
      :none -> {:error, :not_moved}
      {:error, _} = error -> error
    end
  end

  # Leases written before `moved_at_ms` existed count their last expiry.
  defp due(config, lease, grace) do
    ready_at = (lease.moved_at_ms || lease.expires_at_ms) + grace + config.skew_margin_ms
    {clock, clock_config} = config.clock

    if clock.now_ms(clock_config) >= ready_at, do: :ok, else: {:waiting, ready_at}
  end

  defp delete_objects(config, ctx, cell, deadline) do
    lease_key = Keys.cell_prefix(ctx.prefix, cell) <> "/lease"

    with {:ok, keys} <- Objects.list(ctx, Keys.files_prefix(ctx.prefix, cell), deadline) do
      keys = Enum.reject(keys, &(&1 == lease_key))
      {data, fixed} = split_data(ctx, cell, keys)

      opts = [
        grace_ms: config.deletion_grace_ms,
        batch_keys: config.trash.batch_keys,
        concurrency: config.trash.concurrency
      ]

      # Only segments and snapshots wait for the grace: their keys carry
      # TXIDs, so a returning cell cannot collide with a pending manifest.
      # The fixed keys belong to a residency and are deleted now; deferring
      # them would let a manifest delete the marker of the returning cell.
      with {:ok, n} <- Trash.mark(ctx, cell, data, opts, deadline),
           :ok <- delete_fixed(ctx, fixed, deadline) do
        {:ok, n + length(fixed)}
      end
    end
  end

  defp split_data(ctx, cell, keys) do
    data = MapSet.new(Keys.data_keys(ctx.prefix, cell, keys))
    Enum.split_with(keys, &MapSet.member?(data, &1))
  end

  defp delete_fixed(ctx, keys, deadline) do
    keys
    |> Enum.sort()
    |> Enum.reduce_while(:ok, fn key, :ok ->
      case Objects.delete(ctx, key, deadline) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # The version of the leftover entry is unknown, so every version is tried.
  defp drop_index(config, source, cell, deadline) do
    with {:ok, versions} <- Schema.versions(config, source, deadline) do
      Enum.reduce_while(versions, :ok, fn version, :ok ->
        case Schema.drop(config, source, cell, version, deadline) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  defp emit(started, cell, source, result) do
    {measurements, meta} =
      case result do
        {:ok, %{result: :cleaned, objects: n}} -> {%{objects: n}, %{result: :ok}}
        {:ok, %{result: :waiting, ready_at_ms: at}} -> {%{}, %{result: :waiting, ready_at_ms: at}}
        {:error, reason} -> {%{}, %{result: :error, reason: reason}}
      end

    Telemetry.span_stop(
      [:cell, :cleanup],
      started,
      measurements,
      Map.merge(meta, %{cell: cell, location: source.id})
    )
  end
end
