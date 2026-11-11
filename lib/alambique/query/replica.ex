defmodule Alambique.Query.Replica do
  @moduledoc false
  # Read-only replicas of cells, built from the store into the cache (design
  # §14). A replica is rebuilt with `Alambique.Replication.restore/5`, or
  # refreshed by applying only the segments that are newer than the cached
  # one. It never touches the lease or the owner's file.

  alias Alambique.{Fault, Replication, Telemetry}
  alias Alambique.Query.Cache
  alias Alambique.Replication.{Chain, Keys, Native, Objects}

  @type t :: %{
          required(:path) => Path.t(),
          required(:txid) => non_neg_integer,
          required(:bytes) => non_neg_integer,
          optional(:copy) => :reflink | :copy
        }

  @doc """
  A complete replica of `cell` at `target` and where it is.

  Options: `:max_age_ms`, the age below which a cached `:latest` replica is
  used without asking the store (default 0).
  """
  @spec ensure(map, map, String.t(), Replication.target(), integer, keyword) ::
          {:ok, t} | {:error, term}
  def ensure(config, %{ctx: ctx, scope: scope}, cell, target, deadline, opts) do
    started = System.monotonic_time()
    dir = Cache.dir(config)
    # Cache names carry the location, so two locations never share a file.
    key = Cache.key(cell, scope)
    cached = pick(Cache.entries(dir, key), target)

    ctx = Map.put(ctx, :missing_final, true)

    result =
      refresh(ctx, dir, {cell, key}, target, cached, deadline, %{
        max_age_ms: Keyword.get(opts, :max_age_ms, 0),
        attempts: config.query.restore_attempts,
        reflink: config.query.reflink
      })

    meta =
      case result do
        {:ok, replica, source, segments} ->
          %{cell: cell, result: :ok, source: source, txid: replica.txid, segments: segments}
          |> Map.merge(Map.take(replica, [:copy]))

        {:error, reason} ->
          %{cell: cell, result: :error, reason: reason}
      end

    Telemetry.span_stop([:query, :replica], started, meta)

    with {:ok, replica, _source, _segments} <- result, do: {:ok, replica}
  end

  # The cached replica to start from: the newest one a target can use.
  defp pick(_entries, {:timestamp, _}), do: nil
  defp pick([newest | _], :latest), do: newest
  defp pick(entries, {:txid, n}), do: Enum.find(entries, &(&1.txid <= n))
  defp pick([], _target), do: nil

  defp refresh(ctx, dir, cell_key, target, cached, deadline, %{max_age_ms: max_age_ms} = opts) do
    cond do
      cached == nil or cached.txid == 0 ->
        restore(ctx, dir, cell_key, target, deadline, opts.attempts)

      target == {:txid, cached.txid} or fresh?(cached, target, max_age_ms) ->
        use_cached(cached, :hit)

      true ->
        case advance(ctx, dir, cell_key, target, cached, deadline, opts.reflink) do
          {:ok, _, _, _} = ok -> ok
          :unchanged -> use_cached(cached, :unchanged)
          :fallback -> restore(ctx, dir, cell_key, target, deadline, opts.attempts)
          {:error, _} = error -> error
        end
    end
  end

  defp fresh?(cached, :latest, max_age_ms) when max_age_ms > 0 do
    age = Cache.age_ms(cached.path)
    age != nil and age <= max_age_ms
  end

  defp fresh?(_cached, _target, _max_age_ms), do: false

  defp use_cached(cached, source) do
    Cache.touch(cached.path)
    {:ok, %{path: cached.path, txid: cached.txid, bytes: 0}, source, 0}
  end

  # Full restore into a temporary file, then renamed to the TXID it reached.
  # Compaction or retention may delete files of a listing while it is being
  # read: the restore starts again from a fresh listing, `attempts` times in
  # all.
  defp restore(ctx, dir, {cell, key}, target, deadline, attempts) do
    tmp = Cache.tmp(dir, key)

    case Replication.restore(ctx, cell, tmp, target, timeout_ms: remaining(deadline)) do
      {:ok, %{txid: txid, epoch: epoch, checksum: checksum, segments: segments}} ->
        publish(dir, key, tmp, {txid, epoch, checksum}, target, :restore, segments)

      {:error, reason} = error ->
        File.rm(tmp)

        if attempts > 1 and changed?(reason),
          do: restore(ctx, dir, {cell, key}, target, deadline, attempts - 1),
          else: error
    end
  end

  # The listing no longer matches the store: a file is gone, or the history
  # it showed has been merged or cut.
  defp changed?(:not_found), do: true
  defp changed?(:history_truncated), do: true
  defp changed?({:gap, _}), do: true
  defp changed?(_), do: false

  # Applies the level-0 segments after the cached TXID to a copy of the cached
  # file. A tail past a gap or a lower-epoch file ends the run (a pipeline in
  # flight, design §6, R3). Anything else unexpected returns `:fallback`, and
  # a full restore decides.
  defp advance(ctx, dir, {cell, key}, target, cached, deadline, reflink?) do
    after_key = Keys.segment(ctx.prefix, cell, 0, cached.txid, cached.txid)

    with {:ok, snapshot_keys, keys} <- list_levels(ctx, cell, after_key, deadline) do
      {segments, snapshots} = Keys.parse(ctx.prefix, cell, snapshot_keys ++ keys)
      newest_snapshot = newest(snapshots)
      limit = limit(target)

      case Chain.run(Enum.sort_by(segments, & &1.min), cached.txid, limit) do
        _ when newest_snapshot > cached.txid ->
          :fallback

        {[], false} when target == :latest ->
          :unchanged

        {chain, false} when chain != [] ->
          apply_chain(ctx, dir, {cell, key}, target, cached, chain, deadline, reflink?)

        _ ->
          :fallback
      end
    end
  end

  # The snapshot level and level 0 are listed at the same time: the trips are
  # waits for the store, and the listing task ends at the deadline like the
  # one in this process.
  defp list_levels(ctx, cell, after_key, deadline) do
    snapshots =
      Task.async(fn ->
        Objects.list(ctx, Keys.level_prefix(ctx.prefix, cell, Keys.snapshot_level()), deadline)
      end)

    level0 = Objects.list(ctx, Keys.level_prefix(ctx.prefix, cell, 0), deadline, after_key)

    case {Task.await(snapshots, :infinity), level0} do
      {{:ok, snapshot_keys}, {:ok, keys}} -> {:ok, snapshot_keys, keys}
      {{:error, _} = error, _} -> error
      {_, {:error, _} = error} -> error
    end
  end

  defp newest([]), do: 0
  defp newest(snapshots), do: snapshots |> Enum.map(& &1.txid) |> Enum.max()

  defp limit({:txid, n}), do: n
  defp limit(:latest), do: :infinity

  defp apply_chain(ctx, dir, {cell, key}, target, cached, chain, deadline, reflink?) do
    tmp = Cache.tmp(dir, key)

    try do
      with {:ok, page_size} <- page_size(cached.path),
           {:ok, copy} <- Cache.clone(cached.path, tmp, reflink?),
           {:ok, txid, epoch, checksum} <-
             apply_segments(ctx, cell, tmp, chain, deadline, %{
               checksum: cached.checksum,
               page_size: page_size,
               epoch: cached.epoch,
               txid: cached.txid
             }),
           true <- target == :latest or txid == limit(target) or :fallback do
        if txid == cached.txid do
          :unchanged
        else
          Fault.point(:query_refresh_applied, %{cell: cell, txid: txid})
          publish(dir, key, tmp, {txid, epoch, checksum}, target, {:refresh, copy}, length(chain))
        end
      else
        :fallback -> :fallback
        {:error, {:fetch, :not_found}} -> :fallback
        {:error, {:fetch, reason}} -> {:error, reason}
        {:error, _} -> :fallback
      end
    after
      File.rm(tmp)
    end
  end

  defp page_size(path) do
    case Cache.page_size(path) do
      {:ok, size} -> {:ok, size}
      :error -> :fallback
    end
  end

  defp apply_segments(_ctx, _cell, _tmp, [], _deadline, state),
    do: {:ok, state.txid, state.epoch, state.checksum}

  defp apply_segments(ctx, cell, tmp, [seg | rest], deadline, state) do
    with {:ok, body, meta} <- fetch(ctx, seg.key, deadline),
         epoch = Objects.epoch_of(meta),
         {:epoch, true} <- {:epoch, epoch >= state.epoch},
         {:ok, {min, max, _, _, _, _}, _} <- Native.ltx_info(body, false),
         true <- (min == seg.min and max == seg.max) or :fallback,
         {:ok, _header, post} <- Native.apply_ltx(tmp, body, state.checksum, state.page_size) do
      Fault.point(:query_refresh_segment, %{cell: cell, txid: max})
      state = %{state | checksum: post, epoch: epoch, txid: max}
      apply_segments(ctx, cell, tmp, rest, deadline, state)
    else
      {:epoch, false} ->
        if Chain.tail?([seg | rest], [], state.txid),
          do: {:ok, state.txid, state.epoch, state.checksum},
          else: :fallback

      :fallback ->
        :fallback

      {:error, {:fetch, _}} = error ->
        error

      {:error, _} ->
        :fallback
    end
  end

  defp fetch(ctx, key, deadline) do
    case Objects.fetch(ctx, key, deadline) do
      {:ok, _, _} = ok -> ok
      {:error, reason} -> {:error, {:fetch, reason}}
    end
  end

  # Moves the complete temporary file to its final name, and drops the
  # replicas it supersedes.
  defp publish(dir, cell, tmp, {txid, epoch, checksum}, target, source, segments) do
    Cache.sync(tmp)
    final = Cache.path(dir, cell, txid, epoch, checksum)
    File.rename!(tmp, final)

    superseded =
      for entry <- Cache.entries(dir, cell),
          entry.path != final,
          target == :latest or entry.txid == txid,
          do: entry

    Cache.remove(superseded)
    replica = %{path: final, txid: txid, bytes: File.stat!(final).size}

    case source do
      {:refresh, copy} -> {:ok, Map.put(replica, :copy, copy), :refresh, segments}
      source -> {:ok, replica, source, segments}
    end
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 1)
end
