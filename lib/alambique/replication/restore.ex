defmodule Alambique.Replication.Restore do
  @moduledoc false
  # Rebuilds a database file from snapshots and segments (design §6).

  alias Alambique.Fault
  alias Alambique.Telemetry
  alias Alambique.Replication.{Chain, Native, Objects, Retry}

  @flag Bitwise.bsl(1, 63)

  # A lazy restore (option `:lazy`) leaves the database file sparse and gives
  # `:lazy` (the page map, see `Alambique.Replication.Hydrator`) and `:files`
  # (the keys the map refers to, by index) in the result.
  @type result :: %{
          required(:cell) => String.t(),
          required(:path) => Path.t(),
          required(:txid) => non_neg_integer,
          required(:checksum) => integer,
          required(:epoch) => non_neg_integer,
          required(:data) => non_neg_integer,
          required(:page_size) => pos_integer | nil,
          required(:commit) => non_neg_integer,
          required(:timestamp) => integer | nil,
          required(:segments) => non_neg_integer,
          optional(:lazy) => reference,
          optional(:files) => [String.t()]
        }

  def run(ctx, cell, dest, target, opts) do
    started = System.monotonic_time()
    deadline = Retry.deadline_in(Keyword.get(opts, :timeout_ms, ctx.timeout_ms))
    limits = limits(target)

    result =
      with {:ok, {segments, snapshots}} <- Objects.list_cell(ctx, cell, deadline) do
        env = %{
          ctx: ctx,
          cell: cell,
          deadline: deadline,
          limits: limits,
          segments: segments,
          snapshots: snapshots,
          dest: dest,
          tmp: "#{dest}.restore-#{System.unique_integer([:positive])}",
          edge_bytes: ctx.edge_bytes,
          concurrency: ctx.fetch_concurrency,
          prefetch_bytes: ctx.prefetch_bytes,
          lazy: opts[:lazy],
          prefetched: :ets.new(:restore_prefetched, [:set, :public])
        }

        try do
          case attempt_all(env, starts(env), nil) do
            # Too little to be worth a lazy restore: read everything, from the
            # same listing.
            :small -> attempt_all(%{env | lazy: nil}, starts(env), nil)
            result -> result
          end
        after
          :ets.delete(env.prefetched)
        end
      end

    Telemetry.span_stop(
      [:replication, :restore],
      started,
      telemetry_meta(cell, target, result)
    )

    result
  end

  defp telemetry_meta(cell, target, {:ok, r}),
    do: %{
      cell: cell,
      target: target,
      result: :ok,
      txid: r.txid,
      epoch: r.epoch,
      segments: r.segments
    }

  defp telemetry_meta(cell, target, {:error, reason}),
    do: %{cell: cell, target: target, result: :error, reason: reason}

  defp limits(:latest), do: %{txid: :infinity, ts: :infinity, exact: nil}
  defp limits({:txid, n}) when is_integer(n) and n >= 0, do: %{txid: n, ts: :infinity, exact: n}
  defp limits({:timestamp, ms}) when is_integer(ms), do: %{txid: :infinity, ts: ms, exact: nil}

  defp within_txid?(txid, %{txid: :infinity}), do: txid > 0
  defp within_txid?(txid, %{txid: n}), do: txid <= n

  # Snapshots newest first, then "from scratch" (a chain whose first segment
  # starts at TXID 1).
  defp starts(env) do
    snapshots =
      env.snapshots
      |> Enum.filter(&within_txid?(&1.txid, env.limits))
      |> Enum.sort_by(& &1.txid, :desc)

    Enum.map(snapshots, &{:snapshot, &1}) ++ [:scratch]
  end

  defp attempt_all(_env, [], first_error), do: first_error || {:error, :no_chain}

  defp attempt_all(env, [start | rest], first_error) do
    try do
      case attempt(env, start) do
        :small -> :small
        {:ok, state} -> finish(env, state)
        # Never fall back to another chain around an epoch regression.
        {:error, {:epoch_regression, _, _, _}} = error -> error
        {:error, _} = error -> attempt_all(env, rest, first_error || error)
      end
    after
      File.rm(env.tmp)
    end
  end

  defp attempt(env, start) do
    File.rm(env.tmp)

    with :ok <- prefetch(env, start),
         {:ok, state} <- begin_chain(env, start),
         {:ok, state} <- walk(env, state) do
      check_target(env, state)
    end
  end

  defp empty_state do
    %{
      txid: 0,
      checksum: @flag,
      epoch: 0,
      data: 0,
      page_size: nil,
      commit: 0,
      timestamp: nil,
      segments: 0,
      # Lazy restore: the page map, the keys it refers to (newest first) and
      # the size of those files.
      lazy: nil,
      files: [],
      bytes: 0
    }
  end

  # A chain from scratch starts with the file at TXID 1. When files exist but
  # none starts there, retention deleted the early history (design §9): an
  # empty database would be wrong, not old.
  defp begin_chain(env, :scratch) do
    truncated? =
      (env.segments != [] or env.snapshots != []) and not Enum.any?(env.segments, &(&1.min == 1))

    if truncated?, do: {:error, :history_truncated}, else: {:ok, empty_state()}
  end

  defp begin_chain(env, {:snapshot, snap}) do
    state = empty_state()

    with {:ok, state, file, meta} <- load(env, state, snap.key),
         {:ok, state, file, {1, max, ts, _, _, _}, _} when max == snap.txid <-
           inspect_file(env, state, file, :snapshot, snap.key),
         true <- ts_ok?(env, ts) or {:error, {:snapshot_after_target, snap.key}},
         {:ok, state, header, post} <-
           named(apply_file(env, state, file, snap.key, 0, 0), :snapshot, snap.key) do
      {:ok, advance(state, header, post, meta)}
    else
      {:ok, _state, _file, _header, _post} -> {:error, {:bad_snapshot, snap.key}}
      {:error, _} = error -> error
    end
  end

  defp walk(env, state) do
    candidates =
      env.segments
      |> Enum.filter(&(&1.min == state.txid + 1 and within_txid?(&1.max, env.limits)))
      |> Enum.sort_by(&{-&1.max, &1.level})

    case try_candidates(env, state, candidates, [], nil) do
      :end when candidates == [] ->
        if tail?(env, state), do: {:ok, state}, else: {:error, {:gap, state.txid + 1}}

      :end ->
        {:ok, state}

      {:ok, state} ->
        Fault.point(:repl_mid_restore, %{cell: env.cell, txid: state.txid})
        walk(env, state)

      {:error, _} = error ->
        error
    end
  end

  # The files left past the chain inside the target are a tail (design §6,
  # R3), or nothing. Otherwise files are missing, for example after an
  # interrupted retention run, and stopping here would return an older state
  # as if it were the target.
  defp tail?(env, state) do
    segments =
      Enum.filter(env.segments, fn seg ->
        seg.min > state.txid and within_txid?(seg.min, env.limits) and
          (seg.min > state.txid + 1 or within_txid?(seg.max, env.limits))
      end)

    snapshots =
      Enum.filter(env.snapshots, &(&1.txid > state.txid and within_txid?(&1.txid, env.limits)))

    Chain.tail?(segments, snapshots, state.txid)
  end

  # Every candidate was stamped after the target. That proves where the target
  # lies only if one of them is a single TXID; merged files alone would hide
  # a finer file that is missing (design §9, retention).
  defp coarse?([]), do: false
  defp coarse?(late), do: not Enum.any?(late, &(&1.min == &1.max))

  # Applies the first candidate that fits. A candidate from an older epoch
  # than the chain so far is never applied (R2). If nothing else fits, the
  # chain ends there when what is left is a tail (R3), else it is an error
  # rather than a silently shorter chain.
  defp try_candidates(_env, _state, [], _skipped, error) when error != nil, do: error

  defp try_candidates(env, state, [], skipped, nil) do
    late = for {:late, seg} <- skipped, do: seg

    case Enum.find(skipped, &match?({:epoch, _, _}, &1)) do
      {:epoch, key, epoch} ->
        if tail?(env, state),
          do: :end,
          else: {:error, {:epoch_regression, key, epoch, state.epoch}}

      nil ->
        if coarse?(late), do: {:error, {:history_coarse, state.txid + 1}}, else: :end
    end
  end

  defp try_candidates(env, state, [seg | rest], skipped, error) do
    with {:ok, state1, file, meta} <- load(env, state, seg.key),
         epoch = Objects.epoch_of(meta),
         {:epoch, true} <- {:epoch, epoch >= state.epoch} |> or_skip(skipped, seg, epoch),
         {:ok, state1, file, {min, max, ts, _, _, _}, _} <-
           inspect_file(env, state1, file, :segment, seg.key),
         {:range, true} <- {:range, min == seg.min and max == seg.max},
         {:ts, true} <- {:ts, ts_ok?(env, ts)} do
      case apply_file(env, state1, file, seg.key, state.checksum, state.page_size || 0) do
        {:ok, state1, header, post} ->
          {:ok, advance(state1, header, post, meta)}

        {:error, {:pre_apply_mismatch, _, _}} = mismatch ->
          try_candidates(env, state, rest, skipped, error || wrap(mismatch, :segment, seg.key))

        other ->
          wrap(other, :segment, seg.key)
      end
    else
      {:skip, skipped} ->
        try_candidates(env, state, rest, skipped, error)

      {:range, false} ->
        try_candidates(env, state, rest, skipped, error || {:error, {:bad_segment, seg.key}})

      {:ts, false} ->
        try_candidates(env, state, rest, [{:late, seg} | skipped], error)

      {:error, _} = e ->
        e
    end
  end

  defp or_skip({:epoch, true} = ok, _skipped, _seg, _epoch), do: ok

  defp or_skip({:epoch, false}, skipped, seg, epoch),
    do: {:skip, [{:epoch, seg.key, epoch} | skipped]}

  defp check_target(%{limits: %{exact: n}}, %{txid: txid}) when n != nil and txid != n,
    do: {:error, {:target_not_reached, txid}}

  defp check_target(_env, state), do: {:ok, state}

  defp advance(state, {_min, max, ts, commit, _pre, page_size}, post, meta) do
    %{
      state
      | txid: max,
        checksum: post,
        epoch: max(state.epoch, Objects.epoch_of(meta)),
        data: Objects.data_of(meta, max),
        page_size: page_size,
        commit: commit,
        timestamp: ts,
        segments: state.segments + 1
    }
  end

  # A lazy restore ends with the plan: the file is sized and served through
  # the VFS from now on. A chain that is small enough to download at once is
  # restored in full instead.
  defp finish(%{lazy: lazy} = env, %{lazy: handle} = state) when lazy != nil and handle != nil do
    if state.bytes < lazy.min_bytes do
      File.rm(env.dest)
      :small
    else
      case Native.lazy_ready(handle) do
        :ok ->
          {:ok,
           state
           |> Map.update!(:files, &Enum.reverse/1)
           |> Map.drop([:bytes])
           |> Map.merge(%{cell: env.cell, path: env.dest})}

        {:error, reason} ->
          {:error, {:lazy, reason}}
      end
    end
  end

  defp finish(env, state) do
    state = Map.drop(state, [:lazy, :files, :bytes])
    if state.txid == 0, do: File.write!(env.tmp, "")
    sync_file(env.tmp)
    File.rm(env.dest <> "-wal")
    File.rm(env.dest <> "-shm")
    File.rename!(env.tmp, env.dest)

    {:ok, Map.merge(state, %{cell: env.cell, path: env.dest})}
  end

  defp sync_file(path) do
    {:ok, io} = :file.open(path, [:read, :write, :raw, :binary])
    :ok = :file.sync(io)
    :ok = :file.close(io)
  end

  # Reading a file of the chain. Whole files go into the database file one
  # after the other; a lazy restore reads the edges of each (header, page
  # index, trailer) and lets the page map point at the frames in the store.

  defp load(%{lazy: nil} = env, state, key) do
    with {:ok, body, meta} <- fetch(env, key), do: {:ok, state, {:body, body}, meta}
  end

  defp load(env, state, key) do
    with {:ok, {head, tail, total}, meta} <- edges(env, key) do
      {:ok, state, {:edges, head, tail, total}, meta}
    end
  end

  defp inspect_file(_env, state, {:body, body} = file, kind, key) do
    with {:ok, header, post} <- wrap(Native.ltx_info(body, false), kind, key),
         do: {:ok, state, file, header, post}
  end

  defp inspect_file(env, state, {:edges, head, tail, total}, kind, key) do
    with {:ok, state} <- ensure_lazy(env, state, head) do
      idx = length(state.files)

      case Native.lazy_stage(state.lazy, idx, head, tail, total) do
        {:ok, header, post} ->
          {:ok, state, {:staged, header, post, total}, header, post}

        {:need_tail, n} ->
          with {:ok, tail, _total, _meta} <- range(env, key, max(total - n, 0), n),
               do: inspect_file(env, state, {:edges, head, tail, total}, kind, key)

        {:error, reason} ->
          {:error, {kind, key, reason}}
      end
    else
      {:error, reason} -> {:error, {kind, key, reason}}
    end
  end

  defp apply_file(env, state, {:body, body}, _key, current, page_size) do
    case Native.apply_ltx(env.tmp, body, current, page_size) do
      {:ok, header, post} -> {:ok, state, header, post}
      other -> other
    end
  end

  defp apply_file(_env, state, {:staged, header, post, total}, key, current, page_size) do
    {min, _max, _ts, _commit, pre, ps} = header

    cond do
      min != 1 and pre != current ->
        {:error, {:pre_apply_mismatch, pre, current}}

      page_size != 0 and ps != page_size ->
        {:error, {:invalid, "page size differs"}}

      true ->
        case Native.lazy_apply(state.lazy, length(state.files)) do
          :ok ->
            {:ok, %{state | files: [key | state.files], bytes: state.bytes + total}, header, post}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # The page map starts at the first file, whose header names the page size.
  # It creates (and empties) the database file.
  defp ensure_lazy(_env, %{lazy: handle} = state, _head) when handle != nil, do: {:ok, state}

  defp ensure_lazy(env, state, <<_::binary-size(8), page_size::32, _::binary>>) do
    with {:ok, handle} <- Native.lazy_new(env.dest, page_size, env.lazy.wait_ms) do
      File.rm(env.dest <> "-wal")
      File.rm(env.dest <> "-shm")
      {:ok, %{state | lazy: handle}}
    end
  end

  defp ensure_lazy(_env, _state, _head), do: {:error, {:invalid, "short file"}}

  # Edges of a file: its first and last `edge_bytes` bytes, which hold the
  # header and the trailer with the page index, and its size. Fetched ahead of
  # the walk when the chain can be guessed from the listing.
  defp edges(env, key) do
    case :ets.lookup(env.prefetched, key) do
      [{_, result}] -> result
      [] -> fetch_edges(env, key)
    end
  end

  defp fetch_edges(env, key) do
    with {:ok, head, total, meta} <- range(env, key, 0, env.edge_bytes),
         {:ok, tail} <- tail(env, key, head, total) do
      {:ok, {head, tail, total}, meta}
    end
  end

  defp tail(env, _key, head, total) when total <= env.edge_bytes, do: {:ok, head}

  defp tail(env, key, _head, total) do
    with {:ok, tail, _total, _meta} <- range(env, key, total - env.edge_bytes, env.edge_bytes),
         do: {:ok, tail}
  end

  defp range(env, key, offset, length) do
    Objects.fetch_range(env.ctx, key, offset, length, env.deadline)
  end

  # Reads ahead, in parallel, the files a restore will most likely need: from
  # the start, the file that reaches furthest from the next TXID. A wrong
  # guess costs requests, not correctness: the walk reads what is missing.
  defp prefetch(env, start) do
    keys = guess(env, start)
    if env.lazy, do: prefetch_edges(env, keys), else: prefetch_bodies(env, keys)
  end

  defp guess(env, start) do
    first =
      case start do
        {:snapshot, snap} -> [snap.key]
        :scratch -> []
      end

    txid = with {:snapshot, snap} <- start, do: snap.txid, else: (_ -> 0)

    chain =
      Stream.unfold(txid, fn txid ->
        env.segments
        |> Enum.filter(&(&1.min == txid + 1 and within_txid?(&1.max, env.limits)))
        |> Enum.min_by(&{-&1.max, &1.level}, fn -> nil end)
        |> case do
          nil -> nil
          seg -> {seg.key, seg.max}
        end
      end)

    first ++ Enum.to_list(chain)
  end

  # The first range of every file says its size. Files that add up to less than
  # `min_bytes` are not worth a lazy restore, so the trailers are not read.
  defp prefetch_edges(env, keys) do
    heads = parallel(env, keys, &range(env, &1, 0, env.edge_bytes))
    found = for {key, {:ok, head, total, meta}} <- heads, do: {key, head, total, meta}

    if keys != [] and length(found) == length(keys) and
         Enum.sum(for {_, _, total, _} <- found, do: total) < env.lazy.min_bytes do
      :small
    else
      tails = parallel(env, found, fn {key, head, total, _} -> tail(env, key, head, total) end)

      for {{key, head, total, meta}, {:ok, tail}} <- tails do
        :ets.insert(env.prefetched, {key, {:ok, {head, tail, total}, meta}})
      end

      :ok
    end
  end

  # Whole files, up to `prefetch_bytes` of them, so that they are not held in
  # memory without limit.
  defp prefetch_bodies(env, keys) do
    fetched = :atomics.new(1, [])

    parallel(env, keys, fn key ->
      if :atomics.get(fetched, 1) < env.prefetch_bytes do
        with {:ok, body, meta} <- Objects.fetch_optional(env.ctx, key, env.deadline) do
          :atomics.add(fetched, 1, byte_size(body))
          :ets.insert(env.prefetched, {key, {:ok, body, meta}})
        end
      end
    end)

    :ok
  end

  defp parallel(env, items, fun) do
    items
    |> Task.async_stream(&{&1, fun.(&1)}, max_concurrency: env.concurrency, timeout: :infinity)
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp ts_ok?(%{limits: %{ts: :infinity}}, _ts), do: true
  defp ts_ok?(%{limits: %{ts: limit}}, ts), do: ts <= limit

  defp named({:error, reason}, kind, key), do: {:error, {kind, key, reason}}
  defp named(ok, _kind, _key), do: ok

  defp wrap({:ok, header, post}, _kind, _key), do: {:ok, header, post}
  defp wrap({:error, reason}, kind, key), do: {:error, {kind, key, reason}}

  defp fetch(env, key) do
    case :ets.take(env.prefetched, key) do
      [{_, {:ok, body, meta}}] -> {:ok, body, meta}
      [] -> Objects.fetch(env.ctx, key, env.deadline)
    end
  end
end
