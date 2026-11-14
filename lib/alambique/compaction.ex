defmodule Alambique.Compaction do
  @moduledoc """
  Compaction of one cell's replicated files (design §9).

  `run/3` does one pass over a cell, on any node, whether the cell is awake or
  not. It never changes what a restore returns, only how many files restore
  has to read:

    1. Level-0 files, one per replicated batch, are merged `:fanout` at a time
       into level-1 files, level-1 files into level 2, and so on up to
       `:max_level`. The merged files cover fixed, aligned blocks of TXIDs, so
       concurrent compactors plan the same keys.
    2. When enough TXIDs or enough time passed since the newest snapshot, the
       chain is merged into a new snapshot.

  Every object is written with `:if_none_match`. A file that already exists is
  assumed to hold the same content, because it comes from the same chain.
  A group is skipped, never merged, when its epochs do not grow (R2), so a
  write of a fenced-out epoch cannot be moved into a file with a higher epoch.

  Options of `run/3` (all optional):

    * `:fanout` - files merged into one (default 8)
    * `:max_level` - highest level of merged files (default 2, at most 8)
    * `:snapshot_interval_txids` - TXIDs after the newest snapshot that make a
      new one due (default 4096)
    * `:snapshot_ratio` - a snapshot is due when the level-0 bytes written since
      the newest snapshot reach this many times the size of that snapshot
      (default 2.0; a float or an integer above 0). The compactor counts them
      in the `chain` object of the cell. A restore then reads about
      `1 + ratio` times the database at most. Without a count for the newest
      snapshot, only the two limits below apply until the next snapshot.
    * `:snapshot_min_bytes` - the size from which `:snapshot_ratio` counts, for
      a cell with no snapshot or a small one (default 4 194 304)
    * `:snapshot_keep_ms` - a new snapshot makes the previous one redundant,
      and it is marked for deletion unless it is at least this long after the
      snapshot before it, or older than 90% of `:pitr_window_ms` (default
      259 200 000, three days; 0 keeps every snapshot)
    * `:pitr_window_ms` - the retention window, so that no snapshot the anchor
      of retention may be is marked (default none: no limit)
    * `:snapshot_interval_ms` - age of the newest snapshot (or of the first
      file when there is none) that makes a new one due (default seven days). A
      limit on the age for a cell that writes little; the bytes decide first
      (design §9)
    * `:max_merges` - merges per pass; `more: true` in the result says more is
      left (default 32)
    * `:timeout_ms` - time budget of the whole pass (default 120 000)
    * `:delete_superseded` - mark for deletion the merged files that a file of
      the next level replaces (default `true`). Level 0 files stay, because a
      rewind to any TXID of the window needs them; so do the files of the
      highest level. Marked files go through `Alambique.Trash`, so
      `:deletion_grace_ms`, `:batch_keys` and `:concurrency` apply (defaults:
      its own).
    * `:min_tail` - the pass does nothing, and `tail` in its summary says why,
      when the chain a restore would walk holds fewer level-0 files than this
      (default 0: always). A release of a cell uses it to compact only a long
      tail.
    * `:hold` - the instance config whose held bytes (`Compaction.Hold`) the
      pass may use instead of reading the store; `nil` reads everything
      (default)

  A held file is used only when its key is in the listing the pass just made
  and its epoch passes the same check as a fetched one. Merged files are held
  as the inputs of the next level.

  Telemetry is described in `docs/telemetry.md`, section "Compaction events".
  """

  alias Alambique.Fault
  alias Alambique.Compaction.{Hold, Plan}
  alias Alambique.Replication.{Keys, Native, Objects, Retry}
  alias Alambique.{Telemetry, Trash}

  @defaults [
    fanout: 8,
    max_level: 2,
    snapshot_interval_txids: 4096,
    snapshot_interval_ms: 604_800_000,
    snapshot_ratio: 2.0,
    snapshot_min_bytes: 4_194_304,
    snapshot_keep_ms: 259_200_000,
    pitr_window_ms: nil,
    max_merges: 32,
    timeout_ms: 120_000,
    delete_superseded: true
  ]

  @type summary :: %{
          merges: non_neg_integer,
          bytes: non_neg_integer,
          snapshot: pos_integer | nil,
          skipped: [{atom, term}],
          more: boolean,
          head: non_neg_integer,
          tail: non_neg_integer
        }

  @doc "The default options."
  @spec defaults() :: keyword
  def defaults, do: @defaults

  @doc "One compaction pass over `cell`. `ctx` is an `Alambique.Replication.ctx/1`."
  @spec run(Alambique.Replication.ctx(), String.t(), keyword) ::
          {:ok, summary} | {:error, term}
  def run(ctx, cell, opts \\ []) do
    opts = Keyword.merge(@defaults, opts)
    started = System.monotonic_time()
    deadline = Retry.deadline_in(opts[:timeout_ms])

    result =
      with {:ok, state} <- load(ctx, cell, opts, deadline) do
        if state.tail < Keyword.get(opts, :min_tail, 0),
          do: {:ok, summary(state)},
          else: work(state)
      end

    Telemetry.span_stop(
      [:compaction, :pass],
      started,
      pass_measurements(result),
      pass_meta(cell, result)
    )

    result
  end

  defp work(state) do
    with {:ok, state} <- merge_levels(state),
         {:ok, state} <- delete_superseded(state),
         {:ok, state} <- count_bytes(state),
         {:ok, state} <- snapshot(state) do
      {:ok, summary(state)}
    end
  end

  defp pass_measurements({:ok, s}), do: %{merges: s.merges, bytes: s.bytes}
  defp pass_measurements({:error, _}), do: %{merges: 0, bytes: 0}

  defp pass_meta(cell, {:ok, s}),
    do: %{
      cell: cell,
      result: :ok,
      head: s.head,
      snapshot: s.snapshot,
      skipped: s.skipped,
      more: s.more
    }

  defp pass_meta(cell, {:error, reason}), do: %{cell: cell, result: :error, reason: reason}

  defp summary(state) do
    %{
      merges: state.merges,
      bytes: state.bytes,
      snapshot: state.snapshot,
      skipped: Enum.reverse(state.skipped),
      more: state.more,
      head: state.head,
      tail: state.tail
    }
  end

  # State

  defp load(ctx, cell, opts, deadline) do
    with {:ok, {segments, snapshots}} <- Objects.list_cell(ctx, cell, deadline) do
      newest = List.last(snapshots)
      from = if(newest, do: newest.txid, else: 0)
      index = Plan.index(segments)

      {:ok,
       %{
         ctx: ctx,
         cell: cell,
         opts: opts,
         hold: opts[:hold],
         max_merges: opts[:max_merges],
         deadline: deadline,
         index: index,
         snapshot_at: newest && newest.txid,
         snapshot_key: newest && newest.key,
         from: from,
         head: 0,
         tail: tail(index, from, opts),
         merges: 0,
         bytes: 0,
         superseded: [],
         l0_bytes: 0,
         chain: nil,
         snapshots: snapshots,
         snapshot: nil,
         skipped: [],
         more: false
       }}
    end
  end

  # The level-0 files of the chain: what a restore reads that no merge covers.
  # Only counted when the pass may skip on it.
  defp tail(index, from, opts) do
    if Keyword.get(opts, :min_tail, 0) > 0,
      do: index |> Plan.chain(from) |> Enum.count(&(&1.level == 0)),
      else: 0
  end

  # Merging

  defp merge_levels(state) do
    Enum.reduce_while(0..(state.opts[:max_level] - 1)//1, {:ok, state}, fn level, {:ok, state} ->
      head = state.index |> Plan.chain(state.from) |> Plan.head(state.from)
      groups = Plan.groups(state.index, state.from, head, state.opts[:fanout], level)

      case merge_groups(%{state | head: head}, groups) do
        {:ok, state} -> {:cont, {:ok, state}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp merge_groups(state, []), do: {:ok, state}

  defp merge_groups(%{merges: merges, max_merges: max} = state, _groups) when merges >= max,
    do: {:ok, %{state | more: true}}

  defp merge_groups(state, groups) do
    if expired?(state), do: {:ok, %{state | more: true}}, else: merge_next(state, groups)
  end

  # A request past the deadline still gets one attempt (`Retry`), so the pass
  # stops starting merges once it passed, and `more: true` says so.
  defp expired?(state), do: System.monotonic_time(:millisecond) >= state.deadline

  defp merge_next(state, [group | rest]) do
    case merge_group(state, group) do
      {:ok, state} -> merge_groups(state, rest)
      {:error, _} = error -> error
    end
  end

  defp merge_group(state, group) do
    if state.merges > 0, do: Fault.point(:compact_between_merges, %{cell: state.cell})
    started = System.monotonic_time()
    meta = %{cell: state.cell, level: group.level, min: group.min, max: group.max}

    result =
      with {:ok, pred_epoch} <- predecessor_epoch(state, group),
           {:ok, inputs, held} <- fetch_all(state, group.inputs),
           :ok <- check_epochs(inputs, pred_epoch),
           {:ok, merged} <- merge(inputs, group),
           {:ok, outcome, bytes} <- put_merged(state, group, inputs, merged) do
        {:ok, outcome, bytes, held, level0_bytes(group, inputs)}
      end

    case result do
      {:ok, outcome, bytes, held, level0} ->
        Telemetry.span_stop(
          [:compaction, :merge],
          started,
          %{
            bytes: bytes,
            inputs: length(group.inputs),
            held: held,
            fetched: length(group.inputs) - held
          },
          Map.put(meta, :result, outcome)
        )

        {:ok, record(state, group, outcome, bytes, level0)}

      {:skip, reason} ->
        Telemetry.span_stop(
          [:compaction, :merge],
          started,
          %{bytes: 0, inputs: length(group.inputs)},
          Map.merge(meta, %{result: :skipped, reason: reason})
        )

        {:ok, %{state | skipped: [{reason, {group.level, group.min, group.max}} | state.skipped]}}

      {:error, reason} ->
        Telemetry.span_stop(
          [:compaction, :merge],
          started,
          %{bytes: 0, inputs: length(group.inputs)},
          Map.merge(meta, %{result: :error, reason: reason})
        )

        {:error, reason}
    end
  end

  # The bytes of the level-0 files a level-1 merge consumed: each batch is
  # counted once, when its file is merged.
  defp level0_bytes(%{level: 1}, inputs),
    do: inputs |> Enum.map(fn {_file, body, _meta} -> byte_size(body) end) |> Enum.sum()

  defp level0_bytes(_group, _inputs), do: 0

  defp record(state, group, outcome, bytes, level0) do
    seg = %{
      key: Keys.segment(state.ctx.prefix, state.cell, group.level, group.min, group.max),
      level: group.level,
      min: group.min,
      max: group.max
    }

    merges = if outcome == :ok, do: state.merges + 1, else: state.merges

    %{
      state
      | index: Plan.add(state.index, seg),
        merges: merges,
        bytes: state.bytes + bytes,
        l0_bytes: if(outcome == :ok, do: state.l0_bytes + level0, else: state.l0_bytes),
        superseded: superseded(state, group, outcome)
    }
  end

  # The inputs of a merge to level 2 or above that this pass wrote. Level 0
  # stays for the rewind; a file that already existed was another compactor's
  # to mark.
  defp superseded(state, %{level: level, inputs: inputs}, :ok) when level >= 2 do
    if state.opts[:delete_superseded],
      do: Enum.map(inputs, & &1.key) ++ state.superseded,
      else: state.superseded
  end

  defp superseded(state, _group, _outcome), do: state.superseded

  defp delete_superseded(%{superseded: []} = state), do: {:ok, state}

  defp delete_superseded(state) do
    with {:ok, _} <- mark(state, state.superseded), do: {:ok, %{state | superseded: []}}
  end

  defp mark(state, keys) do
    opts =
      [grace_ms: state.opts[:deletion_grace_ms] || Trash.default_grace_ms()] ++
        Keyword.take(state.opts, [:batch_keys, :concurrency])

    Trash.mark(state.ctx, state.cell, keys, opts, state.deadline)
  end

  # The epoch of what comes before the group: the snapshot or the file that
  # ends right before it. A group without a predecessor cannot be placed on
  # the chain.
  defp predecessor_epoch(_state, %{min: 1}), do: {:ok, 0}

  defp predecessor_epoch(state, %{min: min}) do
    files = Plan.ending_at(state.index, min - 1)

    cond do
      files != [] -> epoch_of_files(state, files)
      state.snapshot_at == min - 1 -> epoch_of_file(state, state.snapshot_key)
      true -> {:skip, :no_predecessor}
    end
  end

  # Files that end at the same TXID have the epoch of the same last batch.
  defp epoch_of_files(state, [file | _] = files) do
    case Enum.find_value(files, &held(state, &1)) do
      {meta, _body} -> {:ok, Objects.epoch_of(meta)}
      nil -> epoch_of_file(state, file)
    end
  end

  defp epoch_of_file(state, file) do
    with {:ok, meta} <- Objects.head(state.ctx, file_key(file), state.deadline) do
      {:ok, Objects.epoch_of(meta)}
    else
      {:error, :not_found} -> {:skip, :no_predecessor}
      {:error, _} = error -> error
    end
  end

  defp file_key(key) when is_binary(key), do: key
  defp file_key(%{key: key}), do: key

  # The held bytes of a file of the listing, if any.
  defp held(%{hold: hold, cell: cell}, %{level: level, min: min, max: max}) when hold != nil,
    do: Hold.get(hold, {cell, level, min, max})

  defp held(_state, _file), do: nil

  # `held` of the inputs come from the hold table, the others from the store.
  defp fetch_all(state, files) do
    looked = Enum.map(files, &{&1, held(state, &1)})

    with {:ok, fetched} <- fetch_missing(state, for({file, nil} <- looked, do: file)) do
      {inputs, []} =
        Enum.map_reduce(looked, fetched, fn
          {file, {meta, body}}, rest -> {{file, body, meta}, rest}
          {_file, nil}, [input | rest] -> {input, rest}
        end)

      {:ok, inputs, length(files) - length(fetched)}
    end
  end

  defp fetch_missing(state, files) do
    files
    |> Task.async_stream(
      fn file ->
        case Objects.fetch(state.ctx, file.key, state.deadline) do
          {:ok, body, meta} -> {:ok, file, body, meta}
          {:error, :not_found} -> {:skip, :input_missing}
          {:error, _} = error -> error
        end
      end,
      max_concurrency: 4,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:task_exit, reason}}
    end)
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, file, body, meta}, {:ok, acc} -> {:cont, {:ok, [{file, body, meta} | acc]}}
      other, _ -> {:halt, other}
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      other -> other
    end
  end

  # Epochs must not decrease along the inputs, nor below what precedes them.
  defp check_epochs(inputs, pred_epoch) do
    epochs = Enum.map(inputs, fn {_, _, meta} -> Objects.epoch_of(meta) end)

    if Enum.all?(Enum.zip([pred_epoch | epochs], epochs), fn {a, b} -> a <= b end),
      do: :ok,
      else: {:skip, :epoch_regression}
  end

  defp merge(inputs, _group) do
    case Native.merge_ltx(Enum.map(inputs, fn {_, body, _} -> body end)) do
      {:ok, merged} -> {:ok, merged}
      {:error, reason} -> {:skip, {:merge_failed, reason}}
    end
  end

  defp put_merged(state, group, inputs, merged) do
    key = Keys.segment(state.ctx.prefix, state.cell, group.level, group.min, group.max)
    {_, _, last} = List.last(inputs)

    info = %{
      epoch: Objects.epoch_of(last),
      data: Objects.data_of(last, group.max),
      ts: Objects.int(last, "ts")
    }

    Fault.point(:compact_before_put, %{cell: state.cell, key: key})

    case Objects.put_new(state.ctx, key, merged, info, state.deadline) do
      :ok ->
        Fault.point(:compact_after_put, %{cell: state.cell, key: key})
        keep_merged(state, group, inputs, merged, info)
        {:ok, :ok, byte_size(merged)}

      :exists ->
        keep_merged(state, group, inputs, merged, info)
        {:ok, :exists, 0}

      {:error, _} = error ->
        error
    end
  end

  # The output is the input of the next level; the inputs are done with.
  defp keep_merged(%{hold: nil}, _group, _inputs, _merged, _info), do: :ok

  defp keep_merged(%{hold: hold, cell: cell} = state, group, inputs, merged, info) do
    Hold.drop(hold, for({%{level: l, min: a, max: b}, _, _} <- inputs, do: {cell, l, a, b}))
    Hold.put(hold, state.ctx, {cell, group.level, group.min, group.max}, info, merged)
  end

  # Snapshot

  defp snapshot(state) do
    chain = Plan.chain(state.index, state.from)
    head = Plan.head(chain, state.from)
    state = %{state | head: head}

    with true <- head > state.from and not expired?(state),
         {:ok, true} <- due?(state, chain, head) do
      build_snapshot(state, chain, head)
    else
      false -> {:ok, state}
      {:ok, false} -> {:ok, state}
      {:error, _} = error -> error
    end
  end

  defp due?(state, chain, head) do
    opts = state.opts

    if head - state.from >= opts[:snapshot_interval_txids] do
      {:ok, true}
    else
      case snapshot_ts(state, state.snapshot_key || hd(chain)) do
        {:ok, ts} when ts != nil ->
          if now(state.ctx) - ts >= opts[:snapshot_interval_ms],
            do: {:ok, true},
            else: ratio_due?(state)

        {:ok, nil} ->
          ratio_due?(state)

        {:error, _} = error ->
          error
      end
    end
  end

  # The bytes written since the newest snapshot against its size (design §9).
  defp ratio_due?(state) do
    with {:ok, chain} <- chain(state) do
      case base_bytes(state, chain) do
        nil -> {:ok, false}
        base -> {:ok, chain.bytes >= state.opts[:snapshot_ratio] * base}
      end
    end
  end

  # No snapshot yet: the minimum counts. A snapshot whose size the count does
  # not know gives no base: the ratio waits for
  # the next snapshot.
  defp base_bytes(%{snapshot_at: nil, opts: opts}, _chain), do: opts[:snapshot_min_bytes]

  defp base_bytes(%{opts: opts}, %{snapshot_bytes: bytes}) when is_integer(bytes),
    do: max(bytes, opts[:snapshot_min_bytes])

  defp base_bytes(_state, _chain), do: nil

  # The count of the `chain` object, if it belongs to the newest snapshot.
  defp chain(%{chain: chain}) when chain != nil, do: {:ok, chain}

  defp chain(state) do
    key = Keys.chain(state.ctx.prefix, state.cell)
    empty = %{snapshot_bytes: nil, bytes: 0}

    case Objects.fetch_optional(state.ctx, key, state.deadline) do
      {:ok, body, _meta} -> {:ok, parse_chain(body, state.snapshot_at || 0, empty)}
      :none -> {:ok, empty}
      {:error, _} = error -> error
    end
  end

  defp parse_chain(body, snapshot_at, empty) do
    with [txid, size, bytes] <- String.split(body, ":"),
         {^snapshot_at, ""} <- Integer.parse(txid),
         {size, ""} <- Integer.parse(size),
         {bytes, ""} <- Integer.parse(bytes) do
      %{snapshot_bytes: size, bytes: bytes}
    else
      _ -> empty
    end
  end

  defp put_chain(state, snapshot_at, snapshot_bytes, bytes) do
    key = Keys.chain(state.ctx.prefix, state.cell)
    body = "#{snapshot_at}:#{snapshot_bytes}:#{bytes}"
    # A lost update only delays a snapshot; the age and TXID limits still hold.
    Objects.put(state.ctx, key, body, :none, %{}, state.deadline)
  end

  # Adds the level-0 bytes this pass merged to the count.
  defp count_bytes(%{l0_bytes: 0} = state), do: {:ok, state}

  defp count_bytes(state) do
    with {:ok, chain} <- chain(state) do
      chain = %{chain | bytes: chain.bytes + state.l0_bytes}

      if chain.snapshot_bytes != nil or state.snapshot_at == nil,
        do: put_chain(state, state.snapshot_at || 0, chain.snapshot_bytes || 0, chain.bytes)

      {:ok, %{state | chain: chain, l0_bytes: 0}}
    end
  end

  # The time of the newest snapshot (or first file): a held file knows it; for
  # the others the hold table remembers it per key instead of a head per pass.
  defp snapshot_ts(state, file) do
    case held(state, file) do
      {meta, _body} -> {:ok, Objects.int(meta, "ts")}
      nil -> cached_ts(state, file_key(file))
    end
  end

  defp cached_ts(%{hold: nil} = state, key), do: head_ts(state, key)

  defp cached_ts(%{hold: hold, cell: cell} = state, key) do
    case Hold.age(hold, cell, key) do
      nil ->
        with {:ok, ts} when ts != nil <- head_ts(state, key) do
          Hold.put_age(hold, cell, key, ts)
          {:ok, ts}
        end

      ts ->
        {:ok, ts}
    end
  end

  defp head_ts(state, key) do
    with {:ok, meta} <- Objects.head(state.ctx, key, state.deadline),
         do: {:ok, Objects.int(meta, "ts")}
  end

  defp build_snapshot(state, chain, head) do
    started = System.monotonic_time()
    key = Keys.snapshot(state.ctx.prefix, state.cell, head)
    files = if state.snapshot_key, do: [%{key: state.snapshot_key} | chain], else: chain
    meta = %{cell: state.cell, txid: head}

    result =
      with {:ok, inputs, _held} <- fetch_all(state, files),
           :ok <- check_epochs(inputs, 0),
           {:ok, merged} <- merge(inputs, nil),
           {:ok, {_, _, ts, _, _, _}, _} <- ltx_info(merged) do
        {_, _, last} = List.last(inputs)
        epoch = Objects.epoch_of(last)
        Fault.point(:compact_before_snapshot, %{cell: state.cell, txid: head})
        info = %{epoch: epoch, data: Objects.data_of(last, head), ts: ts, created: now(state.ctx)}

        case Objects.put_new(state.ctx, key, merged, info, state.deadline) do
          :ok ->
            Fault.point(:compact_after_snapshot, %{cell: state.cell, txid: head})
            keep_snapshot(state, key, ts, chain)
            put_chain(state, head, byte_size(merged), 0)
            thin_snapshots(state)
            {:ok, :ok, byte_size(merged)}

          :exists ->
            keep_snapshot(state, key, ts, chain)
            {:ok, :exists, 0}

          {:error, _} = error ->
            error
        end
      end

    case result do
      {:ok, outcome, bytes} ->
        Telemetry.span_stop(
          [:compaction, :snapshot],
          started,
          %{bytes: bytes},
          Map.put(meta, :result, outcome)
        )

        {:ok, %{state | snapshot: head, bytes: state.bytes + bytes}}

      {:skip, reason} ->
        Telemetry.span_stop(
          [:compaction, :snapshot],
          started,
          %{bytes: 0},
          Map.merge(meta, %{result: :skipped, reason: reason})
        )

        {:ok, %{state | skipped: [{reason, {:snapshot, head}} | state.skipped]}}

      {:error, reason} ->
        Telemetry.span_stop(
          [:compaction, :snapshot],
          started,
          %{bytes: 0},
          Map.merge(meta, %{result: :error, reason: reason})
        )

        {:error, reason}
    end
  end

  # The new snapshot replaces the previous one as the base of a restore. The
  # previous one stays only if it is far enough from the one before it to be
  # the start of a window, or inside the window by less than a margin, because
  # retention may take it as its anchor and delete the files before it. Marked
  # files go through the trash; a failure to mark leaves the snapshot for
  # retention to delete with the rest.
  defp thin_snapshots(state) do
    keep = state.opts[:snapshot_keep_ms]

    with true <- keep > 0,
         [older, previous] <- Enum.take(state.snapshots, -2),
         {:ok, previous_ts} when previous_ts != nil <- snapshot_ts(state, previous.key),
         {:ok, older_ts} when older_ts != nil <- head_ts(state, older.key),
         true <- previous_ts - older_ts < keep,
         true <- within_window?(state, previous_ts) do
      mark(state, [previous.key])
    else
      _ -> :ok
    end
  end

  defp within_window?(%{opts: opts} = state, ts) do
    case opts[:pitr_window_ms] do
      nil -> true
      window -> ts > now(state.ctx) - trunc(window * 0.9)
    end
  end

  # The chain is covered by the snapshot; remember the snapshot's age.
  defp keep_snapshot(%{hold: nil}, _key, _ts, _chain), do: :ok

  defp keep_snapshot(%{hold: hold, cell: cell}, key, ts, chain) do
    Hold.drop(hold, for(%{level: l, min: a, max: b} <- chain, do: {cell, l, a, b}))
    Hold.put_age(hold, cell, key, ts)
  end

  defp ltx_info(merged) do
    case Native.ltx_info(merged, false) do
      {:ok, _, _} = ok -> ok
      {:error, reason} -> {:skip, {:merge_failed, reason}}
    end
  end

  defp now(%{clock: {mod, config}}), do: mod.now_ms(config)
end
