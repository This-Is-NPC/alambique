defmodule Alambique.Retention do
  @moduledoc """
  Retention of one cell's replicated files (design §9).

  `run/3` marks the **covered files** for deletion. The *anchor* is the
  newest snapshot stamped at or before `now - :pitr_window_ms`. Every point
  from the anchor on stays restorable, which includes every point of the
  window. Snapshots older than the anchor and files that end at or before it
  are deleted. Nothing is removed when no snapshot is that old, and the newest
  snapshot and everything after the anchor are never touched.

  Retention never deletes a file that is not covered. Lower-epoch files that a
  fenced-out owner left past the chain are ignored by every reader and
  overwritten by the next owner (design §6, R3); a delete by key would race
  with that overwrite.

  ## Safety against a concurrent restore

  A restore lists the store and then fetches what it listed. A deletion could
  remove a file between the two. Covered files therefore go only once the
  anchor snapshot has existed for `:grace_ms` (default ten minutes, far above
  `:op_timeout_ms`, the longest a restore may run): a restore that listed
  before the anchor existed finished within the grace, and one that listed
  after it starts from the anchor or a newer snapshot and never needs the
  deleted files. An anchor without a recorded creation time is not trusted.

  ## Delayed deletion

  Covered files are not deleted: `Alambique.Trash` records them and deletes
  them `:deletion_grace_ms` later (design §9.1), so a restore to a point
  before the anchor keeps working until then. A marker object
  (`<cell prefix>/trashed`) holds the newest anchor whose files were recorded,
  so a later run records only what is new and does not count a file twice.

  Deletion order cannot break a restore: a failure part-way leaves a store in
  which the anchor and everything after it are intact.

  Options of `run/3` (all optional):

    * `:pitr_window_ms` - how far back every point stays restorable (default
      seven days)
    * `:grace_ms` - see above (default 600 000)
    * `:timeout_ms` - time budget of the run (default 120 000)
    * `:deletion_grace_ms`, `:batch_keys` and `:concurrency` - see
      `Alambique.Trash` (defaults: its own; `0` grace deletes at once)

  Telemetry is described in `docs/telemetry.md`, section "Retention events".
  """

  alias Alambique.Fault
  alias Alambique.Replication.{Keys, Native, Objects, Retry}
  alias Alambique.{Telemetry, Trash}

  @defaults [
    pitr_window_ms: 7 * 86_400_000,
    grace_ms: 600_000,
    timeout_ms: 120_000
  ]

  @type summary :: %{
          anchor: pos_integer | nil,
          segments: non_neg_integer,
          snapshots: non_neg_integer,
          waiting: boolean
        }

  @doc "The default options."
  @spec defaults() :: keyword
  def defaults, do: @defaults

  @doc "One retention run over `cell`. `ctx` is an `Alambique.Replication.ctx/1`."
  @spec run(Alambique.Replication.ctx(), String.t(), keyword) :: {:ok, summary} | {:error, term}
  def run(ctx, cell, opts) do
    opts = Keyword.merge(@defaults, opts)
    started = System.monotonic_time()
    deadline = Retry.deadline_in(opts[:timeout_ms])

    result =
      with {:ok, {segments, snapshots}} <- Objects.list_cell(ctx, cell, deadline) do
        env = %{ctx: ctx, cell: cell, opts: opts, deadline: deadline}
        summary = %{anchor: nil, segments: 0, snapshots: 0, waiting: false}

        with {:ok, anchor} <- find_anchor(env, snapshots),
             do: cover(env, anchor, segments, snapshots, summary)
      end

    telemetry(cell, started, result)
    result
  end

  defp telemetry(cell, started, {:ok, s}) do
    Telemetry.span_stop(
      [:retention, :run],
      started,
      %{segments: s.segments, snapshots: s.snapshots},
      %{cell: cell, result: :ok, anchor: s.anchor, waiting: s.waiting}
    )
  end

  defp telemetry(cell, started, {:error, reason}) do
    Telemetry.span_stop(
      [:retention, :run],
      started,
      %{segments: 0, snapshots: 0},
      %{cell: cell, result: :error, reason: reason}
    )
  end

  # Anchor

  defp find_anchor(env, snapshots) do
    cutoff = now(env.ctx) - env.opts[:pitr_window_ms]

    snapshots
    |> Enum.reverse()
    |> Enum.reduce_while({:ok, nil}, fn snap, {:ok, nil} ->
      case snapshot_info(env, snap) do
        {:ok, %{ts: ts} = info} when ts <= cutoff -> {:halt, {:ok, Map.merge(snap, info)}}
        {:ok, _} -> {:cont, {:ok, nil}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # `ts` and `created` come from the object metadata; an older object without
  # them is read once for the stamp of its newest state.
  defp snapshot_info(env, snap) do
    with {:ok, meta} <- Objects.head(env.ctx, snap.key, env.deadline) do
      case Objects.int(meta, "ts") do
        nil -> read_stamp(env, snap)
        ts -> {:ok, %{ts: ts, created: Objects.int(meta, "created")}}
      end
    end
  end

  defp read_stamp(env, snap) do
    with {:ok, body, meta} <- Objects.fetch(env.ctx, snap.key, env.deadline),
         {:ok, {_, _, ts, _, _, _}, _} <- Native.ltx_info(body, false) do
      {:ok, %{ts: ts, created: Objects.int(meta, "created")}}
    else
      {:error, reason} -> {:error, {:snapshot, snap.key, reason}}
    end
  end

  # Covered files

  defp cover(_env, nil, _segments, _snapshots, summary), do: {:ok, summary}

  defp cover(env, anchor, segments, snapshots, summary) do
    summary = %{summary | anchor: anchor.txid}
    old_enough? = anchor.created != nil and now(env.ctx) - anchor.created >= env.opts[:grace_ms]

    if old_enough? do
      {snaps, segs} = covered(anchor.txid, segments, snapshots)
      grace = env.opts[:deletion_grace_ms] || Trash.default_grace_ms()

      with {:ok, previous} <- marked_anchor(env, grace, snaps ++ segs),
           {old_snaps, old_segs} = covered(previous, segments, snapshots),
           snaps = snaps -- old_snaps,
           segs = segs -- old_segs,
           {:ok, _} <- mark(env, snaps ++ segs, grace),
           :ok <- record_anchor(env, grace, anchor.txid, previous, snaps ++ segs) do
        {:ok, %{summary | snapshots: length(snaps), segments: length(segs)}}
      end
    else
      {:ok, %{summary | waiting: true}}
    end
  end

  # Keys of the snapshots older than `txid` and of the segments that end at
  # or before it.
  defp covered(txid, segments, snapshots) do
    {
      for(snap <- snapshots, snap.txid < txid, do: snap.key),
      for(seg <- segments, seg.max <= txid, do: seg.key)
    }
  end

  # The newest anchor whose covered files were recorded; 0 when none was or
  # when files are deleted at once.
  defp marked_anchor(_env, 0, _keys), do: {:ok, 0}
  defp marked_anchor(_env, _grace, []), do: {:ok, 0}

  defp marked_anchor(env, _grace, _keys) do
    case Objects.fetch_optional(env.ctx, Keys.trashed(env.ctx.prefix, env.cell), env.deadline) do
      {:ok, body, _meta} ->
        case Integer.parse(body) do
          {txid, ""} -> {:ok, txid}
          _ -> {:ok, 0}
        end

      :none ->
        {:ok, 0}

      {:error, _} = error ->
        error
    end
  end

  # After the manifests, so that a crash in between records the files again.
  defp record_anchor(_env, 0, _txid, _previous, _keys), do: :ok
  defp record_anchor(_env, _grace, _txid, _previous, []), do: :ok
  defp record_anchor(_env, _grace, txid, previous, _keys) when txid <= previous, do: :ok

  defp record_anchor(env, _grace, txid, _previous, _keys) do
    Fault.point(:retain_before_marker, %{cell: env.cell, txid: txid})
    key = Keys.trashed(env.ctx.prefix, env.cell)

    case Objects.put(env.ctx, key, Integer.to_string(txid), :none, %{}, env.deadline) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  defp mark(env, keys, grace) do
    opts = [grace_ms: grace] ++ Keyword.take(env.opts, [:batch_keys, :concurrency])
    Trash.mark(env.ctx, env.cell, keys, opts, env.deadline)
  end

  defp now(%{clock: {mod, config}}), do: mod.now_ms(config)
end
