defmodule Alambique.Rewind do
  @moduledoc false
  # Replacing the state of a cell with an earlier one (design §20).
  #
  # The switch is one conditional put: a level-0 segment, TXID head + 1, that
  # lists every page of the rewound database and starts from the checksum of
  # the current head. Until it lands the store holds the old chain; after it, a
  # restore gives the new state. Nothing else needs to be undone, so a crash at
  # any point leaves the old state or the new one:
  #
  #   1. Restore the target (history is immutable: no lease needed).
  #   2. Take the lease as owner "rewind" (the owner hibernates first, or its
  #      lease expires): no process serves the cell any more.
  #   3. Restore the current state, carry the host tables over to the target
  #      file and add the feed marker (`Rewind.Reconcile`).
  #   4. Put the segment with the lease's epoch. A 412 means the chain moved
  #      (a stale writer got the TXID): start again from 3.
  #   5. Release the lease. The next wake restores through the segment.

  alias Alambique.{Config, Exclusive, Fault, Inflight, Lease, Placement}
  alias Alambique.{Replication, Schema, Telemetry}
  alias Alambique.Cell.{Db, Local}
  alias Alambique.Feed.Capture
  alias Alambique.Replication.{Objects, Replicator, Retry}
  alias Alambique.Rewind.Reconcile

  require Logger

  @owner "rewind"
  @opts [:keep, :schema, :timeout, :if_head]

  @spec run(Config.t(), String.t(), Replication.target(), keyword) ::
          {:ok, map} | {:error, term}
  def run(config, cell, target, opts) do
    started = System.monotonic_time()

    result =
      with {:ok, args} <- args(config, cell, target, opts),
           {:ok, location} <- Placement.locate(config, cell, fresh: true) do
        rewind(Map.put(args, :location, location))
      end

    Telemetry.span_stop([:cell, :rewind], started, %{}, meta(cell, target, result))
    result
  end

  defp meta(cell, target, {:ok, r}),
    do: %{cell: cell, target: target, result: :ok, from: r.from, to: r.to, txid: r.txid}

  defp meta(cell, target, {:error, reason}),
    do: %{cell: cell, target: target, result: :error, reason: reason}

  defp args(config, cell, target, opts) do
    with :ok <- Placement.validate_id(config, cell),
         :ok <- target(target),
         :ok <- options(opts),
         :ok <- if_head(opts[:if_head]),
         {:ok, keep} <- keep(config, Keyword.get(opts, :keep, :internal)),
         {:ok, schema} <- schema(Keyword.get(opts, :schema, :same)) do
      timeout = Keyword.get(opts, :timeout, config.rewind.timeout_ms)

      {:ok,
       %{
         config: config,
         cell: cell,
         target: target,
         keep: keep,
         schema: schema,
         if_head: opts[:if_head],
         deadline: Retry.deadline_in(timeout)
       }}
    end
  end

  defp target({:txid, n}) when is_integer(n) and n >= 1, do: :ok
  defp target({:timestamp, ms}) when is_integer(ms), do: :ok
  defp target(_), do: {:error, :bad_target}

  defp options(opts) do
    case Keyword.keys(opts) -- @opts do
      [] -> :ok
      [key | _] -> {:error, {:unknown_option, key}}
    end
  end

  defp if_head(nil), do: :ok
  defp if_head(txid) when is_integer(txid) and txid >= 0, do: :ok
  defp if_head({txid, :data}) when is_integer(txid) and txid >= 0, do: :ok
  defp if_head(_), do: {:error, :bad_if_head}

  defp keep(config, :internal), do: {:ok, config.feed_host_prefixes}
  defp keep(_config, :none), do: {:ok, []}
  defp keep(_config, _), do: {:error, :bad_keep}

  defp schema(mode) when mode in [:same, :migrate], do: {:ok, mode}
  defp schema(_), do: {:error, :bad_schema}

  defp rewind(%{config: config, cell: cell, location: location} = args) do
    repl = Config.repl_ctx(config, location)
    lease_ctx = Config.lease_ctx(config, location)
    dir = Local.work_dir(config, :rewind)
    args = Map.merge(args, %{repl: repl, lease_ctx: lease_ctx, dir: dir})

    try do
      Inflight.run(config, cell, :rewind, fn -> prepare(args) end)
    after
      File.rm_rf(dir)
    end
  end

  defp prepare(args) do
    with {:ok, head} <- head(args),
         :ok <- check_head(args, head),
         :ok <- fits(args, head),
         {:ok, target} <- restore_target(args, head),
         :ok <- fits(args, target) do
      take(args, target)
    end
  end

  defp head(%{repl: repl, cell: cell, deadline: deadline}) do
    case Replication.head(repl, cell, timeout_ms: remaining(deadline)) do
      {:ok, nil} -> {:error, :no_history}
      other -> other
    end
  end

  defp check_head(%{if_head: nil}, _head), do: :ok

  defp check_head(%{if_head: {txid, :data}}, %{data: data}) when data > txid,
    do: {:error, {:data_written, data}}

  defp check_head(%{if_head: {_txid, :data}}, _head), do: :ok
  defp check_head(%{if_head: txid}, %{txid: txid}), do: :ok
  defp check_head(_args, head), do: {:error, {:head_moved, head.txid}}

  # The target comes first: an unreachable one costs no downtime.
  defp restore_target(%{target: {:txid, n}}, %{txid: head}) when n >= head,
    do: {:error, {:target_not_behind, head}}

  defp restore_target(args, head) do
    path = Path.join(args.dir, "target.db")

    case Replication.restore(args.repl, args.cell, path, args.target,
           timeout_ms: remaining(args.deadline)
         ) do
      {:ok, %{txid: txid}} when txid >= head.txid -> {:error, {:target_not_behind, head.txid}}
      {:ok, result} -> {:ok, result}
      {:error, reason} -> behind(args, reason)
    end
  end

  # Restore refuses instead of returning an older state (design §6); what it
  # cannot reach is behind the history that is kept.
  @unreachable [:gap, :history_coarse, :snapshot_after_target, :target_not_reached]

  defp behind(args, reason) do
    if reason == :history_truncated or (is_tuple(reason) and elem(reason, 0) in @unreachable) do
      with {:ok, {segments, snapshots}} <- Objects.list_cell(args.repl, args.cell, args.deadline) do
        scratch = if Enum.any?(segments, &(&1.min == 1)), do: [1], else: []
        oldest = Enum.min(scratch ++ Enum.map(snapshots, & &1.txid), fn -> 0 end)
        {:error, {:behind_retention, oldest}}
      end
    else
      {:error, reason}
    end
  end

  defp take(args, target) do
    %{config: config, cell: cell, lease_ctx: lease_ctx, deadline: deadline} = args

    case Exclusive.take(config, cell, lease_ctx, @owner, config.rewind.poll_ms, deadline) do
      {:ok, lease} ->
        Fault.point(:rewind_after_acquire, %{cell: cell})
        result = install(args, lease, target, config.rewind.fence_attempts)
        # Whatever happened, give the lease back: it only keeps readers away,
        # the put is what protects the chain.
        _ = Lease.release(lease_ctx, lease)
        result

      {:moved, id} ->
        {:error, {:moved, id}}

      {:error, _} = error ->
        error
    end
  end

  # One try: the current state, the reconciled target, the put. `target` is
  # the restore made before the lease; a try after a lost race restores again,
  # because reconciling changes the file.
  defp install(args, lease, target, attempts) do
    %{cell: cell, repl: repl, dir: dir, deadline: deadline} = args
    current = Path.join(dir, "current.db")

    with {:ok, head} <- head(args),
         :ok <- check_head(args, head),
         {:ok, target} <- if(target, do: {:ok, target}, else: restore_target(args, head)),
         {:ok, now} <-
           Replication.restore(repl, cell, current, :latest, timeout_ms: remaining(deadline)),
         :ok <- fits(args, now),
         Fault.point(:rewind_after_head, %{cell: cell, txid: now.txid}),
         :ok <- same_page_size(now, target),
         {:ok, info} <- compare(args, current, target.path),
         _ = Fault.point(:rewind_before_reconcile, %{cell: cell, txid: now.txid}),
         {:ok, %{kept: kept, pages: pages}} <- reconcile(args, current, target, now, info),
         :ok <- renew(args, lease),
         :ok <- index(args, info) do
      Fault.point(:rewind_before_put, %{cell: cell, txid: now.txid + 1})

      case Replicator.replace(repl, cell, target.path, now.page_size, now, lease.epoch,
             deadline: deadline,
             pages: pages
           ) do
        {:ok, txid} ->
          Fault.point(:rewind_after_put, %{cell: cell, txid: txid})
          drop_old_entry(args, info)

          {:ok,
           %{
             txid: txid,
             from: now.txid,
             to: target.txid,
             epoch: lease.epoch,
             schema_version: info.to_version,
             kept: kept
           }}

        {:error, :txid_taken} when attempts > 1 ->
          install(args, lease, nil, attempts - 1)

        {:error, :txid_taken} ->
          {:error, {:fence, :txid_taken}}

        # Encoding failed here and nothing was sent: the outcome is known.
        {:error, {:capture, _}} = error ->
          error

        {:error, reason} ->
          Logger.warning("Alambique rewind of #{cell} has an unknown outcome: #{inspect(reason)}")
          {:error, :outcome_unknown}
      end
    end
  end

  # The whole database goes through memory several times over (design §20,
  # "Cost"); `rewind: [max_bytes:]` keeps one rewind from taking the VM down.
  defp fits(%{config: %{rewind: %{max_bytes: nil}}}, _state), do: :ok

  defp fits(%{config: %{rewind: %{max_bytes: max}}}, %{page_size: size, commit: pages}) do
    bytes = size * pages
    if bytes > max, do: {:error, {:too_large, bytes, max}}, else: :ok
  end

  defp same_page_size(%{page_size: size}, %{page_size: size}), do: :ok
  defp same_page_size(_now, _target), do: {:error, :page_size_differs}

  # The schema versions of both states, and the latest feed time of the
  # current one (the marker must not sort before records already published).
  defp compare(args, current, target_path) do
    with {:ok, from_conn} <- Db.open_read_only(current),
         {:ok, to_conn} <- Db.open_read_only(target_path) do
      try do
        with {:ok, from} <- Db.user_version(from_conn),
             {:ok, to} <- Db.user_version(to_conn),
             :ok <- schema_allowed(args, from, to) do
          {:ok, %{from_version: from, to_version: to, floor: Capture.last_time(from_conn)}}
        end
      after
        Db.close(from_conn)
        Db.close(to_conn)
      end
    end
  end

  defp schema_allowed(%{schema: :same}, version, version), do: :ok
  defp schema_allowed(%{schema: :same}, from, to), do: {:error, {:schema_differs, to, from}}

  defp schema_allowed(%{schema: :migrate, keep: [_ | _]}, from, to) when from != to,
    do: {:error, :keep_needs_same_schema}

  defp schema_allowed(_args, _from, _to), do: :ok

  defp reconcile(args, current, target, now, info) do
    {clock, clock_config} = args.config.clock
    ts = max(clock.now_ms(clock_config), info.floor)

    opts = %{
      keep: args.keep,
      select: args.config.feed,
      capture: args.config.feed_capture,
      txid: now.txid + 1,
      ts: ts,
      marker: %{from: now.txid, to: target.txid, target: args.target}
    }

    with {:ok, conn} <- Db.open(target.path, attach: true) do
      try do
        Reconcile.run(conn, current, opts)
      after
        Db.close(conn)
      end
    end
  end

  defp renew(args, lease) do
    case Lease.renew(args.lease_ctx, lease) do
      {:ok, _} -> :ok
      {:error, :lost} -> {:error, :lease_lost}
      {:error, _} = error -> error
    end
  end

  # The index lists the cell at the new version before the segment can make it
  # so, as a wake does after a migration (design §17).
  defp index(%{config: config, cell: cell, location: location, deadline: deadline}, info) do
    if info.to_version == info.from_version,
      do: :ok,
      else: Schema.put(config, location, cell, info.to_version, deadline)
  end

  defp drop_old_entry(%{config: config, cell: cell, location: location}, info) do
    if info.to_version != info.from_version,
      do:
        Schema.drop(
          config,
          location,
          cell,
          info.from_version,
          Retry.deadline_in(config.op_timeout_ms)
        )
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 1)
end
