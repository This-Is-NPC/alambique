defmodule Alambique.Relocate do
  @moduledoc false
  # Moving a cell between locations (design §18), fenced. The order makes the
  # source unwritable before the destination can be reached, so a crash at any
  # step leaves exactly one writable location, and running the move again
  # finishes it:
  #
  #   1. Take the lease at the source (the owner hibernates first) and write
  #      the fence there, so older epochs cannot write.
  #   2. Install a free lease at the destination with epoch + 1 and upload a
  #      snapshot of the fenced state there.
  #   3. Mark the source lease `moved_to` (`If-Match`: fails if anyone took
  #      the cell since step 1). The source never wakes the cell again.
  #   4. Tell the placement (`commit_move/4`). From now on `locate/2` gives
  #      the destination.
  #   5. Delete the source's objects once the grace period passed
  #      (`Relocate.Cleanup`), keeping its lease with `moved_to`. A try that
  #      stops here or finds the grace running leaves the job to the
  #      compactor, which also finds it again after a restart through the
  #      marker written before step 3, or to `Alambique.cleanup_moved/4`.
  #
  # Between 3 and 4 the cell is unavailable: the source refuses and the
  # placement does not point at the destination yet.
  #
  # A destination that the cell left earlier holds a `moved_to` lease; it is
  # accepted once its objects were cleaned up and the cleanup is over (step 2).

  alias Alambique.{Config, Fault, Freeze, Inflight, Lease, Location, Placement, Replication}
  alias Alambique.{Exclusive, Schema}
  alias Alambique.{Compaction.Compactor, Telemetry}
  alias Alambique.Relocate.Cleanup
  alias Alambique.Cell.{Db, Local}
  alias Alambique.Replication.{Objects, Replicator, Retry}

  @owner "relocate"

  @spec run(Config.t(), String.t(), Location.t() | String.t(), keyword) ::
          {:ok, Location.t()} | {:error, term}
  def run(config, cell, to, opts) do
    started = System.monotonic_time()
    timeout = Keyword.get(opts, :timeout, config.relocate.timeout_ms)
    deadline = Retry.deadline_in(timeout)

    with :ok <- Placement.validate_id(config, cell),
         {:ok, dest} <- destination(config, to),
         {:ok, source} <- Placement.locate(config, cell, fresh: true) do
      {result, cleanup} =
        if source.id == dest.id,
          do: {already_there(config, cell, source), :none},
          else: move(config, cell, source, dest, deadline, timeout)

      Telemetry.span_stop([:cell, :relocate], started, %{}, %{
        cell: cell,
        from: source.id,
        to: dest.id,
        result: if(match?({:ok, _}, result), do: :ok, else: :error),
        cleanup: cleanup
      })

      result
    end
  end

  # The placement already names the destination. A source marked as moved
  # elsewhere is a move to finish first, not a cell at rest.
  defp already_there(config, cell, location) do
    case Lease.read(Config.lease_ctx(config, location), cell) do
      {:ok, %{moved_to: to}} when to != nil -> {:error, {:move_pending, to}}
      {:ok, _} -> {:ok, location}
      :none -> {:ok, location}
      {:error, _} = error -> error
    end
  end

  defp destination(config, id) when is_binary(id), do: Placement.location(config, id)

  defp destination(_config, %Location{} = location),
    do: if(Location.valid?(location), do: {:ok, location}, else: {:error, :bad_location})

  defp destination(_config, _), do: {:error, :bad_location}

  defp move(config, cell, source, dest, deadline, timeout) do
    point = %{cell: cell, from: source.id, to: dest.id}
    src_lease_ctx = Config.lease_ctx(config, source)

    case Exclusive.take(config, cell, src_lease_ctx, @owner, config.relocate.poll_ms, deadline) do
      {:ok, lease} ->
        Fault.point(:relocate_after_acquire, point)

        with {:ok, lease, version} <- copy(config, cell, source, dest, lease, deadline),
             :ok <- Cleanup.record(config, source, cell, timeout),
             _ = Fault.point(:relocate_before_mark, point),
             :ok <- Lease.mark_moved(src_lease_ctx, lease, dest.id) do
          Fault.point(:relocate_after_mark, point)
          commit(config, cell, source, dest, version, point)
        else
          # The move is not marked, so the cell stays at the source: give its
          # lease back now rather than at its expiry, as the next try would
          # (`reclaim/4`). A lease that changed meanwhile is left alone.
          {:error, _} = error ->
            _ = Lease.release(src_lease_ctx, lease)
            {error, :none}
        end

      {:moved, id} when id == dest.id ->
        commit(config, cell, source, dest, nil, point)

      {:moved, id} ->
        {{:error, {:moved, id}}, :none}

      {:error, _} = error ->
        {error, :none}
    end
  end

  # `version` is the cell's schema version when this try copied it, `nil` when
  # an earlier try did: the source's index entry is then left to the sweeper.
  defp commit(config, cell, source, dest, version, point) do
    case Placement.commit_move(config, cell, source, dest) do
      :ok ->
        Placement.invalidate(config, cell)
        if version, do: Schema.drop(config, source, cell, version, deadline_for(config))
        Fault.point(:relocate_after_commit, point)
        {{:ok, dest}, cleanup(config, cell, source)}

      {:error, _} = error ->
        {error, :none}
    end
  end

  # The last step. Whatever it cannot finish now is queued for the compactor:
  # the move itself is done.
  defp cleanup(config, cell, source) do
    Fault.point(:relocate_before_cleanup, %{cell: cell, from: source.id})

    case Cleanup.run(config, cell, source) do
      {:ok, %{result: :cleaned}} ->
        :cleaned

      {:ok, %{result: :waiting, ready_at_ms: at}} ->
        Compactor.cleanup(config, cell, source.id, max(at - now_ms(config), 0) + 1)
        :waiting

      {:error, _} ->
        Compactor.cleanup(config, cell, source.id, config.op_timeout_ms)
        :error
    end
  end

  defp now_ms(%{clock: {clock, clock_config}}), do: clock.now_ms(clock_config)

  defp deadline_for(config), do: Retry.deadline_in(config.op_timeout_ms)

  # Steps 2: the destination gets its lease and a snapshot of the fenced state.
  defp copy(config, cell, source, dest, lease, deadline) do
    src = Config.repl_ctx(config, source)
    dst = Config.repl_ctx(config, dest)
    dst_lease = Config.lease_ctx(config, dest)
    dir = Local.work_dir(config, :relocate)
    path = Path.join(dir, "cell.db")

    try do
      Inflight.run(config, cell, :relocate, fn ->
        restore_and_snapshot(
          config,
          cell,
          source,
          dest,
          lease,
          src,
          dst,
          dst_lease,
          path,
          deadline
        )
      end)
    after
      File.rm_rf(dir)
    end
  end

  defp restore_and_snapshot(
         config,
         cell,
         source,
         dest,
         lease,
         src,
         dst,
         dst_lease,
         path,
         deadline
       ) do
    with {:ok, result} <- restore(src, cell, path, deadline),
         :ok <-
           empty_or_ours(config, dst, dest, cell, source.id, result.txid, deadline),
         :ok <- Lease.install(dst_lease, cell, lease.epoch + 1, source.id, @owner),
         _ = Fault.point(:relocate_after_install, %{cell: cell, to: dest.id}),
         {:ok, version} <-
           fence_and_snapshot(
             cell,
             src,
             dst,
             path,
             result,
             lease.epoch,
             deadline,
             config.relocate.fence_attempts,
             Db.script_timeout_ms(config)
           ),
         _ = Fault.point(:relocate_after_snapshot, %{cell: cell, to: dest.id}),
         # The destination lists the cell before it can be reached (design §17).
         :ok <- Schema.put(config, dest, cell, version, deadline),
         :ok <- carry_frozen(src, dst, cell, deadline),
         {:ok, lease} <- Lease.renew(Config.lease_ctx(config, source), lease) do
      {:ok, lease, version}
    end
  end

  # The destination holds nothing of this cell, or only what an earlier
  # attempt of this move left (its lease names the source), or the data of a
  # cell that lived here before, below the restored TXID. After a cleanup,
  # `cleaned_at_ms` says that every data object there was marked for deletion;
  # objects a compaction wrote meanwhile are below the restored TXID too. The
  # check only lists the cell's prefix, never the trash prefix.
  defp empty_or_ours(config, dst, dest, cell, from, txid, deadline) do
    case Lease.read(Config.lease_ctx(config, dest), cell) do
      {:ok, %{moved_from: ^from, moved_to: nil}} ->
        destination_files(dst, cell, txid, deadline, :bounded)

      {:ok, %{moved_to: to} = lease} when to != nil ->
        with :ok <- cleaned?(config, lease, cell, dest),
             do: destination_files(dst, cell, txid, deadline, :bounded)

      {:ok, _} ->
        {:error, :destination_not_empty}

      :none ->
        destination_files(dst, cell, txid, deadline, :empty)

      {:error, _} = error ->
        error
    end
  end

  # No cleanup finished yet: refuse, and queue it for the compactor so that a
  # later try finds it done (the grace may still be running; the cleanup waits
  # for it). A recent one: wait, because cleaning again would restart the gap.
  defp cleaned?(config, %{cleaned_at_ms: nil}, cell, dest) do
    Compactor.cleanup(config, cell, dest.id, 0)
    {:error, :destination_not_cleaned}
  end

  defp cleaned?(config, %{cleaned_at_ms: at}, _cell, _dest) do
    ready_at = at + Cleanup.gap(config) + config.skew_margin_ms

    if now_ms(config) >= ready_at,
      do: :ok,
      else: {:error, {:destination_settling, ready_at}}
  end

  # Every file at the destination must end at or before the restored TXID:
  # the incoming snapshot covers it, and restore picks the newest snapshot, so
  # an old file can never be returned as the latest state. A file beyond it
  # means a write this move does not know about; refuse instead of colliding.
  defp destination_files(dst, cell, txid, deadline, mode) do
    with {:ok, {segments, snapshots}} <- Objects.list_cell(dst, cell, deadline) do
      max =
        Enum.max([0] ++ Enum.map(segments, & &1.max) ++ Enum.map(snapshots, & &1.txid))

      cond do
        max == 0 -> :ok
        mode == :empty -> {:error, :destination_not_empty}
        max <= txid -> :ok
        true -> {:error, {:destination_ahead, max}}
      end
    end
  end

  defp restore(src, cell, path, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 1)
    Replication.restore(src, cell, path, :latest, timeout_ms: timeout)
  end

  defp fence_and_snapshot(cell, src, dst, path, result, epoch, deadline, attempts, timeout_ms) do
    with {:ok, repl} <- continue(result, epoch),
         {:ok, conn} <- Db.open(path, timeout_ms: timeout_ms) do
      try do
        case Replicator.fence(repl, src, deadline: deadline) do
          {:ok, repl} ->
            with {:ok, _key} <- Replicator.snapshot(repl, dst, deadline: deadline),
                 do: Db.user_version(conn)

          {:error, :txid_taken} when attempts > 1 ->
            :retry

          {:error, reason} ->
            {:error, {:fence, reason}}
        end
      after
        Db.close(conn)
      end
      |> case do
        :retry ->
          with {:ok, result} <- restore(src, cell, path, deadline) do
            fence_and_snapshot(
              cell,
              src,
              dst,
              path,
              result,
              epoch,
              deadline,
              attempts - 1,
              timeout_ms
            )
          end

        other ->
          other
      end
    end
  end

  defp continue(result, epoch) do
    {:ok, Replicator.from_restore(result, epoch)}
  rescue
    exception in ArgumentError -> {:error, {:local_file, Exception.message(exception)}}
  end

  defp carry_frozen(src, dst, cell, deadline) do
    case Freeze.frozen?(src, cell, deadline: deadline) do
      {:ok, true} -> Freeze.freeze(dst, cell, deadline: deadline)
      {:ok, false} -> :ok
      {:error, _} = error -> error
    end
  end
end
