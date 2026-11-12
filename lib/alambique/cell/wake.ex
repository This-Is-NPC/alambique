defmodule Alambique.Cell.Wake do
  @moduledoc false
  require Logger
  # Wake order of a cell (design §7): the lease, the frozen check and the read
  # of the store's state run together; then fence, migrations. Every step
  # returns `{:ok, state}` or `{:error, reason, state}` with the resources
  # taken so far, so the caller can release them.

  alias Alambique.{Config, Fault, Freeze, Lease, Placement, Replication, Schema}
  alias Alambique.Cell.{Commit, Db, Hydration, Local, Pipe, Sql, State}
  alias Alambique.Feed.{Capture, Publisher}
  alias Alambique.Compaction.{Compactor, Hold}
  alias Alambique.Store.Metered
  alias Alambique.Replication.{Replicator, Retry}
  alias Alambique.Tx
  alias Alambique.Telemetry

  @spec run(State.t()) :: {:ok, State.t()} | {:error, term, State.t()}
  def run(%State{} = state) do
    with :ok <- prepare(state) do
      Hold.drop_cell(state.config, state.id)

      with {:ok, state} <- locate(state),
           {:ok, state} <- choose_migrations(state),
           {:ok, state, found} <- overlap(state),
           {:ok, state} <- load(state, found),
           {:ok, state} <- migrate(state) do
        {:ok, Publisher.start(state)}
      end
    end
  end

  # The directory is the first write of a wake: a full disk fails it before
  # anything is held.
  defp prepare(state) do
    with {:error, reason} <- Local.prepare(state.path), do: {:error, reason, state}
  end

  # The lease, the frozen check and the read of the store's state do not
  # depend on each other, so they run together and a wake takes the time of
  # the slowest instead of their sum. Nothing serves, and nothing is written
  # but the lease, until all three are done: the fence below is a conditional
  # write at the TXID after the state that was read, so a state that went
  # stale, because a previous owner wrote before the lease was ours, makes it
  # fail with `:txid_taken` and the wake reads again.
  #
  # The error reported is the lease's, then the frozen marker's, then the
  # read's. Whatever the read made but no cell will use is released.
  defp overlap(state) do
    lease = Task.async(fn -> acquire(state) end)
    frozen = Task.async(fn -> frozen(state) end)
    data = Task.async(fn -> read_state(state) end)

    case {Task.await(lease, :infinity), Task.await(frozen, :infinity),
          Task.await(data, :infinity)} do
      {{:ok, state}, :ok, {:ok, found}} ->
        {:ok, state, found}

      {{:ok, state}, :ok, {:error, reason}} ->
        {:error, reason, state}

      {{:ok, state}, {:error, reason}, data} ->
        release_found(data)
        {:error, reason, state}

      {{:error, reason, state}, _, data} ->
        release_found(data)
        {:error, reason, state}
    end
  end

  defp release_found({:ok, {:restored, result, lazy}}), do: Hydration.release(result, lazy)
  defp release_found(_), do: :ok

  # Lease

  # An early takeover (design §5) replaces the lease whatever its state;
  # otherwise the lease is taken only when it is free.
  #
  # The location comes from the placement itself, never from its cached answer:
  # a cell that moved away and back leaves a free lease at the destination
  # before the placement points there, and a node that remembered the old
  # answer must not take it. A location that says the cell moved sends the
  # caller to the new one.
  defp acquire(state) do
    node_id = state.config.node_id

    case take(state, node_id) do
      {:ok, lease} -> {:ok, %{state | lease: lease}}
      {:ok, lease, previous} -> {:ok, took_over(state, lease, previous)}
      {:error, {:owned, ^node_id, _}} -> reclaim(state)
      {:error, {:moved, id}} -> {:error, {:moved, moved_to(state, id)}, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # The lease the router read just before the wake saves the first read when
  # it is of the location this wake locates.
  defp take(%State{takeover: nil} = state, node_id),
    do: Lease.acquire(state.lease_ctx, state.id, node_id, seen: seen(state))

  defp take(%State{takeover: takeover} = state, node_id) do
    Lease.take_over(state.lease_ctx, state.id, node_id,
      epoch: takeover.epoch,
      cause: takeover.reason
    )
  end

  defp seen(%State{lease_seen: %{location: location, answer: answer}, location: location}),
    do: answer

  defp seen(_state), do: nil

  defp took_over(state, lease, previous),
    do: %{state | lease: lease, takeover: Map.put(state.takeover, :previous, previous)}

  defp locate(state) do
    case Placement.locate(state.config, state.id, fresh: true) do
      {:ok, location} ->
        trips = if state.lease_seen, do: state.lease_seen.trips, else: Metered.new()

        {:ok,
         %{
           state
           | location: location,
             trips: trips,
             lease_ctx: meter(Config.lease_ctx(state.config, location), trips),
             repl_ctx: meter(Config.repl_ctx(state.config, location), trips)
         }}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp meter(ctx, trips), do: %{ctx | store: Metered.wrap(ctx.store, trips)}

  @doc """
  Ends the counting of the wake: the contexts go back to the plain store, and
  the counts so far are returned.
  """
  @spec unmeter(State.t()) :: {State.t(), map}
  def unmeter(%State{location: nil} = state), do: {state, %{}}

  def unmeter(%State{trips: trips} = state) do
    state = %{
      state
      | lease_ctx: Config.lease_ctx(state.config, state.location),
        repl_ctx: Config.repl_ctx(state.config, state.location)
    }

    {state, Metered.counts(trips)}
  end

  defp moved_to(state, id) do
    case Placement.location(state.config, id) do
      {:ok, location} -> location
      {:error, _} -> %Alambique.Location{id: id, store: nil, prefix: nil}
    end
  end

  # The lease names this instance (node id and incarnation) but no process of
  # this node holds the cell, so a previous process died with it: release it on
  # its behalf. The same node id with another incarnation is another instance,
  # which holds the cell: the lease is not ours to release (design §5).
  defp reclaim(state) do
    ctx = state.lease_ctx
    config = state.config

    case Lease.read(ctx, state.id) do
      {:ok, stale} ->
        if Lease.held_by?(stale, config.node_id, config.incarnation),
          do: release_and_take(state, stale),
          else: {:error, {:owned, stale.owner, stale.expires_at_ms}, state}

      :none ->
        {:error, :lease_unreadable, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp release_and_take(state, stale) do
    ctx = state.lease_ctx

    with :ok <- Lease.release(ctx, stale),
         {:ok, lease} <- Lease.acquire(ctx, state.id, state.config.node_id) do
      {:ok, %{state | lease: lease}}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp frozen(state) do
    deadline = Retry.deadline_in(state.config.op_timeout_ms)

    case Freeze.frozen?(state.repl_ctx, state.id, deadline: deadline) do
      {:ok, false} -> :ok
      {:ok, true} -> {:error, :frozen}
      {:error, reason} -> {:error, {:frozen_check, reason}}
    end
  end

  # Renews when less than half of the usable window is left.
  defp ensure_lease(state) do
    config = state.config
    remaining = state.lease.expires_at_ms - config.skew_margin_ms - State.now_ms(state)

    if remaining * 2 >= config.ttl_ms do
      {:ok, state}
    else
      case Lease.renew(state.lease_ctx, state.lease) do
        {:ok, lease} -> {:ok, %{state | lease: lease}}
        {:error, reason} -> {:error, {:lease, reason}, state}
      end
    end
  end

  # The state of the store, read beside the lease: the head of the chain when
  # the local file may be reused, else a restore. Nothing here needs the
  # lease. A restore replaces the local file, so the marker that vouches for
  # it goes first; a file that is reused keeps its marker until the wake has
  # the lease.
  defp read_state(state) do
    case Local.read(state.path) do
      nil ->
        fetch_restore(state)

      marker ->
        case matching_head(state, marker) do
          {:ok, head} ->
            {:ok, {:local, head}}

          :miss ->
            fetch_restore(state)
        end
    end
  end

  # The local file is the store's head when the marker names its TXID and
  # checksum; from_restore() then checks the file itself against it.
  defp matching_head(state, marker) do
    with {:ok, %{txid: txid, checksum: sum} = head}
         when txid == marker.txid and sum == marker.checksum <-
           Replication.head(state.repl_ctx, state.id) do
      {:ok, head}
    else
      _ -> :miss
    end
  end

  defp fetch_restore(state) do
    Local.forget(state.path)
    fetch_restore(state, Hydration.restore_opts(state), true)
  end

  defp fetch_restore(state, opts, lazy_fallback?) do
    case Replication.restore(state.repl_ctx, state.id, state.path, :latest, opts) do
      {:ok, result} ->
        case Hydration.prepare(state, result) do
          {:ok, lazy} ->
            {:ok, {:restored, result, lazy}}

          {:error, {:lazy, reason}} when lazy_fallback? ->
            # A page the lazy plan cannot fetch must not fail the wake: the
            # file is discarded and the chain is read in full instead.
            Logger.warning(
              "Alambique restores #{state.id} fully after a lazy failure: #{inspect(reason)}"
            )

            Fault.point(:wake_lazy_fallback, %{cell: state.id, reason: reason})
            fetch_restore(state, Keyword.delete(opts, :lazy), false)

          {:error, reason} ->
            {:error, {:hydration, reason}}
        end

      {:error, reason} ->
        {:error, {:restore, reason}}
    end
  end

  # Database file

  defp load(state, found), do: load(state, found, state.config.wake.fence_attempts)

  defp load(state, {:local, head}, attempts) do
    Local.forget(state.path)

    case continue(state, Map.merge(head, %{cell: state.id, path: state.path}), []) do
      {:ok, repl} -> fence(state, repl, :local, attempts)
      {:error, _reason, _state} -> restore_and_fence(state, attempts)
    end
  end

  defp load(state, {:restored, result, lazy}, attempts) do
    state = Hydration.start(state, lazy)

    with {:ok, repl} <- continue(state, result, lazy: lazy) do
      fence(state, repl, if(result.txid == 0, do: :empty, else: :restore), attempts)
    end
  end

  defp restore_and_fence(state, attempts) do
    case fetch_restore(state) do
      {:ok, found} -> load(state, found, attempts)
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp continue(state, result, opts) do
    {:ok, Replicator.from_restore(result, state.lease.epoch, opts)}
  rescue
    exception in ArgumentError -> {:error, {:local_file, Exception.message(exception)}, state}
  end

  defp fence(state, repl, source, attempts) do
    with {:ok, state} <- ensure_lease(state),
         {:ok, conn} <- open(state) do
      state = %{state | conn: conn, repl: repl, source: source}
      indexing = start_index(state)

      fenced =
        Replicator.fence(repl, state.repl_ctx,
          deadline: State.deadline(state),
          landed: &Hold.put_upload(state.config, state.repl_ctx, &1)
        )

      indexed = await_index(state, indexing)

      case {fenced, indexed} do
        {{:ok, repl}, {:ok, state}} ->
          Compactor.notify(state.config, state.id, repl.txid)
          {:ok, %{state | repl: repl, pipe: %{state.pipe | acked: Pipe.acked(repl)}}}

        {{:ok, _repl}, {:error, _reason, _state} = error} ->
          error

        {{:error, :txid_taken}, _} when attempts > 1 ->
          # A stale owner wrote the next TXID first: take it with a restore.
          Db.close(conn)
          restore_and_fence(Hydration.discard(%{state | conn: nil}), attempts - 1)

        {{:error, reason}, _} ->
          {:error, {:fence, reason}, state}
      end
    end
  end

  # A cell with no migration to run has its schema index entry written below
  # at its current version, which does not depend on the fence: the entry only
  # records the version the file already has. Written beside the fence, it
  # costs no round trip. A fence that fails leaves an entry at the version the
  # cell has anyway, which the sweeper removes if a newer one exists (design §17).
  defp start_index(%State{source: :local}), do: nil

  defp start_index(state) do
    with {:ok, current} <- Db.user_version(state.conn),
         :serve <- Schema.window(state.config, length(state.migrations), current),
         [] <- Enum.filter(state.migrations, fn {version, _} -> version > current end) do
      deadline = State.deadline(state)

      {current,
       Task.async(fn ->
         Schema.put(state.config, state.location, state.id, current, deadline, state.repl_ctx)
       end)}
    else
      _ -> nil
    end
  end

  defp await_index(state, nil), do: {:ok, state}

  defp await_index(state, {version, task}) do
    case Task.await(task, :infinity) do
      :ok -> {:ok, %{state | indexed: [version | state.indexed], preindexed: true}}
      {:error, reason} -> {:error, {:schema_index, reason}, state}
    end
  end

  defp open(state) do
    case Db.open(state.path,
           cache_kib: state.config.awake.cache_kib,
           timeout_ms: Db.script_timeout_ms(state.config)
         ) do
      {:ok, conn} ->
        {:ok, conn}

      {:error, reason} ->
        {:error, {:open, reason}, state}
    end
  end

  # Migrations

  # The list of this wake, chosen before anything is restored or uploaded.
  defp choose_migrations(state) do
    case Config.migrations_for(state.config, state.id) do
      {:ok, migrations} -> {:ok, %{state | migrations: migrations}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  @doc """
  Runs the migration step again on a serving cell, with the list chosen now.
  The caller has uploaded every pending write. When a migration fails, the
  ones before it that committed are uploaded and indexed, so the cell, its
  durable state and the index agree on the last version that applied; the
  error is `{:migration, version, reason}`.
  """
  @spec remigrate(State.t()) :: {:ok, State.t()} | {:error, term, State.t()}
  def remigrate(state) do
    with {:ok, state} <- choose_migrations(state), do: migrate(state)
  end

  defp migrate(state) do
    state = Publisher.restore_floor(state)

    with {:ok, current} <- user_version(state),
         :ok <- check_window(state, current) do
      pending = Enum.filter(state.migrations, fn {version, _} -> version > current end)

      case apply_all(state, pending) do
        {:ok, state} ->
          settle(state, current, pending)

        {:error, reason, [], state} ->
          {:error, reason, state}

        {:error, reason, applied, state} ->
          with {:ok, state} <- settle(state, current, applied), do: {:error, reason, state}
      end
    end
  end

  defp settle(state, current, applied) do
    with {:ok, state, made?} <- feed_schema(state),
         {:ok, state} <- index(state, current, applied) do
      if applied == [] and not made?,
        do: {:ok, state},
        else: finish_migration(state, current, applied)
    end
  end

  # A cell further ahead than the window cannot be served by this code (§17).
  defp check_window(state, current) do
    case Schema.window(state.config, length(state.migrations), current) do
      :serve -> :ok
      :too_new -> {:error, {:schema_too_new, current}, state}
    end
  end

  # The cell's entry in the schema index (§17) is written before the batch
  # with the migrations is uploaded and the entry of the previous version is
  # removed after it, so the index never shows the cell nowhere. A cell whose
  # file was reused had its entry written by the wake before, and nothing
  # moves it since.
  defp index(%State{source: :local} = state, _current, []), do: {:ok, state}

  # The entry was written beside the fence (`start_index/1`).
  defp index(%State{preindexed: true} = state, _current, []),
    do: {:ok, %{state | preindexed: false}}

  defp index(state, current, pending) do
    version = pending |> List.last({current, nil}) |> elem(0)

    case Schema.put(
           state.config,
           state.location,
           state.id,
           version,
           State.deadline(state),
           state.repl_ctx
         ) do
      :ok -> {:ok, %{state | indexed: [version | state.indexed]}}
      {:error, reason} -> {:error, {:schema_index, reason}, state}
    end
  end

  defp user_version(state) do
    case Db.user_version(state.conn) do
      {:ok, version} -> {:ok, version}
      {:error, reason} -> {:error, {:migration, reason}, state}
    end
  end

  # `{:error, reason, applied, state}` lists the migrations that committed
  # before the one that failed.
  defp apply_all(state, pending) do
    result =
      Enum.reduce_while(pending, [], fn {version, migration} = entry, applied ->
        started = System.monotonic_time()
        txid = Replicator.next_txid(state.repl)

        case Db.transaction(state.conn, txid, &apply_one(&1, version, migration),
               select: state.config.feed,
               limits: Db.migration_limits(state.config)
             ) do
          {:ok, _} ->
            Telemetry.span_stop([:cell, :migrate], started, %{}, %{
              cell: state.id,
              version: version
            })

            {:cont, [entry | applied]}

          {:error, reason} ->
            {:halt, {:error, {:migration, version, reason}, Enum.reverse(applied)}}
        end
      end)

    case result do
      {:error, reason, applied} -> {:error, reason, applied, state}
      _ -> {:ok, state}
    end
  end

  # Creates the internal tables and the triggers once the file has a user
  # table, and again whenever the schema no longer matches them. This is part
  # of the migration batch.
  defp feed_schema(state) do
    if Capture.current?(state.conn, state.config.feed) do
      {:ok, state, false}
    else
      txid = Replicator.next_txid(state.repl)

      install = &Capture.install(&1.conn, state.config.feed)

      case Db.transaction(state.conn, txid, install, capture: false) do
        {:ok, _} -> {:ok, state, true}
        {:error, reason} -> {:error, {:migration, :feed, reason}, state}
      end
    end
  end

  defp finish_migration(state, previous, pending) do
    state = Publisher.stamp(state)

    case sync(state, length(pending)) do
      {:ok, state, txid} ->
        if txid, do: Commit.after_ack(state, txid)
        if pending != [], do: drop_previous(state, previous)
        {:ok, state}

      {:error, _reason, _state} = error ->
        error
    end
  end

  # The cell is durable at its new version. A cell that was restored empty has
  # only the entries this process wrote, which saves a request when a new cell
  # migrates. A stale entry left behind by a failure here is removed by the
  # sweeper, or at version 0 stays and is listed once (`list_cells/2`).
  defp drop_previous(%State{source: :empty} = state, previous) do
    if previous in state.indexed, do: drop(state, previous), else: :ok
  end

  defp drop_previous(state, previous), do: drop(state, previous)

  defp drop(state, previous) do
    Schema.drop(
      state.config,
      state.location,
      state.id,
      previous,
      State.deadline(state),
      state.repl_ctx
    )

    :ok
  end

  # Each statement goes through the transaction, so DML is captured by the
  # change feed and DDL regenerates the triggers (design §13).
  defp apply_one(tx, version, sql) when is_binary(sql) do
    for statement <- Sql.split(sql) do
      case Tx.execute(tx, statement) do
        {:ok, _} -> :ok
        {:error, reason} -> Tx.rollback(tx, reason)
      end
    end

    set_version(tx, version)
  end

  defp apply_one(tx, version, fun) when is_function(fun, 1) do
    fun.(tx)
    set_version(tx, version)
  end

  defp set_version(tx, version), do: :ok = Db.script(tx.conn, "PRAGMA user_version = #{version}")

  defp sync(state, size) do
    case Commit.run(state, size) do
      {:ok, state, txid} -> {:ok, state, txid}
      {:error, reason, state} -> {:error, {:migration_sync, reason}, state}
    end
  end
end
