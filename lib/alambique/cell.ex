defmodule Alambique.Cell do
  @moduledoc false
  # One awake cell (design §7): a process that owns the lease and the SQLite
  # connection of one cell and serves every call for it.
  #
  # Group commit (design §6) comes from the mailbox. A write commits locally
  # and waits for a `:flush` message that is queued behind everything already
  # in the mailbox; the flush encodes all those commits as one batch and hands
  # it to an upload process. With a pipeline deeper than one, later batches are
  # encoded while earlier ones upload; callers are answered only in TXID order,
  # when a batch and every batch before it have landed (`Cell.Pipe`).

  use GenServer, restart: :temporary, shutdown: 30_000

  require Logger

  alias Alambique.Cell.{Commit, Db, Hydration, Local, Pipe, State, Wake}
  alias Alambique.Compaction.{Compactor, Hold}

  alias Alambique.{
    Admission,
    Config,
    DiskFull,
    Fault,
    Freeze,
    Inflight,
    Lease,
    Membership,
    Telemetry,
    Tx
  }

  alias Alambique.Feed.Publisher
  alias Alambique.Store.{Breaker, Metered}
  alias Alambique.Replication.{Replicator, Retry}

  # Lifecycle

  def start_link({config, id}), do: start_link({config, id, []})

  # Options: `:takeover`, `%{reason: term, epoch: pos_integer | nil}`, and
  # `:lease_seen`, the lease the router read (see `State`).
  def start_link({config, id, opts}) do
    # The registry value is the cell's published load (see `load/2`).
    load = new_load()

    GenServer.start_link(__MODULE__, {config, id, opts, load},
      name: {:via, Registry, {config.registry, id, load}}
    )
  end

  @spec whereis(Alambique.Config.t(), String.t()) :: pid | nil
  def whereis(config, id) do
    with {pid, _load} <- find(config, id), do: pid
  end

  @doc false
  # The process of `id` and its load counter, from one registry lookup.
  @spec find(Alambique.Config.t(), String.t()) :: {pid, :atomics.atomics_ref()} | nil
  def find(config, id) do
    # The registry drops a dead process a moment after its exit.
    case Registry.lookup(config.registry, id) do
      [{pid, load}] -> if Process.alive?(pid), do: {pid, load}
      [] -> nil
    end
  end

  @doc """
  The calls waiting on the cell process of `id` on this node, without a call
  into it: the calls sent and not yet answered or parked plus the callers
  it accepted and has not answered (`State.load`). Other messages of the
  mailbox do not count. Also whether the wake is still running (`waking?/1`).
  `:none` when it has no process.
  """
  @spec load(Alambique.Config.t(), String.t()) ::
          {:ok, non_neg_integer, boolean} | :none
  def load(config, id) do
    with [{pid, load}] <- Registry.lookup(config.registry, id),
         {:ok, pending} <- pending(pid, load) do
      {:ok, pending, waking?(load)}
    else
      _ -> :none
    end
  end

  @doc false
  # Slot 1 counts the callers the cell accepted and has not answered; slot 2
  # the calls the router sent that the cell has not finished with (`admit/2`);
  # slot 3 is the monotonic ms of the last call, 0 until the wake ends; slot 4
  # is the size of the WAL after the last landed batch or checkpoint.
  @spec pending(pid, :atomics.atomics_ref()) :: {:ok, non_neg_integer} | :none
  def pending(pid, load) do
    if Process.alive?(pid),
      do: {:ok, :atomics.get(load, 1) + :atomics.get(load, 2)},
      else: :none
  end

  @doc false
  @spec new_load() :: :atomics.atomics_ref()
  def new_load, do: :atomics.new(4, [])

  @doc false
  # The monotonic ms of the last call, `nil` until the wake ends.
  @spec last_used(:atomics.atomics_ref()) :: integer | nil
  def last_used(load) do
    case :atomics.get(load, 3) do
      0 -> nil
      last -> last
    end
  end

  @doc false
  # The wake has not ended: slot 3 is set when the cell starts serving.
  @spec waking?(:atomics.atomics_ref()) :: boolean
  def waking?(load), do: :atomics.get(load, 3) == 0

  @doc false
  @spec wal_bytes(:atomics.atomics_ref()) :: non_neg_integer
  def wal_bytes(load), do: :atomics.get(load, 4)

  @doc false
  # Counts a call before it is sent, so it counts while it waits in the
  # mailbox, while it runs and after its caller gave up; `handle_call/3`
  # takes it back. The increment is atomic, so no more than `max` calls are
  # ever admitted: the one that finds the count past `max` gives its place
  # back and is refused. The callers the cell accepted (slot 1) only grow
  # from calls already admitted, so the sum never under-counts.
  # A hibernation is not counted: callers outside the router send it too.
  @spec admit(:atomics.atomics_ref(), pos_integer) :: :ok | :busy
  def admit(load, max) do
    if :atomics.add_get(load, 2, 1) + :atomics.get(load, 1) <= max do
      :ok
    else
      :atomics.sub(load, 2, 1)
      :busy
    end
  end

  @impl true
  def init({config, id, opts, load}) do
    Process.flag(:trap_exit, true)

    state = %State{
      config: config,
      id: id,
      path: Local.db_path(config, id),
      takeover: opts[:takeover],
      lease_seen: opts[:lease_seen],
      inflight: Inflight.open(config, id, :cell),
      pipe: Pipe.new(config),
      load: load,
      holder: opts[:holder]
    }

    # Last: nothing after it can fail, so a slot is never held by a cell that
    # never started.
    Admission.waking(config, self())
    {:ok, state, {:continue, :wake}}
  end

  # Every callback runs with the marker busy (design §10): they are the only
  # places where the cell process runs native code.
  @impl true
  def handle_continue(:wake, state), do: Inflight.busy(state.inflight, fn -> wake(state) end)

  # The cell holds a wake slot before any work: its caller's, or its own if
  # that caller died (design §7). Without one it does not wake, and the
  # callers already routed to it get the same `:overloaded` as any wake.
  defp wake(state) do
    Fault.point(:cell_before_take_over, %{cell: state.id})

    case Admission.take_over(state.config, state.holder) do
      :ok -> run_wake(state)
      {:error, reason} -> {:stop, {:shutdown, {:wake_failed, reason}}, %{state | mode: :stopped}}
    end
  end

  defp run_wake(state) do
    started = System.monotonic_time()
    {:reductions, reductions} = Process.info(self(), :reductions)

    case Wake.run(state) do
      {:ok, state} ->
        Admission.end_wake(state.config, self())
        Admission.recovered(state.config, state.id)
        {state, requests, store_time} = Wake.unmeter(state)

        Telemetry.span_stop(
          [:cell, :wake],
          started,
          wake_measurements(requests, store_time, reductions),
          %{
            cell: state.id,
            location: state.location.id,
            result: :ok,
            epoch: state.lease.epoch,
            txid: state.repl.txid,
            source: state.source,
            requests: requests
          }
        )

        notify_previous(state)
        {:noreply, serve(state)}

      {:error, reason, state} ->
        reason = reason |> DiskFull.classify() |> Breaker.classify()
        Admission.end_wake(state.config, self())
        {state, requests, store_time} = Wake.unmeter(state)

        Telemetry.span_stop(
          [:cell, :wake],
          started,
          wake_measurements(requests, store_time, reductions),
          %{
            cell: state.id,
            result: :error,
            reason: reason,
            requests: requests
          }
        )

        abandon(state)
        {:stop, {:shutdown, {:wake_failed, reason}}, %{state | mode: :stopped}}
    end
  end

  # The requests of the wake, the time they took summed, and the reductions the
  # cell process ran since the wake began.
  defp wake_measurements(requests, store_time, reductions) do
    {:reductions, now} = Process.info(self(), :reductions)

    requests
    |> Metered.measurements()
    |> Map.merge(%{store_time: store_time, reductions: now - reductions})
  end

  # Tells the node of the previous owner, if it is a member, that its cell
  # was taken over, so the old cell checks its lease at once instead of at
  # its next renewal. Only a shortcut: fencing does not depend on it.
  defp notify_previous(%State{takeover: %{previous: %{owner: owner, incarnation: inc}}} = state) do
    case Membership.node_of(state.config, owner, inc) do
      {:ok, target} when target != node() ->
        :erpc.cast(target, __MODULE__, :superseded, [
          state.config.name,
          state.id,
          state.lease.epoch
        ])

      _ ->
        :ok
    end
  end

  defp notify_previous(_state), do: :ok

  @doc false
  # Runs on the node of a previous owner: its cell for `id`, if still serving
  # an older epoch than `epoch`, checks its lease now.
  @spec superseded(atom, String.t(), pos_integer) :: :ok
  def superseded(name, id, epoch) do
    with pid when is_pid(pid) <- whereis(Config.fetch!(name), id),
         do: send(pid, {:superseded, epoch})

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp serve(state) do
    config = state.config
    Process.send_after(self(), :renew, renew_interval(config))
    Process.send_after(self(), :idle, config.idle_timeout_ms)
    state = %{state | mode: :serving, last_active: System.monotonic_time(:millisecond)}
    :atomics.put(state.load, 3, state.last_active)
    wal_size(state)
    track(state)
    state
  end

  @doc false
  # A third of the TTL, spread by `lease_renew_jitter` so that cells woken
  # together do not renew together.
  @spec renew_interval(Alambique.Config.t()) :: pos_integer
  def renew_interval(config) do
    base = div(config.ttl_ms, 3)
    spread = trunc(base * config.renew_jitter)
    base + :rand.uniform(2 * spread + 1) - spread - 1
  end

  # Calls

  @impl true
  # The router stamps a call with the time its caller stops waiting. A call
  # taken from the mailbox after that is dropped unrun: nobody can read its
  # answer (`GenServer.call` does not cancel it).
  def handle_call({:deadline, at, request}, from, state) do
    if System.monotonic_time(:millisecond) < at do
      handle_call(request, from, state)
    else
      :atomics.sub(state.load, 2, 1)
      {:noreply, state}
    end
  end

  def handle_call(request, from, %State{mode: :serving} = state) do
    answer = state.inflight |> Inflight.busy(fn -> call(request, from, state) end) |> tracked()
    # Taken back once the call is parked or answered, so it counts all along.
    if request != :hibernate, do: :atomics.sub(state.load, 2, 1)
    answer
  end

  # Records the memory of the connection after a call or a message, which is
  # what changes it. The state is the last element of every answer.
  defp tracked(answer) do
    case elem(answer, tuple_size(answer) - 1) do
      %State{mode: :serving} = state -> track(state)
      _ -> :ok
    end

    answer
  end

  defp track(state),
    do: Admission.track(state.config, self(), state.conn)

  defp call(request, from, state) do
    state = %{state | last_active: System.monotonic_time(:millisecond)}
    :atomics.put(state.load, 3, state.last_active)

    if State.lease_valid?(state) do
      serve_call(request, from, state)
    else
      stop_serving(state, :lease_expired, {:error, :lease_expired})
    end
  end

  # A read that may see unacknowledged writes is answered after the newest
  # batch is durable, which implies every earlier one is, so no caller
  # observes a write that could still be lost.
  defp serve_call({:query, sql, params, ms}, from, state) do
    reply = Db.read(state.conn, sql, params, Db.limits(state.config, ms))

    if idle?(state),
      do: {:reply, reply, state},
      else: {:noreply, wait(state, {:read, from, reply})}
  end

  # A checked read (design §5) is answered only if the lease is still ours
  # after the read ran. When it is not, the caller gets no answer and the
  # router sends the call to the new owner.
  defp serve_call({:checked_query, sql, params, ms}, from, state) do
    reply = Db.read(state.conn, sql, params, Db.limits(state.config, ms))

    if idle?(state) do
      case check_lease(state) do
        :ok -> {:reply, reply, state}
        :lost -> stop_serving(state, :lease_lost)
        {:unreadable, reason} -> {:reply, {:error, {:lease_unreadable, reason}}, state}
      end
    else
      {:noreply, wait(state, {:checked_read, from, reply})}
    end
  end

  defp serve_call({:transaction, fun, ms}, from, state), do: write(state, from, fun, ms)

  defp serve_call({:execute, sql, params, ms}, from, state),
    do: write(state, from, execute_fun(sql, params), ms)

  # An isolated write owns its batch (design §13): the pending commits are
  # encoded first, and its own batch is encoded before the next call runs.
  defp serve_call({:isolated, request}, from, state) do
    case flush_now(state) do
      {:ok, state} -> isolated(request, from, state)
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  # Every earlier commit is durable, so the TXID is a cut: all writes committed
  # before this call are at or below it, all after are above.
  defp serve_call(:position, _from, state) do
    case drain(state) do
      {:ok, %State{pipe: %{acked: acked}} = state} ->
        {:reply, {:ok, Map.put(acked, :awake, true)}, state}

      {:stop, reason, state} ->
        stop_serving(state, reason, {:error, reason})
    end
  end

  defp serve_call(:touch, _from, state), do: {:reply, :ok, state}

  # Like `hibernate/2` and the checkpoint of a big WAL: every upload is
  # acknowledged before the WAL is truncated, but the cell stays awake. A queue
  # that does not empty within the deadline leaves the WAL for later.
  defp serve_call({:checkpoint, deadline_ms}, _from, state) do
    case drain_within(state, deadline_ms) do
      {:ok, state} -> requested_checkpoint(state)
      {:busy, state} -> {:reply, {:error, :busy}, state}
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  defp serve_call(:hibernate, _from, state) do
    case hibernate(state, :request) do
      {:ok, state, released} -> {:stop, :normal, released, %{state | mode: :stopped}}
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  # Writes are durable before the migrations run, which then upload their own
  # batch. The wake already migrated a cell that `{:migrate, true}` woke.
  defp serve_call({:migrate, _wake}, _from, state) do
    case drain(state) do
      {:ok, state} -> migrate(state)
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  defp execute_fun(sql, params) do
    fn tx ->
      case Tx.execute(tx, sql, params) do
        {:ok, result} -> result
        {:error, reason} -> Tx.rollback(tx, reason)
      end
    end
  end

  defp isolated({:transaction, fun, ms}, from, state), do: write(state, from, fun, ms, true)

  defp isolated({:execute, sql, params, ms}, from, state),
    do: write(state, from, execute_fun(sql, params), ms, true)

  defp write(state, from, fun, ms, isolated? \\ false) do
    txid = Replicator.next_txid(state.repl)

    case Db.transaction(state.conn, txid, Publisher.wrap(state, fun),
           select: state.config.feed,
           isolated: isolated?,
           limits: Db.write_limits(state.config, ms)
         ) do
      {:ok, value} ->
        state = Publisher.committed(state)
        state = %{state | pending: [{:write, from, {:ok, value}} | state.pending]}
        :atomics.add(state.load, 1, 1)
        Fault.point(:cell_after_commit, %{cell: state.id, txid: state.repl.txid})
        if isolated?, do: encode_now(state), else: {:noreply, queue_flush(state)}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp encode_now(state) do
    case flush_now(state) do
      {:ok, state} -> {:noreply, state}
      {:stop, reason, state} -> stop_serving(state, reason)
    end
  end

  # A refused or failed migration leaves the cell serving; anything else, such
  # as a failed upload, stops it like a failed commit.
  defp migrate(%State{repl: %{txid: before}} = state) do
    case Wake.remigrate(state) do
      {:ok, state} ->
        migrated(state, before, nil)

      {:error, reason, state} when elem(reason, 0) in [:bad_migrations, :schema_too_new] ->
        {:reply, {:error, reason}, state}

      {:error, {:migration, version, _} = reason, state} when is_integer(version) ->
        migrated(state, before, DiskFull.classify(reason))

      {:error, reason, state} ->
        stop_reason = stop_reason(reason)
        stop_serving(state, stop_reason, {:error, stop_reason})
    end
  end

  defp migrated(state, before, error) do
    state = if state.repl.txid == before, do: state, else: Publisher.after_ack(state)

    case {error, Db.user_version(state.conn)} do
      {nil, {:ok, version}} -> {:reply, {:ok, version}, state}
      {nil, {:error, reason}} -> {:reply, {:error, {:migration, reason}}, state}
      {error, _} -> {:reply, {:error, error}, state}
    end
  end

  defp stop_reason({:migration_sync, :fenced}), do: :fenced
  defp stop_reason({:migration_sync, reason}), do: {:sync_failed, reason}
  defp stop_reason(reason), do: {:migrate_failed, reason}

  # The flush message lands behind every call already in the mailbox.
  defp queue_flush(%State{flush_queued: true} = state), do: state

  defp queue_flush(state) do
    send(self(), :flush)
    %{state | flush_queued: true}
  end

  # Timers and messages

  @impl true
  def handle_info(message, state),
    do: state.inflight |> Inflight.busy(fn -> info(message, state) end) |> tracked()

  defp info(:flush, %State{mode: :serving} = state) do
    case flush(%{state | flush_queued: false}) do
      {:ok, state} -> {:noreply, state}
      {:stop, reason, state} -> stop_serving(state, reason)
    end
  end

  defp info({:landed, ref, result}, %State{mode: :serving} = state) do
    with {:ok, state} <- landed(state, ref, result),
         {:ok, state} <- checkpoint_if_due(state) do
      {:noreply, state}
    else
      {:stop, reason, state} -> stop_serving(state, reason)
    end
  end

  defp info({:retry_upload, ref}, %State{mode: :serving} = state) do
    case retry_upload(state, ref) do
      {:ok, state} -> {:noreply, state}
      {:stop, reason, state} -> stop_serving(state, reason)
    end
  end

  defp info({:feed_published, pid, txid, time, count}, %State{mode: :serving} = state),
    do: {:noreply, Publisher.published(state, pid, txid, time, count)}

  defp info({:EXIT, pid, _reason}, %State{mode: :serving, feed_pid: pid} = state),
    do: {:noreply, Publisher.sender_down(state, pid)}

  # A frozen cell (design §10) hibernates at its next renewal.
  defp info(:renew, %State{mode: :serving} = state) do
    if frozen?(state) do
      case hibernate(state, :frozen) do
        {:ok, state, _released} -> {:stop, :normal, %{state | mode: :stopped}}
        {:stop, reason, state} -> stop_serving(state, reason)
      end
    else
      info(:renew_lease, state)
    end
  end

  defp info(:renew_lease, %State{mode: :serving} = state) do
    case Lease.renew(state.lease_ctx, state.lease) do
      {:ok, lease} ->
        Process.send_after(self(), :renew, renew_interval(state.config))
        {:noreply, %{state | lease: lease}}

      {:error, :lost} ->
        stop_serving(state, :lease_lost)

      {:error, _reason} ->
        if State.lease_valid?(state) do
          Process.send_after(self(), :renew, renew_interval(state.config))
          {:noreply, state}
        else
          stop_serving(state, :lease_expired)
        end
    end
  end

  # The cell was taken over: look at the lease now. `:renew` keeps its own timer.
  defp info({:superseded, epoch}, %State{mode: :serving, lease: %{epoch: current}} = state)
       when current < epoch do
    case Lease.renew(state.lease_ctx, state.lease) do
      {:ok, lease} -> {:noreply, %{state | lease: lease}}
      {:error, :lost} -> stop_serving(state, :lease_lost)
      {:error, _reason} -> {:noreply, state}
    end
  end

  defp info(:idle, %State{mode: :serving} = state) do
    idle = state.config.idle_timeout_ms
    elapsed = System.monotonic_time(:millisecond) - state.last_active

    busy? = not idle?(state) or :atomics.get(state.load, 2) != 0

    cond do
      elapsed < idle ->
        Process.send_after(self(), :idle, idle - elapsed)
        {:noreply, state}

      busy? ->
        Process.send_after(self(), :idle, idle)
        {:noreply, state}

      true ->
        case hibernate(state, :idle) do
          {:ok, state, _released} -> {:stop, :normal, %{state | mode: :stopped}}
          {:stop, reason, state} -> stop_serving(state, reason)
        end
    end
  end

  # The lazy phase of the file (design §16) ended.
  defp info({:hydrated, pid}, %State{hydrator: pid} = state) do
    case Hydration.done(state) do
      {:ok, state} -> {:noreply, state}
      {:error, reason} -> stop_serving(state, {:hydration_failed, reason})
    end
  end

  defp info({:hydration_failed, pid, reason}, %State{hydrator: pid} = state),
    do: stop_serving(state, {:hydration_failed, reason})

  defp info({:EXIT, pid, reason}, %State{hydrator: pid} = state) when reason != :normal,
    do: stop_serving(state, {:hydration_failed, reason})

  defp info({:EXIT, pid, reason}, %State{mode: :serving} = state) when reason != :normal do
    case Enum.find(state.pipe.queue, &(&1.pid == pid and not &1.landed)) do
      nil -> {:noreply, state}
      batch -> fail_serving(upload_failed(state, batch, {:upload_exit, reason}))
    end
  end

  defp info(_message, state), do: {:noreply, state}

  # One attempt: the cell cannot serve while it waits, and a retry that ran to
  # the end of the lease would leave no time for the renewal. A failed check
  # counts as not frozen; the next renewal looks again.
  defp frozen?(state),
    do: Freeze.frozen?(state.repl_ctx, state.id, deadline: Retry.deadline_in(0)) == {:ok, true}

  # Commit

  defp idle?(state), do: state.pending == [] and state.pipe.queue == []

  # Makes an item wait for the newest batch, which is the open one when it has
  # items. Its release implies that every earlier write is acknowledged.
  defp wait(%State{pending: []} = state, item) do
    :atomics.add(state.load, 1, 1)
    %{state | pipe: Pipe.attach(state.pipe, [item])}
  end

  defp wait(state, item) do
    :atomics.add(state.load, 1, 1)
    %{state | pending: [item | state.pending]}
  end

  # Encodes the pending commits as the next batch. A full pipeline defers the
  # flush to the next acknowledgement: the commits meanwhile grow the batch.
  defp flush(%State{pending: []} = state), do: {:ok, state}

  defp flush(state) do
    if Pipe.full?(state.pipe),
      do: {:ok, put_in(state.pipe.flush_wanted, true)},
      else: send_batch(state)
  end

  # Like `flush/1`, but waits for a free slot.
  defp flush_now(%State{pending: []} = state), do: {:ok, state}

  defp flush_now(state) do
    if Pipe.full?(state.pipe) do
      with {:ok, state} <- await(state), do: flush_now(state)
    else
      send_batch(state)
    end
  end

  # The items of a batch stay newest first, as `pending` keeps them.
  defp send_batch(%State{pending: items} = state) do
    writes = Enum.count(items, &match?({:write, _, _}, &1))
    state = if writes > 0, do: Publisher.stamp(state), else: state
    started = System.monotonic_time()
    deadline = State.deadline(state)

    case Replicator.encode(state.repl, state.repl_ctx, :batch, deadline: deadline) do
      :none ->
        release(%{state | pending: []}, items)

      {:ok, repl, up} ->
        ref = make_ref()
        pid = start_upload(state, up, ref)

        batch = %{
          txid: up.txid,
          ref: ref,
          pid: pid,
          landed: false,
          attempts: 0,
          last: nil,
          items: items,
          writes: writes,
          started: started,
          inflight: length(state.pipe.queue) + 1,
          epoch: up.info.epoch,
          data: up.info.data,
          upload: up
        }

        pipe = %{Pipe.push(state.pipe, batch) | flush_wanted: false}
        {:ok, %{state | repl: repl, pending: [], pipe: pipe}}

      {:error, reason} ->
        {:stop, {:sync_failed, reason}, state}
    end
  end

  # The upload process of a batch. It reports to the cell and ends; a crash
  # before the report reaches the cell as an exit signal.
  defp start_upload(state, up, ref) do
    parent = self()
    ctx = state.repl_ctx
    deadline = State.deadline(state)

    spawn_link(fn ->
      result = Replicator.upload(up, ctx, deadline: deadline)
      Process.unlink(parent)
      send(parent, {:landed, ref, result})
    end)
  end

  # Items of a flush that changed nothing wait for the queue, if any.
  defp release(%State{pipe: %{queue: []}} = state, items) do
    verdict = check_reads(items, state)
    answer(state, items, verdict)
    if verdict == :lost, do: {:stop, :lease_lost, state}, else: {:ok, state}
  end

  defp release(state, items), do: {:ok, %{state | pipe: Pipe.attach(state.pipe, items)}}

  # An upload process reported. A landed batch is acknowledged once every
  # batch before it has landed. A fencing conflict or a crashed upload stops
  # the cell (nothing is re-encoded, the outcome of the batches behind it is
  # unknown); a store failure keeps the batch for a new upload.
  defp landed(state, ref, result) do
    case Pipe.find(state.pipe, ref) do
      nil ->
        {:ok, state}

      batch when result == :ok ->
        Fault.point(:cell_after_land, %{cell: state.id, txid: batch.txid})
        pop(%{state | pipe: Pipe.land(state.pipe, ref)}, false)

      batch ->
        failed(state, batch, result)
    end
  end

  # A store failure is no verdict on the lease: the same key and bytes are put
  # again after a backoff (an earlier attempt that landed is found and
  # accepted, so the batch is never written twice), while the lease is valid.
  # The batch keeps its place, so the acknowledgements stay in order.
  defp failed(state, batch, {:error, reason} = result) do
    if State.lease_valid?(state) do
      %{retry_backoff_ms: base, retry_backoff_max_ms: max} = state.config.upload
      delay = min(base * Integer.pow(2, min(batch.attempts, 20)), max)
      Process.send_after(self(), {:retry_upload, batch.ref}, delay)

      Telemetry.emit(
        [:cell, :upload_retry],
        %{delay_ms: delay},
        Map.merge(commit_meta(state, batch), %{attempt: batch.attempts + 1, reason: reason})
      )

      pipe =
        Pipe.update(
          state.pipe,
          batch.ref,
          &%{&1 | pid: nil, attempts: &1.attempts + 1, last: reason}
        )

      {:ok, %{state | pipe: pipe}}
    else
      upload_failed(state, batch, result)
    end
  end

  defp failed(state, batch, result), do: upload_failed(state, batch, result)

  # The backoff of a batch ended. A lease that ran out meanwhile stops the cell
  # with the last failure.
  defp retry_upload(state, ref) do
    case Pipe.find(state.pipe, ref) do
      nil ->
        {:ok, state}

      batch ->
        if State.lease_valid?(state) do
          pid = start_upload(state, batch.upload, ref)
          {:ok, %{state | pipe: Pipe.update(state.pipe, ref, &%{&1 | pid: pid})}}
        else
          upload_failed(state, batch, {:error, batch.last})
        end
    end
  end

  defp fail_serving({:stop, reason, state}), do: stop_serving(state, reason)

  defp upload_failed(state, batch, result) do
    {stop, result, reason} =
      case result do
        :taken -> {:fenced, :fenced, :fenced}
        {:error, reason} -> {{:sync_failed, reason}, :error, reason}
        reason -> {{:sync_failed, reason}, :error, reason}
      end

    meta = Map.merge(commit_meta(state, batch), %{result: result, reason: reason})
    Commit.emit(batch.started, commit_measures(batch), meta)
    {:stop, stop, refuse_taken(state, batch, result)}
  end

  # A put that found another writer's object proves the batch is not in the
  # chain (design §6): its callers get a definite refusal. Any other failure
  # leaves the outcome open, which `stop_serving/3` answers.
  defp refuse_taken(state, batch, :fenced) do
    Enum.each(batch.items, fn {_kind, from, _} -> GenServer.reply(from, {:error, :fenced}) end)
    %{state | pipe: Pipe.drop(state.pipe, batch.ref)}
  end

  defp refuse_taken(state, _batch, _result), do: state

  # Acknowledges the landed batches at the head of the queue, in order.
  defp pop(%State{pipe: %{queue: [%{landed: true} = batch | rest]}} = state, _popped) do
    Fault.point(:cell_before_ack, %{cell: state.id, txid: batch.txid})
    acked = %{txid: batch.txid, epoch: batch.epoch, data: batch.data}
    state = %{state | pipe: %{state.pipe | queue: rest, acked: acked}}
    Hold.put_upload(state.config, state.repl_ctx, batch.upload)
    Fault.point(:hold_after_ack, %{cell: state.id, txid: batch.txid})
    Compactor.notify(state.config, state.id, batch.txid)
    now = System.monotonic_time()

    Commit.emit(
      batch.started,
      Map.put(commit_measures(batch), :queued_ms, ms(now - batch.started)),
      Map.put(commit_meta(state, batch), :result, :ok)
    )

    verdict = check_reads(batch.items, state)
    answer(state, batch.items, verdict)
    Commit.after_ack(state, batch.txid)
    Fault.point(:cell_after_ack, %{cell: state.id, txid: batch.txid})

    if verdict == :lost, do: {:stop, :lease_lost, state}, else: pop(state, true)
  end

  defp pop(state, popped?) do
    state = if popped?, do: Publisher.after_ack(state), else: state
    if state.pipe.flush_wanted, do: flush(state), else: {:ok, state}
  end

  defp commit_meta(state, batch),
    do: %{cell: state.id, location: state.location.id, epoch: batch.epoch, txid: batch.txid}

  defp commit_measures(batch), do: %{transactions: batch.writes, inflight: batch.inflight}

  defp ms(native), do: System.convert_time_unit(native, :native, :millisecond)

  # Checkpoint while awake (design §6): once the WAL passes the limit, encode
  # what is pending, wait for the queue to empty and move the WAL into the
  # file, so a cell that never sleeps does not grow its WAL without bound.
  # A file that is still being filled waits for the end of the hydration.
  defp checkpoint_if_due(%State{conn: conn} = state) when conn != nil do
    limit = state.config.wal.checkpoint_bytes
    size = wal_size(state)

    with true <- size >= limit,
         false <- Hydration.hydrating?(state),
         {:ok, state} <- drain(state) do
      {_result, state} = checkpoint(state, size)
      {:ok, state}
    else
      {:stop, _, _} = stop -> stop
      _ -> {:ok, state}
    end
  end

  defp checkpoint_if_due(state), do: {:ok, state}

  # A file that is still being filled has no WAL to move yet.
  defp requested_checkpoint(state) do
    if Hydration.hydrating?(state) do
      {:reply, {:error, :busy}, state}
    else
      case checkpoint(state, wal_size(state)) do
        {:ok, state} -> {:reply, :ok, state}
        {:error, state} -> {:reply, {:error, :busy}, state}
      end
    end
  end

  # Publishes the size of the WAL for `Alambique.cells/2`; the stat is the
  # one the checkpoint check needs anyway.
  defp wal_size(state) do
    size =
      case File.stat(state.path <> "-wal") do
        {:ok, %{size: size}} -> size
        _ -> 0
      end

    :atomics.put(state.load, 4, size)
    size
  end

  # Busy or unreplicated frames leave the WAL for the next acknowledgement.
  defp checkpoint(state, bytes) do
    started = System.monotonic_time()
    result = Replicator.checkpoint(state.repl, state.conn)

    Telemetry.span_stop(
      [:cell, :checkpoint],
      started,
      %{bytes: bytes},
      %{cell: state.id, result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    case result do
      {:ok, repl} ->
        Fault.point(:cell_after_checkpoint, %{cell: state.id, txid: repl.txid})
        wal_size(state)
        {:ok, %{state | repl: repl}}

      {:error, _} ->
        wal_size(state)
        {:error, state}
    end
  end

  # Waits for one upload process, bounded by the upload deadlines.
  defp await(state) do
    case await(state, 2 * state.config.op_timeout_ms + state.config.upload.retry_backoff_max_ms) do
      {:timeout, state} -> upload_failed(state, hd(state.pipe.queue), :timeout)
      answer -> answer
    end
  end

  defp await(state, timeout_ms) do
    pids = Pipe.pids(state.pipe)

    receive do
      {:landed, ref, result} ->
        landed(state, ref, result)

      {:retry_upload, ref} ->
        retry_upload(state, ref)

      {:EXIT, pid, reason} when reason != :normal and is_map_key(pids, pid) ->
        upload_failed(
          state,
          Enum.find(state.pipe.queue, &(&1.pid == pid)),
          {:upload_exit, reason}
        )
    after
      timeout_ms -> {:timeout, state}
    end
  end

  # Encodes what is pending and waits until every batch is acknowledged.
  defp drain(state) do
    with {:ok, state} <- flush_now(state), do: settle(state)
  end

  # Like `drain/1`, but gives up at the deadline and leaves the queue as it
  # is: the uploads go on and their callers are answered as usual.
  defp drain_within(state, deadline_ms) do
    limit = System.monotonic_time(:millisecond) + deadline_ms
    with {:ok, state} <- flush_now(state), do: settle_within(state, limit)
  end

  defp settle_within(%State{pipe: %{queue: []}} = state, _limit), do: {:ok, state}

  defp settle_within(state, limit) do
    case await(state, max(limit - System.monotonic_time(:millisecond), 0)) do
      {:ok, state} -> settle_within(state, limit)
      {:timeout, state} -> {:busy, state}
      {:stop, _, _} = stop -> stop
    end
  end

  defp settle(%State{pipe: %{queue: []}} = state), do: {:ok, state}

  defp settle(state) do
    with {:ok, state} <- await(state), do: settle(state)
  end

  # One read of the lease serves every checked read of the batch.
  defp check_reads(pending, state) do
    if Enum.any?(pending, &match?({:checked_read, _, _}, &1)), do: check_lease(state), else: :ok
  end

  # A checked read whose lease is lost gets no answer: it exits with the cell.
  defp answer(state, pending, verdict) do
    :atomics.sub(state.load, 1, length(pending))

    # Items are kept newest first; callers are answered in arrival order.
    pending
    |> Enum.reverse()
    |> Enum.each(fn
      {:checked_read, from, reply} -> answer_checked(verdict, from, reply)
      {_kind, from, reply} -> GenServer.reply(from, reply)
    end)
  end

  defp answer_checked(:ok, from, reply), do: GenServer.reply(from, reply)
  defp answer_checked(:lost, _from, _reply), do: :ok

  defp answer_checked({:unreadable, reason}, from, _reply),
    do: GenServer.reply(from, {:error, {:lease_unreadable, reason}})

  defp check_lease(state) do
    case Lease.verify(state.lease_ctx, state.lease) do
      :ok -> :ok
      {:error, :lost} -> :lost
      {:error, reason} -> {:unreadable, reason}
    end
  end

  # Ending

  # Hibernation (design §7): upload what is pending, checkpoint, release. The
  # third element is `:ok`, or the error of a release that failed: the cell
  # is asleep and its lease expires as after a crash.
  defp hibernate(state, why) do
    started = System.monotonic_time()

    with {:ok, state} <- drain(state) do
      state = state |> Publisher.flush() |> finish_local()
      {compacted, tail, spent} = compact_tail(state, why)
      released = release(state)

      Telemetry.span_stop([:cell, :hibernate], started, %{tail: tail, compact_duration: spent}, %{
        cell: state.id,
        location: state.location.id,
        epoch: state.lease.epoch,
        reason: why,
        compaction: compacted
      })

      {:ok, state, released}
    end
  end

  # A graceful release (`Alambique.hibernate/3`, a drain, a stop) compacts a
  # long level-0 tail before it gives the lease away, so the next node wakes on
  # a short chain (design §9). It stops at `release_deadline_ms`, or earlier at
  # the end of the lease, and hands over with what it merged. Returns the
  # outcome (`:none`, `:done`, `:deadline` or `:error`), the tail it found and
  # the time spent.
  defp compact_tail(%State{config: %{compaction: c} = config} = state, why)
       when why in [:request, :shutdown] and c != nil do
    budget = min(c.release_deadline_ms, max(State.lease_left_ms(state), 0))

    if budget == 0 do
      {:none, 0, 0}
    else
      started = System.monotonic_time()
      deadline = Retry.deadline_in(budget)
      result = Compactor.compact(config, state.repl_ctx, state.id, budget)
      spent = System.monotonic_time() - started
      {outcome, tail} = release_compaction(result, deadline, state.id, c.release_min_tail)
      {outcome, tail, spent}
    end
  end

  defp compact_tail(_state, _why), do: {:none, 0, 0}

  defp release_compaction({:ok, %{tail: tail}}, _deadline, _cell, min) when tail < min,
    do: {:none, tail}

  defp release_compaction({:ok, %{more: false, tail: tail}}, _deadline, _cell, _min),
    do: {:done, tail}

  defp release_compaction({:ok, %{more: true, tail: tail}}, _deadline, _cell, _min),
    do: {:deadline, tail}

  defp release_compaction({:error, reason}, deadline, cell, _min) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:deadline, 0}
    else
      Logger.warning(
        "Alambique could not compact #{cell} before releasing it: #{inspect(reason)}"
      )

      {:error, 0}
    end
  end

  # The checkpoint follows the last upload, so the file holds every
  # replicated frame before the marker says so. Closing never checkpoints
  # (design §6).
  defp finish_local(%State{pending: [], pipe: %{queue: []}} = state) do
    checkpointed = Replicator.checkpoint(state.repl, state.conn)
    Db.close(state.conn)
    state = %{state | conn: nil}

    # A file that is still being filled is not kept (design §16).
    if Hydration.hydrating?(state) do
      Hydration.discard(state)
    else
      with {:ok, repl} <- checkpointed do
        # Closing leaves the emptied WAL and its index behind.
        for suffix <- ["-wal", "-shm"], do: File.rm(state.path <> suffix)
        Local.mark(state.path, repl.txid, repl.checksum, repl.page_size)
      end

      state
    end
  end

  defp release(%State{lease: lease, lease_ctx: ctx}) do
    case Lease.release(ctx, lease) do
      :ok ->
        :ok

      # Someone took the cell meanwhile (design §5); the lease is theirs.
      {:error, :lost} ->
        :ok

      {:error, reason} ->
        Logger.warning("Alambique could not release a lease: #{inspect(reason)}")
        {:error, {:release_failed, reason}}
    end
  end

  # Serving ended without a clean hibernation: answer every waiting caller
  # with an error, free what is held, and stop. A write of a batch that was
  # sent may be durable whatever stopped the cell, so it gets
  # `:outcome_unknown`. Pending items were never sent and a read changes
  # nothing: they get the reason.
  defp stop_serving(state, reason, reply \\ nil) do
    error = {:error, reason}
    Enum.each(state.pending, fn {_kind, from, _} -> GenServer.reply(from, error) end)

    for {kind, from, _} <- Pipe.items(state.pipe) do
      GenServer.reply(from, if(kind == :write, do: {:error, :outcome_unknown}, else: error))
    end

    Pipe.kill(state.pipe)

    Telemetry.emit([:cell, :stop], %{}, %{
      cell: state.id,
      epoch: state.lease && state.lease.epoch,
      reason: reason
    })

    state = %{state | pending: [], pipe: %{state.pipe | queue: []}}
    # A fenced or lost lease belongs to someone else; any other lease is ours to give back.
    # An expired one no longer lets us publish. A file that failed to hydrate cannot be read.
    if reason not in [:fenced, :lease_lost, :lease_expired] and
         not match?({:hydration_failed, _}, reason),
       do: Publisher.flush(state)

    if reason not in [:fenced, :lease_lost], do: abandon(state), else: close(state)
    state = %{state | mode: :stopped, conn: nil}

    if reply,
      do: {:stop, {:shutdown, reason}, reply, state},
      else: {:stop, {:shutdown, reason}, state}
  end

  defp abandon(%State{lease: nil} = state), do: close(state)

  defp abandon(state) do
    close(state)
    release(state)
  end

  defp close(%State{conn: nil}), do: :ok
  defp close(%State{conn: conn}), do: Db.close(conn)

  @impl true
  def terminate(reason, state) do
    result = Inflight.busy(state.inflight, fn -> finish(reason, state) end)
    # After the last acknowledgement of a clean stop.
    Hold.drop_cell(state.config, state.id)
    Admission.forget(state.config, self())
    result
  end

  defp finish(:shutdown, %State{mode: :serving} = state) do
    case hibernate(state, :shutdown) do
      {:ok, _state, _released} -> :ok
      {:stop, _reason, state} -> abandon(state)
    end
  end

  # A cell that ends before its wake did gives its wake slot back.
  defp finish(_reason, %State{mode: :waking} = state) do
    Admission.end_wake(state.config, self())
    close(state)
  end

  defp finish(_reason, state), do: close(state)
end
