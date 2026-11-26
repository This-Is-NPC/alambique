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

  # What the supervisor waits for a cell to stop; the release compaction of a
  # stop fits in it.
  @shutdown_ms 30_000

  use GenServer, restart: :temporary, shutdown: @shutdown_ms

  require Logger
  require Alambique.Cell.Layout

  alias Alambique.Cell.{Commit, Db, Facts, Hydration, Layout, Local, Options, Pipe, State, Wake}
  alias Alambique.Compaction.{Compactor, Hold, Slots}

  alias Alambique.{
    Admission,
    Config,
    DiskFull,
    Fault,
    Freeze,
    Inflight,
    Lease,
    Membership,
    Sqlite,
    Telemetry,
    Tx
  }

  alias Alambique.Feed.Publisher
  alias Alambique.Replication.{Replicator, Retry, Sidecar}
  alias Alambique.Store.{Breaker, Metered}

  # Lifecycle

  # Options: `:takeover`, `%{reason: term, epoch: pos_integer | nil}`, and
  # `:lease_seen`, the lease the router read (see `State`).
  @spec start_link({Config.t(), String.t()} | {Config.t(), String.t(), keyword}) ::
          GenServer.on_start()
  def start_link({config, id}), do: start_link({config, id, []})

  def start_link({config, id, opts}) do
    # The registry value is the cell's published load (see `load/2`).
    load = Facts.new()

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
  # The slots are described in `Facts`.
  @spec pending(pid, :atomics.atomics_ref()) :: {:ok, non_neg_integer} | :none
  def pending(pid, load) do
    if Process.alive?(pid),
      do: {:ok, pending_calls(load)},
      else: :none
  end

  @doc false
  @spec pending_calls(:atomics.atomics_ref()) :: non_neg_integer
  def pending_calls(load),
    do: :atomics.get(load, Layout.slot(:callers)) + :atomics.get(load, Layout.slot(:admitted))

  @doc false
  # The process of `id` unless it already stopped serving and only has to
  # exit: it is alive for the time it closes its database and releases the
  # lease, and is no longer the owner then.
  @spec serving(Alambique.Config.t(), String.t()) :: pid | nil
  def serving(config, id) do
    with {pid, load} <- find(config, id),
         do: if(:atomics.get(load, Layout.slot(:stopped)) == 0, do: pid)
  end

  # Set when the cell stops serving, before its stop event.
  defp mark_stopped(state), do: :atomics.put(state.load, Layout.slot(:stopped), 1)

  @doc false
  # The monotonic ms of the last call, `nil` until the wake ends.
  @spec last_used(:atomics.atomics_ref()) :: integer | nil
  def last_used(load) do
    case :atomics.get(load, Layout.slot(:last_used)) do
      0 -> nil
      last -> last
    end
  end

  @doc false
  # The wake has not ended: `last_used` is set when the cell starts serving.
  @spec waking?(:atomics.atomics_ref()) :: boolean
  def waking?(load), do: :atomics.get(load, Layout.slot(:last_used)) == 0

  @doc false
  @spec wal_bytes(:atomics.atomics_ref()) :: non_neg_integer
  def wal_bytes(load), do: :atomics.get(load, Layout.slot(:wal_bytes))

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
    admitted = Layout.slot(:admitted)

    if :atomics.add_get(load, admitted, 1) + :atomics.get(load, Layout.slot(:callers)) <= max do
      :ok
    else
      :atomics.sub(load, admitted, 1)
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
      inflight: Inflight.open(config, id, :cell),
      pipe: Pipe.new(config),
      load: load,
      origin: %{
        takeover: opts[:takeover],
        lease_seen: opts[:lease_seen],
        holder: opts[:holder],
        trips: nil
      }
    }

    Facts.put(load, :wake_started_at, System.system_time(:millisecond))
    Facts.put(load, :wake_started_at_mono, System.monotonic_time(:millisecond))

    # Last: nothing after it can fail, so a slot is never held by a cell that
    # never started.
    Admission.waking(config, self())
    {:ok, state, {:continue, :wake}}
  end

  # Every callback runs with the marker busy (design §10): they are the only
  # places where the cell process runs native code.
  @impl true
  def handle_continue(:wake, state), do: wake(state)

  # The cell holds a wake slot before any work: its caller's, or its own if
  # that caller died (design §7). Without one it does not wake, and the
  # callers already routed to it get the same `:overloaded` as any wake.
  #
  # The options of the cell are resolved outside the busy marker: the wait
  # for the callback runs no native code, and a VM crash during it must not
  # mark the cell as crashed (design §10).
  defp wake(state) do
    Fault.point(:cell_before_take_over, %{cell: state.id})

    case Admission.take_over(state.config, state.origin.holder) do
      :ok ->
        started = System.monotonic_time()

        {options, info} =
          case Options.resolve(state.config, state.id) do
            {:ok, opts, info} -> {{:ok, opts}, info}
            {:error, message, info} -> {{:error, message}, info}
          end

        Inflight.busy(state.inflight, fn -> run_wake(state, options, info, started) end)

      {:error, reason} ->
        {:stop, {:shutdown, {:wake_failed, reason}}, %{state | mode: :stopped}}
    end
  end

  defp run_wake(state, options, info, started) do
    {:reductions, reductions} = Process.info(self(), :reductions)

    # The options are resolved once: the wake re-enters its fence on
    # `:txid_taken` and must not call the callback again.
    result =
      case options do
        {:ok, opts} -> Wake.run(%{state | cell_opts: opts})
        {:error, message} -> {:error, {:cell_options, message}, state}
      end

    case result do
      {:ok, state} -> wake_done(state, info, started, reductions)
      {:error, reason, state} -> wake_failed(state, reason, info, started, reductions)
    end
  end

  defp wake_done(state, info, started, reductions) do
    Admission.end_wake(state.config, self())
    Admission.recovered(state.config, state.id)
    {state, requests, store_time} = Wake.unmeter(state)

    Telemetry.span_stop(
      [:cell, :wake],
      started,
      wake_measurements(requests, store_time, reductions, info),
      %{
        cell: state.id,
        location: state.location.id,
        result: :ok,
        options: info.source,
        clamped: info.clamped,
        repaired: info.repaired,
        epoch: state.lease.epoch,
        txid: state.repl.txid,
        source: state.source,
        requests: requests
      }
    )

    notify_previous(state)
    {:noreply, serve(state)}
  end

  defp wake_failed(state, reason, info, started, reductions) do
    reason = reason |> DiskFull.classify() |> Breaker.classify()
    Admission.end_wake(state.config, self())
    {state, requests, store_time} = Wake.unmeter(state)

    Telemetry.span_stop(
      [:cell, :wake],
      started,
      wake_measurements(requests, store_time, reductions, info),
      %{
        cell: state.id,
        result: :error,
        options: info.source,
        reason: reason,
        requests: requests
      }
    )

    # The wake failed: the release answer changes nothing, it logs its failure.
    _ = abandon(state)
    {:stop, {:shutdown, {:wake_failed, reason}}, %{state | mode: :stopped}}
  end

  # The requests of the wake, the time they took summed, the reductions the
  # cell process ran since the wake began and the time of the options callback.
  defp wake_measurements(requests, store_time, reductions, info) do
    {:reductions, now} = Process.info(self(), :reductions)

    requests
    |> Metered.measurements()
    |> Map.merge(%{
      store_time: store_time,
      reductions: now - reductions,
      options_time: info.time
    })
  end

  # Tells the node of the previous owner, if it is a member, that its cell
  # was taken over, so the old cell checks its lease at once instead of at
  # its next renewal. Only a shortcut: fencing does not depend on it.
  defp notify_previous(
         %State{origin: %{takeover: %{previous: %{owner: owner, incarnation: inc}}}} = state
       ) do
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
    Process.send_after(self(), :renew, first_renewal_in(state))
    idle_timer = :erlang.start_timer(State.cell_opt(state, :idle_timeout_ms), self(), :idle)

    state = %{
      state
      | mode: :serving,
        last_active: System.monotonic_time(:millisecond),
        idle: %{state.idle | timer: idle_timer}
    }

    wal_size(state)
    Facts.publish(state, true)
    Facts.put(state.load, :awake_since, System.system_time(:millisecond))
    Facts.put(state.load, :awake_since_mono, state.last_active)
    # Last: a reader that finds it set finds every fact above.
    :atomics.put(state.load, Layout.slot(:last_used), state.last_active)
    state |> arm_idle_cache() |> arm_idle_close()
  end

  defp arm_idle(state, ms),
    do: %{state | idle: %{state.idle | timer: :erlang.start_timer(ms, self(), :idle)}}

  # The shrink timer carries its ref, so a message that was already queued
  # when the timer was replaced is ignored.
  defp arm_idle_cache(state) do
    case State.cell_opt(state, :idle_cache_after_ms) do
      nil -> state
      ms -> arm_idle_cache(state, ms)
    end
  end

  defp arm_idle_cache(state, ms),
    do: %{state | idle: %{state.idle | cache: :erlang.start_timer(ms, self(), :idle_cache)}}

  # The close timer carries its ref like the shrink timer does.
  defp arm_idle_close(state) do
    case State.cell_opt(state, :idle_close_after_ms) do
      nil -> state
      ms -> arm_idle_close(state, ms)
    end
  end

  defp arm_idle_close(state, ms),
    do: %{state | idle: %{state.idle | close: :erlang.start_timer(ms, self(), :idle_close)}}

  # The first call after a shrink gives the cache its full limit back. If the
  # pragma fails the cache is still small: the cell stays `:shrunk`, so the
  # facts tell the truth and the next call tries again.
  defp restore_cache(%State{idle: %{cache: :shrunk}} = state) do
    case Sqlite.execute(state.conn, "PRAGMA cache_size = -#{State.cell_opt(state, :cache_kib)}",
           write: true
         ) do
      :ok -> arm_idle_cache(state)
      {:error, _reason} -> state
    end
  end

  defp restore_cache(state), do: state

  # Refresh of the options (`Alambique.refresh_options/3`)

  # Resolves the options again with the one resolver the wake uses, in a
  # process of its own: the cell keeps serving and renewing its lease while the
  # callback runs. One resolve runs at a time; a caller that asks meanwhile is
  # queued and served by the next one, which starts after the first ends, so
  # the newest request is always applied last.
  defp refresh_options(%State{refresh: nil, config: %{cell_options: nil}} = state, from) do
    # No callback: the options are the node's and resolving takes no time.
    refresh = %{started: System.monotonic_time(), waiting: [from]}

    {reply, state} =
      apply_refresh(state, refresh, Options.resolve(state.config, state.id, :refresh))

    tracked({:reply, reply, state}, true)
  end

  defp refresh_options(%State{refresh: nil} = state, from),
    do: {:noreply, start_refresh(state, [from], [])}

  defp refresh_options(%State{refresh: refresh} = state, from),
    do: {:noreply, %{state | refresh: %{refresh | queued: [from | refresh.queued]}}}

  defp start_refresh(state, waiting, queued) do
    cell = self()
    gen = make_ref()
    {config, id} = {state.config, state.id}

    {pid, mon} =
      spawn_monitor(fn ->
        send(cell, {:options_resolved, gen, Options.resolve(config, id, :refresh)})
      end)

    refresh = %{
      gen: gen,
      pid: pid,
      mon: mon,
      started: System.monotonic_time(),
      waiting: waiting,
      queued: queued
    }

    %{state | refresh: refresh}
  end

  defp options_resolved(%State{refresh: refresh} = state, result) do
    Process.demonitor(refresh.mon, [:flush])
    {reply, state} = apply_refresh(%{state | refresh: nil}, refresh, result)
    for from <- refresh.waiting, do: GenServer.reply(from, reply)

    state =
      if refresh.queued == [],
        do: state,
        else: start_refresh(state, Enum.reverse(refresh.queued), [])

    {:noreply, state}
  end

  # The pragma goes first: when it fails nothing else changes, so the cell
  # never holds a part of the new options.
  defp apply_refresh(state, refresh, {:ok, override, info}) do
    case apply_options(state, override) do
      {:ok, state, changed, rearmed} ->
        refresh_done(
          state,
          refresh,
          :ok,
          %{
            options_time: info.time,
            timers_rearmed: rearmed
          },
          %{
            options: info.source,
            changed: changed,
            clamped: info.clamped,
            repaired: info.repaired
          }
        )

        {{:ok, info.source, changed: changed}, state}

      {:error, message} ->
        refresh_done(state, refresh, :error, %{options_time: info.time, timers_rearmed: 0}, %{
          options: :kept,
          reason: message
        })

        {{:error, {:cell_options, message}}, state}
    end
  end

  defp apply_refresh(state, refresh, {:error, message, info}) do
    refresh_done(state, refresh, :error, %{options_time: info.time, timers_rearmed: 0}, %{
      options: :kept,
      reason: message
    })

    {{:error, {:cell_options, message}}, state}
  end

  defp refresh_done(state, refresh, result, measurements, meta) do
    Telemetry.span_stop(
      [:cell, :options, :refresh],
      refresh.started,
      measurements,
      Map.merge(meta, %{cell: state.id, result: result, callers: length(refresh.waiting)})
    )
  end

  defp apply_options(state, override) do
    next = %{state | cell_opts: override}

    changed =
      for key <- Config.cell_option_keys(),
          State.cell_opt(state, key) != State.cell_opt(next, key),
          do: key

    with :ok <- locking_unchanged(state, next),
         :ok <- refresh_cache(state, next, changed) do
      {next, rearmed} = rearm(state, next, changed)

      if :track_bytes in changed and not Facts.track_bytes?(next),
        do: Facts.clear_memory(next.load)

      {:ok, next, changed, rearmed}
    end
  end

  # The lock is taken when the connection opens, and the cell chose it for
  # what the connection does (the idle close and the feed depend on it), so a
  # refresh cannot change it: the whole refresh is refused and the cell keeps
  # its options. The new value applies at the next wake.
  defp locking_unchanged(state, next) do
    if State.cell_opt(state, :locking_mode) == State.cell_opt(next, :locking_mode),
      do: :ok,
      else: {:error, "locking_mode cannot change while the cell is awake"}
  end

  # The cache limit the new options ask for, if the connection holds another.
  # A cell that is shrunk keeps the idle limit, and gets its full one back at
  # the next call (`restore_cache/1`) unless the shrink is now off. A closed
  # connection takes the options when it opens again; a refresh never opens it.
  defp refresh_cache(%State{conn: nil}, _next, _changed), do: :ok

  defp refresh_cache(%State{idle: %{cache: :shrunk}} = state, next, changed) do
    cond do
      State.cell_opt(next, :idle_cache_after_ms) == nil ->
        set_cache(state, State.cell_opt(next, :cache_kib))

      :idle_cache_kib in changed ->
        set_cache(state, State.cell_opt(next, :idle_cache_kib))

      true ->
        :ok
    end
  end

  defp refresh_cache(state, next, changed) do
    if :cache_kib in changed, do: set_cache(state, State.cell_opt(next, :cache_kib)), else: :ok
  end

  # Only the pragma runs native code, so only it holds the marker.
  defp set_cache(state, kib) do
    Inflight.busy(state.inflight, fn ->
      with :ok <- pragma_fault(state, :cell_before_refresh_pragma),
           :ok <- Sqlite.execute(state.conn, "PRAGMA cache_size = -#{kib}", write: true) do
        :ok
      else
        {:error, reason} -> {:error, "cache_size failed: " <> inspect(reason)}
      end
    end)
  end

  defp pragma_fault(state, point) do
    Fault.check(point, %{cell: state.id})
  end

  # Timers that the changed options moved: each is cancelled and armed again
  # for what is left of the new time since the last call. A message of a
  # cancelled timer that was queued carries a ref nobody holds any more.
  defp rearm(state, next, changed) do
    elapsed = System.monotonic_time(:millisecond) - state.last_active
    left = fn key -> max(State.cell_opt(next, key) - elapsed, 0) end

    {next, 0}
    |> rearm_idle(state, changed, left)
    |> rearm_idle_cache(state, changed, left)
    |> rearm_idle_close(state, changed, left)
    |> rearm_cleanup(state, changed, left)
  end

  defp rearm_idle({next, rearmed}, state, changed, left) do
    if :idle_timeout_ms in changed do
      cancel_timer(state.idle.timer)
      {arm_idle(next, left.(:idle_timeout_ms)), rearmed + 1}
    else
      {next, rearmed}
    end
  end

  defp rearm_idle_cache({next, rearmed}, state, changed, left) do
    cond do
      :idle_cache_after_ms not in changed or state.conn == nil ->
        {next, rearmed}

      state.idle.cache == :shrunk ->
        # Stays shrunk; the shrink off means the pragma above gave the cache back.
        if State.cell_opt(next, :idle_cache_after_ms) == nil,
          do: {%{next | idle: %{next.idle | cache: nil}}, rearmed},
          else: {next, rearmed}

      true ->
        cancel_timer(state.idle.cache)

        arm_or_off(
          %{next | idle: %{next.idle | cache: nil}},
          rearmed,
          :idle_cache_after_ms,
          left,
          &arm_idle_cache/2
        )
    end
  end

  defp rearm_idle_close({next, rearmed}, state, changed, left) do
    if :idle_close_after_ms in changed and state.conn != nil do
      cancel_timer(state.idle.close)

      arm_or_off(
        %{next | idle: %{next.idle | close: nil}},
        rearmed,
        :idle_close_after_ms,
        left,
        &arm_idle_close/2
      )
    else
      {next, rearmed}
    end
  end

  # The timer is armed again for what is left of the option, or stays off when
  # the option is off.
  defp arm_or_off(next, rearmed, key, left, arm) do
    case State.cell_opt(next, key) do
      nil -> {next, rearmed + 1}
      _ -> {arm.(next, left.(key)), rearmed + 1}
    end
  end

  # The wait before a feed cleanup. A timer that waits out a failure keeps
  # its backoff, and `:after_publish` waits for nothing.
  defp rearm_cleanup({next, rearmed}, state, changed, left) do
    if :cleanup_after_ms in changed and is_reference(state.feed.cleanup) and
         state.feed.cleanup_failures == 0 and state.config.feed.cleanup != :after_publish do
      cancel_timer(state.feed.cleanup)

      {arm_cleanup(%{next | feed: %{next.feed | cleanup: nil}}, left.(:cleanup_after_ms)),
       rearmed + 1}
    else
      {next, rearmed}
    end
  end

  defp cancel_timer(ref) when is_reference(ref) do
    _ = :erlang.cancel_timer(ref, async: true, info: false)
    :ok
  end

  defp cancel_timer(_ref), do: :ok

  @doc false
  # A third of the TTL, spread by `lease_renew_jitter` so that cells woken
  # together do not renew together.
  @spec renew_interval(Alambique.Config.t()) :: pos_integer
  def renew_interval(config) do
    base = div(config.ttl_ms, 3)
    spread = trunc(base * config.renew_jitter)
    base + :rand.uniform(2 * spread + 1) - spread - 1
  end

  # The first renewal counts from the put of the lease, not from the end of
  # the wake: a slow wake would otherwise leave room for only one renewal
  # before the lease runs out.
  defp first_renewal_in(%State{config: config, lease: lease} = state) do
    age = config.ttl_ms - (lease.expires_at_ms - State.now_ms(state))
    max(renew_interval(config) - age, 0)
  end

  # Calls

  @impl true
  # The router stamps a call with the time its caller stops waiting. A call
  # taken from the mailbox after that is dropped unrun: nobody can read its
  # answer (`GenServer.call` does not cancel it).
  def handle_call({:deadline, at, request} = call, from, state) do
    if System.monotonic_time(:millisecond) < at do
      if parkable?(state, request),
        do: park(state, call, from),
        else: handle_call(request, from, state)
    else
      :atomics.sub(state.load, Layout.slot(:admitted), 1)
      {:noreply, state}
    end
  end

  # A refresh of the options is no activity and no write: it leaves the idle
  # clock, the cache and the marker alone, and does not wait for a hydration.
  # The answer comes from `info({:options_resolved, ...})`.
  def handle_call(:refresh_options, from, %State{mode: :serving} = state) do
    # The router counted the call (`admit/2`); taken back at once, as the
    # call is answered later without being parked.
    :atomics.sub(state.load, Layout.slot(:admitted), 1)
    refresh_options(state, from)
  end

  def handle_call(request, from, %State{mode: :serving} = state) do
    if parkable?(state, request) do
      park(state, request, from)
    else
      answer =
        state.inflight |> Inflight.busy(fn -> call(request, from, state) end) |> tracked(true)

      # Taken back once the call is parked or answered, so it counts all along.
      unless hibernation?(request), do: :atomics.sub(state.load, Layout.slot(:admitted), 1)
      answer
    end
  end

  # A write that arrives while the file is still being filled waits for the
  # end of the hydration (design §16): until then the pages the transaction
  # reads, and the CRCs of its segment, may come from a file that no checksum
  # has covered. With `verify_reads` so does a read, so that no statement
  # blocks a dirty IO scheduler on a page that is not there yet. The call keeps
  # counting in `load`, so the cell is not idle.
  defp parkable?(%State{mode: :serving, hydrator: hydrator} = state, request)
       when hydrator != nil do
    lazy = state.config.lazy_restore

    (lazy.verify_writes and write_call?(request)) or (lazy.verify_reads and read_call?(request))
  end

  defp parkable?(_state, _request), do: false

  defp write_call?({:transaction, _fun, _ms}), do: true
  defp write_call?({:execute, _sql, _params, _ms}), do: true
  defp write_call?({:isolated, _request}), do: true
  defp write_call?({:migrate, _wake}), do: true
  defp write_call?(_request), do: false

  defp read_call?({:query, _sql, _params, _ms}), do: true
  defp read_call?({:checked_query, _sql, _params, _ms}), do: true
  defp read_call?(_request), do: false

  defp park(state, call, from) do
    state = %{state | parked: [{call, from, System.monotonic_time()} | state.parked]}
    Facts.publish(state)
    {:noreply, state}
  end

  # Runs the parked calls in arrival order once the file is complete.
  defp release_parked(%State{parked: []} = state), do: {:noreply, Publisher.after_ack(state)}

  defp release_parked(state) do
    parked = Enum.reverse(state.parked)
    {_call, _from, oldest} = hd(parked)

    Telemetry.emit(
      [:cell, :writes_released],
      %{count: length(parked), wait_ms: ms(System.monotonic_time() - oldest)},
      %{cell: state.id}
    )

    # What the previous owner left unpublished waited for the checksum too.
    state = Publisher.after_ack(state)

    Enum.reduce_while(parked, {:noreply, %{state | parked: []}}, fn {call, from, _at},
                                                                    {:noreply, state} ->
      case handle_call(call, from, state) do
        {:noreply, state} ->
          {:cont, {:noreply, state}}

        {:reply, reply, state} ->
          GenServer.reply(from, reply)
          {:cont, {:noreply, state}}

        {:stop, reason, reply, state} ->
          GenServer.reply(from, reply)
          {:halt, {:stop, reason, state}}

        {:stop, _reason, _state} = stop ->
          {:halt, stop}
      end
    end)
  end

  defp hibernation?({:hibernate, _, _}), do: true
  defp hibernation?({:hibernate_if_idle, _, _}), do: true
  defp hibernation?({:evict, _, _}), do: true
  defp hibernation?(_), do: false

  # Publishes the facts of the state after a call or a message (`Facts`), and
  # with `measure?` the memory of the connections: a call changes it, most
  # messages do not. The state is the last element of every answer.
  defp tracked(answer, measure?) do
    case elem(answer, tuple_size(answer) - 1) do
      %State{mode: :serving} = state -> Facts.publish(state, measure?)
      _ -> :ok
    end

    answer
  end

  # The messages after which the memory is measured: the idle cache shrink and
  # the end of the hydration.
  defp measures?({:timeout, _ref, :idle_cache}), do: true
  defp measures?({:timeout, _ref, :idle_close}), do: true
  defp measures?({:hydrated, _pid}), do: true
  defp measures?(_message), do: false

  defp call(request, from, state) do
    state = %{state | last_active: System.monotonic_time(:millisecond)}
    :atomics.put(state.load, Layout.slot(:last_used), state.last_active)

    if State.lease_valid?(state) do
      case reopen_for(request, state) do
        {:ok, state} -> serve_call(request, from, restore_cache(state))
        {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
      end
    else
      stop_serving(state, :lease_expired, {:error, :lease_expired})
    end
  end

  # Only a request that runs SQL opens a closed database again.
  defp reopen_for(request, %State{conn: nil} = state) do
    if sql?(request), do: reopen(state), else: {:ok, state}
  end

  defp reopen_for(_request, state), do: {:ok, state}

  defp sql?({:query, _sql, _params, _ms}), do: true
  defp sql?({:checked_query, _sql, _params, _ms}), do: true
  defp sql?({:transaction, _fun, _ms}), do: true
  defp sql?({:execute, _sql, _params, _ms}), do: true
  defp sql?({:isolated, _request}), do: true
  defp sql?({:migrate, _wake}), do: true
  defp sql?(_request), do: false

  # A read that may see unacknowledged writes is answered after the newest
  # batch that carries one is durable, which implies every earlier batch is, so
  # no caller observes a write that could still be lost. A feed cleanup batch
  # carries none: it only removes rows already published, so reads do not wait
  # for it.
  defp serve_call({:query, sql, params, ms}, from, state) do
    reply = Db.read(state.conn, sql, params, Db.limits(state.config, ms))

    case wait(state, {:read, from, reply}) do
      {:ok, state} -> {:noreply, state}
      :none -> {:reply, reply, state}
    end
  end

  # A checked read (design §5) is answered only if the lease is still ours
  # after the read ran. When it is not, the caller gets no answer and the
  # router sends the call to the new owner.
  defp serve_call({:checked_query, sql, params, ms}, from, state) do
    reply = Db.read(state.conn, sql, params, Db.limits(state.config, ms))

    case wait(state, {:checked_read, from, reply}) do
      {:ok, state} ->
        {:noreply, state}

      :none ->
        case check_lease(state) do
          :ok -> {:reply, reply, state}
          :lost -> stop_serving(state, :lease_lost)
          {:unreadable, reason} -> {:reply, {:error, {:lease_unreadable, reason}}, state}
        end
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
  # A closed database has checkpointed: nothing is left in the WAL.
  defp serve_call({:checkpoint, _deadline_ms}, _from, %State{conn: nil} = state),
    do: {:reply, :ok, state}

  defp serve_call({:checkpoint, deadline_ms}, _from, state) do
    case drain_within(state, deadline_ms) do
      {:ok, state} -> requested_checkpoint(state)
      {:busy, state} -> {:reply, {:error, :busy}, state}
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  # `at` is the time the caller stops waiting (`:infinity` for none);
  # `release_ms` replaces `release_deadline_ms` for this release (`nil`: keep it).
  defp serve_call({:hibernate, at, release_ms}, _from, state) do
    case hibernate(state, :request, at, release_ms) do
      {:ok, state, released} -> {:stop, :normal, released, %{state | mode: :stopped}}
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  # Another cell needs the room (`Admission`): a hibernation like the request's,
  # with its own reason so that `compaction: [release_on:]` can tell them apart.
  defp serve_call({:evict, at, release_ms}, _from, state) do
    case hibernate(state, :evict, at, release_ms) do
      {:ok, state, released} -> {:stop, :normal, released, %{state | mode: :stopped}}
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  # A move that must not disturb the cell (`Alambique.move/4`,
  # `only_if_idle: true`). The request itself is not counted, so a count above
  # zero is another call: one waiting in the mailbox or parked on a write. The
  # wake is over: a call that arrived during it is counted.
  defp serve_call({:hibernate_if_idle, at, release_ms}, from, state) do
    case pending_calls(state.load) do
      0 -> serve_call({:hibernate, at, release_ms}, from, state)
      _ -> {:reply, {:error, :busy}, state}
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

  defp write(state, from, fun, ms, isolated? \\ false, retried? \\ false) do
    txid = Replicator.next_txid(state.repl)

    case Db.transaction(state.conn, txid, Publisher.wrap(state, fun),
           select: state.config.feed,
           isolated: isolated?,
           limits: Db.write_limits(state.config, ms)
         ) do
      {:ok, value} ->
        committed(state, from, value, isolated?)

      # The write did not run: something else wrote to the file.
      {:error, :foreign_write} ->
        foreign_write(state, from, {fun, ms, isolated?}, retried?)

      {:error, reason} ->
        write_failed(state, from, reason)
    end
  end

  defp committed(state, from, value, isolated?) do
    state = Publisher.committed(state)
    state = %{state | pending: [{:write, from, {:ok, value}} | state.pending]}
    :atomics.add(state.load, Layout.slot(:callers), 1)
    Fault.point(:cell_after_commit, %{cell: state.id, txid: state.repl.txid})
    if isolated?, do: encode_now(state), else: {:noreply, queue_flush(state)}
  end

  # A function closed the connection: nothing can be read from it, so
  # nothing is handed to the feed.
  defp write_failed(state, _from, {:rollback_failed, :connection_closed} = reason),
    do: stop_serving(%{state | conn: nil}, reason, {:error, reason})

  defp write_failed(state, _from, {:rollback_failed, _} = reason),
    do: stop_serving(state, reason, {:error, reason})

  defp write_failed(state, from, reason) do
    cond do
      Sqlite.wal_advanced?(state.conn) -> orphaned_write(state, from, reason)
      # A commit of no frames whose `query_only` failed.
      Db.fatal?(reason) -> stop_serving(state, reason, {:error, reason})
      true -> {:reply, {:error, reason}, state}
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

      {:error, {:migration, version, inner} = reason, state} when is_integer(version) ->
        if Db.fatal?(inner),
          do: stop_migrating(state, reason),
          else: migrated(state, before, DiskFull.classify(reason))

      {:error, reason, state} ->
        stop_migrating(state, reason)
    end
  end

  defp stop_migrating(state, reason) do
    stop = stop_reason(reason)
    stop_serving(state, stop, {:error, stop})
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
  # The refresh of the options does its own marking: only the pragma runs
  # native code, and the marker must not stay busy while the cell waits.
  def handle_info({:options_resolved, gen, result}, %State{refresh: %{gen: gen}} = state),
    do: state |> options_resolved(result) |> tracked(true)

  def handle_info({:DOWN, mon, :process, _pid, reason}, %State{refresh: %{mon: mon}} = state) do
    failed = Options.failed("failed: " <> Exception.format_exit(reason))
    state |> options_resolved(failed) |> tracked(true)
  end

  def handle_info(message, state) do
    state.inflight |> Inflight.busy(fn -> info(message, state) end) |> tracked(measures?(message))
  end

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
    do: {:noreply, state |> Publisher.published(pid, txid, time, count) |> arm_cleanup()}

  defp info({:feed_read, pid, upto}, %State{mode: :serving} = state),
    do: {:noreply, Publisher.read_done(state, pid, upto)}

  defp info({:feed_part, ref}, %State{mode: :serving} = state),
    do: {:noreply, Publisher.continue(state, ref)}

  defp info({:timeout, ref, :feed_cleanup}, %State{mode: :serving, feed: %{cleanup: ref}} = state) do
    state = %{state | feed: %{state.feed | cleanup: nil}}

    cond do
      not Publisher.cleanup_due?(state) ->
        {:noreply, state}

      (wait = cleanup_quiet_left(state)) > 0 ->
        {:noreply, arm_cleanup(state, wait)}

      not cleanup_ready?(state) ->
        {:noreply, arm_cleanup(state, State.cell_opt(state, :cleanup_after_ms))}

      true ->
        case cleanup(state) do
          {:ok, state} -> {:noreply, state}
          {:stop, reason, state} -> stop_serving(state, reason)
        end
    end
  end

  defp info({:EXIT, pid, _reason}, %State{mode: :serving, feed: %{pid: pid}} = state),
    do: {:noreply, Publisher.sender_down(state, pid)}

  # A frozen cell (design §10) hibernates at its next renewal.
  defp info(:renew, %State{mode: :serving} = state) do
    if frozen?(state) do
      case hibernate(state, :frozen, :infinity) do
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

  # The idle timer carries its ref, so a message that was already queued when
  # the timer was replaced is ignored.
  defp info({:timeout, ref, :idle}, %State{mode: :serving, idle: %{timer: ref}} = state) do
    idle = State.cell_opt(state, :idle_timeout_ms)
    elapsed = System.monotonic_time(:millisecond) - state.last_active

    busy? = not idle?(state) or :atomics.get(state.load, Layout.slot(:admitted)) != 0

    cond do
      elapsed < idle ->
        {:noreply, arm_idle(state, idle - elapsed)}

      busy? ->
        {:noreply, arm_idle(state, idle)}

      true ->
        case hibernate(state, :idle, :infinity) do
          {:ok, state, _released} -> {:stop, :normal, %{state | mode: :stopped}}
          {:stop, reason, state} -> stop_serving(state, reason)
        end
    end
  end

  defp info({:timeout, ref, :idle_cache}, %State{mode: :serving, idle: %{cache: ref}} = state) do
    after_ms = State.cell_opt(state, :idle_cache_after_ms)
    elapsed = System.monotonic_time(:millisecond) - state.last_active

    cond do
      elapsed < after_ms ->
        {:noreply, arm_idle_cache(state, after_ms - elapsed)}

      not idle?(state) or :atomics.get(state.load, Layout.slot(:admitted)) != 0 ->
        {:noreply, arm_idle_cache(state)}

      true ->
        # The published rows go first: the cleanup reads pages that the shrink
        # would drop and leaves garbage that the collection would not see. Rows
        # that remain (a bounded cleanup, a failure) postpone the shrink.
        case cleanup_if_due(state) do
          {:ok, state} -> shrink_or_wait(state)
          {:stop, reason, state} -> stop_serving(state, reason)
        end
    end
  end

  # Closes the connection of a cell that stayed idle (`idle_close_after_ms`).
  # The published feed rows go first, as for the shrink. The cleanup may have
  # sent a batch, so the checks run again after it.
  defp info({:timeout, ref, :idle_close}, %State{mode: :serving, idle: %{close: ref}} = state) do
    state = %{state | idle: %{state.idle | close: nil}}
    after_ms = State.cell_opt(state, :idle_close_after_ms)
    elapsed = System.monotonic_time(:millisecond) - state.last_active

    cond do
      elapsed < after_ms ->
        {:noreply, arm_idle_close(state, after_ms - elapsed)}

      not quiet?(state) ->
        {:noreply, arm_idle_close(state)}

      true ->
        cleanup_then_close(state)
    end
  end

  # The lazy phase of the file (design §16) ended.
  defp info({:hydrated, pid}, %State{hydrator: pid} = state) do
    case Hydration.done(state) do
      {:ok, state} -> release_parked(state)
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

  defp cleanup_then_close(state) do
    case cleanup_if_due(state) do
      {:ok, state} -> if closable?(state), do: close_idle(state), else: rearmed(state)
      {:stop, reason, state} -> stop_serving(state, reason)
    end
  end

  # Feed cleanup (design §13)

  # The published rows are removed in a transaction of the cell's own, sent as
  # a batch through `flush/1` like any commit, so the depth of the pipeline
  # and the order of the batches apply to it.
  defp cleanup_if_due(state) do
    if Publisher.cleanup_due?(state) and cleanup_ready?(state),
      do: cleanup(state),
      else: {:ok, state}
  end

  # Frames no write carries are accounted for first: the cleanup batch must
  # not carry them, and its deletion would take their feed rows away unsent.
  defp cleanup(state) do
    with {:ok, state} <- backstop(state, false), do: clean(state)
  end

  defp clean(state) do
    case Publisher.cleanup(state) do
      {:ok, state} ->
        Fault.point(:feed_after_cleanup, %{cell: state.id, txid: state.repl.txid})

        # Rows left by a bounded cleanup follow at once.
        case flush(%{state | feed: %{state.feed | unsent: true, cleanup_failures: 0}}) do
          {:ok, state} -> {:ok, arm_cleanup(state, 0)}
          {:stop, _reason, _state} = stop -> stop
        end

      {:stop, reason, state} ->
        {:stop, reason, state}

      {:error, state} ->
        %{retry_backoff_max_ms: max} = state.config.upload
        base = State.cell_opt(state, :cleanup_after_ms)
        failures = state.feed.cleanup_failures
        delay = min(base * Integer.pow(2, min(failures, 20)), max(max, base))
        {:ok, arm_cleanup(%{state | feed: %{state.feed | cleanup_failures: failures + 1}}, delay)}
    end
  end

  # What a cleanup needs besides the rows: a cell that answers, with a valid
  # lease and a complete file, and a pipeline that takes a batch. The idle
  # modes also want no call waiting or uploading.
  defp cleanup_ready?(state) do
    state.mode == :serving and State.lease_valid?(state) and not Hydration.hydrating?(state) and
      if state.config.feed.cleanup == :after_publish,
        do: not Pipe.full?(state.pipe),
        else: idle?(state) and :atomics.get(state.load, Layout.slot(:admitted)) == 0
  end

  # Quiet time the idle modes still wait for; `:after_publish` waits for none.
  defp cleanup_quiet_left(%State{config: %{feed: %{cleanup: :after_publish}}}), do: 0

  defp cleanup_quiet_left(state) do
    elapsed = System.monotonic_time(:millisecond) - state.last_active
    max(State.cell_opt(state, :cleanup_after_ms) - elapsed, 0)
  end

  # A published batch has rows to remove. The timer carries its ref, so a
  # message that was already queued when the timer was replaced is ignored.
  defp arm_cleanup(%State{feed: %{cleanup: nil}} = state) do
    delay =
      if state.config.feed.cleanup == :after_publish,
        do: 0,
        else: State.cell_opt(state, :cleanup_after_ms)

    arm_cleanup(state, delay)
  end

  defp arm_cleanup(state), do: state

  defp arm_cleanup(%State{feed: %{cleanup: nil}} = state, ms) do
    if Publisher.cleanup_due?(state),
      do: %{state | feed: %{state.feed | cleanup: :erlang.start_timer(ms, self(), :feed_cleanup)}},
      else: state
  end

  defp arm_cleanup(state, _ms), do: state

  defp rearmed(state), do: {:noreply, arm_idle_close(state)}

  # Nothing in flight, parked or waiting for the feed, so no message or call
  # that is already queued needs the connection: the feed sender, whose
  # exit would read it, has nothing left to publish (`Publisher.delivered?/1`).
  # A cleanup that is due is the close's own first step, so `quiet?/1` leaves
  # it out and `closable?/1` does not.
  defp quiet?(%State{conn: conn} = state) do
    conn != nil and idle?(state) and :atomics.get(state.load, Layout.slot(:admitted)) == 0 and
      state.parked == [] and state.hydrator == nil and not state.feed.unsent and
      Publisher.delivered?(state) and State.lease_valid?(state)
  end

  defp closable?(state), do: quiet?(state) and not Publisher.cleanup_due?(state)

  # Idle close (`idle_close_after_ms`): checkpoint, close the connection and
  # keep the hold on the file. The cell stays the owner: lease, renewals,
  # timers and facts go on, and the next request that needs SQLite opens the
  # database again (`reopen/1`). A checkpoint that cannot finish leaves the
  # connection open.
  defp close_idle(state) do
    started = System.monotonic_time()
    bytes = wal_size(state)
    # The reader never outlives the connection it reads next to.
    state = Publisher.close_reader(state)
    floor = Facts.closed_floor(state)

    case Replicator.checkpoint(state.repl, state.conn) do
      {:ok, repl} -> close_kept(%{state | repl: repl}, started, bytes, floor)
      {:error, reason} -> not_closed(state, started, bytes, close_failure(reason))
    end
  end

  defp close_kept(state, started, bytes, floor) do
    case Sqlite.close_keep(state.conn) do
      {:ok, kept} ->
        state = %{
          state
          | conn: nil,
            kept: kept,
            reopen_floor: floor,
            idle: %{state.idle | cache: nil}
        }

        wal_size(state)
        idle_closed(state, started, bytes, :ok)
        # The page cache and the statements are gone: so is their garbage.
        if State.cell_opt(state, :idle_gc), do: :erlang.garbage_collect()
        {:noreply, state}

      # The file is not the one the connection held: it is closed already.
      {:error, :changed} ->
        idle_closed(state, started, bytes, :changed)
        stop_serving(%{state | conn: nil}, :orphan_frames)

      {:error, reason} ->
        not_closed(state, started, bytes, close_failure(reason))
    end
  end

  defp not_closed(state, started, bytes, result) do
    idle_closed(state, started, bytes, result)
    wal_size(state)
    rearmed(state)
  end

  defp close_failure(:unreplicated_frames), do: :unreplicated
  defp close_failure(:checkpoint_busy), do: :checkpoint_error
  defp close_failure({:checkpoint, _}), do: :checkpoint_error
  defp close_failure({:wal, _}), do: :checkpoint_error
  defp close_failure(_reason), do: :close_error

  defp idle_closed(state, started, bytes, result) do
    Telemetry.span_stop([:cell, :idle_close], started, %{bytes: bytes}, %{
      cell: state.id,
      result: result
    })
  end

  # Opens the database again after an idle close. What a foreign writer did
  # while it was closed is judged before the new connection takes the WAL as
  # its baseline (`closed_verdict/1`). A failed open or a lost file stops the
  # cell like a failed wake.
  defp reopen(%State{conn: nil} = state) do
    started = System.monotonic_time()
    verdict = closed_verdict(state)

    outcome =
      case verdict do
        :lost -> {:stop, :orphan_frames, state}
        _ -> open_again(state, verdict)
      end

    Telemetry.span_stop([:cell, :reopen], started, %{}, %{
      cell: state.id,
      wal_dirty?: verdict == :frames,
      result: if(match?({:ok, _}, outcome), do: :ok, else: :error)
    })

    with {:ok, state} <- outcome do
      Facts.publish(state, true)
      {:ok, state}
    end
  end

  defp open_again(state, verdict) do
    with :ok <- reopen_fault(state),
         {:ok, conn} <- reopened(state) do
      state = %{state | conn: conn, kept: nil, reopen_floor: 0}
      state = state |> arm_idle_cache() |> arm_idle_close()
      if verdict == :frames, do: closed_frames(state), else: {:ok, state}
    else
      {:error, reason} -> {:stop, {:reopen, reason}, state}
    end
  end

  defp reopen_fault(state) do
    Fault.check(:cell_before_reopen, %{cell: state.id})
  end

  defp reopened(state) do
    case Wake.open(state) do
      {:ok, conn} -> {:ok, conn}
      {:error, {:open, reason}, _state} -> {:error, reason}
    end
  end

  # What the files of the closed cell say: `:clean` (as the close left them),
  # `:frames` (committed frames in the WAL that no batch carried) or `:lost`
  # (the database file is not the one that was closed, or a rollback journal
  # exists), which no batch can repair.
  defp closed_verdict(%State{path: path, kept: kept} = state) do
    if journal?(path) or not same_file?(kept, path), do: :lost, else: wal_verdict(state)
  end

  defp journal?(path), do: File.exists?(path <> "-journal")

  defp same_file?(kept, path), do: Sqlite.kept_state(kept, path) == :same

  defp wal_verdict(state) do
    case wal_size(state) do
      0 -> :clean
      _ -> if Replicator.unreplicated?(state.repl), do: :frames, else: :clean
    end
  end

  # Frames were committed in the WAL while the cell was closed: the new
  # connection has taken them as its baseline, so they are judged as any
  # other frames no write carries (`sql: [orphan_frames:]`).
  defp closed_frames(state) do
    log_orphans(state)

    case state.config.sql.orphan_frames do
      :stop -> {:stop, :orphan_frames, state}
      :replicate -> replicate_orphans(state)
    end
  end

  defp shrink_or_wait(state) do
    if Publisher.cleanup_due?(state) do
      {:noreply, arm_idle_cache(state)}
    else
      shrink(state)
    end
  end

  # A pragma that fails leaves the cache as it was: not shrunk, tried again.
  defp shrink(state) do
    with :ok <- pragma_fault(state, :cell_before_shrink_pragma),
         :ok <-
           Sqlite.execute(
             state.conn,
             "PRAGMA cache_size = -#{State.cell_opt(state, :idle_cache_kib)}",
             write: true
           ) do
      collect_idle_garbage(state)
      {:noreply, %{state | idle: %{state.idle | cache: :shrunk}}}
    else
      {:error, _reason} -> {:noreply, arm_idle_cache(state)}
    end
  end

  # The garbage of the last calls would stay for as long as the cell is idle.
  defp collect_idle_garbage(state) do
    if State.cell_opt(state, :idle_gc), do: :erlang.garbage_collect()
    :ok
  end

  # One attempt: the cell cannot serve while it waits, and a retry that ran to
  # the end of the lease would leave no time for the renewal. A failed check
  # counts as not frozen; the next renewal looks again.
  defp frozen?(state), do: Freeze.check(state.repl_ctx, state.id) == {:ok, true}

  # Commit

  defp idle?(state), do: state.pending == [] and state.pipe.queue == []

  # Makes a read wait for the open batch when it has items, else for the newest
  # batch with a user write; its release implies that every earlier write is
  # acknowledged. `:none` when no user write is pending or unacknowledged: the
  # batches without one, the feed cleanups, are not waited for.
  defp wait(%State{pending: []} = state, item) do
    with {:ok, pipe} <- Pipe.attach_to_write(state.pipe, [item]) do
      :atomics.add(state.load, Layout.slot(:callers), 1)
      {:ok, %{state | pipe: pipe}}
    end
  end

  defp wait(state, item) do
    :atomics.add(state.load, Layout.slot(:callers), 1)
    {:ok, %{state | pending: [item | state.pending]}}
  end

  # Encodes the pending commits as the next batch. A full pipeline defers the
  # flush to the next acknowledgement: the commits meanwhile grow the batch.
  defp flush(%State{pending: [], feed: %{unsent: false}} = state), do: {:ok, state}

  defp flush(state) do
    if Pipe.full?(state.pipe),
      do: {:ok, put_in(state.pipe.flush_wanted, true)},
      else: send_batch(state)
  end

  # Like `flush/1`, but waits for a free slot.
  defp flush_now(%State{pending: [], feed: %{unsent: false}} = state), do: {:ok, state}

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

    case if(writes > 0, do: Publisher.try_stamp(state), else: {:ok, state}) do
      {:ok, state} -> upload_batch(state, items, writes)
      {:error, reason} -> {:stop, {:stamp_failed, reason}, state}
    end
  end

  defp upload_batch(state, items, writes) do
    started = System.monotonic_time()
    deadline = State.deadline(state)

    case Replicator.encode(state.repl, state.repl_ctx, :batch,
           deadline: deadline,
           conn: state.conn
         ) do
      :none ->
        release(%{state | pending: [], feed: %{state.feed | unsent: false}}, items)

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
        {:ok, %{state | repl: repl, pending: [], pipe: pipe, feed: %{state.feed | unsent: false}}}

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

  # Items of a flush that changed nothing wait for the newest batch with a
  # user write, if any.
  defp release(state, items) do
    case Pipe.attach_to_write(state.pipe, items) do
      {:ok, pipe} ->
        {:ok, %{state | pipe: pipe}}

      :none ->
        verdict = check_reads(items, state)
        answer(state, items, verdict)
        if verdict == :lost, do: {:stop, :lease_lost, state}, else: {:ok, state}
    end
  end

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
        state = write_sidecar(state, batch.upload)
        pop(%{state | pipe: Pipe.land(state.pipe, ref)}, false)

      batch ->
        failed(state, batch, result)
    end
  end

  # The page-hash sidecar of a landed segment (design §16), in a process of its
  # own so that no commit waits for it. A cell has one sidecar write under way
  # at a time, so a slow store cannot pile them up: a segment that lands
  # meanwhile has none, which a restore answers by reading it whole.
  defp write_sidecar(state, up) do
    ctx = state.repl_ctx

    cond do
      not Sidecar.wanted?(ctx, byte_size(up.segment)) ->
        state

      is_pid(state.sidecar) and Process.alive?(state.sidecar) ->
        Telemetry.emit([:replication, :sidecar], %{key: up.key, result: :busy, reason: nil})
        state

      true ->
        %{state | sidecar: spawn_link(fn -> Replicator.write_sidecar(up, ctx) end)}
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

    :atomics.put(state.load, Layout.slot(:wal_bytes), size)
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
    with {:ok, state} <- flush_now(state),
         {:ok, state} <- settle(state),
         do: backstop(state)
  end

  # Committed frames that no write carries (design §15): no write waits for a
  # batch, yet the WAL holds frames no batch has encoded. The write path keeps
  # this from happening; the check is for what it cannot foresee. It compares
  # the wal-index header with what the cell's own writes left, and looks at
  # the WAL file, so it runs where a cut or a sleep is reported and before every
  # feed cleanup, whether batches are in flight or not: the frames of a batch
  # in flight are encoded already. A write that failed is judged by the
  # position of the WAL at its start instead (`Sqlite.wal_advanced?/1`).
  # `:frames` are in the WAL and a batch can carry them; `:lost` says the WAL
  # restarted or the header cannot be read, so no batch can: the cell stops
  # whatever `sql: [orphan_frames:]` says.
  defp orphan(%State{conn: conn, pending: [], feed: %{unsent: false}} = state) when conn != nil do
    case Sqlite.foreign_write(conn) do
      :none -> if Replicator.unreplicated?(state.repl, conn), do: :frames, else: false
      :advanced -> :frames
      _restarted_or_error -> :lost
    end
  end

  defp orphan(_state), do: false

  defp orphan?(state), do: orphan(state) != false

  # What `sql: [orphan_frames:]` says: `:stop` stops the cell, which discards
  # the local file (the marker is gone while the cell is awake), so the next
  # wake restores what the store holds; `:replicate` sends them as a batch
  # of their own, with the feed rows stamped, and goes on once it is durable.
  # A cleanup does not wait for the batches (`wait?: false`): the orphan batch
  # goes behind those in flight.
  defp backstop(state, wait? \\ true) do
    case orphan(state) do
      false ->
        {:ok, state}

      found ->
        log_orphans(state)

        case {found, state.config.sql.orphan_frames} do
          {:lost, _} -> {:stop, :orphan_frames, state}
          {:frames, :stop} -> {:stop, :orphan_frames, state}
          {:frames, :replicate} when wait? -> replicate_orphans(state)
          {:frames, :replicate} -> orphan_batch(state)
        end
    end
  end

  defp log_orphans(state),
    do: Logger.error("Alambique cell #{state.id} had committed frames no write carries")

  defp replicated(state),
    do: if(orphan?(state), do: {:stop, :orphan_frames, state}, else: {:ok, state})

  defp replicate_orphans(state) do
    with {:ok, state} <- orphan_batch(state),
         {:ok, state} <- settle(state) do
      replicated(state)
    end
  end

  # The frames as a batch of their own, with the feed rows stamped. The header
  # the check saw is accounted for first: the stamp is a commit of the cell's
  # own, and the batch encodes the WAL after the check.
  defp orphan_batch(state) do
    with :ok <- Sqlite.accept_foreign_write(state.conn),
         {:ok, state} <- Publisher.try_stamp(state) do
      flush_now(%{state | feed: %{state.feed | unsent: true}})
    else
      {:error, reason} -> {:stop, {:stamp_failed, reason}, state}
    end
  end

  # The connection found a header its own writes do not account for before the
  # write began (`Db.transaction/4`) and did not run it. `:stop` stops the
  # cell and the caller gets `{:error, :orphan_frames}`. `:replicate` makes
  # the frames a batch, when they are in the WAL, and runs the write again;
  # a WAL that restarted, or a second refusal, stops the cell.
  defp foreign_write(state, from, {fun, ms, isolated?}, retried?) do
    log_orphans(state)

    found = Sqlite.foreign_write(state.conn)

    if found == :advanced and state.config.sql.orphan_frames == :replicate and not retried? do
      replicate_and_retry(state, from, {fun, ms, isolated?})
    else
      stop_serving(state, :orphan_frames, {:error, :orphan_frames})
    end
  end

  defp replicate_and_retry(state, from, {fun, ms, isolated?}) do
    with {:ok, state} <- orphan_batch(state),
         {:ok, state} <- settle(state) do
      write(state, from, fun, ms, isolated?, true)
    else
      {:stop, reason, state} -> stop_serving(state, reason, {:error, reason})
    end
  end

  # A write failed and left committed frames (the WAL moved during its
  # transaction). `:stop` answers the caller with its error and stops: writes
  # not sent get the stop reason and those in flight `:outcome_unknown`.
  # `:replicate` parks the caller as a write: the stamp, the reads' wait and
  # the batch apply, and it is answered after the batch with
  # `{:error, {:committed, reason}}`, because its rows are durable then. A
  # failed `query_only` stops the cell after that batch.
  defp orphaned_write(state, from, reason) do
    log_orphans(state)

    case state.config.sql.orphan_frames do
      :stop ->
        stop_serving(state, :orphan_frames, {:error, reason})

      :replicate ->
        item = {:write, from, {:error, {:committed, reason}}}
        :atomics.add(state.load, Layout.slot(:callers), 1)
        state = %{state | pending: [item | state.pending]}

        if Db.fatal?(reason),
          do: replicate_and_stop(state, reason),
          else: {:noreply, queue_flush(state)}
    end
  end

  # The connection is in an unknown mode after a commit: what it committed is
  # made durable and the callers are answered, then the cell stops.
  defp replicate_and_stop(state, reason) do
    case with({:ok, state} <- orphan_batch(state), do: settle(state)) do
      {:ok, state} -> stop_serving(state, reason)
      {:stop, reason, state} -> stop_serving(state, reason)
    end
  end

  # Like `drain/1`, but gives up at the deadline and leaves the queue as it
  # is: the uploads go on and their callers are answered as usual.
  defp drain_within(state, deadline_ms) do
    limit = System.monotonic_time(:millisecond) + deadline_ms
    with {:ok, state} <- flush_now(state), do: settle_within(state, limit)
  end

  # The orphan batch of `:replicate` is awaited within the deadline too.
  defp settle_within(state, limit, replicated? \\ false)

  defp settle_within(%State{pipe: %{queue: []}} = state, limit, replicated?) do
    case orphan(state) do
      false ->
        {:ok, state}

      found when found == :lost or replicated? or state.config.sql.orphan_frames == :stop ->
        {:stop, :orphan_frames, state}

      :frames ->
        log_orphans(state)
        with {:ok, state} <- orphan_batch(state), do: settle_within(state, limit, true)
    end
  end

  defp settle_within(state, limit, replicated?) do
    case await(state, max(limit - System.monotonic_time(:millisecond), 0)) do
      {:ok, state} -> settle_within(state, limit, replicated?)
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
    :atomics.sub(state.load, Layout.slot(:callers), length(pending))

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
  defp hibernate(state, why, deadline, release_ms \\ nil) do
    started = System.monotonic_time()

    with {:ok, state} <- drain(state),
         {:ok, state} <- reopen_if_dirty(state),
         state = Publisher.flush(state),
         # Never answer a release with `:ok` while frames are unreplicated,
         # whatever the option says.
         {:ok, state} <- replicated(state) do
      state = finish_local(state)
      {compacted, tail, spent} = compact_tail(state, why, deadline, release_ms)
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

  # A closed database whose files are as the close left them has nothing to
  # hibernate; any other state is judged with the connection open.
  defp reopen_if_dirty(%State{conn: nil} = state) do
    if closed_verdict(state) == :clean, do: {:ok, state}, else: reopen(state)
  end

  defp reopen_if_dirty(state), do: {:ok, state}

  # A release compacts a long level-0 tail before it gives the lease away, so
  # the next node wakes on a short chain (design §9), when its reason is in
  # `compaction: [release_on:]`: `:request` (`Alambique.hibernate/3`, a drain,
  # a move), `:shutdown` (the stop of the instance), `:evict` (room for another
  # cell), `:idle` and `:frozen`. It stops at `release_deadline_ms`, or earlier at
  # the end of the lease, and `release_reserve_ms` before the time the caller
  # stops waiting (`deadline`, monotonic; the stop of the instance has its
  # shutdown time), and hands over with what it merged. Returns the outcome
  # (`:none`, `:busy`, `:done`, `:deadline`, `:deadline_error` or `:error`), the level-0
  # files still in the chain (0 when it did not look) and the time spent.
  defp compact_tail(%State{config: %{compaction: c}} = state, why, deadline, release_ms) do
    budget =
      if c != nil and why in c.release_on,
        do:
          [
            release_ms || c.release_deadline_ms,
            State.lease_left_ms(state),
            call_left(deadline, c)
          ]
          |> Enum.min()
          |> max(0),
        else: 0

    if budget == 0, do: {:none, 0, 0}, else: compact_within(state, c, budget)
  end

  # Waits for one of the `release_concurrency` places of the node inside the
  # budget, and compacts with what is left of it. A cell that finds none by
  # then releases without looking at the chain (`:busy`).
  defp compact_within(%State{config: config} = state, c, budget) do
    started = System.monotonic_time()
    deadline = Retry.deadline_in(budget)

    {outcome, tail} =
      case Slots.acquire(config, c.release_concurrency, deadline) do
        :busy ->
          {:busy, 0}

        {:ok, place} ->
          try do
            compact_left(state, c, deadline)
          after
            Slots.release(config, place)
          end
      end

    {outcome, tail, System.monotonic_time() - started}
  end

  defp compact_left(state, c, deadline) do
    case deadline - System.monotonic_time(:millisecond) do
      left when left > 0 ->
        result = Compactor.compact(state.config, state.repl_ctx, state.id, left)
        release_compaction(result, deadline, state.id, c.release_min_tail)

      _ ->
        {:busy, 0}
    end
  end

  defp call_left(:infinity, _c), do: :infinity

  defp call_left(deadline, c),
    do: deadline - System.monotonic_time(:millisecond) - c.release_reserve_ms

  # A pass that found a short tail did nothing; one that merged and left a
  # short tail is `:done`.
  defp release_compaction({:ok, %{merges: 0, tail: tail}}, _deadline, _cell, min)
       when tail < min,
       do: {:none, tail}

  defp release_compaction({:ok, %{more: false, tail: tail}}, _deadline, _cell, _min),
    do: {:done, tail}

  defp release_compaction({:ok, %{more: true, tail: tail}}, _deadline, _cell, _min),
    do: {:deadline, tail}

  defp release_compaction({:error, reason}, deadline, cell, _min) do
    if System.monotonic_time(:millisecond) >= deadline do
      # The budget ended inside a request before the pass knew the chain.
      {:deadline_error, 0}
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
  defp finish_local(%State{conn: nil, kept: kept, pending: [], pipe: %{queue: []}} = state)
       when kept != nil do
    %{repl: repl, path: path} = state

    # The idle close checkpointed; closing left the emptied WAL and its index.
    Enum.each(["-wal", "-shm"], &File.rm(path <> &1))
    mark_local(path, repl)
    Sqlite.release(kept)
    %{state | kept: nil}
  end

  defp finish_local(%State{pending: [], pipe: %{queue: []}} = state) do
    # A reader would keep the checkpoint from finishing and the file open.
    state = Publisher.close_reader(state)
    checkpointed = Replicator.checkpoint(state.repl, state.conn)
    _ = Db.close(state.conn)
    state = %{state | conn: nil}

    # A file that is still being filled is not kept (design §16).
    if Hydration.hydrating?(state) do
      Hydration.discard(state)
    else
      case checkpointed do
        {:ok, repl} ->
          # Closing leaves the emptied WAL and its index behind.
          Enum.each(["-wal", "-shm"], &File.rm(state.path <> &1))
          mark_local(state.path, repl)

        _ ->
          :ok
      end

      state
    end
  end

  # The marker only lets the next wake reuse the file: without it the file is
  # restored again.
  defp mark_local(path, repl) do
    _ = Local.mark(path, repl.txid, repl.checksum, repl.page_size)
    :ok
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
    Enum.each(state.parked, fn {_call, from, _at} -> GenServer.reply(from, error) end)

    for {kind, from, _} <- Pipe.items(state.pipe) do
      GenServer.reply(from, if(kind == :write, do: {:error, :outcome_unknown}, else: error))
    end

    Pipe.kill(state.pipe)
    mark_stopped(state)

    Telemetry.emit([:cell, :stop], %{}, %{
      cell: state.id,
      epoch: state.lease && state.lease.epoch,
      reason: reason
    })

    state = %{state | pending: [], parked: [], pipe: %{state.pipe | queue: []}}
    # A fenced or lost lease belongs to someone else; any other lease is ours to give back.
    # An expired one no longer lets us publish. A file that failed to hydrate cannot be read.
    state =
      if reason not in [:fenced, :lease_lost, :lease_expired] and
           not match?({:hydration_failed, _}, reason),
         do: Publisher.flush(state),
         else: state

    # The cell stops whatever the release answers, and it logs its failure.
    _ = if reason in [:fenced, :lease_lost], do: close(state), else: abandon(state)
    state = %{state | mode: :stopped, conn: nil, kept: nil, feed: %{state.feed | reader: nil}}

    if reply,
      do: {:stop, {:shutdown, reason}, reply, state},
      else: {:stop, {:shutdown, reason}, state}
  end

  defp abandon(%State{lease: nil} = state), do: close(state)

  defp abandon(state) do
    _ = close(state)
    release(state)
  end

  # The reader first: it never outlives the connection it reads next to.
  defp close(%State{conn: conn, kept: kept} = state) do
    _ = Publisher.close_reader(state)
    if kept, do: Sqlite.release(kept)
    if conn, do: Db.close(conn), else: :ok
  end

  @impl true
  def terminate(reason, state) do
    # A sidecar write does not outlive the cell: a link does not end it on a
    # normal stop.
    if is_pid(state.sidecar), do: Process.exit(state.sidecar, :kill)
    mark_stopped(state)
    result = Inflight.busy(state.inflight, fn -> finish(reason, state) end)
    # After the last acknowledgement of a clean stop.
    Hold.drop_cell(state.config, state.id)
    Admission.forget(state.config, self())
    result
  end

  defp finish(:shutdown, %State{mode: :serving} = state) do
    case hibernate(state, :shutdown, System.monotonic_time(:millisecond) + @shutdown_ms) do
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
