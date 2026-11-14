defmodule Alambique.Compaction.Compactor do
  @moduledoc false
  # The background compactor of one instance (design §9). One process, one
  # timer, bounded concurrency; no process or timer per cell.
  #
  # Cells tell it about commits through `notify/3`, which writes one row per
  # cell into a bounded public ETS table: `{cell, first_txid, last_txid,
  # due_ms}`. A row is ready when `fanout` TXIDs were committed since it
  # appeared or when `max_age_ms` passed. A pass takes a ready row: the rows
  # with `max_chain_segments` TXIDs or more first, then those that waited
  # longest. At most `concurrency` passes run at once, and
  # `concurrency + urgent_concurrency` while the row that starts one is at the
  # cap. Passes are not rounds: a pass that ends starts the next one at once,
  # and a row that reaches the cap starts its pass at once if a slot is free,
  # whatever the passes running take. A cell nobody writes has no row and
  # costs nothing. Notifications for new cells
  # are dropped while the table is full; the next commit makes a new one.
  #
  # The cleanups of moved cells (design §18) are jobs in a second table. The
  # store holds the truth: every `moved_scan_ms` the compactor lists the
  # pending-cleanup markers of each location, one page of `moved_scan_page`
  # per tick, and queues what it finds, so a restart forgets nothing.

  use GenServer

  require Logger

  alias Alambique.{Compaction, Config, Inflight, Placement, Retention, Telemetry}
  alias Alambique.Compaction.Hold
  alias Alambique.Relocate.Cleanup

  # Cleanup jobs a tick takes, per unit of `concurrency`.
  @cleanup_batch 4

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @doc "Records that `cell` committed `txid`. Cheap: one ETS update, no message."
  @spec notify(Alambique.Config.t(), String.t(), pos_integer) :: :ok
  def notify(%{compaction: nil}, _cell, _txid), do: :ok

  def notify(%{compactor: table, compaction: c}, cell, txid) do
    if :ets.update_element(table, cell, {3, txid}) do
      if rem(txid, c.fanout) == 0, do: check_cap(table, c, cell, txid)
    else
      if :ets.info(table, :size) < c.queue_limit do
        :ets.insert_new(table, {cell, txid, txid, now_ms() + c.max_age_ms})
      end
    end

    :ok
  rescue
    # The compactor is not running (yet): nothing to tell.
    ArgumentError -> :ok
  end

  # A row that reached the cap wakes the compactor, so its pass does not wait
  # for the next tick. Checked every `fanout` TXIDs, to keep the commit path
  # to one more read.
  defp check_cap(table, c, cell, txid) do
    first = :ets.lookup_element(table, cell, 2, 0)

    if first > 0 and txid - first >= c.max_chain_segments - 1,
      do: send(:ets.info(table, :owner), :urgent)
  end

  @doc """
  One pass over `cell` for a cell that is about to hand its lease over: it
  does nothing when the chain holds fewer than `min_tail` level-0 files, and
  otherwise merges until the chain is compacted or `timeout_ms` passes
  (`more: true` then). `ctx` is the replication context of the cell's location.
  """
  @spec compact(Alambique.Config.t(), Alambique.Replication.ctx(), String.t(), pos_integer) ::
          {:ok, Compaction.summary()} | {:error, term}
  def compact(config, ctx, cell, timeout_ms) do
    # The deadline bounds the merges, not their number.
    opts =
      Keyword.merge(compaction_opts(config.compaction, config),
        timeout_ms: timeout_ms,
        max_merges: 1_000_000,
        min_tail: config.compaction.release_min_tail
      )

    Compaction.run(%{ctx | timeout_ms: timeout_ms}, cell, opts)
  end

  @doc """
  Asks for the cleanup of what `cell` left at the location `location_id` after a
  move (design §18), in `delay_ms`. A cleanup that finds the grace period still
  running schedules itself again. The jobs live in memory; the periodic scan
  of the markers in the store brings them back after a restart.
  """
  @spec cleanup(Alambique.Config.t(), String.t(), String.t(), non_neg_integer) :: :ok
  def cleanup(%{compaction: nil}, _cell, _location_id, _delay_ms), do: :ok

  def cleanup(%{cleanups: table}, cell, location_id, delay_ms) do
    :ets.insert(table, {{cell, location_id}, now_ms() + delay_ms})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Process

  @impl true
  def init(config) do
    # A crashing pass must not take the compactor down.
    Process.flag(:trap_exit, true)

    :ets.new(config.compactor, [
      :named_table,
      :public,
      :set,
      write_concurrency: true,
      read_concurrency: false
    ])

    :ets.new(config.cleanups, [:named_table, :public, :set])
    Hold.create(config)

    state = %{
      config: config,
      timer: nil,
      scan_at: now_ms(),
      scan: nil,
      running: %{},
      failed: MapSet.new(),
      busy: nil
    }

    {:ok, schedule(config, state)}
  end

  @impl true
  def handle_info(:tick, %{config: config} = state) do
    state = dispatch(%{state | failed: MapSet.new()})
    cleanups(config, config.compaction.concurrency * @cleanup_batch)
    {:noreply, state |> scan() |> then(&schedule(config, &1))}
  end

  # A row reached the cap: start its pass now if a slot is free, without
  # moving the timer.
  def handle_info(:urgent, state) do
    flush_urgent()
    {:noreply, dispatch(state)}
  end

  def handle_info({:pass_result, pid, {row, result}}, state),
    do: {:noreply, finish(state, pid, row, result)}

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    case state.running do
      %{^pid => %{row: row}} -> {:noreply, finish(state, pid, row, {:error, {:exit, reason}})}
      _ -> {:noreply, state}
    end
  end

  def handle_info({:pass_timeout, pid}, state) do
    Process.exit(pid, :kill)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp flush_urgent do
    receive do
      :urgent -> flush_urgent()
    after
      0 -> :ok
    end
  end

  defp schedule(config, state) do
    timer = Process.send_after(self(), :tick, config.compaction.interval_ms)
    %{state | timer: timer}
  end

  # Starts the passes the slots allow, for the ready rows of cells with no pass.
  # A cell whose pass failed waits for the next tick, so that a failing store
  # is not asked again at once.
  defp dispatch(%{config: config, running: running} = state) do
    c = config.compaction

    if map_size(running) >= c.concurrency + c.urgent_concurrency do
      state
    else
      busy = for {_pid, %{row: {cell, _, _, _}}} <- running, into: state.failed, do: cell
      rows = pick(ready(config.compactor, c, now_ms()), busy, map_size(running), c)
      Enum.reduce(rows, state, &start(&2, &1))
    end
  end

  # One pass for one row. The row is deleted first, but only if nothing was
  # committed meanwhile (the last TXID is unchanged), so that a commit during
  # the pass leaves a row behind.
  defp start(%{config: config, running: running} = state, {cell, first, last, _due} = row) do
    :ets.select_delete(config.compactor, [{{cell, first, last, :_}, [], [true]}])
    parent = self()

    {pid, ref} =
      Process.spawn(fn -> send(parent, {:pass_result, self(), pass(config, row)}) end, [
        :link,
        :monitor
      ])

    timer = Process.send_after(self(), {:pass_timeout, pid}, config.compaction.timeout_ms + 5_000)
    period = state.busy || %{started: System.monotonic_time(), cells: 0}

    %{
      state
      | running: Map.put(running, pid, %{row: row, ref: ref, timer: timer}),
        busy: %{period | cells: period.cells + 1}
    }
  end

  defp finish(%{config: config} = state, pid, row, result) do
    {%{ref: ref, timer: timer}, running} = Map.pop!(state.running, pid)
    Process.cancel_timer(timer)
    Process.demonitor(ref, [:flush])
    after_pass(config, row, result)
    state = %{state | running: running, failed: failed(state.failed, row, result)}
    state = dispatch(state)
    if state.running == %{}, do: close_period(state), else: state
  end

  defp failed(failed, {cell, _, _, _}, {:error, _}), do: MapSet.put(failed, cell)
  defp failed(failed, _row, _result), do: failed

  # What the passes did from one idle compactor to the next.
  defp close_period(%{config: config, busy: busy} = state) do
    Telemetry.emit(
      [:compactor, :tick],
      %{duration: System.monotonic_time() - busy.started, cells: busy.cells},
      %{instance: config.name}
    )

    %{state | busy: nil}
  end

  # One page of markers per tick; a scan starts when `scan_at` passed.
  defp scan(%{scan: nil, scan_at: at, config: config} = state) do
    now = now_ms()

    with true <- now >= at,
         {:ok, locations} <- Placement.locations(config) do
      scan(%{state | scan: {locations, nil}, scan_at: now + config.compaction.moved_scan_ms})
    else
      false ->
        state

      {:error, reason} ->
        Logger.warning("Alambique cleanup scan could not list locations: #{inspect(reason)}")
        %{state | scan_at: now + config.compaction.moved_scan_ms}
    end
  end

  defp scan(%{scan: {[], _}} = state), do: %{state | scan: nil}

  defp scan(%{scan: {[location | rest] = locations, cursor}, config: config} = state) do
    case Cleanup.pending(config, location, cursor, config.compaction.moved_scan_page) do
      {:ok, cells, next} ->
        for cell <- cells, do: :ets.insert_new(config.cleanups, {{cell, location.id}, now_ms()})
        %{state | scan: if(next, do: {locations, next}, else: {rest, nil})}

      {:error, reason} ->
        Logger.warning("Alambique cleanup scan could not list #{location.id}: #{inspect(reason)}")

        %{state | scan: {rest, nil}}
    end
  end

  defp cleanups(config, limit) do
    spec = [{{:"$1", :"$2"}, [{:"=<", :"$2", now_ms()}], [:"$1"]}]

    jobs =
      case :ets.select(config.cleanups, spec, limit) do
        {jobs, _continuation} -> jobs
        :"$end_of_table" -> []
      end

    for job <- jobs, do: :ets.delete(config.cleanups, job)

    jobs
    |> Task.async_stream(&cleanup_job(config, &1),
      max_concurrency: config.compaction.concurrency,
      timeout: Cleanup.budget(config) + 5_000,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    )
    |> Enum.each(&settle_cleanup(config, &1))
  end

  defp cleanup_job(config, {cell, location_id}) do
    {{cell, location_id}, Cleanup.run(config, cell, location_id)}
  rescue
    exception -> {{cell, location_id}, {:error, {:crash, Exception.message(exception)}}}
  catch
    kind, reason -> {{cell, location_id}, {:error, {:crash, {kind, reason}}}}
  end

  defp settle_cleanup(config, {:ok, {{cell, location_id}, result}}),
    do: after_cleanup(config, cell, location_id, result)

  defp settle_cleanup(config, {:exit, {{cell, location_id}, reason}}),
    do: after_cleanup(config, cell, location_id, {:error, {:exit, reason}})

  defp after_cleanup(config, cell, location_id, {:ok, %{result: :waiting, ready_at_ms: at}}) do
    {clock, clock_config} = config.clock
    cleanup(config, cell, location_id, max(at - clock.now_ms(clock_config), 0) + 1)
  end

  defp after_cleanup(_config, _cell, _location_id, {:ok, _}), do: :ok

  # The cell is live there or never moved: the marker is a stray once the
  # deadline of a move has passed, and goes then; otherwise the next scan
  # looks again.
  defp after_cleanup(config, cell, location_id, {:error, reason})
       when reason in [:live_location, :not_moved] do
    _ = Cleanup.drop_stray(config, cell, location_id)
    :ok
  end

  # Another cleanup runs: a marker still in the store is found by the next scan.
  defp after_cleanup(_config, _cell, _location_id, {:error, :contended}), do: :ok

  defp after_cleanup(config, cell, location_id, {:error, reason}) do
    Logger.warning("Alambique cleanup of #{cell} at #{location_id} failed: #{inspect(reason)}")
    cleanup(config, cell, location_id, config.compaction.retry_ms)
  end

  @doc false
  # The rows to start now, from `rows` in the order of `ready/3`: the cells in
  # `busy` have a pass and are skipped, and `running` passes already use slots.
  # A row at the cap may use `concurrency + urgent_concurrency` slots, the
  # others `concurrency`; as the rows at the cap come first, the first row
  # that finds no slot ends the choice.
  @spec pick([tuple], MapSet.t(), non_neg_integer, map) :: [tuple]
  def pick(rows, busy, running, c) do
    rows
    |> Enum.reject(fn {cell, _, _, _} -> MapSet.member?(busy, cell) end)
    |> Enum.reduce_while({[], running}, fn row, {picked, used} ->
      limit = c.concurrency + if(over_cap?(row, c), do: c.urgent_concurrency, else: 0)
      if used < limit, do: {:cont, {[row | picked], used + 1}}, else: {:halt, {picked, used}}
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp over_cap?({_cell, first, last, _due}, c),
    do: first > 0 and last - first >= c.max_chain_segments - 1

  @doc false
  # The ready rows, in the order passes take them: chains over
  # `max_chain_segments` first, then the rows that waited longest (smallest
  # `due`). Every ready row is read and sorted, so a tick costs O(ready log
  # ready), bounded by `queue_limit`; hash order would let the same cells
  # take every slot while others never get one.
  @spec ready(atom, map, integer) :: [tuple]
  def ready(table, c, now) do
    spec = [
      {{:"$1", :"$2", :"$3", :"$4"},
       [{:orelse, {:>=, {:-, :"$3", :"$2"}, c.fanout - 1}, {:"=<", :"$4", now}}],
       [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ]

    table
    |> :ets.select(spec)
    |> Enum.sort_by(fn {cell, first, last, due} ->
      {if(over_cap?({cell, first, last, due}, c), do: 0, else: 1), due, cell}
    end)
  end

  # The work of one pass, in a process of its own.
  defp pass(config, {cell, _first, _last, due} = row) do
    aged? = due <= now_ms()
    c = config.compaction

    try do
      Inflight.run(config, cell, :compaction, fn ->
        with {:ok, location} <- Placement.locate(config, cell),
             ctx = %{Config.repl_ctx(config, location) | timeout_ms: c.timeout_ms},
             {:ok, summary} <- Compaction.run(ctx, cell, compaction_opts(c, config)),
             {:ok, _} <- maybe_retain(config, ctx, cell, summary, aged?) do
          {:ok, summary}
        end
      end)
      |> then(&{row, &1})
    rescue
      exception -> {row, {:error, {:crash, Exception.message(exception)}}}
    catch
      kind, reason -> {row, {:error, {:crash, {kind, reason}}}}
    end
  end

  defp compaction_opts(c, config) do
    hold = if Hold.enabled?(config), do: config
    trash = config.trash
    window = config.retention && config.retention[:pitr_window_ms]

    (Map.take(c, Keyword.keys(Compaction.defaults())) |> Map.to_list()) ++
      [
        hold: hold,
        pitr_window_ms: window,
        deletion_grace_ms: config.deletion_grace_ms,
        batch_keys: trash.batch_keys,
        concurrency: trash.concurrency
      ]
  end

  # Retention follows a new snapshot (what it covers can go) and a row that
  # waited for `max_age_ms`.
  defp maybe_retain(%{retention: nil}, _ctx, _cell, _summary, _aged?), do: {:ok, nil}

  defp maybe_retain(config, ctx, cell, summary, aged?) do
    if summary.snapshot != nil or aged? do
      Retention.run(ctx, cell, retention_opts(config))
    else
      {:ok, nil}
    end
  end

  defp retention_opts(config) do
    trash = config.trash

    config.retention ++
      [
        deletion_grace_ms: config.deletion_grace_ms,
        batch_keys: trash.batch_keys,
        concurrency: trash.concurrency
      ]
  end

  defp after_pass(config, row, {:ok, %{more: true}}), do: requeue(config, row, 0)
  defp after_pass(_config, _row, {:ok, _}), do: :ok

  defp after_pass(config, {cell, _, _, _} = row, {:error, reason}) do
    Logger.warning("Alambique compaction of #{cell} failed: #{inspect(reason)}")
    requeue(config, row, config.compaction.retry_ms)
  end

  # The row keeps the TXIDs it counted, which are still uncompacted, so the
  # cap sees the chain; a commit made meanwhile made a row of its own, which
  # takes the older start.
  defp requeue(config, {cell, first, last, _due}, delay) do
    due = now_ms() + delay

    unless :ets.insert_new(config.compactor, {cell, first, last, due}) do
      case :ets.lookup(config.compactor, cell) do
        [{_, f, _, d}] ->
          start = for(n <- [f, first], n > 0, do: n) |> Enum.min(fn -> 0 end)
          :ets.update_element(config.compactor, cell, [{2, start}, {4, min(d, due)}])

        [] ->
          :ok
      end
    end

    :ok
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
