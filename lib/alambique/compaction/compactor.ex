defmodule Alambique.Compaction.Compactor do
  @moduledoc false
  # The background compactor of one instance (design §9). One process, one
  # timer, bounded concurrency; no process or timer per cell.
  #
  # Cells tell it about commits through `notify/3`, which writes one row per
  # cell into a bounded public ETS table: `{cell, first_txid, last_txid,
  # due_ms}`. A row is ready when `fanout` TXIDs were committed since it
  # appeared or when `max_age_ms` passed. A tick takes the ready rows with
  # `max_chain_segments` TXIDs or more first, then those that waited longest,
  # deletes them and runs one pass for each, `concurrency` at a time. A cell
  # nobody writes has no row and costs nothing. Notifications for new cells
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

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @doc "Records that `cell` committed `txid`. Cheap: one ETS update, no message."
  @spec notify(Alambique.Config.t(), String.t(), pos_integer) :: :ok
  def notify(%{compaction: nil}, _cell, _txid), do: :ok

  def notify(%{compactor: table, compaction: c}, cell, txid) do
    unless :ets.update_element(table, cell, {3, txid}) do
      if :ets.info(table, :size) < c.queue_limit do
        :ets.insert_new(table, {cell, txid, txid, now_ms() + c.max_age_ms})
      end
    end

    :ok
  rescue
    # The compactor is not running (yet): nothing to tell.
    ArgumentError -> :ok
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

    state = %{config: config, timer: nil, scan_at: now_ms(), scan: nil}
    {:ok, schedule(config, state)}
  end

  @impl true
  def handle_info(:tick, state) do
    {:noreply, tick(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp schedule(config, state, delay \\ nil) do
    timer = Process.send_after(self(), :tick, delay || config.compaction.interval_ms)
    %{state | timer: timer}
  end

  defp tick(%{config: config} = state) do
    c = config.compaction
    started = System.monotonic_time()
    batch = c.concurrency * c.batch_factor
    rows = ready(config.compactor, c, now_ms())

    rows
    |> Task.async_stream(&pass(config, &1),
      max_concurrency: c.concurrency,
      timeout: c.timeout_ms + 5_000,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    )
    |> Enum.each(&settle(config, &1))

    cleanups(config, batch)
    state = scan(state)

    if rows != [] do
      Telemetry.emit(
        [:compactor, :tick],
        %{duration: System.monotonic_time() - started, cells: length(rows)},
        %{
          instance: config.name
        }
      )
    end

    # A full batch means more rows may be waiting.
    schedule(config, state, if(length(rows) == batch, do: 0))
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
  # The rows a tick takes: at most `concurrency * batch_factor`, chains over
  # `max_chain_segments` first, then the rows that waited longest (smallest
  # `due`). Every ready row is read and sorted, so a tick costs O(ready log
  # ready), bounded by `queue_limit`; hash order would let the same cells
  # take every batch while others never get one.
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
      {if(first > 0 and last - first >= c.max_chain_segments - 1, do: 0, else: 1), due, cell}
    end)
    |> Enum.take(c.concurrency * c.batch_factor)
  end

  # One pass for one row. The row is deleted first, but only if nothing was
  # committed meanwhile (the last TXID is unchanged), so that a commit during
  # the pass leaves a row behind.
  defp pass(config, {cell, first, last, due} = row) do
    :ets.select_delete(config.compactor, [
      {{cell, first, last, :_}, [], [true]}
    ])

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

  defp settle(config, {:ok, {{cell, _, _, _}, result}}), do: after_pass(config, cell, result)

  defp settle(config, {:exit, {{cell, _, _, _}, reason}}),
    do: after_pass(config, cell, {:error, {:exit, reason}})

  defp after_pass(config, cell, {:ok, %{more: true}}), do: requeue(config, cell, 0)
  defp after_pass(_config, _cell, {:ok, _}), do: :ok

  defp after_pass(config, cell, {:error, reason}) do
    Logger.warning("Alambique compaction of #{cell} failed: #{inspect(reason)}")
    requeue(config, cell, config.compaction.retry_ms)
  end

  defp requeue(config, cell, delay) do
    :ets.insert_new(config.compactor, {cell, 0, 0, now_ms() + delay})
    :ok
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
