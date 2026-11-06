defmodule Alambique.Schema.Sweeper do
  @moduledoc false
  # Migrates idle cells in the background (design §17). One process per
  # instance and one timer: it lists the schema index of every location
  # below the code's version, page by page, and wakes and hibernates the
  # cells it finds through the public API, so leases, fencing and the change
  # feed apply as for any call. A cell nobody lists costs nothing.
  #
  # With migrations chosen per cell there is no single version: it lists every
  # version and asks the function for each listed cell, waking only those
  # below their own.

  use GenServer

  require Logger

  alias Alambique.{Config, Placement, Schema, Telemetry}
  alias Alambique.Replication.Retry

  @type summary :: %{
          cells: non_neg_integer,
          migrated: non_neg_integer,
          stale: non_neg_integer,
          skipped: non_neg_integer,
          errors: non_neg_integer
        }

  @empty %{cells: 0, migrated: 0, stale: 0, skipped: 0, errors: 0}

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  # One pass

  @doc """
  Sweeps every location once. Options override the instance's `:sweeper`
  settings: `:concurrency`, `:rate` (cells per second), `:page_size` and
  `:timeout_ms` (of one cell's wake). `{:error, {:listing, failures, summary}}`
  means some location could not be listed; the summary covers the rest.
  """
  @spec run(Config.t(), keyword) :: {:ok, summary} | {:error, term}
  def run(config, opts \\ []) do
    started = System.monotonic_time()
    settings = settings(config, opts)

    result =
      with {:ok, locations} <- Placement.locations(config) do
        {summary, failures} =
          Enum.reduce(locations, {@empty, []}, fn location, {acc, failures} ->
            case sweep_location(config, settings, location, acc) do
              {:ok, acc} -> {acc, failures}
              {:error, reason, acc} -> {acc, [{location.id, reason} | failures]}
            end
          end)

        if failures == [],
          do: {:ok, summary},
          else: {:error, {:listing, Enum.reverse(failures), summary}}
      end

    summary =
      case result do
        {:ok, summary} -> summary
        {:error, {:listing, _failures, summary}} -> summary
        {:error, _} -> @empty
      end

    Telemetry.span_stop([:sweeper, :round], started, summary, %{
      instance: config.name,
      result: if(match?({:ok, _}, result), do: :ok, else: :error)
    })

    result
  end

  defp settings(config, opts) do
    config.sweeper
    |> Kernel.||(Map.new(Config.sweeper_defaults()))
    |> Map.put(:timeout_ms, config.call_timeout_ms)
    |> Map.merge(Map.new(Keyword.take(opts, [:concurrency, :rate, :page_size, :timeout_ms])))
  end

  defp sweep_location(config, settings, location, acc) do
    case Schema.outdated_versions(config, location, listing_deadline(config)) do
      {:ok, versions} ->
        sweep_versions(config, settings, location, candidates(config, versions), acc)

      {:error, reason} ->
        Logger.warning("Alambique sweeper could not list #{location.id}: #{inspect(reason)}")
        {:error, reason, acc}
    end
  end

  defp candidates(%{migrations: {_module, _function, _args}}, versions), do: versions
  defp candidates(config, versions), do: Enum.filter(versions, &(&1 < config.schema_version))

  defp sweep_versions(_config, _settings, _location, [], acc), do: {:ok, acc}

  defp sweep_versions(config, settings, location, [version | rest], acc) do
    case sweep_pages(config, settings, location, version, nil, acc) do
      {:ok, acc} -> sweep_versions(config, settings, location, rest, acc)
      {:error, _reason, _acc} = error -> error
    end
  end

  # Entries leave the listing as cells migrate, so the cursor is the last
  # cell of the page and a cell that failed is left for the next round.
  defp sweep_pages(config, settings, location, version, cursor, acc) do
    deadline = listing_deadline(config)

    case Schema.page(config, location, version, cursor, settings.page_size, deadline) do
      {:ok, [], _next} ->
        {:ok, acc}

      {:ok, cells, next} ->
        started = System.monotonic_time(:millisecond)
        {due, failed} = due(config, version, cells)
        results = sweep_cells(config, settings, location, version, due)
        acc = Enum.reduce(failed ++ results, acc, &count/2)
        pace(settings, length(due), started)
        if next, do: sweep_pages(config, settings, location, version, next, acc), else: {:ok, acc}

      {:error, reason} ->
        Logger.warning(
          "Alambique sweeper could not list #{location.id} version #{version}: #{inspect(reason)}"
        )

        {:error, reason, acc}
    end
  end

  # The cells of the page that are below their own schema version, and the
  # failures of the function for the others. A static instance lists only
  # cells that are due.
  defp due(%{migrations: {_module, _function, _args}} = config, version, cells) do
    Enum.reduce(cells, {[], []}, fn cell, {due, failed} ->
      case Config.migrations_for(config, cell) do
        {:ok, list} when version < length(list) -> {[cell | due], failed}
        {:ok, _} -> {due, failed}
        {:error, reason} -> {due, [{:error, reason} | failed]}
      end
    end)
  end

  defp due(_config, _version, cells), do: {cells, []}

  defp count(result, acc) do
    acc = %{acc | cells: acc.cells + 1}

    case result do
      {:ok, :migrated} -> %{acc | migrated: acc.migrated + 1}
      {:ok, :stale} -> %{acc | stale: acc.stale + 1}
      {:ok, :skipped} -> %{acc | skipped: acc.skipped + 1}
      {:error, _} -> %{acc | errors: acc.errors + 1}
    end
  end

  defp sweep_cells(config, settings, location, version, cells) do
    cells
    |> Task.async_stream(&sweep_cell(config, settings, location, version, &1),
      max_concurrency: settings.concurrency,
      timeout: settings.timeout_ms * 3,
      on_timeout: :kill_task,
      ordered: false
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:exit, reason}}
    end)
  end

  # Holds the pass to `rate` cells per second.
  defp pace(settings, count, started) do
    wait = div(count * 1000, settings.rate) - (System.monotonic_time(:millisecond) - started)
    if wait > 0, do: Process.sleep(wait)
  end

  defp listing_deadline(config), do: Retry.deadline_in(config.schema.listing_timeout_ms)

  # One cell

  defp sweep_cell(config, settings, location, version, cell) do
    started = System.monotonic_time()
    result = migrate(config, settings, location, version, cell)
    meta = %{cell: cell, location: location.id, version: version}

    meta =
      case result do
        {:ok, what} -> Map.put(meta, :result, what)
        {:error, reason} -> Map.merge(meta, %{result: :error, reason: reason})
      end

    Telemetry.span_stop([:sweeper, :cell], started, %{}, meta)
    result
  end

  defp migrate(config, settings, location, version, cell) do
    case Placement.locate(config, cell) do
      {:ok, %{id: id}} when id == location.id ->
        wake(config, settings, location, version, cell)

      # The cell was relocated: its entry here is a leftover.
      {:ok, _elsewhere} ->
        forget(config, location, version, cell, :stale)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp forget(config, location, version, cell, result) do
    case Schema.drop(config, location, cell, version, listing_deadline(config)) do
      :ok -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  # Asking for the version wakes the cell, and the wake migrates it. A cell
  # that was awake is left awake. A cell found at a version other than the
  # listed one has an entry that was left behind; it goes either way.
  defp wake(config, settings, location, version, cell) do
    name = config.name
    options = [timeout: settings.timeout_ms]

    with {:ok, asleep?} <- asleep?(name, cell),
         {:ok, current} <- current_version(name, cell, options),
         :ok <- if(asleep?, do: Alambique.hibernate(name, cell, options), else: :ok) do
      if current == version,
        do: {:error, {:not_migrated, current}},
        else: forget(config, location, version, cell, :migrated)
    else
      {:error, :frozen} -> {:ok, :skipped}
      {:error, reason} -> {:error, reason}
    end
  end

  defp asleep?(name, cell) do
    case Alambique.owner(name, cell) do
      :none -> {:ok, true}
      {:ok, _node} -> {:ok, false}
      {:error, _} = error -> error
    end
  end

  defp current_version(name, cell, options) do
    case Alambique.query(name, cell, "PRAGMA user_version", [], options) do
      {:ok, %{rows: [[current]]}} -> {:ok, current}
      {:error, _} = error -> error
    end
  end

  # Process

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    state = %{config: config, failures: 0, task: nil}
    # The first round is spread over an interval so nodes do not sweep together.
    {:ok, schedule(state, :rand.uniform(config.sweeper.interval_ms))}
  end

  @impl true
  def handle_info(:sweep, %{task: nil} = state) do
    config = state.config
    {:noreply, %{state | task: Task.async(fn -> run(config) end)}}
  end

  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, finished(%{state | task: nil}, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state),
    do: {:noreply, finished(%{state | task: nil}, {:error, {:crashed, reason}})}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{task: %Task{} = task}), do: Task.shutdown(task, :brutal_kill)
  def terminate(_reason, _state), do: :ok

  # A round with an error waits twice as long as the round before, up to
  # `:max_backoff_ms`; a clean round goes back to the interval.
  defp finished(state, result) do
    %{interval_ms: interval, max_backoff_ms: cap} = state.config.sweeper
    clean? = match?({:ok, %{errors: 0}}, result)

    unless clean?,
      do: Logger.warning("Alambique sweeper round did not end cleanly: #{inspect(result)}")

    failures = if clean?, do: 0, else: state.failures + 1
    delay = min(interval * Integer.pow(2, min(failures, 20)), max(cap, interval))
    schedule(%{state | failures: failures}, if(clean?, do: interval, else: delay))
  end

  defp schedule(state, delay) do
    Process.send_after(self(), :sweep, delay)
    state
  end
end
