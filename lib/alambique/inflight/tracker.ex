defmodule Alambique.Inflight.Tracker do
  @moduledoc false
  # The process behind the markers of an instance (design §10).
  #
  # Start: it reads the markers the previous VM left busy, adds one crash for
  # each cell to the store, and freezes the cells that reached the threshold.
  # It runs before the cells can wake and takes at most `:startup_timeout_ms`.
  # What it could not record stays on disk and is tried again every
  # `:retry_ms`.
  #
  # Afterwards: it removes the marker of every process that ends, so a crash
  # of one process (not of the VM) never counts, and those of the live ones
  # when the instance stops.

  use GenServer

  require Logger

  alias Alambique.{Config, Freeze, Inflight, Placement, Quarantine, Telemetry}
  alias Alambique.Replication.Retry

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: config.inflight)

  @doc "Remembers the marker at `path` of the calling process."
  @spec track(Alambique.Config.t(), Path.t()) :: :ok
  def track(config, path), do: GenServer.cast(config.inflight, {:track, self(), path})

  @doc "The calling process removed the marker at `path` itself."
  @spec untrack(Alambique.Config.t(), Path.t()) :: :ok
  def untrack(config, path), do: GenServer.cast(config.inflight, {:untrack, self(), path})

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    File.mkdir_p!(Inflight.dir(config))
    state = %{config: config, procs: %{}, pending: scan(config), timer: nil}
    {:ok, recover(state)}
  end

  @impl true
  def handle_cast({:track, pid, path}, state) do
    procs =
      Map.update(state.procs, pid, {Process.monitor(pid), [path]}, fn {ref, paths} ->
        {ref, [path | paths]}
      end)

    {:noreply, %{state | procs: procs}}
  end

  def handle_cast({:untrack, pid, path}, state) do
    procs =
      case state.procs do
        %{^pid => {ref, paths}} ->
          case List.delete(paths, path) do
            [] ->
              Process.demonitor(ref, [:flush])
              Map.delete(state.procs, pid)

            rest ->
              Map.put(state.procs, pid, {ref, rest})
          end

        procs ->
          procs
      end

    {:noreply, %{state | procs: procs}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {entry, procs} = Map.pop(state.procs, pid)
    if entry, do: Enum.each(elem(entry, 1), &File.rm/1)
    {:noreply, %{state | procs: procs}}
  end

  def handle_info(:retry, state), do: {:noreply, recover(%{state | timer: nil})}
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    for {_pid, {_ref, paths}} <- state.procs, path <- paths, do: File.rm(path)
    :ok
  end

  # Start

  # The busy markers left by the previous VM, by cell. Idle ones prove nothing
  # and are removed.
  defp scan(config) do
    dir = Inflight.dir(config)

    dir
    |> File.ls!()
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.reduce(%{}, fn path, acc ->
      case read(path) do
        {cell, kind} ->
          Map.update(acc, cell, entry(path, kind), fn e ->
            %{e | files: [path | e.files], kinds: [kind | e.kinds]}
          end)

        nil ->
          acc
      end
    end)
  end

  defp entry(path, kind), do: %{files: [path], kinds: [kind], counter: nil}

  defp read(path) do
    with {:ok, <<1, json::binary>>} <- File.read(path),
         {:ok, %{"cell" => cell, "kind" => kind}} when is_binary(cell) <- Jason.decode(json) do
      {cell, kind}
    else
      _ ->
        File.rm(path)
        nil
    end
  end

  # One job per cell with something to settle, a few at a time, within the
  # startup timeout. A job that does not finish is left for the next try.
  defp recover(%{pending: pending} = state) when map_size(pending) == 0, do: state

  defp recover(%{config: config, pending: pending} = state) do
    %{startup_timeout_ms: timeout, retry_ms: retry} = config.quarantine
    deadline = Retry.deadline_in(timeout)

    pending =
      pending
      |> Task.async_stream(fn {cell, entry} -> {cell, settle(config, cell, entry, deadline)} end,
        max_concurrency: config.quarantine.concurrency,
        timeout: timeout,
        on_timeout: :kill_task,
        zip_input_on_exit: true
      )
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, {input, _reason}} -> input
      end)
      |> Enum.reject(&match?({_cell, :done}, &1))
      |> Map.new()

    timer = if pending == %{}, do: nil, else: Process.send_after(self(), :retry, retry)
    %{state | pending: pending, timer: timer}
  end

  # Records the crash, then freezes the cell when the threshold is reached.
  # The entry remembers the first step, so a retry does not count again.
  defp settle(config, cell, entry, deadline) do
    {mod, opts} = config.placement

    # The placement itself: its cache belongs to a process that starts later.
    with :ok <- Placement.validate_id(config, cell),
         {:ok, location} <- mod.locate(opts, cell) do
      ctx = Config.repl_ctx(config, location)

      case record(config, ctx, location, cell, entry, deadline) do
        {:ok, entry} ->
          case quarantine(config, ctx, location, cell, entry.counter, deadline) do
            :ok -> :done
            {:error, reason} -> unsettled(cell, entry, reason)
          end

        {:error, reason} ->
          unsettled(cell, entry, reason)
      end
    else
      {:error, reason} -> unsettled(cell, entry, reason)
    end
  rescue
    exception -> unsettled(cell, entry, Exception.message(exception))
  end

  defp unsettled(cell, entry, reason) do
    Logger.warning("Alambique could not settle the crash of #{cell}: #{inspect(reason)}")
    entry
  end

  defp record(_config, _ctx, _location, _cell, %{counter: %{}} = entry, _deadline),
    do: {:ok, entry}

  defp record(config, ctx, location, cell, entry, deadline) do
    kinds = Enum.uniq(entry.kinds)

    with {:ok, counter} <-
           Quarantine.record(ctx, cell, config.quarantine, kinds, deadline: deadline) do
      Enum.each(entry.files, &File.rm/1)

      Telemetry.emit(
        [:cell, :crash_recorded],
        %{count: length(counter.recent), total: counter.total},
        %{cell: cell, location: location.id, kinds: kinds, node: config.node_id}
      )

      {:ok, %{entry | files: [], counter: counter}}
    end
  end

  defp quarantine(config, ctx, location, cell, counter, deadline) do
    %{threshold: threshold, window_ms: window} = config.quarantine
    count = length(counter.recent)

    if count < threshold do
      :ok
    else
      with :ok <- Freeze.freeze(ctx, cell, deadline: deadline) do
        Logger.error(
          "Alambique froze #{cell}: it crashed the VM #{count} times within #{window} ms (threshold #{threshold})"
        )

        Telemetry.emit(
          [:cell, :quarantined],
          %{count: count},
          %{
            cell: cell,
            location: location.id,
            threshold: threshold,
            window_ms: window,
            crashes: Enum.map(counter.recent, & &1.at_ms)
          }
        )
      end
    end
  end
end
