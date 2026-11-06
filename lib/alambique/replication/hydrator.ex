defmodule Alambique.Replication.Hydrator do
  @moduledoc false
  # The process of a cell that restores lazily (design §16). It answers the
  # `{:page_miss, ...}` messages of the VFS with ranged reads, fetches the
  # remaining pages in the background and, when every page is local and the
  # database checksum equals the restored one, tells its owner and ends.
  #
  # Messages to the owner: `{:hydrated, pid}` and `{:hydration_failed, pid, reason}`.

  use GenServer

  alias Alambique.Replication.{Lazy, Native, Retry}
  alias Alambique.{Fault, Telemetry}

  @doc """
  Starts the hydrator for `lazy` (an `Alambique.Replication.Lazy`), linked to the
  caller, which becomes the owner. Options: `:concurrency` (background reads
  at once, default 4), `:fetch_ms` (time budget of one read, default the
  context's `timeout_ms`) and `:config`, the instance's configuration, whose
  `hydrator.poll_ms` sets how often it looks for missed pages.
  """
  @spec start_link(Lazy.t(), keyword) :: GenServer.on_start()
  def start_link(%Lazy{} = lazy, opts) do
    GenServer.start_link(__MODULE__, {lazy, self(), opts})
  end

  @doc "Stops the hydrator and fails the reads that wait for it."
  @spec stop(pid) :: :ok
  def stop(pid) do
    GenServer.stop(pid, :normal)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init({lazy, owner, opts}) do
    Process.monitor(owner)
    :ok = Native.lazy_attach(lazy.handle, self())
    {total, _unknown} = Native.lazy_progress(lazy.handle)

    state = %{
      lazy: lazy,
      owner: owner,
      concurrency: Keyword.get(opts, :concurrency, 4),
      fetch_ms: Keyword.get(opts, :fetch_ms, lazy.ctx.timeout_ms),
      poll_ms: poll_ms(opts),
      tasks: %{},
      failures: 0,
      fill_timer: nil,
      started: System.monotonic_time(),
      pages: total,
      misses: 0
    }

    {:ok, state, {:continue, :fill}}
  end

  @impl true
  def handle_continue(:fill, state), do: fill(state)

  @impl true
  def handle_info(:fill, state), do: fill(%{state | fill_timer: nil})

  def handle_info({:page_miss, pgno, _file, _offset, _size}, state) do
    deadline = Retry.deadline_in(state.fetch_ms)
    lazy = state.lazy
    Fault.point(:hydrate_before_miss, %{cell: lazy.cell, hydrator: self(), pgno: pgno})

    task =
      Task.async(fn ->
        result = Lazy.fetch_page(lazy, pgno, deadline)

        with {:error, reason} <- result,
             do: Native.lazy_fail(lazy.handle, pgno, "page #{pgno}: #{inspect(reason)}")

        result
      end)

    {:noreply, %{state | tasks: Map.put(state.tasks, task.ref, :miss), misses: state.misses + 1}}
  end

  def handle_info({ref, result}, %{tasks: tasks} = state) when is_map_key(tasks, ref) do
    Process.demonitor(ref, [:flush])
    {kind, tasks} = Map.pop!(tasks, ref)
    state = %{state | tasks: tasks}

    case {kind, result} do
      {:miss, _} -> maybe_finish(state)
      {{:bg, _pages}, {:ok, _done}} -> fill(%{state | failures: 0})
      {{:bg, pages}, {:error, reason}} -> background_failed(state, pages, reason)
    end
  end

  def handle_info({:DOWN, _ref, :process, owner, _reason}, %{owner: owner} = state),
    do: {:stop, :normal, state}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{tasks: tasks} = state)
      when is_map_key(tasks, ref),
      do: {:stop, {:fetch_crashed, reason}, state}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Native.lazy_abort(state.lazy.handle, "hydrator stopped")
  end

  # Starts background reads until `concurrency` are under way.
  defp fill(state) do
    background = Enum.count(state.tasks, &match?({_, {:bg, _}}, &1))

    cond do
      state.fill_timer != nil ->
        {:noreply, state}

      background >= state.concurrency ->
        {:noreply, state}

      true ->
        case Native.lazy_next_batch(state.lazy.handle, state.lazy.batch_bytes) do
          :none -> maybe_finish(state)
          run -> fill(start_background(state, run))
        end
    end
  end

  defp start_background(state, {_file, _offset, _len, pages} = run) do
    lazy = state.lazy
    fetch_ms = state.fetch_ms
    hydrator = self()

    task =
      Task.async(fn ->
        Fault.point(:hydrate_before_fetch, %{cell: lazy.cell, hydrator: hydrator})
        Lazy.fetch_run(lazy, run, Retry.deadline_in(fetch_ms))
      end)

    numbers = for {pgno, _, _} <- pages, do: pgno
    %{state | tasks: Map.put(state.tasks, task.ref, {:bg, numbers})}
  end

  defp background_failed(state, pages, reason) do
    :ok = Native.lazy_requeue(state.lazy.handle, pages)

    if permanent?(reason) do
      fail(state, reason)
    else
      failures = state.failures + 1
      delay = min(state.lazy.ctx.backoff_ms * Integer.pow(2, min(failures, 6)), 5_000)
      timer = Process.send_after(self(), :fill, delay)
      {:noreply, %{state | failures: failures, fill_timer: timer}}
    end
  end

  # What no retry can fix: an object that is gone or a frame that does not
  # decode.
  defp permanent?({:not_found, _key}), do: true
  defp permanent?({:short_read, _key}), do: true
  defp permanent?({:invalid, _}), do: true
  defp permanent?(_), do: false

  # Done when no page is missing; otherwise look again soon, because pages
  # that the cell fetched for its own sums arrive without a message.
  defp maybe_finish(state) do
    running = map_size(state.tasks)

    case Native.lazy_progress(state.lazy.handle) do
      {_pages, 0} when running == 0 -> verify(state)
      {_pages, 0} -> {:noreply, state}
      _ when running == 0 -> {:noreply, schedule(state)}
      _ -> {:noreply, state}
    end
  end

  defp schedule(%{fill_timer: nil} = state),
    do: %{state | fill_timer: Process.send_after(self(), :fill, state.poll_ms)}

  defp schedule(state), do: state

  defp poll_ms(opts) do
    case Keyword.get(opts, :config) do
      %{hydrator: %{poll_ms: ms}} -> ms
      _ -> Keyword.get(opts, :poll_ms, 100)
    end
  end

  defp verify(state) do
    verified = Native.lazy_verify(state.lazy.handle, state.lazy.checksum)

    case verified do
      :ok ->
        stopped(state, %{result: :ok})
        send(state.owner, {:hydrated, self()})
        {:stop, :normal, state}

      {:error, reason} ->
        fail(state, reason)
    end
  end

  defp stopped(state, meta) do
    Telemetry.span_stop(
      [:replication, :hydrate],
      state.started,
      %{pages: state.pages, misses: state.misses},
      Map.put(meta, :cell, state.lazy.cell)
    )
  end

  defp fail(state, reason) do
    stopped(state, %{result: :error, reason: reason})
    send(state.owner, {:hydration_failed, self(), reason})
    {:stop, :normal, state}
  end
end
