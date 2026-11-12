defmodule Alambique.Admission.Wakes do
  @moduledoc false
  # The wake slots of an instance (`awake.max_wakes`, design §7) and the queue
  # of wakes waiting for one. One process owns them, so a slot has exactly one
  # holder at any moment and every change of holder is one message:
  #
  # - `{:begin, wait_ms}` (call): the caller holds a slot, or waits in the
  #   queue until one is handed to it, or is refused.
  # - `{:transfer, holder, cell}` (call): the cell that `holder` started takes
  #   the slot over, or is told `:lost` when the holder is already gone and its
  #   slot was freed; the cell then asks for a slot of its own.
  # - `{:end, pid}` (cast): `pid` gives its slot back, at most once.
  #
  # Every holder and every waiter is monitored, so a slot comes back when its
  # holder dies, whether it is a caller on its way to starting a cell, or a
  # cell in the middle of its wake, and a caller that gave up leaves the queue.
  # A freed slot goes to the head of the queue in the same step, so it is never
  # idle while a wake waits.

  use GenServer

  alias Alambique.Telemetry

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: config.wakes)

  @impl true
  def init(config),
    do: {:ok, %{config: config, holders: %{}, order: :queue.new(), waiting: %{}}}

  @impl true
  def handle_call({:begin, wait_ms}, {pid, _} = from, state) do
    %{config: %{awake: awake}} = state

    cond do
      map_size(state.waiting) == 0 and map_size(state.holders) < awake.max_wakes ->
        {:reply, :ok, hold(state, pid, Process.monitor(pid))}

      wait_ms <= 0 ->
        {:reply, {:error, :overloaded}, state}

      map_size(state.waiting) >= awake.max_wake_queue ->
        emit(System.monotonic_time(), :full)
        {:reply, {:error, :overloaded}, state}

      true ->
        mon = Process.monitor(pid)
        timer = Process.send_after(self(), {:expire, mon}, wait_ms)
        entry = %{from: from, pid: pid, timer: timer, at: System.monotonic_time()}

        {:noreply,
         %{
           state
           | order: :queue.in(mon, state.order),
             waiting: Map.put(state.waiting, mon, entry)
         }}
    end
  end

  def handle_call({:transfer, holder, cell}, _from, state) do
    case Map.pop(state.holders, holder) do
      {nil, _} ->
        {:reply, :lost, state}

      {mon, holders} ->
        Process.demonitor(mon, [:flush])
        {:reply, :ok, hold(%{state | holders: holders}, cell, Process.monitor(cell))}
    end
  end

  @impl true
  def handle_cast({:end, pid}, state) do
    case Map.pop(state.holders, pid) do
      {nil, _} ->
        {:noreply, state}

      {mon, holders} ->
        Process.demonitor(mon, [:flush])
        {:noreply, grant(%{state | holders: holders})}
    end
  end

  @impl true
  def handle_info({:DOWN, mon, :process, pid, _}, state) do
    cond do
      Map.get(state.holders, pid) == mon ->
        {:noreply, grant(%{state | holders: Map.delete(state.holders, pid)})}

      Map.has_key?(state.waiting, mon) ->
        {entry, waiting} = Map.pop!(state.waiting, mon)
        Process.cancel_timer(entry.timer)
        {:noreply, %{state | waiting: waiting}}

      true ->
        {:noreply, state}
    end
  end

  def handle_info({:expire, mon}, state) do
    case Map.pop(state.waiting, mon) do
      {nil, _} ->
        {:noreply, state}

      {entry, waiting} ->
        Process.demonitor(mon, [:flush])
        GenServer.reply(entry.from, {:error, :overloaded})
        emit(entry.at, :expired)
        {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp hold(state, pid, mon), do: %{state | holders: Map.put(state.holders, pid, mon)}

  # Hands free slots to the head of the queue; the waiter's monitor becomes
  # the monitor of the holder.
  defp grant(%{config: %{awake: awake}} = state) do
    with true <- map_size(state.holders) < awake.max_wakes,
         {{:value, mon}, order} <- :queue.out(state.order) do
      state = %{state | order: order}

      case Map.pop(state.waiting, mon) do
        {nil, _} ->
          grant(state)

        {entry, waiting} ->
          Process.cancel_timer(entry.timer)
          GenServer.reply(entry.from, :ok)
          emit(entry.at, :granted)
          grant(hold(%{state | waiting: waiting}, entry.pid, mon))
      end
    else
      _ -> state
    end
  end

  defp emit(started, result),
    do: Telemetry.span_stop([:admission, :wake_queue], started, %{result: result})
end
