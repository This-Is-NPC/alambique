defmodule Alambique.Query.Gate do
  @moduledoc false
  # Limits how many cells of one query run their SQLite statement at once, so
  # the query never takes more statement slots than it may (design §14). It
  # only orders the cells of that query: other callers still meet the slots
  # and get `:busy` at once (design §7).
  use GenServer

  @doc "A gate of `limit` places, stopped with `stop/1` or when the caller dies."
  @spec start(pos_integer) :: pid
  def start(limit) do
    {:ok, gate} = GenServer.start(__MODULE__, {limit, self()})
    gate
  end

  @spec stop(pid) :: :ok
  def stop(gate), do: GenServer.stop(gate)

  @doc "Runs `fun` holding a place; `{:error, :timeout}` when none is free within `timeout_ms`."
  @spec run(pid, pos_integer, (-> result)) :: result | {:error, :timeout} when result: var
  def run(gate, timeout_ms, fun) do
    GenServer.call(gate, :acquire, timeout_ms)

    try do
      fun.()
    after
      GenServer.cast(gate, {:release, self()})
    end
  catch
    :exit, {:timeout, _} ->
      GenServer.cast(gate, {:release, self()})
      {:error, :timeout}
  end

  @impl true
  def init({limit, owner}) do
    Process.monitor(owner)
    {:ok, %{free: limit, waiting: :queue.new(), holders: MapSet.new(), mons: %{}, owner: owner}}
  end

  @impl true
  def handle_call(:acquire, {pid, _} = from, state) do
    state = %{state | mons: Map.put(state.mons, pid, Process.monitor(pid))}

    if state.free > 0 do
      {:reply, :ok, %{state | free: state.free - 1, holders: MapSet.put(state.holders, pid)}}
    else
      {:noreply, %{state | waiting: :queue.in(from, state.waiting)}}
    end
  end

  @impl true
  def handle_cast({:release, pid}, state), do: {:noreply, release(state, pid)}

  @impl true
  def handle_info({:DOWN, _ref, :process, owner, _}, %{owner: owner} = state),
    do: {:stop, :normal, state}

  def handle_info({:DOWN, _ref, :process, pid, _}, state), do: {:noreply, release(state, pid)}

  # A holder gives its place to the next waiter; a waiter that gave up or died
  # leaves the queue.
  defp release(state, pid) do
    {mon, mons} = Map.pop(state.mons, pid)
    if mon, do: Process.demonitor(mon, [:flush])
    waiting = :queue.filter(fn {waiter, _} -> waiter != pid end, state.waiting)
    state = %{state | mons: mons, waiting: waiting}

    if MapSet.member?(state.holders, pid),
      do: grant(%{state | holders: MapSet.delete(state.holders, pid)}),
      else: state
  end

  defp grant(state) do
    case :queue.out(state.waiting) do
      {{:value, {pid, _} = from}, waiting} ->
        GenServer.reply(from, :ok)
        %{state | waiting: waiting, holders: MapSet.put(state.holders, pid)}

      {:empty, _} ->
        %{state | free: state.free + 1}
    end
  end
end
