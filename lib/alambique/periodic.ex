defmodule Alambique.Periodic do
  @moduledoc false
  # A background job of an instance: one process, one timer, one round at a
  # time. The first round starts after a random part of the interval, so
  # nodes do not run together. A round that raises or exits is logged and the
  # next one runs on time.

  use GenServer

  require Logger

  @doc "Starts the job `label`; `round` is a function of no arguments."
  @spec start_link(String.t(), pos_integer, (-> term)) :: GenServer.on_start()
  def start_link(label, interval_ms, round),
    do: GenServer.start_link(__MODULE__, {label, interval_ms, round})

  @impl true
  def init({label, interval_ms, round}) do
    Process.flag(:trap_exit, true)
    state = %{label: label, interval: interval_ms, round: round, task: nil}
    {:ok, schedule(state, :rand.uniform(interval_ms))}
  end

  @impl true
  def handle_info(:round, %{task: nil} = state),
    do: {:noreply, %{state | task: Task.async(state.round)}}

  def handle_info({ref, _result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, schedule(%{state | task: nil}, state.interval)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    Logger.error("Alambique #{state.label} round crashed: #{inspect(reason)}")
    {:noreply, schedule(%{state | task: nil}, state.interval)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{task: %Task{} = task}), do: Task.shutdown(task, :brutal_kill)
  def terminate(_reason, _state), do: :ok

  defp schedule(state, delay) do
    Process.send_after(self(), :round, delay)
    state
  end
end
