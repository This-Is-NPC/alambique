defmodule Alambique.Load.Service.Probe do
  @moduledoc false
  # Dirty IO pool use over a window: the wall-time utilization of the dirty IO
  # schedulers and the user statements running now (the slot counter that
  # `max_concurrent_statements` limits), sampled every 50 ms.

  use GenServer

  @sample_ms 50

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc "Starts a window."
  def begin, do: GenServer.call(__MODULE__, :begin)

  @doc "Ends the window and returns what it saw."
  def finish, do: GenServer.call(__MODULE__, :finish)

  @impl true
  def init(_), do: {:ok, %{wall: nil, statements: []}}

  @impl true
  def handle_call(:begin, _from, _state) do
    :erlang.system_flag(:scheduler_wall_time, true)
    Process.send_after(self(), :sample, @sample_ms)
    {:reply, :ok, %{wall: wall(), statements: []}}
  end

  def handle_call(:finish, _from, %{wall: nil} = state), do: {:reply, %{}, state}

  def handle_call(:finish, _from, state) do
    report = report(state)
    :erlang.system_flag(:scheduler_wall_time, false)
    {:reply, report, %{wall: nil, statements: []}}
  end

  @impl true
  def handle_info(:sample, %{wall: nil} = state), do: {:noreply, state}

  def handle_info(:sample, state) do
    Process.send_after(self(), :sample, @sample_ms)
    {:noreply, %{state | statements: [Alambique.Sqlite.user_statements() | state.statements]}}
  end

  defp wall, do: :erlang.statistics(:scheduler_wall_time_all) |> Enum.sort()

  defp report(%{wall: before, statements: statements}) do
    normal = :erlang.system_info(:schedulers)
    cpu = :erlang.system_info(:dirty_cpu_schedulers)

    groups =
      Enum.zip(before, wall())
      |> Enum.group_by(fn {{id, _, _}, _} ->
        cond do
          id <= normal -> :normal
          id <= normal + cpu -> :dirty_cpu
          true -> :dirty_io
        end
      end)

    util =
      for {group, pairs} <- groups, into: %{} do
        active = Enum.sum(for {{_, a0, _}, {_, a1, _}} <- pairs, do: a1 - a0)
        total = Enum.sum(for {{_, _, t0}, {_, _, t1}} <- pairs, do: t1 - t0)
        {group, if(total > 0, do: Float.round(active / total, 4), else: 0.0)}
      end

    %{
      utilization: util,
      dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers),
      user_statements_max: Enum.max(statements, fn -> 0 end),
      user_statements_mean:
        if(statements == [],
          do: 0.0,
          else: Float.round(Enum.sum(statements) / length(statements), 3)
        )
    }
  end
end
