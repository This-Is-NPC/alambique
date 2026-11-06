defmodule Alambique.Live do
  @moduledoc false
  # Ordered index of the cells with a process on this node (design §8), kept
  # by listening to the instance's registry. It starts before the registry,
  # which sends it `:register` and `:unregister` messages.
  #
  # The table `{id, pid}` may lag the registry by a message, so a walk skips
  # entries whose process is gone. A removal only deletes the exact `{id,
  # pid}` pair: a new process under the same id is never removed by the old
  # one's exit, in either message order.

  use GenServer

  def start_link(config), do: GenServer.start_link(__MODULE__, config.live, name: config.live)

  @impl true
  def init(table) do
    :ets.new(table, [:named_table, :protected, :ordered_set, read_concurrency: true])
    {:ok, %{table: table, monitors: %{}}}
  end

  @impl true
  def handle_info({:register, _registry, id, pid, _value}, state) do
    :ets.insert(state.table, {id, pid})
    ref = Process.monitor(pid)
    {:noreply, put_in(state.monitors[pid], {id, ref})}
  end

  def handle_info({:unregister, _registry, id, pid}, state) do
    {:noreply, drop(state, id, pid)}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case state.monitors do
      %{^pid => {id, _}} -> {:noreply, drop(state, id, pid)}
      _ -> {:noreply, state}
    end
  end

  defp drop(state, id, pid) do
    :ets.delete_object(state.table, {id, pid})

    case Map.pop(state.monitors, pid) do
      {{_, ref}, monitors} ->
        Process.demonitor(ref, [:flush])
        %{state | monitors: monitors}

      {nil, _} ->
        state
    end
  end

  @doc "Page of live cell ids after `after_id`; see `Alambique.cells/2`."
  @spec page(Alambique.Config.t(), String.t() | nil, pos_integer) :: map
  def page(config, after_id, limit) do
    table = config.live
    first = if after_id, do: :ets.next(table, after_id), else: :ets.first(table)
    # One more than the page tells whether anything follows it.
    ids = walk(table, first, limit + 1)
    {page, rest} = Enum.split(ids, limit)

    %{
      cells: page,
      next: if(rest == [], do: nil, else: List.last(page)),
      count: Registry.count(config.registry)
    }
  end

  defp walk(_table, :"$end_of_table", _), do: []
  defp walk(_table, _key, 0), do: []

  defp walk(table, key, left) do
    next = :ets.next(table, key)

    case :ets.lookup(table, key) do
      [{id, pid}] ->
        if Process.alive?(pid),
          do: [id | walk(table, next, left - 1)],
          else: walk(table, next, left)

      [] ->
        walk(table, next, left)
    end
  end
end
