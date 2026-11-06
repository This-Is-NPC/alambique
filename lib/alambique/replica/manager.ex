defmodule Alambique.Replica.Manager do
  @moduledoc false
  # Admission of read replicas on one node (design §21). It owns two tables:
  # `config.replica_manager` holds one row per replica, `{key, pid, bytes,
  # last_read_ms}` with `key = {location id, cell}`, and `config.replica_memo`
  # holds short-lived answers, `{key, value, expires_ms}`: the replicas switch
  # of a cell, and cells that were refused. Starting a replica and reserving
  # bytes go through this process, so the limits hold whatever the number of
  # readers. A replica never calls this process itself, only its tasks do, so
  # the manager can stop a replica while waiting for nobody.

  use GenServer

  alias Alambique.Replica

  def start_link(config),
    do: GenServer.start_link(__MODULE__, config, name: config.replica_manager)

  @impl true
  def init(config) do
    opts = [:named_table, :public, :set, read_concurrency: true]
    :ets.new(config.replica_manager, opts)
    :ets.new(config.replica_memo, opts)
    {:ok, %{config: config, monitors: %{}}}
  end

  @doc "Asks for a replica of `cell`; it starts if the limits allow, and builds in the background."
  @spec start_replica(map, String.t(), Alambique.Location.t()) :: :ok
  def start_replica(config, cell, location) do
    if memo(config, {:refused, {location.id, cell}}) == nil,
      do: GenServer.cast(config.replica_manager, {:start, cell, location})

    :ok
  end

  @doc """
  Sets the bytes of the row of `key`: `:ok` when the files of all the replicas
  of the node fit in `max_bytes`, else `:refused` and nothing changes.
  """
  @spec reserve(map, term, non_neg_integer) :: :ok | :refused
  def reserve(config, key, bytes),
    do: GenServer.call(config.replica_manager, {:reserve, key, bytes}, :infinity)

  @doc "Records that `key` was read at `now` (monotonic milliseconds)."
  @spec touch(map, term, integer) :: boolean
  def touch(config, key, now), do: :ets.update_element(config.replica_manager, key, {4, now})

  @doc "The unexpired value stored under `key`, or `nil`."
  @spec memo(map, term) :: term | nil
  def memo(config, key) do
    case :ets.lookup(config.replica_memo, key) do
      [{_, value, expires}] -> if now() < expires, do: value
      [] -> nil
    end
  end

  @doc "Remembers `value` under `key` for `ttl_ms`; at `hint_limit` entries a sixteenth is evicted."
  @spec memo_put(map, term, term, pos_integer) :: true
  def memo_put(config, key, value, ttl_ms),
    do:
      Alambique.BoundedTable.put(
        config.replica_memo,
        config.hint_limit,
        {key, value, now() + ttl_ms}
      )

  @doc "Replicas of this node and the bytes they hold."
  @spec usage(map) :: %{cells: non_neg_integer, bytes: non_neg_integer}
  def usage(config) do
    rows = :ets.tab2list(config.replica_manager)
    %{cells: length(rows), bytes: rows |> Enum.map(&elem(&1, 2)) |> Enum.sum()}
  end

  @impl true
  def handle_cast({:start, cell, location}, state) do
    config = state.config
    key = {location.id, cell}

    state =
      cond do
        :ets.member(config.replica_manager, key) -> state
        memo(config, {:refused, key}) != nil -> state
        room?(config) -> launch(state, key, cell, location)
        true -> refuse(state, key)
      end

    {:noreply, state}
  end

  @impl true
  def handle_call({:reserve, key, bytes}, _from, state) do
    config = state.config

    others =
      for [k, b] <- :ets.match(config.replica_manager, {:"$1", :_, :"$2", :_}), k != key, do: b

    if Enum.sum(others) + bytes <= config.replicas.max_bytes do
      :ets.update_element(config.replica_manager, key, {3, bytes})
      {:reply, :ok, state}
    else
      {:reply, :refused, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _}, state) do
    :ets.match_delete(state.config.replica_manager, {:_, pid, :_, :_})
    {:noreply, %{state | monitors: Map.delete(state.monitors, ref)}}
  end

  defp launch(state, key, cell, location) do
    config = state.config
    child = {Replica, {config, cell, location}}

    case DynamicSupervisor.start_child(config.replica_supervisor, child) do
      {:ok, pid} ->
        :ets.insert(config.replica_manager, {key, pid, 0, now()})
        %{state | monitors: Map.put(state.monitors, Process.monitor(pid), key)}

      {:error, _} ->
        refuse(state, key)
    end
  end

  # Room for one more replica: below the limit, or the replica read longest
  # ago is idle (not read for `marker_ttl_ms`) and makes way.
  defp room?(config) do
    rows = :ets.tab2list(config.replica_manager)

    if length(rows) < config.replicas.max_cells do
      true
    else
      {key, pid, _bytes, last_read} = Enum.min_by(rows, &elem(&1, 3))

      if now() - last_read >= config.replicas.marker_ttl_ms do
        :ets.delete(config.replica_manager, key)
        Replica.stop(pid, :evicted)
        true
      else
        false
      end
    end
  end

  defp refuse(state, key) do
    memo_put(state.config, {:refused, key}, true, state.config.replicas.marker_ttl_ms)
    state
  end

  defp now, do: System.monotonic_time(:millisecond)
end
