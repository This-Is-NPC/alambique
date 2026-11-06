defmodule Alambique.Membership do
  @moduledoc false
  # Cluster view of one instance (design §8): which nodes run it, the
  # rendezvous ring over them, and the bounded owner-hint cache.
  #
  # Every node runs one process of this module. It joins a `:pg` group that
  # maps a node id to its node, and the group of members unless the instance
  # starts with `join: false`. The state (`:not_joined`, `:member` or `:left`)
  # lives in a `:persistent_term` of the instance, so a restart of this process
  # or of the drain finds the groups as they were. It owns the hint table and
  # the table of cached cell locations (`Alambique.Placement`) and the admission
  # table (`Alambique.Admission`).
  #
  # The cluster view (members and foreign nodes) is
  # computed from `:pg` and kept in a `:persistent_term`, so a call reads it
  # without scanning the groups. The process refreshes it when the scope
  # reports a join or a leave, and `sync/1` asks it to for a change made here.
  # Only this process stores the view, so a slower caller cannot overwrite a
  # newer one.

  use GenServer

  alias Alambique.Config

  @members :members

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    :ets.new(config.hints, [:named_table, :public, :set, read_concurrency: true])
    :ets.new(config.locations, [:named_table, :public, :set, read_concurrency: true])
    Alambique.Admission.init(config)
    {_ref, _groups} = :pg.monitor_scope(config.scope)
    :ok = :pg.join(config.scope, {:node_id, config.node_id}, self())
    # The identity first: a member over another store must be foreign when
    # peers first see it. `sync/1` stores the view after both joins.
    :ok = :pg.join(config.scope, {:alambique, :store, config.store_identity}, self())
    sync(config)
    {:ok, config}
  end

  @impl true
  def handle_call(:refresh_view, _from, config) do
    store_view(config)
    {:reply, :ok, config}
  end

  @impl true
  def handle_cast({:monitor_wake, pid}, config) do
    Process.monitor(pid)
    {:noreply, config}
  end

  # A cell that died before its wake ended did not give its wake slot back,
  # and one that was killed did not drop its memory record.
  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, config) do
    Alambique.Admission.end_wake(config, pid)
    Alambique.Admission.forget(config, pid)
    {:noreply, config}
  end

  # A node that joins or leaves makes several events (one per group). A
  # `:persistent_term` put scans every process, so one refresh, queued behind
  # the events already waiting, answers the whole burst.
  def handle_info({_ref, event, _group, _pids}, config) when event in [:join, :leave] do
    drain()

    unless Process.put(:view_pending, true),
      do: send(self(), :refresh_view)

    {:noreply, config}
  end

  def handle_info(:refresh_view, config) do
    Process.delete(:view_pending)
    store_view(config)
    {:noreply, config}
  end

  def handle_info(_message, config), do: {:noreply, config}

  defp drain do
    receive do
      {_ref, event, _group, _pids} when event in [:join, :leave] -> drain()
    after
      0 -> :ok
    end
  end

  @impl true
  def terminate(_reason, config) do
    :persistent_term.erase(state_key(config))
    :persistent_term.erase(view_key(config))
    Config.delete(config.name)
  end

  defp state_key(config), do: {__MODULE__, config.name}
  defp view_key(config), do: {__MODULE__, :view, config.name}

  # Computes the cluster view from the groups of the scope and stores it when
  # it changed. Only the Membership process stores it: a caller that computed
  # earlier could otherwise overwrite a newer view.
  defp store_view(config) do
    view = compute(config)
    # An unchanged view is not put again: a put scans all processes.
    if :persistent_term.get(view_key(config), nil) != view,
      do: :persistent_term.put(view_key(config), view)

    :ok
  end

  defp compute(config) do
    foreign = pg_foreign(config)

    members =
      case config.scope |> :pg.get_members(@members) |> Enum.map(&node/1) do
        [] -> [node()]
        nodes -> nodes |> Enum.uniq() |> Kernel.--(foreign) |> Enum.sort()
      end

    %{members: members, foreign: foreign}
  end

  defp pg_foreign(%{scope: scope, store_identity: identity}) do
    for {:alambique, :store, other} = group <- :pg.which_groups(scope),
        other != identity,
        pid <- :pg.get_members(scope, group),
        uniq: true,
        do: node(pid)
  end

  @doc """
  Nodes that announced another store identity (design §8). A node that has
  not announced one yet is not foreign.
  """
  @spec foreign_nodes(Config.t()) :: [node]
  def foreign_nodes(config), do: view(config).foreign

  @doc false
  # The view computed from the groups now, not stored.
  @spec fresh_view(Config.t()) :: map
  def fresh_view(config), do: compute(config)

  @doc false
  @spec view(Config.t()) :: map
  def view(config), do: :persistent_term.get(view_key(config), nil) || compute(config)

  @doc "Sets the state of a starting instance from `:join`; a stopped instance keeps none."
  @spec init_state(Config.t()) :: :ok
  def init_state(config),
    do: :persistent_term.put(state_key(config), if(config.join, do: :member, else: :not_joined))

  @doc """
  The state of this node: `:not_joined` (`join: false`, not yet joined),
  `:member`, or `:left` (drained).
  """
  @spec state(Config.t()) :: :not_joined | :member | :left
  def state(config) do
    :persistent_term.get(state_key(config), nil) ||
      if(config.join, do: :member, else: :not_joined)
  end

  @doc false
  @spec put_state(Config.t(), :not_joined | :member | :left) :: :ok
  def put_state(config, state), do: :persistent_term.put(state_key(config), state)

  @doc """
  Makes the group of members match the state. Peers read only that group.
  """
  @spec sync(Config.t()) :: :ok
  def sync(config) do
    member? = state(config) == :member
    members = :pg.get_local_members(config.scope, @members)

    for pid <- :pg.get_local_members(config.scope, {:node_id, config.node_id}) do
      cond do
        member? and pid not in members -> :ok = :pg.join(config.scope, @members, pid)
        not member? and pid in members -> :ok = :pg.leave(config.scope, @members, pid)
        true -> :ok
      end
    end

    refresh_view(config)
  end

  # The Membership process stores the view; other callers ask it to.
  defp refresh_view(config) do
    case :pg.get_local_members(config.scope, {:node_id, config.node_id}) do
      [pid | _] when pid == self() -> store_view(config)
      [pid | _] -> GenServer.call(pid, :refresh_view, config.op_timeout_ms)
      [] -> :ok
    end
  end

  @doc """
  Nodes currently running the instance over this store, including this one.
  Nodes that announced another store identity are left out (design §8).
  """
  @spec nodes(Config.t()) :: [node]
  def nodes(config) do
    view(config).members
  end

  @doc "Like `nodes/1`, but reads the groups now, for a rare operation such as a move."
  @spec fresh_nodes(Config.t()) :: [node]
  def fresh_nodes(config), do: fresh_view(config).members

  @doc """
  Leaves the group of members, so other nodes stop preferring or forwarding to
  this one (design §8), and marks the node as drained: it refuses to wake
  cells. The group that maps a `node_id` to its node stays, so calls for cells
  that are still awake here keep reaching them. `join/1` undoes it.
  """
  @spec leave(Config.t()) :: :ok
  def leave(config) do
    put_state(config, :left)
    sync(config)
  end

  @doc """
  Joins the group of members, and ends the drain if the node left (`leave/1`).
  `:ok` if already a member.
  """
  @spec join(Config.t()) :: :ok
  def join(config) do
    put_state(config, :member)
    sync(config)
  end

  @doc """
  Whether this node left the group of members (`leave/1`) and has not joined
  again. A node that never joined (`join: false`) is not draining: it serves
  its own calls.
  """
  @spec draining?(Config.t()) :: boolean
  def draining?(config), do: state(config) == :left

  @doc "The node that runs the instance under `node_id`, if it is alive."
  @spec node_of(Config.t(), String.t()) :: {:ok, node} | :error
  def node_of(config, node_id) do
    case :pg.get_members(config.scope, {:node_id, node_id}) do
      [pid | _] -> {:ok, node(pid)}
      [] -> :error
    end
  end

  @doc "Preferred node of `cell`: rendezvous hashing over the live nodes."
  @spec preferred(Config.t(), String.t()) :: node
  def preferred(config, cell),
    do: config |> nodes() |> Enum.max_by(&{:erlang.phash2({cell, &1}, 4_294_967_296), &1})

  @doc "Cached owner of `cell`, if any."
  @spec hint(Config.t(), String.t()) :: {:ok, node} | :error
  def hint(config, cell) do
    case :ets.lookup(config.hints, cell) do
      [{_, node}] -> {:ok, node}
      [] -> :error
    end
  end

  @doc """
  Remembers `node` as the owner of `cell`. A hint that is already stored is not
  written again; at `hint_limit` entries a sixteenth of them is evicted.
  """
  @spec put_hint(Config.t(), String.t(), node) :: true
  def put_hint(config, cell, node),
    do: Alambique.BoundedTable.put(config.hints, config.hint_limit, {cell, node})

  @spec delete_hint(Config.t(), String.t()) :: true
  def delete_hint(config, cell), do: :ets.delete(config.hints, cell)
end
