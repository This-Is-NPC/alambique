defmodule Alambique.Router do
  @moduledoc false
  # Finds the node that serves a cell, and wakes the cell where it belongs
  # (design §8).
  #
  # `call/4` runs on the node that received the call; `remote/5` is what it
  # runs on another node through `:erpc`. Both end in `serve/6`:
  #
  #   1. A local process answers the call.
  #   2. On the node that received the call, an owner hint or the preferred
  #      node gets it next.
  #   3. A node that is asked and has no process reads the lease. A live owner
  #      gets the call; a free lease means waking the cell on the preferred
  #      node, or here when this node is the preferred one or the hops ran out.
  #
  # Every function returns `{served_by, reply}`, so that the first node can
  # remember which node actually served the cell.

  require Logger

  alias Alambique.{
    Admission,
    Cell,
    Config,
    Fault,
    Lease,
    Membership,
    Placement,
    Replication,
    Store.Metered,
    Telemetry
  }

  # `idempotent?` marks a write that may be repeated when the outcome of a
  # forwarded call is unknown (design §8).
  @spec call(atom, String.t(), term, timeout | nil, boolean) :: term
  def call(name, cell, request, timeout, idempotent? \\ false) do
    request = if idempotent?, do: {:idempotent, request}, else: request
    {_served_by, reply} = route(name, cell, request, timeout)
    reply
  end

  @spec touch(atom, String.t(), timeout | nil) :: {:ok, node} | {:error, term}
  def touch(name, cell, timeout) do
    case route(name, cell, :touch, timeout) do
      {served_by, :ok} -> {:ok, served_by}
      {_, {:error, _} = error} -> error
    end
  end

  @spec take_over(atom, String.t(), term, keyword) :: {:ok, node} | {:error, term}
  def take_over(name, cell, reason, opts) do
    config = Config.fetch!(name)

    with :ok <- Placement.validate_id(config, cell),
         true <- not Membership.draining?(config) or {:error, :draining} do
      timeout = opts[:timeout] || config.call_timeout_ms
      args = {config, cell, :touch, timeout, 0, config.routing.attempts}

      case Cell.whereis(config, cell) do
        pid when is_pid(pid) ->
          {:ok, node()}

        nil ->
          Logger.info("Alambique takes over cell #{cell}: #{inspect(reason)}")
          takeover = %{reason: reason, epoch: opts[:epoch]}

          case start_cell(args, {config, cell, takeover: takeover}) do
            {_, :ok} ->
              # Whatever node the hint named, the cell is here now.
              Membership.delete_hint(config, cell)
              {:ok, node()}

            {_, {:error, _} = error} ->
              error
          end
      end
    end
  end

  # Runs the call and remembers which node served it.
  defp route(name, cell, request, timeout) do
    config = Config.fetch!(name)

    case Placement.validate_id(config, cell) do
      :ok ->
        timeout = timeout || config.call_timeout_ms

        {served_by, _} =
          routed =
          serve(config, cell, request, timeout, config.routing.hops, config.routing.attempts)

        if served_by not in [nil, node()], do: Membership.put_hint(config, cell, served_by)
        routed

      {:error, _} = error ->
        {nil, error}
    end
  end

  # `source` is `:lease` (read the lease), `:hint` (owner-hint cache first) or
  # `:peers` (the registries of the member nodes, no store request).
  @spec owner(atom, String.t(), :lease | :hint | :peers) :: {:ok, node} | :none | {:error, term}
  def owner(name, cell, source \\ :lease) do
    config = Config.fetch!(name)

    with :ok <- Placement.validate_id(config, cell) do
      case Cell.whereis(config, cell) do
        pid when is_pid(pid) -> {:ok, node()}
        nil when source == :hint -> hinted_owner(config, cell)
        nil when source == :peers -> peer_owner(config, cell)
        nil -> lease_owner(config, cell)
      end
    end
  end

  # Asks the other members in parallel whether their registry holds the cell.
  # A node that does not answer within the call timeout counts as not holding it.
  defp peer_owner(config, cell) do
    peers = Membership.nodes(config) -- [node()]

    results =
      :erpc.multicall(peers, __MODULE__, :holds?, [config.name, cell], config.call_timeout_ms)

    peers
    |> Enum.zip(results)
    |> Enum.find_value(:none, fn
      {peer, {:ok, true}} -> {:ok, peer}
      _ -> nil
    end)
  end

  @doc false
  # Whether a process of this node holds the cell; the `:erpc` target of `peer_owner/2`.
  @spec holds?(atom, String.t()) :: boolean
  def holds?(name, cell) do
    case Config.fetch(name) do
      {:ok, config} -> Cell.whereis(config, cell) != nil
      :error -> false
    end
  end

  # A hint names a member node or is dropped; a lease answer fills the hint.
  defp hinted_owner(config, cell) do
    case Membership.hint(config, cell) do
      {:ok, node} ->
        if node in Membership.nodes(config) do
          {:ok, node}
        else
          Membership.delete_hint(config, cell)
          read_owner(config, cell)
        end

      :error ->
        read_owner(config, cell)
    end
  end

  defp read_owner(config, cell) do
    case lease_owner(config, cell) do
      {:ok, node} = answer ->
        Membership.put_hint(config, cell, node)
        answer

      other ->
        other
    end
  end

  defp lease_owner(config, cell) do
    case read_lease_of(config, cell) do
      {:error, reason} ->
        {:error, {:lease_unreadable, reason}}

      %{answer: :none} ->
        :none

      %{answer: {:ok, lease}} ->
        with true <- held_elsewhere?(config, lease) || :none do
          case Membership.node_of(config, lease.owner, lease.incarnation) do
            {:ok, node} -> {:ok, node}
            :error -> {:error, {:unavailable, lease.owner, lease.expires_at_ms}}
          end
        end
    end
  end

  @spec move(atom, String.t(), node, timeout | nil) :: {:ok, node} | {:error, term}
  def move(name, cell, target, timeout) do
    config = Config.fetch!(name)
    timeout = timeout || config.call_timeout_ms

    with :ok <- Placement.validate_id(config, cell),
         true <- target in Membership.fresh_nodes(config) or {:error, :unknown_node},
         false <- owner(name, cell) == {:ok, target},
         :ok <- call(name, cell, :hibernate, timeout) do
      # Hops 0: the target wakes the cell itself instead of forwarding again.
      case :erpc.call(
             target,
             __MODULE__,
             :remote,
             [name, cell, :touch, timeout, 0],
             timeout + config.routing.slack_ms
           ) do
        {^target, :ok} ->
          Membership.put_hint(config, cell, target)
          {:ok, target}

        {_, {:error, _} = error} ->
          error
      end
    else
      true -> {:ok, target}
      {:error, _} = error -> error
    end
  catch
    :error, {:erpc, reason} -> {:error, {:forward_failed, reason}}
  end

  @doc false
  # An instance that stopped while the VM is up refuses like a draining node:
  # the call did not run.
  def remote(name, cell, request, timeout, hops) do
    case Config.fetch(name) do
      {:ok, config} -> serve_peer(config, cell, request, timeout, hops)
      :error -> {nil, {:error, :draining}}
    end
  end

  defp serve_peer(config, cell, request, timeout, hops) do
    # A peer may send any id: it is checked as for a local call.
    case Placement.validate_id(config, cell) do
      :ok ->
        reply = serve(config, cell, request, timeout, hops, config.routing.attempts)
        Fault.point(:router_after_serve, %{cell: cell, request: kind(request)})
        reply

      {:error, _} = error ->
        {nil, error}
    end
  end

  defp kind({:idempotent, request}), do: kind(request)
  defp kind(request) when is_tuple(request), do: elem(request, 0)
  defp kind(request), do: request

  defp serve(config, cell, request, timeout, hops, attempts) do
    args = {config, cell, request, timeout, hops, attempts}

    case Cell.find(config, cell) do
      {pid, load} -> deliver(pid, load, args)
      nil when hops == config.routing.hops -> first_hop(args)
      nil -> read_lease(args)
    end
  end

  # The node that received the call sends it to the hinted or preferred node.
  defp first_hop({config, cell, _, _, _, _} = args) do
    target =
      case Membership.hint(config, cell) do
        {:ok, node} -> node
        :error -> Membership.preferred(config, cell)
      end

    if target == node() do
      read_lease(args)
    else
      case forward(target, args) do
        {:error, :noconnection} -> again(args, {:error, {:forward_failed, :noconnection}})
        {:error, reason} -> {nil, {:error, reason}}
        {:ok, answer} -> answer
      end
    end
  end

  defp read_lease({config, cell, _, _, _, _} = args) do
    case read_lease_of(config, cell) do
      {:error, reason} ->
        {nil, {:error, {:lease_unreadable, reason}}}

      %{answer: :none} = seen ->
        free(args, seen)

      %{answer: {:ok, lease}} = seen ->
        if held_elsewhere?(config, lease),
          do: owned(lease, args),
          else: free(args, seen)
    end
  end

  # The lease lives in the location of the cell. A wake on this node continues
  # from the answer: it reuses it for the same location, and adds its requests
  # to the count of this one.
  defp read_lease_of(config, cell) do
    trips = Metered.new()

    with {:ok, location} <- Placement.locate(config, cell),
         ctx =
           config |> Config.lease_ctx(location) |> Map.update!(:store, &Metered.wrap(&1, trips)),
         answer when answer == :none or elem(answer, 0) == :ok <- Lease.read(ctx, cell),
         do: %{location: location, answer: answer, trips: trips}
  end

  # A lease of this instance (node id and incarnation) while no process of this
  # node holds the cell is a leftover of a process that died; waking the cell
  # reclaims it. The same node id with another incarnation is another holder.
  defp held_elsewhere?(config, lease) do
    not lease.released and not Lease.held_by?(lease, config.node_id, config.incarnation) and
      clock_ms(config) <= lease.expires_at_ms + config.skew_margin_ms
  end

  defp owned(lease, {config, _cell, _, _, hops, _} = args) do
    case Membership.node_of(config, lease.owner, lease.incarnation) do
      {:ok, node} when hops > 0 ->
        case forward(node, args) do
          {:ok, {nil, {:error, {:owned, _}}}} ->
            again(args, {:error, {:owned, node}})

          {:ok, answer} ->
            answer

          {:error, :noconnection} ->
            {nil, {:error, {:unavailable, lease.owner, lease.expires_at_ms}}}

          {:error, reason} ->
            {nil, {:error, reason}}
        end

      {:ok, node} ->
        {nil, {:error, {:owned, node}}}

      :error ->
        {nil, {:error, {:unavailable, lease.owner, lease.expires_at_ms}}}
    end
  end

  defp free({_, _, :hibernate, _, _, _}, _seen), do: {nil, :ok}

  # An asleep cell is not woken to say where its chain ends.
  defp free({config, cell, :position, _, _, _}, _seen), do: {nil, stored_position(config, cell)}
  defp free({_, _, {:migrate, false}, _, _, _}, _seen), do: {nil, {:ok, :asleep}}
  defp free({_, _, {:checkpoint, _}, _, _, _}, _seen), do: {nil, :asleep}

  defp free({config, cell, _, _, hops, _} = args, seen) do
    preferred = Membership.preferred(config, cell)

    if preferred != node() and hops > 0 do
      case forward(preferred, args) do
        # The preferred node left the members after we chose it.
        {:ok, {nil, {:error, :draining}}} -> again(args, {:error, :draining})
        {:ok, answer} -> answer
        {:error, :noconnection} -> wake(args, seen)
        {:error, reason} -> {nil, {:error, reason}}
      end
    else
      wake(args, seen)
    end
  end

  defp stored_position(config, cell) do
    with {:ok, location} <- Placement.locate(config, cell),
         {:ok, head} <- Replication.head(Config.repl_ctx(config, location), cell) do
      {:ok,
       case head do
         nil ->
           %{txid: 0, epoch: 0, data: 0, awake: false}

         %{txid: txid, epoch: epoch, data: data} ->
           %{txid: txid, epoch: epoch, data: data, awake: false}
       end}
    end
  end

  # `seen` is the lease this node just read, with its location; the new cell
  # takes the lease from it instead of reading it again.
  defp wake({config, cell, _, _, _, _} = args, seen) do
    if Membership.draining?(config),
      do: {nil, {:error, :draining}},
      else: start_cell(args, {config, cell, lease_seen: seen})
  end

  # A wake takes a slot (`awake.max_wakes`) that the cell gives back when its
  # wake ends; a start that created no cell gives it back here. Without a free
  # slot it waits in the queue, never longer than its caller does.
  defp start_cell({config, cell, _, timeout, _, _} = args, child_arg) do
    with nil <- Admission.failure(config, cell),
         :ok <- make_room(config),
         :ok <- Admission.begin_wake(config, timeout) do
      Fault.point(:wake_slot_taken, %{cell: cell})

      case DynamicSupervisor.start_child(config.supervisor, {Cell, with_holder(child_arg)}) do
        {:ok, pid} ->
          deliver(pid, config |> Cell.find(cell) |> load_of(), args)

        {:error, {:already_started, pid}} ->
          Admission.release(config)
          deliver(pid, config |> Cell.find(cell) |> load_of(), args)

        {:error, reason} ->
          Admission.release(config)

          {nil,
           {:error, if(reason == :max_children, do: :overloaded, else: {:wake_failed, reason})}}
      end
    else
      {:error, _} = error -> {nil, error}
    end
  end

  # The cell takes over the slot this process holds.
  defp with_holder({config, cell, opts}), do: {config, cell, [{:holder, self()} | opts]}

  # A cell wakes only while there is room: while the awake cells fill
  # `awake.max_cells`, or their memory is over `awake.max_bytes`, the least
  # recently used cells without waiting callers hibernate, and when none is
  # left to hibernate the wake is refused.
  defp make_room(config) do
    if Admission.crowded?(config),
      do: evict_lru(config, Admission.least_recently_used(config)),
      else: :ok
  end

  defp evict_lru(config, []),
    do: if(Admission.crowded?(config), do: {:error, :overloaded}, else: :ok)

  defp evict_lru(config, [{pid, load, id} | rest]) do
    cond do
      not Admission.crowded?(config) ->
        :ok

      Cell.pending(pid, load) == {:ok, 0} ->
        evict(config, pid, id)
        evict_lru(config, rest)

      true ->
        evict_lru(config, rest)
    end
  end

  defp evict(config, pid, id) do
    with :ok <- hibernate_pid(pid, config.call_timeout_ms) do
      Telemetry.emit([:admission, :evict], %{bytes: Admission.bytes(config)}, %{
        cell: id,
        max_cells: config.awake.max_cells,
        max_bytes: config.awake.max_bytes
      })
    end
  end

  # The cell may have stopped between the lookup and the call. A call that a
  # stopped cell never ran is looked up again; a crash during the call leaves
  # it unknown whether it ran, so that is reported and never retried.
  defp deliver(pid, load, {config, _, request, timeout, _, _} = args) do
    case admit(config, load, request) do
      :ok -> {node(), call_cell(pid, plain(request), timeout)}
      :busy -> {node(), {:error, :busy}}
    end
  catch
    :exit, {reason, {GenServer, :call, _}} ->
      case reason do
        :noproc -> again(args, {:error, :unavailable})
        :normal -> after_exit(pid, args, {:error, :unavailable})
        {:shutdown, {:wake_failed, {:owned, _, _} = why}} -> after_exit(pid, args, {:error, why})
        {:shutdown, {:wake_failed, why}} -> wake_failed(args, why)
        {:shutdown, why} -> after_exit(pid, args, {:error, why})
        :timeout -> {nil, {:error, :timeout}}
        other -> {nil, {:error, {:cell_down, other}}}
      end
  end

  # A cell with `awake.max_pending` callers waiting refuses more, so its
  # mailbox does not grow past the limit while the callers time out. The
  # place is taken atomically (`Cell.admit/2`) and the cell gives it back.
  # A hibernation is not counted. `load` is the counter of the registry lookup
  # the caller made, `nil` when the cell is gone.
  defp admit(_config, _load, :hibernate), do: :ok
  defp admit(_config, nil, _request), do: :ok
  defp admit(config, load, _request), do: Cell.admit(load, config.awake.max_pending)

  defp load_of({_pid, load}), do: load
  defp load_of(nil), do: nil

  defp wake_failed({config, cell, _, _, _, _}, why) do
    Admission.failed(config, cell, why)
    {nil, {:error, why}}
  end

  defp plain({:idempotent, request}), do: request
  defp plain(request), do: request

  # The cell is on its way out: look again once it is gone from the registry.
  defp after_exit(pid, {_, _, _, timeout, _, _} = args, last) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      timeout -> Process.demonitor(ref, [:flush])
    end

    again(args, last)
  end

  @doc false
  # Hibernates the cell if this node holds it; never routes elsewhere.
  @spec hibernate_local(Config.t(), String.t(), timeout) :: :ok | {:error, term}
  def hibernate_local(config, cell, timeout) do
    case Cell.whereis(config, cell) do
      nil -> :ok
      pid -> hibernate_pid(pid, timeout)
    end
  end

  defp hibernate_pid(pid, timeout) do
    call_cell(pid, :hibernate, timeout)
  catch
    :exit, {reason, {GenServer, :call, _}} when reason in [:noproc, :normal] -> :ok
    :exit, {{:shutdown, why}, {GenServer, :call, _}} -> {:error, why}
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
    :exit, other -> {:error, {:cell_down, other}}
  end

  # A hibernating cell answers before it exits; wait for the exit so that the
  # caller sees the cell asleep.
  defp call_cell(pid, :hibernate, timeout) do
    ref = Process.monitor(pid)

    try do
      with :ok <- GenServer.call(pid, :hibernate, timeout) do
        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          timeout -> {:error, :timeout}
        end
      end
    after
      Process.demonitor(ref, [:flush])
    end
  end

  # The cell drops the call if it reaches it after the caller gave up.
  defp call_cell(pid, request, :infinity), do: GenServer.call(pid, request, :infinity)

  defp call_cell(pid, request, timeout) do
    at = System.monotonic_time(:millisecond) + timeout
    GenServer.call(pid, {:deadline, at, request}, timeout)
  end

  defp again({config, cell, request, timeout, hops, attempts}, {:error, _} = last) do
    if attempts > 1,
      do: serve(config, cell, request, timeout, hops, attempts - 1),
      else: {nil, last}
  end

  # Runs the call on another node. `{:ok, {served_by, reply}}` is that node's
  # answer. `{:error, :noconnection}` means the call may be sent elsewhere: it
  # was not sent, or it is safe to repeat. `{:error, :outcome_unknown}` means a
  # write was sent and the connection failed before the answer, so it may have
  # run (design §8).
  defp forward(target, {config, cell, _, _, _, _} = args) do
    cond do
      target in Membership.foreign_nodes(config) ->
        Membership.delete_hint(config, cell)
        {:error, {:foreign_store, target}}

      true ->
        send_forward(target, args)
    end
  end

  defp send_forward(target, {config, cell, request, timeout, hops, _}) do
    started = System.monotonic_time()
    args = [config.name, cell, request, timeout, hops - 1]
    {sent, outcome} = dispatch(target, args, timeout)

    outcome =
      if sent and outcome == {:error, :noconnection},
        do: lost(target, cell, request),
        else: outcome

    with {:error, reason} when reason in [:noconnection, :outcome_unknown] <- outcome,
         do: Membership.delete_hint(config, cell)

    result =
      case outcome do
        {:ok, _} -> :ok
        {:error, reason} when is_atom(reason) -> reason
        {:error, _} -> :error
      end

    Telemetry.span_stop([:router, :forward], started, %{}, %{
      cell: cell,
      target: target,
      result: result,
      sent: sent
    })

    outcome
  end

  defp lost(target, cell, request) do
    Fault.point(:router_forward_lost, %{cell: cell, target: target, request: kind(request)})
    if repeatable?(request), do: {:error, :noconnection}, else: {:error, :outcome_unknown}
  end

  # The call counts as sent once `:erpc.call` was issued to a connected node;
  # a connection lost between the check and the send is reported as sent.
  defp dispatch(target, args, timeout) do
    if target in Node.list() or Node.connect(target) == true do
      {true, erpc(target, args, timeout)}
    else
      {false, {:error, :noconnection}}
    end
  end

  defp erpc(target, [name | _] = args, timeout) do
    slack = Config.fetch!(name).routing.slack_ms
    {:ok, :erpc.call(target, __MODULE__, :remote, args, timeout + slack)}
  catch
    :error, {:erpc, :noconnection} -> {:error, :noconnection}
    :error, {:erpc, :timeout} -> {:error, :timeout}
    kind, reason -> {:error, {:forward_failed, {kind, reason}}}
  end

  # Requests that give the same result when they run twice.
  defp repeatable?({:idempotent, _}), do: true
  # Migrating again runs only the migrations still pending.
  defp repeatable?({:migrate, _wake}), do: true
  defp repeatable?({:checkpoint, _deadline}), do: true
  defp repeatable?({kind, _, _, _}), do: kind in [:query, :checked_query]
  defp repeatable?(request), do: request in [:touch, :hibernate]

  defp clock_ms(%{clock: {clock, clock_config}}), do: clock.now_ms(clock_config)
end
