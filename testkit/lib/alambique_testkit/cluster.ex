defmodule AlambiqueTestkit.Cluster do
  @moduledoc """
  A cluster of peer nodes for failover tests (design §11).

  Every peer runs one `Alambique` instance over one `AlambiqueTestkit.MemoryStore`
  that lives on the test node. Peers are named by atoms (`:n1`, `:n2`, ...);
  `node/2` gives the node name.

      {:ok, cluster} = Cluster.start_link(names: [:n1, :n2, :n3], ttl_ms: 2_000, skew_margin_ms: 400)
      {:ok, _} = Cluster.call(cluster, :n1, Alambique, :execute, [Cluster.instance(cluster), "c", "CREATE TABLE t (x)"])
      Cluster.kill(cluster, :n1)

  Functions run on a peer through `call/6` must come from modules the peer can
  load, which means modules compiled to disk (the library and `test/support`),
  not modules defined inside a test file.

  ## What can go wrong

    * `kill/2` halts the VM at once, without hibernating cells, and
      `restart/2` starts it again with the same node name and data directory.
    * `partition_store/2` makes every store request of one node fail, and
      `inject/2` adds any other `MemoryStore` fault.
    * `partition_nodes/3` cuts the distribution link between two peers and
      keeps it cut: each side changes the cookie it uses for the other, so a
      reconnect attempt fails the handshake. `heal_nodes/3` undoes it.
    * `set_clock_offset/3` skews the clock one node gives to leases.

  ## Telemetry

  Every `[:alambique, :cell | :router | :lease | :replication | :feed, ...]` event of
  every peer is forwarded to the cluster; `events/1` returns them as
  `%{node: name, event: [atom], measurements: map, meta: map, at: microseconds}`
  in arrival order. `at` is the operating system clock, which the peers share.
  """

  use GenServer

  alias AlambiqueTestkit.MemoryStore

  @forwarded [
    [:alambique, :cell, :wake],
    [:alambique, :cell, :hibernate],
    [:alambique, :cell, :commit],
    [:alambique, :cell, :migrate],
    [:alambique, :cell, :stop],
    [:alambique, :cell, :crash_recorded],
    [:alambique, :cell, :quarantined],
    [:alambique, :cell, :relocate],
    [:alambique, :cell, :cleanup],
    [:alambique, :router, :forward],
    [:alambique, :lease, :acquire],
    [:alambique, :lease, :take_over],
    [:alambique, :lease, :renew],
    [:alambique, :lease, :lost],
    [:alambique, :replication, :fenced],
    [:alambique, :feed, :publish]
  ]

  @partitioned_cookie :alambique_partitioned

  # Client

  @doc """
  Starts the cluster and waits until every peer runs its instance.

  Options:

    * `:names` - peer names (default `[:n1, :n2, :n3]`)
    * `:instance` - instance name (default `:cells`)
    * `:ttl_ms`, `:skew_margin_ms`, `:idle_timeout_ms`, `:op_timeout_ms`,
      `:prefix`, `:migrations`, `:after_ack` - passed to the instances
    * `:placement` - function of the shared store's `{module, config}` that
      returns the `{module, opts}` placement of the instances; without it they
      use the shared store and `:prefix`
    * `:instance_opts` - any other instance option
    * `:not_joined` - peer names that start with `join: false`
    * `:peer_args` - extra command-line arguments for every peer VM, for
      example `["+SDio", "1"]` to give it one dirty I/O scheduler
  """
  @spec start_link(keyword) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts)

  @doc "Stops every peer, the store and the data directories."
  @spec stop(GenServer.server()) :: :ok
  def stop(cluster), do: GenServer.stop(cluster)

  @doc "Name of the instance that every peer runs."
  @spec instance(GenServer.server()) :: atom
  def instance(cluster), do: GenServer.call(cluster, :instance)

  @doc "`{module, config}` of the shared store."
  @spec store(GenServer.server()) :: {module, MemoryStore.config()}
  def store(cluster), do: GenServer.call(cluster, :store)

  @doc "Peer names."
  @spec names(GenServer.server()) :: [atom]
  def names(cluster), do: GenServer.call(cluster, :names)

  @doc "Node name of a peer, which changes when it is restarted."
  @spec node(GenServer.server(), atom) :: node
  def node(cluster, name), do: GenServer.call(cluster, {:node, name})

  @doc "Whether the peer's VM is running."
  @spec alive?(GenServer.server(), atom) :: boolean
  def alive?(cluster, name), do: GenServer.call(cluster, {:alive?, name})

  @doc """
  Runs `module.function(args)` on a peer. A dead or unreachable peer gives
  `{:error, {:erpc, reason}}`; otherwise the function's result is returned as
  `{:ok, result}`.
  """
  @spec call(GenServer.server(), atom, module, atom, list, timeout) ::
          {:ok, term} | {:error, term}
  def call(cluster, name, module, function, args, timeout \\ 30_000) do
    node = node(cluster, name)

    try do
      {:ok, :erpc.call(node, module, function, args, timeout)}
    catch
      :error, {:erpc, reason} -> {:error, {:erpc, reason}}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  @doc "Halts the peer's VM without any shutdown."
  @spec kill(GenServer.server(), atom) :: :ok
  def kill(cluster, name), do: GenServer.call(cluster, {:kill, name})

  @doc "Starts a killed peer again with the same name and data directory."
  @spec restart(GenServer.server(), atom) :: :ok
  def restart(cluster, name), do: GenServer.call(cluster, {:restart, name}, 60_000)

  @doc "Every store request of the peer fails with `{:error, :partitioned}` until healed."
  @spec partition_store(GenServer.server(), atom) :: :ok
  def partition_store(cluster, name), do: GenServer.call(cluster, {:partition_store, name})

  @doc "Ends `partition_store/2`."
  @spec heal_store(GenServer.server(), atom) :: :ok
  def heal_store(cluster, name), do: GenServer.call(cluster, {:heal_store, name})

  @doc """
  Injects a `MemoryStore` fault (`MemoryStore.inject/2` options) for requests of
  the peer. Removed by `heal_all/1`.
  """
  @spec inject(GenServer.server(), atom, keyword) :: :ok
  def inject(cluster, name, opts), do: GenServer.call(cluster, {:inject, name, opts})

  @doc "Cuts the link between two peers until `heal_nodes/3`."
  @spec partition_nodes(GenServer.server(), atom, atom) :: :ok
  def partition_nodes(cluster, a, b), do: GenServer.call(cluster, {:partition_nodes, a, b})

  @doc "Restores the link between two peers."
  @spec heal_nodes(GenServer.server(), atom, atom) :: :ok
  def heal_nodes(cluster, a, b), do: GenServer.call(cluster, {:heal_nodes, a, b})

  @doc "Moves the peer's lease clock `ms` away from the real time."
  @spec set_clock_offset(GenServer.server(), atom, integer) :: :ok
  def set_clock_offset(cluster, name, ms), do: GenServer.call(cluster, {:offset, name, ms})

  @doc "Removes every store fault and restores every link; dead peers stay dead."
  @spec heal_all(GenServer.server()) :: :ok
  def heal_all(cluster), do: GenServer.call(cluster, :heal_all)

  @doc "Telemetry events of all peers so far."
  @spec events(GenServer.server()) :: [map]
  def events(cluster), do: GenServer.call(cluster, :events)

  # Server

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    ensure_distributed!()

    names = Keyword.get(opts, :names, [:n1, :n2, :n3])
    {:ok, store_pid} = MemoryStore.start_link()
    # The OS pid keeps node names and directories apart between test runs on
    # one machine, for example from several checkouts at once.
    run_id = "#{System.pid()}_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "alambique-cluster-#{run_id}")
    File.mkdir_p!(dir)

    store = MemoryStore.config(store_pid)

    state = %{
      store_pid: store_pid,
      store: store,
      placement: placement_option(opts, store),
      dir: dir,
      instance: Keyword.get(opts, :instance, :cells),
      opts: opts,
      cookie: Node.get_cookie(),
      id: run_id,
      peers: %{},
      offsets: %{},
      cuts: MapSet.new(),
      faults: %{},
      events: []
    }

    {:ok, Enum.reduce(names, state, fn name, acc -> start_peer(acc, name) end)}
  end

  @impl true
  def handle_call(:instance, _from, state), do: {:reply, state.instance, state}
  def handle_call(:store, _from, state), do: {:reply, {MemoryStore, state.store}, state}
  def handle_call(:names, _from, state), do: {:reply, Map.keys(state.peers), state}
  def handle_call({:node, name}, _from, state), do: {:reply, state.peers[name].node, state}

  def handle_call({:alive?, name}, _from, state),
    do: {:reply, state.peers[name].peer != nil, state}

  def handle_call(:events, _from, state), do: {:reply, Enum.reverse(state.events), state}

  def handle_call({:kill, name}, _from, state) do
    %{peer: peer, node: node} = state.peers[name]
    if peer, do: halt(peer, node)
    {:reply, :ok, put_in(state.peers[name].peer, nil)}
  end

  def handle_call({:restart, name}, _from, state) do
    {:reply, :ok, start_peer(state, name)}
  end

  def handle_call({:partition_store, name}, _from, state) do
    id = MemoryStore.partition(state.store, state.peers[name].node)
    {:reply, :ok, put_in(state.faults[{:partition, name}], id)}
  end

  def handle_call({:heal_store, name}, _from, state) do
    {id, faults} = Map.pop(state.faults, {:partition, name})
    if id, do: MemoryStore.remove(state.store, id)
    {:reply, :ok, %{state | faults: faults}}
  end

  def handle_call({:inject, name, opts}, _from, state) do
    id = MemoryStore.inject(state.store, Keyword.put(opts, :node, state.peers[name].node))
    {:reply, :ok, put_in(state.faults[{:fault, make_ref()}], id)}
  end

  def handle_call({:partition_nodes, a, b}, _from, state) do
    {:reply, :ok, cut(state, a, b)}
  end

  def handle_call({:heal_nodes, a, b}, _from, state) do
    {:reply, :ok, mend(state, a, b)}
  end

  def handle_call({:offset, name, ms}, _from, state) do
    state = put_in(state.offsets[name], ms)
    if state.peers[name].peer, do: set_offset(state.peers[name], ms)
    {:reply, :ok, state}
  end

  def handle_call(:heal_all, _from, state) do
    MemoryStore.clear(state.store)

    state =
      state.cuts
      |> Enum.reduce(%{state | faults: %{}}, fn {a, b}, acc -> mend(acc, a, b) end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:telemetry, name, event, measurements, meta, at}, state) do
    entry = %{node: name, event: event, measurements: measurements, meta: meta, at: at}
    {:noreply, %{state | events: [entry | state.events]}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    for {_name, %{peer: peer, node: node}} <- state.peers, peer != nil do
      try do
        if Process.whereis(:cover_server), do: :cover.flush(node)
        :peer.stop(peer)
      catch
        _, _ -> :ok
      end
    end

    File.rm_rf(state.dir)
  end

  # Peers

  defp start_peer(state, name) do
    old_node = state.peers[name][:node]

    peer_name =
      if old_node,
        do: old_node |> Atom.to_string() |> String.split("@") |> hd(),
        else: "alambique_#{state.id}_#{name}"

    extra_args =
      state.opts
      |> Keyword.get(:peer_args, [])
      |> Enum.map(&to_charlist/1)

    args =
      extra_args ++
        Enum.flat_map(:code.get_path(), &[~c"-pz", &1]) ++ [~c"-connect_all", ~c"false"]

    {peer, node} =
      start_with_retry(%{
        name: String.to_charlist(peer_name),
        host: ~c"127.0.0.1",
        longnames: true,
        args: args,
        wait_boot: 30_000
      })

    config = %{
      peer: peer,
      node: node
    }

    state = put_in(state.peers[name], config)
    if Process.whereis(:cover_server), do: :cover.start(node)
    boot(state, config)

    for {other, %{peer: other_peer, node: other_node}} <- state.peers,
        other != name,
        other_peer != nil,
        not cut?(state, name, other) do
      :erpc.call(node, Node, :connect, [other_node])
    end

    state
  end

  # A restarted VM reuses its name; the old registration may take a moment to go.
  defp start_with_retry(opts, attempts \\ 50) do
    case :peer.start(opts) do
      {:ok, peer, node} ->
        {peer, node}

      {:error, _} when attempts > 1 ->
        Process.sleep(100)
        start_with_retry(opts, attempts - 1)

      {:error, reason} ->
        raise "could not start peer: #{inspect(reason)}"
    end
  end

  defp boot(state, %{node: node}) do
    name = Enum.find_value(state.peers, fn {name, c} -> c.node == node && name end)

    instance_opts =
      Keyword.merge(
        [
          name: state.instance,
          data_dir: Path.join(state.dir, node |> Atom.to_string() |> String.split("@") |> hd()),
          clock: {AlambiqueTestkit.Clock, nil},
          lease_ttl_ms: Keyword.get(state.opts, :ttl_ms, 2_000),
          skew_margin_ms: Keyword.get(state.opts, :skew_margin_ms, 400),
          idle_timeout_ms: Keyword.get(state.opts, :idle_timeout_ms, 60_000),
          op_timeout_ms: Keyword.get(state.opts, :op_timeout_ms, 1_500),
          migrations: Keyword.get(state.opts, :migrations, []),
          after_ack: Keyword.get(state.opts, :after_ack)
        ],
        Keyword.get(state.opts, :instance_opts, [])
      )
      |> Keyword.merge(if name in Keyword.get(state.opts, :not_joined, []), do: [join: false], else: [])
      |> Keyword.merge(state.placement)

    :ok = :erpc.call(node, __MODULE__.Boot, :start, [instance_opts, self(), name, @forwarded])
    set_offset(%{node: node}, Map.get(state.offsets, name, 0))
  end

  # The instances' placement: the function's, or the shared store with a prefix.
  defp placement_option(opts, store) do
    case Keyword.get(opts, :placement) do
      nil -> [store: {MemoryStore, store}, prefix: Keyword.get(opts, :prefix, "cluster")]
      fun -> [placement: fun.({MemoryStore, store})]
    end
  end

  defp set_offset(%{node: node}, ms),
    do: :erpc.call(node, AlambiqueTestkit.Clock, :set_offset, [ms])

  defp halt(peer, node) do
    :erpc.cast(node, :erlang, :halt, [1, [flush: false]])

    try do
      :peer.stop(peer)
    catch
      _, _ -> :ok
    end
  end

  # Links

  defp cut(state, a, b) do
    na = state.peers[a].node
    nb = state.peers[b].node

    for {from, to_node} <- [{na, nb}, {nb, na}], alive_node?(from) do
      :erpc.call(from, :erlang, :set_cookie, [to_node, @partitioned_cookie])
      :erpc.call(from, Node, :disconnect, [to_node])
    end

    %{state | cuts: MapSet.put(state.cuts, key(a, b))}
  end

  defp mend(state, a, b) do
    na = state.peers[a].node
    nb = state.peers[b].node

    for {from, to_node} <- [{na, nb}, {nb, na}], alive_node?(from) do
      :erpc.call(from, :erlang, :set_cookie, [to_node, state.cookie])
    end

    if alive_node?(na) and alive_node?(nb), do: :erpc.call(na, Node, :connect, [nb])
    %{state | cuts: MapSet.delete(state.cuts, key(a, b))}
  end

  defp cut?(state, a, b), do: MapSet.member?(state.cuts, key(a, b))
  defp key(a, b), do: if(a < b, do: {a, b}, else: {b, a})
  defp alive_node?(node), do: Node.ping(node) == :pong

  defp ensure_distributed! do
    unless Node.alive?() do
      System.cmd("epmd", ["-daemon"])
      {:ok, _} = Node.start(:"alambique_test_#{System.pid()}@127.0.0.1", :longnames)
    end
  end

  defmodule Boot do
    @moduledoc false
    # Runs on a peer: starts the application, the instance and the telemetry relay.

    def start(instance_opts, collector, name, events) do
      {:ok, _} = Application.ensure_all_started(:alambique)
      :logger.set_primary_config(:level, :error)

      :telemetry.attach_many(
        {__MODULE__, name},
        events,
        &__MODULE__.relay/4,
        {collector, name}
      )

      parent = self()

      spawn(fn ->
        send(
          parent,
          {:started, Supervisor.start_link([{Alambique, instance_opts}], strategy: :one_for_one)}
        )

        Process.sleep(:infinity)
      end)

      receive do
        {:started, {:ok, _}} -> :ok
      after
        30_000 -> raise "instance did not start"
      end
    end

    def relay(event, measurements, meta, {collector, name}),
      do:
        send(
          collector,
          {:telemetry, name, event, measurements, meta, System.os_time(:microsecond)}
        )
  end
end
