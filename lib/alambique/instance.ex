defmodule Alambique.Instance do
  @moduledoc false
  # Supervision tree of one instance (design §1). Children start in the order
  # they are needed and stop in reverse, so cells hibernate while the
  # registry, the group and the configuration are still there.

  use Supervisor

  alias Alambique.Cell.Local
  alias Alambique.Cell.LocalCache
  alias Alambique.{Config, Membership, Store}
  alias Alambique.Replication.PageArena

  require Logger

  # The store is checked and the data directory locked before the tree
  # starts, so a refused start touches no file (design §1).
  @spec start_link(Config.t()) :: Supervisor.on_start()
  def start_link(config) do
    PageArena.check_applied(config.name)

    with :ok <- check_stores(config),
         {:ok, lock} <- Alambique.DataDir.acquire(config.data_dir) do
      case Supervisor.start_link(__MODULE__, {config, lock}, name: config.name) do
        {:ok, pid} ->
          {:ok, pid}

        error ->
          Alambique.DataDir.release(lock)
          error
      end
    end
  end

  # One check per distinct store: locations that share one store share its
  # behaviour. Only a store that fails it refuses the start; one that cannot be
  # reached is left to the breaker, as at any other time.
  defp check_stores(%{check_store: false}), do: :ok

  defp check_stores(%{placement: {placement, opts}} = config) do
    {:ok, locations} = placement.locations(opts)

    locations
    |> Enum.uniq_by(&Alambique.Store.location(&1.store))
    |> Enum.find_value(:ok, &check_store(config, &1))
  end

  defp check_store(config, location) do
    case Store.Conformance.check(location.store, location.prefix, config.op_timeout_ms) do
      :ok ->
        nil

      {:error, {:not_conformant, failed}} ->
        {:error, {:store_not_conformant, location.id, failed}}

      {:inconclusive, reason} ->
        Logger.warning(
          "Alambique #{inspect(config.name)}: could not check the conditional writes " <>
            "of location #{location.id}: #{inspect(reason)}"
        )

        nil
    end
  end

  @impl true
  def init({config, lock}) do
    # A replica file never outlives its process (design §21).
    _ = File.rm_rf!(Alambique.Replica.dir(config))
    Local.clean_work_dirs(config)
    Config.put(config)
    Membership.init_state(config)

    Supervisor.init(children(config, lock), strategy: :rest_for_one)
  end

  # The compactor and the node registry come last, so that a crash of either
  # restarts nothing else; they stop first, so the node leaves the registry
  # before its cells hibernate.
  defp children(config, lock) do
    # Last, so that a compactor crash restarts nothing else. It stops first.
    # Last, like the compactor: a crash restarts nothing else, and it stops
    # first, so the node leaves the registry before its cells hibernate.
    base(config, lock) ++
      tracker(config) ++
      cells(config) ++
      only_if(config.feed.retention_ms, fn -> [{Alambique.Feed.Janitor, config}] end) ++
      only_if(config.compaction, fn -> [compactor(config)] end) ++
      only_if(config.sweeper, fn -> [sweeper(config)] end) ++
      Alambique.Trash.children(config) ++
      Alambique.Scrub.children(config) ++
      Alambique.WalBudget.children(config) ++
      LocalCache.children(config) ++
      replicas(config) ++
      only_if(config.nodes, fn -> [Alambique.NodeRegistry.child_spec(config)] end)
  end

  defp base(config, lock) do
    [
      %{id: DataDir, start: {Alambique.DataDir, :start_link, [lock]}},
      %{id: :pg, start: {:pg, :start_link, [config.scope]}},
      %{id: Live, start: {Alambique.Live, :start_link, [config]}},
      {Registry, keys: :unique, name: config.registry, listeners: [config.live]}
    ]
  end

  defp cells(config) do
    [
      %{id: Wakes, start: {Alambique.Admission.Wakes, :start_link, [config]}},
      %{id: Membership, start: {Membership, :start_link, [config]}},
      {DynamicSupervisor,
       strategy: :one_for_one, name: config.supervisor, max_children: config.awake.max_cells},
      # Stops before the supervisor: leaves the members and hibernates the cells.
      {Alambique.Drain, config},
      {Task.Supervisor, name: config.query_supervisor}
    ]
  end

  # The children are built only when the setting is on.
  defp only_if(setting, children), do: if(setting, do: children.(), else: [])

  # Before the membership, so the crashes of the previous VM are settled before
  # this node takes calls. It stops after the cells.
  defp tracker(%{quarantine: nil}), do: []

  defp tracker(config),
    do: [%{id: Inflight, start: {Alambique.Inflight.Tracker, :start_link, [config]}}]

  defp replicas(%{replicas: nil}), do: []

  defp replicas(config) do
    [
      {Registry, keys: :unique, name: config.replica_registry},
      {Alambique.Replica.Manager, config},
      {DynamicSupervisor, strategy: :one_for_one, name: config.replica_supervisor}
    ]
  end

  defp sweeper(config),
    do: %{id: Alambique.Schema.Sweeper, start: {Alambique.Schema.Sweeper, :start_link, [config]}}

  defp compactor(config),
    do: %{id: Compactor, start: {Alambique.Compaction.Compactor, :start_link, [config]}}
end
