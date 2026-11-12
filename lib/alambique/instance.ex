defmodule Alambique.Instance do
  @moduledoc false
  # Supervision tree of one instance (design §1). Children start in the order
  # they are needed and stop in reverse, so cells hibernate while the
  # registry, the group and the configuration are still there.

  use Supervisor

  alias Alambique.{Config, Membership}

  # The data directory is locked before the tree starts, so a refused start
  # touches no file (design §1).
  def start_link(config) do
    with {:ok, lock} <- Alambique.DataDir.acquire(config.data_dir) do
      case Supervisor.start_link(__MODULE__, {config, lock}, name: config.name) do
        {:ok, pid} ->
          {:ok, pid}

        error ->
          Alambique.DataDir.release(lock)
          error
      end
    end
  end

  @impl true
  def init({config, lock}) do
    # A replica file never outlives its process (design §21).
    File.rm_rf!(Alambique.Replica.dir(config))
    Alambique.Cell.Local.clean_work_dirs(config)
    Config.put(config)
    Membership.init_state(config)

    children = [
      %{id: DataDir, start: {Alambique.DataDir, :start_link, [lock]}},
      %{id: :pg, start: {:pg, :start_link, [config.scope]}},
      %{id: Live, start: {Alambique.Live, :start_link, [config]}},
      {Registry, keys: :unique, name: config.registry, listeners: [config.live]}
    ]

    children = children ++ tracker(config)

    children =
      children ++
        [
          %{id: Wakes, start: {Alambique.Admission.Wakes, :start_link, [config]}},
          %{id: Membership, start: {Membership, :start_link, [config]}},
          {DynamicSupervisor,
           strategy: :one_for_one, name: config.supervisor, max_children: config.awake.max_cells},
          # Stops before the supervisor: leaves the members and hibernates the cells.
          {Alambique.Drain, config},
          {Task.Supervisor, name: config.query_supervisor}
        ]

    children =
      if config.feed.retention_ms,
        do: children ++ [{Alambique.Feed.Janitor, config}],
        else: children

    # Last, so that a compactor crash restarts nothing else. It stops first.
    children = if config.compaction, do: children ++ [compactor(config)], else: children

    children = if config.sweeper, do: children ++ [sweeper(config)], else: children

    children =
      children ++
        Alambique.Trash.children(config) ++
        Alambique.Scrub.children(config) ++ Alambique.Cell.LocalCache.children(config)

    children = children ++ replicas(config)

    Supervisor.init(children, strategy: :rest_for_one)
  end

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
