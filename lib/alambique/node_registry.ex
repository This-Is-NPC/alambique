defmodule Alambique.NodeRegistry do
  @moduledoc false
  # The process that keeps this node in the node registry (design §22): it
  # registers at start, renews every `renew_ms`, lists the live peers every
  # `refresh_ms`, and deregisters when the node drains or the instance stops.
  # The registry lives in the first location of the placement by id.
  #
  # A store failure is logged and reported with `[:alambique, :registry,
  # :error]`; the next round tries again, and the instance never notices. The
  # last listing of peers stays readable meanwhile.

  use GenServer

  require Logger

  alias Alambique.{Config, Membership, Nodes, Placement, Telemetry}

  @doc false
  def child_spec(config) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [config]},
      # The deregistration is one request, bounded by the TTL.
      shutdown: config.nodes.ttl_ms
    }
  end

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: config.node_registry)

  @doc "The live peers of this node from the last listing; `[]` when the registry is off."
  @spec peers(Config.t()) :: [map]
  def peers(%{nodes: nil}), do: []

  def peers(config) do
    case :ets.lookup(config.peers, :peers) do
      [{_, peers}] -> peers
      [] -> []
    end
  rescue
    ArgumentError -> []
  end

  @doc "Deregisters this node now (a drain). The next round registers it again unless it still drains."
  @spec deregister(Config.t()) :: :ok
  def deregister(%{nodes: nil}), do: :ok

  def deregister(config) do
    GenServer.call(config.node_registry, :deregister, config.nodes.ttl_ms + 1_000)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    :ets.new(config.peers, [:named_table, :protected, :set, read_concurrency: true])
    :ets.insert(config.peers, {:peers, []})
    send(self(), :renew)
    send(self(), :refresh)

    {:ok, %{config: config, entry: nil, registered?: false, peers: [], last_sweep: now_mono()}}
  end

  @impl true
  def handle_call(:deregister, _from, state), do: {:reply, :ok, leave(state)}

  @impl true
  def handle_info(:renew, %{config: config} = state) do
    Process.send_after(self(), :renew, config.nodes.renew_ms)

    state =
      cond do
        Membership.draining?(config) -> leave(state)
        state.entry -> state |> renew() |> sweep()
        true -> state |> register() |> sweep()
      end

    {:noreply, state}
  end

  def handle_info(:refresh, %{config: config} = state) do
    Process.send_after(self(), :refresh, config.nodes.refresh_ms)
    {:noreply, refresh(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    leave(state)
    :ok
  end

  # The first registration may take over the live entry of the previous run of
  # this node id. After a loss only a free entry is taken, so two live instances
  # with one node id do not fight for the key. The incarnation stays: it is the
  # same instance.
  defp register(%{config: config} = state) do
    takeover = if state.registered?, do: :free, else: :restart

    with {:ok, ctx} <- ctx(config),
         {:ok, entry} <-
           Nodes.register(ctx, config.node_id, config.incarnation, config.nodes.advertise,
             meta: config.nodes.meta,
             takeover: takeover
           ) do
      %{state | entry: entry, registered?: true}
    else
      # Another live instance holds the id: wait for it to leave or expire.
      {:error, {:held, _, _}} -> state
      {:error, reason} -> failed(state, :register, reason)
    end
  end

  defp renew(%{config: config} = state) do
    with {:ok, ctx} <- ctx(config), {:ok, entry} <- Nodes.renew(ctx, state.entry) do
      %{state | entry: entry}
    else
      {:error, :lost} ->
        Logger.warning(
          "Alambique #{config.name}: lost the node registry entry, registering again"
        )

        register(%{state | entry: nil})

      {:error, reason} ->
        failed(state, :renew, reason)
    end
  end

  defp leave(%{entry: nil} = state), do: state

  defp leave(%{config: config, entry: entry} = state) do
    with {:ok, ctx} <- ctx(config), :ok <- Nodes.deregister(ctx, entry) do
      %{state | entry: nil}
    else
      {:error, :lost} -> %{state | entry: nil}
      {:error, reason} -> failed(state, :deregister, reason)
    end
  end

  defp sweep(%{config: config} = state) do
    if now_mono() - state.last_sweep >= config.nodes.sweep_ms do
      with {:ok, ctx} <- ctx(config), {:ok, _} <- Nodes.sweep(ctx) do
        %{state | last_sweep: now_mono()}
      else
        {:error, reason} -> failed(%{state | last_sweep: now_mono()}, :sweep, reason)
      end
    else
      state
    end
  end

  defp refresh(%{config: config} = state) do
    with {:ok, ctx} <- ctx(config), {:ok, entries} <- Nodes.list(ctx) do
      peers =
        for entry <- entries, entry.node_id != config.node_id do
          %{
            node_id: entry.node_id,
            incarnation: entry.incarnation,
            advertise: entry.advertise,
            meta: entry.meta
          }
        end

      changed(state, peers)
    else
      {:error, reason} -> failed(state, :list, reason)
    end
  end

  defp changed(%{peers: peers} = state, peers), do: state

  defp changed(%{config: config, peers: old} = state, peers) do
    :ets.insert(config.peers, {:peers, peers})

    Telemetry.emit([:registry, :peers_changed], %{peers: length(peers)}, %{
      instance: config.name,
      node_id: config.node_id,
      joined: peers -- old,
      left: old -- peers
    })

    %{state | peers: peers}
  end

  defp failed(%{config: config} = state, op, reason) do
    Logger.warning("Alambique #{config.name}: node registry #{op} failed: #{inspect(reason)}")

    Telemetry.emit([:registry, :error], %{
      instance: config.name,
      node_id: config.node_id,
      op: op,
      reason: reason
    })

    state
  end

  defp ctx(config) do
    with {:ok, locations} <- Placement.locations(config) do
      {:ok, Config.nodes_ctx(config, Enum.min_by(locations, & &1.id))}
    end
  end

  defp now_mono, do: System.monotonic_time(:millisecond)
end
