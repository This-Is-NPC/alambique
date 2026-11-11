defmodule Alambique.Load.Sampler do
  @moduledoc false
  # Samples the service's cgroup and the store's space every second and the host every five, for the
  # whole run, so that the net-load average (see `Alambique.Load.Host`) is
  # continuous. `mark/0` starts a window; `collect/0` returns what it saw.

  use GenServer

  alias Alambique.Load.{Capacity, Cgroup, Host}

  @interval 1_000
  @host_every Host.tick_s()

  # `store_dir`: the directory of a store on disk (its file system's free
  # space is sampled), or nil for a store on a tmpfs (the shared memory of its
  # cgroup is).
  def start_link(%{service: _, store: _, store_dir: _} = dirs),
    do: GenServer.start_link(__MODULE__, dirs, name: __MODULE__)

  @doc "Starts a window."
  def mark, do: GenServer.call(__MODULE__, :mark)

  @doc """
  The samples since `mark/0`: `%{cgroup: [reading], host: [reading], swap:
  [reading], store: [reading]}`.
  """
  def collect, do: GenServer.call(__MODULE__, :collect)

  @doc """
  The latest reading of the store's space, or nil before the first: `%{kind:
  :tmpfs, used_mb}` or `%{kind: :disk, free_mb, used_mb}` (used since the
  start of the sampler).
  """
  def store, do: GenServer.call(__MODULE__, :store)

  @doc "The latest host reading, or nil before the first."
  def host, do: GenServer.call(__MODULE__, :host)

  @doc "The latest cgroup reading of the service: `{:ok, reading}` or `:error` when it is gone."
  def cgroup, do: GenServer.call(__MODULE__, :cgroup)

  @impl true
  def init(%{service: service, store: store, store_dir: store_dir}) do
    state = %{
      service: service,
      store: store,
      store_dir: store_dir,
      store_free_start: nil,
      store_now: nil,
      pids: [service, store],
      ticks: 0,
      ema: nil,
      cgroup: Cgroup.read(service),
      host: nil,
      window: empty_window()
    }

    send(self(), :tick)
    {:ok, state}
  end

  defp empty_window, do: %{cgroup: [], host: [], swap: [], store: []}

  @impl true
  def handle_call(:mark, _from, state), do: {:reply, :ok, %{state | window: empty_window()}}

  def handle_call(:collect, _from, state) do
    {:reply, Map.new(state.window, fn {key, list} -> {key, Enum.reverse(list)} end), state}
  end

  def handle_call(:store, _from, state), do: {:reply, state.store_now, state}

  def handle_call(:host, _from, state), do: {:reply, state.host, state}
  def handle_call(:cgroup, _from, state), do: {:reply, state.cgroup, state}

  @impl true
  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, @interval)
    reading = Cgroup.read(state.service)
    state = %{state | cgroup: reading}

    state =
      case reading do
        {:ok, r} -> update_in(state.window.cgroup, &[Map.put(r, :at, now_ms()) | &1])
        :error -> state
      end

    swap = %{
      host_mb: Host.swap_used_mb(),
      driver_mb: Host.own_swap_mb(),
      store_mb: Cgroup.swap_mb(state.store),
      service_mb: Cgroup.swap_mb(state.service)
    }

    state = update_in(state.window.swap, &[swap | &1])
    state = store_tick(state)

    {:noreply,
     if(rem(state.ticks, @host_every) == 0, do: host_tick(state), else: state)
     |> Map.update!(:ticks, &(&1 + 1))}
  end

  defp store_tick(state) do
    case read_store(state) do
      nil ->
        state

      reading ->
        start = state.store_free_start || reading[:free_mb]
        state = %{state | store_now: reading, store_free_start: start}
        update_in(state.window.store, &[reading | &1])
    end
  end

  # A tmpfs is shared memory of the store's cgroup: no process is started. A
  # disk is the free space of its file system, from `df`.
  defp read_store(%{store_dir: nil, store: cgroup}) do
    case Cgroup.read(cgroup) do
      {:ok, reading} -> %{kind: :tmpfs, used_mb: reading.shmem_mb}
      :error -> nil
    end
  end

  defp read_store(%{store_dir: dir} = state) do
    with {text, 0} <- System.cmd("df", ["-P", "-B1", dir], stderr_to_stdout: true),
         free when is_number(free) <- Capacity.parse_df_free_mb(text) do
      %{kind: :disk, free_mb: free, used_mb: max((state.store_free_start || free) - free, 0.0)}
    else
      _ -> nil
    end
  end

  defp host_tick(state) do
    pids = Enum.flat_map(state.pids, &Cgroup.pids/1) ++ [System.pid()]
    ema = Host.ema(state.ema, Host.runnable_tasks(pids))
    loadavg = Host.loadavg1()

    host = %{
      at: now_ms(),
      loadavg1: loadavg,
      own_runnable_avg: ema,
      net_load: Host.net_load(loadavg, ema),
      mem_available_gib: Host.mem_available_gib()
    }

    %{state | ema: ema, host: host, window: %{state.window | host: [host | state.window.host]}}
  end

  defp now_ms, do: System.system_time(:millisecond)
end
