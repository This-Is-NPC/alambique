defmodule Alambique.Load.Sampler do
  @moduledoc false
  # Samples the service's cgroup every second and the host every five, for the
  # whole run, so that the net-load average (see `Alambique.Load.Host`) is
  # continuous. `mark/0` starts a window; `collect/0` returns what it saw.

  use GenServer

  alias Alambique.Load.{Cgroup, Host}

  @interval 1_000
  @host_every 5

  def start_link(cgroup_dirs), do: GenServer.start_link(__MODULE__, cgroup_dirs, name: __MODULE__)

  @doc "Starts a window."
  def mark, do: GenServer.call(__MODULE__, :mark)

  @doc "The samples since `mark/0`: `%{cgroup: [reading], host: [reading]}`."
  def collect, do: GenServer.call(__MODULE__, :collect)

  @doc "The latest host reading, or nil before the first."
  def host, do: GenServer.call(__MODULE__, :host)

  @doc "The latest cgroup reading of the service: `{:ok, reading}` or `:error` when it is gone."
  def cgroup, do: GenServer.call(__MODULE__, :cgroup)

  @impl true
  def init(%{service: service, store: store}) do
    state = %{
      service: service,
      pids: [service, store],
      ticks: 0,
      ema: nil,
      cgroup: Cgroup.read(service),
      host: nil,
      window: %{cgroup: [], host: []}
    }

    send(self(), :tick)
    {:ok, state}
  end

  @impl true
  def handle_call(:mark, _from, state),
    do: {:reply, :ok, %{state | window: %{cgroup: [], host: []}}}

  def handle_call(:collect, _from, state) do
    {:reply, %{cgroup: Enum.reverse(state.window.cgroup), host: Enum.reverse(state.window.host)},
     state}
  end

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

    {:noreply,
     if(rem(state.ticks, @host_every) == 0, do: host_tick(state), else: state)
     |> Map.update!(:ticks, &(&1 + 1))}
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
