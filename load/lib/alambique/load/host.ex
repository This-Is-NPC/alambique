defmodule Alambique.Load.Host do
  @moduledoc false
  # What the shared host is doing besides the test.
  #
  # Net load = the host's 1-minute load average minus the same average of the
  # tasks the test itself keeps runnable (state R or D in the containers and in
  # the driver). The kernel averages the count of such tasks every 5 s with a
  # 60 s decay; `ema/2` applies the same decay to our own count, sampled at the
  # same pace, so that the two are comparable. A step is invalid when the net
  # load, at any sample during it, is above the limit.

  @tick_s 5
  @decay :math.exp(-@tick_s / 60)

  @doc "Seconds between host samples."
  @spec tick_s() :: pos_integer
  def tick_s, do: @tick_s

  @doc "The 1-minute load average from the text of /proc/loadavg."
  @spec parse_loadavg(String.t()) :: float
  def parse_loadavg(text) do
    [one | _] = String.split(text)
    {value, _} = Float.parse(one)
    value
  end

  @doc "MemAvailable, in GiB, from the text of /proc/meminfo."
  @spec parse_mem_available_gib(String.t()) :: float
  def parse_mem_available_gib(text) do
    [_, kib] = Regex.run(~r/^MemAvailable:\s+(\d+) kB/m, text)
    String.to_integer(kib) / 1_048_576
  end

  @doc "Swap in use (SwapTotal - SwapFree), in MiB, from the text of /proc/meminfo."
  @spec parse_swap_used_mb(String.t()) :: float
  def parse_swap_used_mb(text) do
    [_, total] = Regex.run(~r/^SwapTotal:\s+(\d+) kB/m, text)
    [_, free] = Regex.run(~r/^SwapFree:\s+(\d+) kB/m, text)
    (String.to_integer(total) - String.to_integer(free)) / 1024
  end

  @doc "VmSwap, in MiB, from the text of /proc/<pid>/status; nil when the line is missing."
  @spec parse_vmswap_mb(String.t()) :: float | nil
  def parse_vmswap_mb(text) do
    case Regex.run(~r/^VmSwap:\s+(\d+) kB/m, text) do
      [_, kib] -> String.to_integer(kib) / 1024
      nil -> nil
    end
  end

  @doc "Swap in use on the host, in MiB."
  @spec swap_used_mb() :: float
  def swap_used_mb, do: "/proc/meminfo" |> File.read!() |> parse_swap_used_mb()

  @doc "Swap in use by this VM (the driver), in MiB, or nil."
  @spec own_swap_mb() :: float | nil
  def own_swap_mb do
    case File.read("/proc/self/status") do
      {:ok, text} -> parse_vmswap_mb(text)
      _ -> nil
    end
  end

  @swap_keys [:host_mb, :driver_mb, :store_mb, :service_mb]

  @doc """
  What the swap samples of a step say. Each sample has `host_mb`, `driver_mb`,
  `store_mb` and `service_mb` (nil when unreadable). Per key: `start`, `max`,
  `end` and `growth` (max minus start), or nil when it was never readable.
  """
  @spec swap_stats([map]) :: map
  def swap_stats(samples) do
    for key <- @swap_keys, into: %{} do
      case samples |> Enum.map(& &1[key]) |> Enum.reject(&is_nil/1) do
        [] ->
          {key, nil}

        [first | _] = values ->
          max = Enum.max(values)
          {key, %{start: first, max: max, end: List.last(values), growth: max - first}}
      end
    end
  end

  @doc "The state letter of /proc/<pid>/task/<tid>/stat text (the field after the command in parentheses)."
  @spec task_state(String.t()) :: String.t()
  def task_state(stat) do
    [_, state] = Regex.run(~r/^.*\) (\S)/s, stat)
    state
  end

  @doc "Whether a task in `state` counts in the load average."
  @spec runnable?(String.t()) :: boolean
  def runnable?(state), do: state in ["R", "D"]

  @doc "One step of the exponential average the kernel keeps, with our own `count` of runnable tasks."
  @spec ema(float | nil, number) :: float
  def ema(nil, count), do: count * 1.0
  def ema(previous, count), do: previous * @decay + count * (1 - @decay)

  @doc "The load the host has besides the test."
  @spec net_load(float, float) :: float
  def net_load(loadavg1, own_ema), do: max(loadavg1 - own_ema, 0.0)

  @doc "Whether memory is too low to go on."
  @spec low_memory?(float, number) :: boolean
  def low_memory?(available_gib, min_gib), do: available_gib < min_gib

  @doc "The runnable tasks of the processes `pids`, from /proc."
  @spec runnable_tasks([String.t()]) :: non_neg_integer
  def runnable_tasks(pids) do
    for pid <- pids,
        tid <- File.ls("/proc/#{pid}/task") |> ok_list(),
        {:ok, stat} <- [File.read("/proc/#{pid}/task/#{tid}/stat")],
        runnable?(task_state(stat)),
        reduce: 0 do
      count -> count + 1
    end
  end

  defp ok_list({:ok, list}), do: list
  defp ok_list(_), do: []

  @spec loadavg1() :: float
  def loadavg1, do: "/proc/loadavg" |> File.read!() |> parse_loadavg()

  @spec mem_available_gib() :: float
  def mem_available_gib, do: "/proc/meminfo" |> File.read!() |> parse_mem_available_gib()
end
