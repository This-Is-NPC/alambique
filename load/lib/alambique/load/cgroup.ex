defmodule Alambique.Load.Cgroup do
  @moduledoc false
  # Reads of a container's cgroup v2 files. Process memory is
  # `memory.current - shmem`: the data directory of the service is a tmpfs, and
  # its pages are shared memory, counted apart.

  @mib 1_048_576

  @doc "The cgroup directory of a container, by its full id (systemd driver; `ALAMBIQUE_LOAD_CGROUP_ROOT` overrides)."
  @spec path(String.t()) :: Path.t()
  def path(container_id) do
    root = System.get_env("ALAMBIQUE_LOAD_CGROUP_ROOT", "/sys/fs/cgroup/system.slice")
    Path.join(root, "docker-#{container_id}.scope")
  end

  @doc "Parses `key value` lines into a map of integers."
  @spec parse(String.t()) :: %{String.t() => integer}
  def parse(text) do
    for line <- String.split(text, "\n", trim: true),
        [key, value] <- [String.split(line)],
        match?({_, ""}, Integer.parse(value)),
        into: %{},
        do: {key, String.to_integer(value)}
  end

  @doc "Process memory in bytes from the `memory.current` value and the parsed `memory.stat`."
  @spec process_bytes(integer, map) :: integer
  def process_bytes(current, stat), do: current - Map.get(stat, "shmem", 0)

  @doc "One reading: CPU time and throttling, memory in MiB."
  @spec read(Path.t()) :: {:ok, map} | :error
  def read(dir) do
    with {:ok, cpu} <- File.read(Path.join(dir, "cpu.stat")),
         {:ok, current} <- File.read(Path.join(dir, "memory.current")),
         {:ok, mem} <- File.read(Path.join(dir, "memory.stat")) do
      cpu = parse(cpu)
      mem = parse(mem)
      current = current |> String.trim() |> String.to_integer()

      {:ok,
       %{
         cpu_usec: cpu["usage_usec"],
         throttled_usec: cpu["throttled_usec"] || 0,
         throttled_periods: cpu["nr_throttled"] || 0,
         process_mb: process_bytes(current, mem) / @mib,
         shmem_mb: Map.get(mem, "shmem", 0) / @mib,
         anon_mb: Map.get(mem, "anon", 0) / @mib,
         file_mb: Map.get(mem, "file", 0) / @mib
       }}
    else
      _ -> :error
    end
  end

  @doc "The pids in the cgroup, as strings."
  @spec pids(Path.t()) :: [String.t()]
  def pids(dir) do
    case File.read(Path.join(dir, "cgroup.procs")) do
      {:ok, text} -> String.split(text, "\n", trim: true)
      _ -> []
    end
  end
end
