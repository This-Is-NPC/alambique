defmodule Alambique.Cell.LocalCache do
  @moduledoc false
  # The local files of hibernated cells are a cache of the store (design §7.1).
  # A round adds up the size of every `<cell>.cell` directory and, while the
  # total exceeds `max_bytes`, removes the directory of the cell whose files
  # were used longest ago.
  #
  # A directory is removed only by a process that holds the id of its cell in
  # the registry of the instance, the name every cell process takes. A cell
  # that has a process (or is waking: it registers before it touches its
  # files) makes the claim fail, and a cell that wakes while the claim lasts
  # finds the registry name taken. A call that reaches the claim sees the
  # process exit with `:normal`, which the router answers by looking again
  # (design §8). So a wake and an eviction never touch the same directory.
  # A directory without a marker is not reusable either, so it goes the same way.

  alias Alambique.Telemetry

  @defaults [max_bytes: 21_474_836_480, interval_ms: 60_000]

  @doc "Defaults of the `:local_cache` option."
  @spec defaults() :: keyword
  def defaults, do: @defaults

  @spec children(map) :: [Supervisor.child_spec()]
  def children(%{local_cache: nil}), do: []

  def children(config) do
    [
      %{
        id: __MODULE__,
        start:
          {Alambique.Periodic, :start_link,
           ["local cache", config.local_cache.interval_ms, fn -> sweep(config) end]}
      }
    ]
  end

  @doc """
  One round: removes the oldest directories of cells without a process until
  the total is under the limit. Returns what it did.
  """
  @spec sweep(map) :: map
  def sweep(config) do
    started = System.monotonic_time()
    dirs = scan(config)
    total = dirs |> Enum.map(& &1.bytes) |> Enum.sum()
    excess = total - config.local_cache.max_bytes

    result =
      dirs
      |> Enum.sort_by(& &1.used)
      |> Enum.reduce_while(%{evicted: 0, busy: 0, freed: 0}, fn dir, acc ->
        if excess - acc.freed <= 0, do: {:halt, acc}, else: {:cont, evict(config, dir, acc)}
      end)
      |> Map.merge(%{cells: length(dirs), bytes: total})

    Telemetry.span_stop([:local_cache, :sweep], started, result, %{instance: config.name})
    result
  end

  defp scan(config) do
    case File.ls(config.data_dir) do
      {:ok, names} ->
        for name <- names, String.ends_with?(name, ".cell"), info = info(config, name), do: info

      {:error, _} ->
        []
    end
  end

  # Size and last use of a directory; the marker is written when the cell hibernates.
  defp info(config, name) do
    dir = Path.join(config.data_dir, name)

    with {:ok, files} <- File.ls(dir) do
      stats =
        for file <- files,
            {:ok, stat} <- [File.stat(Path.join(dir, file), time: :posix)],
            do: stat

      used =
        case File.stat(Path.join(dir, "cell.db.meta"), time: :posix) do
          {:ok, stat} -> stat.mtime
          {:error, _} -> stats |> Enum.map(& &1.mtime) |> Enum.max(fn -> 0 end)
        end

      %{
        cell: String.replace_suffix(name, ".cell", ""),
        dir: dir,
        bytes: stats |> Enum.map(& &1.size) |> Enum.sum(),
        used: used
      }
    else
      _ -> nil
    end
  end

  defp evict(config, dir, acc) do
    task =
      Task.async(fn ->
        case Registry.register(config.registry, dir.cell, Alambique.Cell.new_load()) do
          {:ok, _} ->
            File.rm_rf(dir.dir)
            :evicted

          {:error, {:already_registered, _}} ->
            :busy
        end
      end)

    case Task.await(task, :infinity) do
      :evicted -> %{acc | evicted: acc.evicted + 1, freed: acc.freed + dir.bytes}
      :busy -> %{acc | busy: acc.busy + 1}
    end
  end
end
