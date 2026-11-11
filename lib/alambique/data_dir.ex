defmodule Alambique.DataDir do
  @moduledoc false
  # The data directory belongs to one live instance (design §1). An exclusive
  # `flock` on `<data_dir>/lock` enforces it, for other instances of this VM
  # and of other OS processes alike; the kernel drops it when the process
  # dies, so a crash leaves no stale lock.

  use GenServer

  alias Alambique.Cell.Local
  alias Alambique.Replication.Native

  @lock_file "lock"

  @type error ::
          {:data_dir_in_use, Path.t()}
          | {:data_dir_unusable, Path.t(), term}

  @doc """
  Creates the directory and takes its lock. Touches nothing else.
  """
  @spec acquire(Path.t()) :: {:ok, reference} | {:error, error}
  def acquire(dir) do
    Local.private_dir!(dir)

    case Native.dir_lock(Path.join(dir, @lock_file)) do
      {:ok, ref} -> {:ok, ref}
      {:error, :in_use} -> {:error, {:data_dir_in_use, dir}}
      {:error, {:io, reason}} -> {:error, {:data_dir_unusable, dir, reason}}
    end
  end

  @doc "Releases a lock from `acquire/1`."
  @spec release(reference) :: :ok
  def release(lock), do: Native.dir_unlock(lock)

  @doc "The first child of the instance: holds the lock until a clean stop."
  def start_link(lock), do: GenServer.start_link(__MODULE__, lock)

  @impl true
  def init(lock) do
    Process.flag(:trap_exit, true)
    {:ok, lock}
  end

  # Only a clean stop unlocks. After a crash the supervisor restarts this
  # process with the same lock; a killed instance frees it with the process.
  @impl true
  def terminate(:shutdown, lock), do: release(lock)
  def terminate(_reason, _lock), do: :ok
end
