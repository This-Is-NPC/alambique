defmodule Alambique.DataDir do
  @moduledoc false
  # The data directory belongs to one live instance on a case-sensitive file
  # system (design §1). An exclusive `flock` on `<data_dir>/lock` enforces the
  # first, for other instances of this VM and of other OS processes alike; the
  # kernel drops it when the process dies, so a crash leaves no stale lock.
  # A probe file enforces the second.

  use GenServer

  alias Alambique.Cell.Local
  alias Alambique.Replication.Native

  @lock_file "lock"

  @type error ::
          {:data_dir_in_use, Path.t()}
          | {:data_dir_case_insensitive, Path.t()}
          | {:data_dir_unusable, Path.t(), term}

  @doc """
  Creates the directory, takes its lock and checks its case sensitivity.
  Touches nothing else. `lock` and `case_probe` are the checks themselves, so
  tests can stand in for a file system that has the property.
  """
  @spec acquire(Path.t(), (Path.t() -> term), (Path.t() -> boolean)) ::
          {:ok, reference} | {:error, error}
  def acquire(dir, lock \\ &Native.dir_lock/1, case_probe \\ &case_insensitive?/1) do
    Local.private_dir!(dir)

    with {:ok, ref} <- take(dir, lock) do
      if case_probe.(dir) do
        release(ref)
        {:error, {:data_dir_case_insensitive, dir}}
      else
        {:ok, ref}
      end
    end
  end

  defp take(dir, lock) do
    case lock.(Path.join(dir, @lock_file)) do
      {:ok, ref} -> {:ok, ref}
      {:error, :in_use} -> {:error, {:data_dir_in_use, dir}}
      {:error, {:io, reason}} -> {:error, {:data_dir_unusable, dir, reason}}
    end
  end

  @doc "True when `dir` treats names that differ only in case as one file."
  @spec case_insensitive?(Path.t(), (Path.t() -> boolean)) :: boolean
  def case_insensitive?(dir, exists? \\ &File.exists?/1) do
    name = "case-probe-" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
    path = Path.join(dir, name)

    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        File.close(io)
        found = exists?.(Path.join(dir, String.upcase(name)))
        File.rm(path)
        found

      {:error, _} ->
        false
    end
  end

  @doc "Releases a lock from `acquire/3`."
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
