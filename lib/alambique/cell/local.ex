defmodule Alambique.Cell.Local do
  @moduledoc false
  # The local copy of a cell: `<data_dir>/<cell>.cell/cell.db` and the marker
  # that says it equals a known TXID of the store.
  #
  # The marker is written when a cell hibernates cleanly (everything
  # replicated, WAL checkpointed) and removed the moment the next wake
  # starts changing the file. A file without a marker is never reused.
  # Cell ids may be `.` or `..`, so the directory name carries a suffix.
  # Cell ids that differ only in case need a case-sensitive file system.
  #
  # The directories are private to the owner (mode 0700), which also covers the
  # files inside; SQLite creates its -wal and -shm files with the mode of the
  # database. Temporary directories live under `<data_dir>/work`, a name no
  # cell directory can have, because those end in `.cell`.

  require Logger

  alias Alambique.Fault

  @file_name "cell.db"
  @marker "cell.db.meta"

  @spec dir(Alambique.Config.t(), String.t()) :: Path.t()
  def dir(config, cell), do: Path.join(config.data_dir, cell <> ".cell")

  @spec db_path(Alambique.Config.t(), String.t()) :: Path.t()
  def db_path(config, cell), do: Path.join(dir(config, cell), @file_name)

  # Operations that rebuild a cell outside its process work in a directory of their own.
  @work_kinds [:relocate, :rewind, :scrub]

  @doc "Creates `dir` (and its parents) readable only by the owner."
  @spec private_dir!(Path.t()) :: :ok
  def private_dir!(dir) do
    File.mkdir_p!(dir)

    case File.chmod(dir, 0o700) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Alambique could not restrict #{dir}: #{inspect(reason)}")
    end
  end

  @doc """
  A new, empty directory under `<data_dir>/work` for the files of a `kind` of
  operation. The operation removes it when it ends.
  """
  @spec work_dir(Alambique.Config.t(), :relocate | :rewind | :scrub) :: Path.t()
  def work_dir(config, kind) when kind in @work_kinds do
    dir = Path.join(work_root(config), "#{kind}-#{System.unique_integer([:positive])}")
    private_dir!(dir)
    dir
  end

  @doc """
  Removes the work directories that a VM crash left. It runs when the instance
  starts, when none of its operations can be running: the data directory
  belongs to one instance, as the files of its cells do.
  """
  @spec clean_work_dirs(Alambique.Config.t()) :: :ok
  def clean_work_dirs(config) do
    File.rm_rf(work_root(config))
    :ok
  end

  defp work_root(config), do: Path.join(config.data_dir, "work")

  @doc "Creates the directory and removes temporary files of an interrupted restore."
  @spec prepare(Path.t()) :: :ok
  def prepare(db_path) do
    dir = Path.dirname(db_path)
    private_dir!(dir)
    temp = @file_name <> ".restore-"

    for name <- File.ls!(dir), String.starts_with?(name, temp), do: File.rm(Path.join(dir, name))
    :ok
  end

  @doc "Reads the marker of the local file, if it is valid."
  @spec read(Path.t()) :: %{txid: pos_integer, checksum: integer, page_size: pos_integer} | nil
  def read(db_path) do
    with {:ok, body} <- File.read(marker(db_path)),
         {:ok, %{"txid" => txid, "checksum" => sum, "page_size" => size}}
         when is_integer(txid) and is_integer(sum) and is_integer(size) <- Jason.decode(body) do
      %{txid: txid, checksum: sum, page_size: size}
    else
      _ -> nil
    end
  end

  @doc "Removes the marker: the file is about to change, or is no longer trusted."
  @spec forget(Path.t()) :: :ok
  def forget(db_path) do
    File.rm(marker(db_path))
    :ok
  end

  @doc "Records that the closed local file equals the store at `txid`."
  @spec mark(Path.t(), pos_integer, integer, pos_integer) :: :ok | {:error, File.posix()}
  def mark(db_path, txid, checksum, page_size) do
    body = Jason.encode!(%{txid: txid, checksum: checksum, page_size: page_size})
    tmp = marker(db_path) <> ".tmp"
    Fault.point(:local_before_mark, %{path: db_path})

    with :ok <- File.write(tmp, body) do
      :ok = File.rename(tmp, marker(db_path))
      Fault.point(:local_after_mark, %{path: db_path})
    end
  end

  defp marker(db_path), do: Path.join(Path.dirname(db_path), @marker)
end
