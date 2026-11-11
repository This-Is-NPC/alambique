defmodule Alambique.Query.Cache do
  @moduledoc false
  # On-disk cache of read-only replicas (design §14). One directory per cell,
  # `<cell>.r/`, with one file per TXID:
  # `<txid hex>-<epoch>-<checksum hex>.db`, the database checksum (LTX) of the
  # file, so a refresh need not read it to continue the chain. Files are
  # built under a `.tmp-*` name and renamed into place, so a name that looks
  # like a replica is always a complete one. The modification time is the time
  # of the last use, which orders the eviction.

  @type entry :: %{
          txid: non_neg_integer,
          epoch: non_neg_integer,
          checksum: non_neg_integer,
          path: Path.t()
        }

  @spec dir(map) :: Path.t()
  def dir(config), do: Path.join(config.data_dir, "replicas")

  @doc "Cache name of a cell at a location."
  @spec key(String.t(), String.t()) :: String.t()
  def key(cell, scope), do: cell <> "@" <> scope

  @spec cell_dir(Path.t(), String.t()) :: Path.t()
  def cell_dir(dir, cell), do: Path.join(dir, cell <> ".r")

  @doc """
  A fresh temporary file beside the final ones of `cell`, created empty. A
  directory without files is removed by `trim/3`, so the file keeps it alive
  while the replica is built.
  """
  @spec tmp(Path.t(), String.t()) :: Path.t()
  def tmp(dir, cell) do
    cell_dir = cell_dir(dir, cell)
    path = Path.join(cell_dir, ".tmp-#{System.unique_integer([:positive])}")
    File.mkdir_p!(cell_dir)

    case File.write(path, "", [:exclusive]) do
      :ok -> path
      # `trim/3` removed the directory just created: make it again.
      {:error, :enoent} -> tmp(dir, cell)
    end
  end

  @spec path(Path.t(), String.t(), non_neg_integer, non_neg_integer, non_neg_integer) :: Path.t()
  def path(dir, cell, txid, epoch, checksum) do
    hex = &Alambique.Replication.Keys.hex/1
    Path.join(cell_dir(dir, cell), "#{hex.(txid)}-#{epoch}-#{hex.(checksum)}.db")
  end

  @doc "Complete replicas of `cell`, newest TXID first."
  @spec entries(Path.t(), String.t()) :: [entry]
  def entries(dir, cell) do
    cell_dir = cell_dir(dir, cell)

    case File.ls(cell_dir) do
      {:ok, names} ->
        names
        |> Enum.flat_map(&List.wrap(parse(cell_dir, &1)))
        |> Enum.sort_by(&{&1.txid, &1.epoch}, :desc)

      {:error, _} ->
        []
    end
  end

  defp parse(cell_dir, name) do
    with <<hex::binary-size(16), ?-, rest::binary>> <- name,
         {epoch, <<?-, sum::binary-size(16), ".db">>} <- Integer.parse(rest),
         {checksum, ""} <- Integer.parse(sum, 16),
         {txid, ""} <- Integer.parse(hex, 16) do
      %{txid: txid, epoch: epoch, checksum: checksum, path: Path.join(cell_dir, name)}
    else
      _ -> nil
    end
  end

  @doc "Marks `path` as used now."
  @spec touch(Path.t()) :: :ok
  def touch(path) do
    File.touch(path)
    :ok
  end

  @doc "Milliseconds since `path` was last used (whole seconds); `nil` when it is gone."
  @spec age_ms(Path.t()) :: non_neg_integer | nil
  def age_ms(path) do
    case File.stat(path, time: :posix) do
      {:ok, %{mtime: mtime}} -> max(System.os_time(:second) - mtime, 0) * 1000
      {:error, _} -> nil
    end
  end

  @doc "Removes the files of the given replicas."
  @spec remove([entry]) :: :ok
  def remove(entries) do
    Enum.each(entries, fn %{path: path} ->
      File.rm(path)
      File.rm(path <> "-wal")
      File.rm(path <> "-shm")
    end)
  end

  @doc """
  Makes the empty file `dst` a copy of `src`: a copy-on-write clone when
  `reflink?` and the filesystem has them, else a plain copy. Returns which.
  """
  @spec clone(Path.t(), Path.t(), boolean) :: {:ok, :reflink | :copy} | {:error, term}
  def clone(src, dst, reflink?) do
    case reflink? and Alambique.Replication.Native.reflink(src, dst) do
      {:ok, true} ->
        {:ok, :reflink}

      _ ->
        with {:ok, _} <- File.copy(src, dst), do: {:ok, :copy}
    end
  end

  @doc "Flushes a file to disk, so a renamed replica is never a torn one."
  @spec sync(Path.t()) :: :ok
  def sync(path) do
    {:ok, io} = :file.open(path, [:read, :write, :raw, :binary])
    :ok = :file.sync(io)
    :ok = :file.close(io)
  end

  @doc "Page size from the header of a SQLite file."
  @spec page_size(Path.t()) :: {:ok, pos_integer} | :error
  def page_size(path) do
    with {:ok, io} <- :file.open(path, [:read, :raw, :binary]),
         {:ok, <<size::16>>} <- :file.pread(io, 16, 2) do
      :file.close(io)
      if size == 1, do: {:ok, 65_536}, else: {:ok, size}
    else
      _ -> :error
    end
  end

  @doc """
  Deletes the least recently used replicas until the cache holds at most
  `max_bytes`, and temporary files older than `stale_tmp_seconds`. Replicas
  still being built count against `max_bytes` and are not deleted. Returns the
  number of files deleted and the bytes that remain.
  """
  @spec trim(Path.t(), non_neg_integer, non_neg_integer) ::
          {non_neg_integer, non_neg_integer}
  def trim(dir, max_bytes, stale_tmp_seconds) do
    now = System.os_time(:second)
    files = scan(dir)

    {stale, live} =
      Enum.split_with(files, &(&1.tmp? and now - &1.mtime > stale_tmp_seconds))

    Enum.each(stale, &File.rm(&1.path))
    # Replicas being built count against the bound, but only finished ones
    # can be evicted.
    {building, live} = Enum.split_with(live, & &1.tmp?)
    total = Enum.sum_by(live ++ building, & &1.size)
    {evicted, remaining} = evict(Enum.sort_by(live, & &1.mtime), total, max_bytes)
    Enum.each(evicted, &remove([&1]))

    for dir <- cell_dirs(dir), do: File.rmdir(dir)
    {length(stale) + length(evicted), remaining}
  end

  defp evict(files, total, max) do
    {evicted, remaining} =
      Enum.reduce_while(files, {[], total}, fn
        _file, {evicted, total} when total <= max -> {:halt, {evicted, total}}
        file, {evicted, total} -> {:cont, {[file | evicted], total - file.size}}
      end)

    {evicted, remaining}
  end

  defp cell_dirs(dir) do
    case File.ls(dir) do
      {:ok, names} -> for name <- names, String.ends_with?(name, ".r"), do: Path.join(dir, name)
      {:error, _} -> []
    end
  end

  defp scan(dir) do
    for cell_dir <- cell_dirs(dir),
        {:ok, names} <- [File.ls(cell_dir)],
        name <- names,
        path = Path.join(cell_dir, name),
        not String.ends_with?(name, ["-wal", "-shm"]),
        {:ok, %{size: size, mtime: mtime}} <- [File.stat(path, time: :posix)] do
      %{path: path, size: size, mtime: mtime, tmp?: String.starts_with?(name, ".tmp-")}
    end
  end
end
