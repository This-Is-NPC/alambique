defmodule Alambique.Replication.Keys do
  @moduledoc false
  # Object keys of a cell's segments and snapshots (design §4).

  @hex ~r/\A[0-9a-f]{16}\z/
  # Snapshots live at this level and merged files below it, as in Litestream's layout.
  @snapshot_level 9

  @doc "Level of the snapshots."
  def snapshot_level, do: @snapshot_level

  @spec cell_prefix(String.t(), String.t()) :: String.t()
  def cell_prefix(prefix, cell), do: "#{prefix}/cells/#{cell}"

  @spec hex(non_neg_integer) :: String.t()
  def hex(txid),
    do: txid |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(16, "0")

  @doc """
  Key of a segment at `level` (0 to 8). The layout is the one Litestream's
  S3 replica client reads: `<cell prefix>/<level as 4 hex digits>/<min>-<max>.ltx`
  (docs/segment-format.md).
  """
  @spec segment(String.t(), String.t(), non_neg_integer, pos_integer, pos_integer) :: String.t()
  def segment(prefix, cell, level, min, max) when level >= 0 and level < @snapshot_level,
    do: file(prefix, cell, level, min, max)

  @doc "Key of the snapshot of TXID `txid`: a file of the snapshot level starting at TXID 1."
  @spec snapshot(String.t(), String.t(), pos_integer) :: String.t()
  def snapshot(prefix, cell, txid), do: file(prefix, cell, @snapshot_level, 1, txid)

  defp file(prefix, cell, level, min, max) do
    "#{level_prefix(prefix, cell, level)}#{hex(min)}-#{hex(max)}.ltx"
  end

  defp level_dir(level), do: level |> Integer.to_string(16) |> String.pad_leading(4, "0")

  @doc "Key whose presence freezes the cell (design §10)."
  @spec frozen(String.t(), String.t()) :: String.t()
  def frozen(prefix, cell), do: cell_prefix(prefix, cell) <> "/frozen"

  @doc "Key whose presence turns read replicas on for the cell (design §21)."
  @spec replicas(String.t(), String.t()) :: String.t()
  def replicas(prefix, cell), do: cell_prefix(prefix, cell) <> "/replicas"

  @doc "Key of the object that records the newest anchor whose files were marked for deletion (design §9.1)."
  @spec trashed(String.t(), String.t()) :: String.t()
  def trashed(prefix, cell), do: cell_prefix(prefix, cell) <> "/trashed"

  @doc "Key of the compactor's count of the bytes written since the newest snapshot (design §9)."
  @spec chain(String.t(), String.t()) :: String.t()
  def chain(prefix, cell), do: cell_prefix(prefix, cell) <> "/chain"

  @doc "Key of the crash counter of the cell (design §10)."
  @spec crashes(String.t(), String.t()) :: String.t()
  def crashes(prefix, cell), do: cell_prefix(prefix, cell) <> "/crashes"

  @doc "Prefix under which every object of the cell lives."
  @spec files_prefix(String.t(), String.t()) :: String.t()
  def files_prefix(prefix, cell), do: cell_prefix(prefix, cell) <> "/"

  @doc "Prefix of the files of one level of the cell."
  @spec level_prefix(String.t(), String.t(), non_neg_integer) :: String.t()
  def level_prefix(prefix, cell, level),
    do: files_prefix(prefix, cell) <> level_dir(level) <> "/"

  @doc """
  The keys in `keys` that name a segment or a snapshot, in the order of the
  input for segments and oldest first for snapshots. Fixed keys such as the
  lease, the frozen marker or the crash counter are dropped.
  """
  @spec data_keys(String.t(), String.t(), [String.t()]) :: [String.t()]
  def data_keys(prefix, cell, keys) do
    {segments, snapshots} = parse(prefix, cell, keys)
    Enum.map(segments ++ snapshots, & &1.key)
  end

  @doc """
  Splits the keys listed under `files_prefix` into segments (levels 0 to 8) and
  snapshots (oldest first). Other keys, such as the lease, are dropped.
  """
  @spec parse(String.t(), String.t(), [String.t()]) ::
          {[%{key: String.t(), level: non_neg_integer, min: pos_integer, max: pos_integer}],
           [%{key: String.t(), txid: pos_integer}]}
  def parse(prefix, cell, keys) do
    files_prefix = files_prefix(prefix, cell)

    {segments, snapshots} =
      keys
      |> Enum.flat_map(&List.wrap(parse_file(files_prefix, &1)))
      |> Enum.split_with(&(&1.level < @snapshot_level))

    {segments, snapshots |> Enum.sort_by(& &1.max) |> Enum.map(&%{key: &1.key, txid: &1.max})}
  end

  defp parse_file(files_prefix, key) do
    with true <- String.starts_with?(key, files_prefix),
         [<<level::binary-size(4)>>, file] <-
           key |> String.replace_prefix(files_prefix, "") |> String.split("/"),
         true <- Regex.match?(~r/\A[0-9a-f]{4}\z/, level),
         <<min::binary-size(16), ?-, max::binary-size(16), ".ltx">> <- file,
         true <- Regex.match?(@hex, min) and Regex.match?(@hex, max),
         level = String.to_integer(level, 16),
         min = String.to_integer(min, 16),
         max = String.to_integer(max, 16),
         true <- level <= @snapshot_level and min >= 1 and min <= max,
         true <- level < @snapshot_level or min == 1 do
      %{key: key, level: level, min: min, max: max}
    else
      _ -> nil
    end
  end
end
