defmodule Alambique.Load.Capacity do
  @moduledoc false
  # The room the harness has: the tmpfs of the service data and the store's
  # space. A step that would fill either measures the harness, not the
  # service, so it is invalid with its own reason.

  @mib 1_048_576

  @doc """
  Bytes one asleep cell takes on the data tmpfs: the growth of `shmem_mb`
  over `base_mb` spread over `populated` cells, or `default` before any.
  """
  @spec cell_bytes(number, number, non_neg_integer, number) :: number
  def cell_bytes(_shmem_mb, _base_mb, 0, default), do: default

  def cell_bytes(shmem_mb, base_mb, populated, _default),
    do: max((shmem_mb - base_mb) * @mib / populated, 1)

  @doc "Whether `fresh_cells` more cells would take the data tmpfs over `fraction` of `data_mb`."
  @spec data_exceeds?(number, non_neg_integer, number, number, number) :: boolean
  def data_exceeds?(used_mb, fresh_cells, cell_bytes, data_mb, fraction),
    do: used_mb + fresh_cells * cell_bytes / @mib > fraction * data_mb

  @doc "Whether a failed preparation was the data tmpfs filling (`:data_full`) or something else."
  @spec prepare_failure(number, number, number) :: :data_full | :prepare_failed
  def prepare_failure(shmem_mb, data_mb, fraction),
    do: if(shmem_mb >= fraction * data_mb, do: :data_full, else: :prepare_failed)

  @doc """
  Whether the store is out of room. A tmpfs reading (`%{kind: :tmpfs, used_mb}`)
  is full at `store_full_fraction` of `store_mb`; a disk reading (`%{kind:
  :disk, free_mb}`) at less than `store_min_free_mb` free.
  """
  @spec store_full?(map, map) :: boolean
  def store_full?(%{kind: :tmpfs, used_mb: used}, limits),
    do: used >= limits.store_full_fraction * limits.store_mb

  def store_full?(%{kind: :disk, free_mb: free}, limits), do: free < limits.store_min_free_mb

  @doc "Available MiB from the output of `df -P -B1 <dir>`."
  @spec parse_df_free_mb(String.t()) :: float | nil
  def parse_df_free_mb(text) do
    with [_head, line | _] <- String.split(text, "\n", trim: true),
         [_fs, _size, _used, avail | _] <- String.split(line),
         {bytes, ""} <- Integer.parse(avail) do
      bytes / @mib
    else
      _ -> nil
    end
  end
end
