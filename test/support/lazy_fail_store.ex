defmodule AlambiqueTestkit.LazyFailStore do
  @moduledoc """
  A memory store that fails one ranged read. The lazy restore asks for the
  page-1 frame at offset 100 of the newest segment; a full restore reads the
  whole object and succeeds.
  """

  @behaviour Alambique.Store

  alias AlambiqueTestkit.MemoryStore

  @impl true
  def location(%{inner: inner}), do: inner

  @impl true
  def put(%{inner: inner}, key, body, condition, meta),
    do: MemoryStore.put(inner, key, body, condition, meta)

  @impl true
  def get(%{inner: inner}, key), do: MemoryStore.get(inner, key)

  @impl true
  def get_range(%{inner: inner, key: key, offset: offset}, range_key, range_offset, length) do
    if range_key == key and range_offset == offset do
      {:error, :injected}
    else
      MemoryStore.get_range(inner, range_key, range_offset, length)
    end
  end

  @impl true
  def head(%{inner: inner}, key), do: MemoryStore.head(inner, key)

  @impl true
  def list(%{inner: inner}, prefix, start_after, limit),
    do: MemoryStore.list(inner, prefix, start_after, limit)

  @impl true
  def delete(%{inner: inner}, key), do: MemoryStore.delete(inner, key)
end
