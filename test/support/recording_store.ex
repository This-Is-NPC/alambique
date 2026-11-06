defmodule Alambique.RecordingStore do
  @moduledoc """
  Wraps a store and reports every `list/4` limit to the test process as
  `{:store_list, prefix, limit}`. Used to show that listing options are
  honoured.
  """

  @behaviour Alambique.Store

  def wrap({module, config}, owner) do
    {__MODULE__, %{inner: {module, config}, owner: owner}}
  end

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}}, key, body, condition, meta), do: m.put(c, key, body, condition, meta)

  @impl true
  def get(%{inner: {m, c}}, key), do: m.get(c, key)

  @impl true
  def get_range(%{inner: {m, c}}, key, offset, length), do: m.get_range(c, key, offset, length)

  @impl true
  def head(%{inner: {m, c}}, key), do: m.head(c, key)

  @impl true
  def list(%{inner: {m, c}, owner: owner}, prefix, start_after, limit) do
    send(owner, {:store_list, prefix, limit})
    m.list(c, prefix, start_after, limit)
  end

  @impl true
  def delete(%{inner: {m, c}}, key), do: m.delete(c, key)
end
