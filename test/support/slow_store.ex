defmodule Alambique.SlowStore do
  @moduledoc """
  Wraps a store and sleeps `ms` before every request, the round trip of a
  remote store.
  """
  @behaviour Alambique.Store

  @doc "Wraps `{module, config}`; every request takes `ms` more."
  def wrap({_module, _config} = inner, ms), do: {__MODULE__, %{inner: inner, ms: ms}}

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}, ms: ms}, key, body, condition, meta) do
    Process.sleep(ms)
    m.put(c, key, body, condition, meta)
  end

  @impl true
  def get(%{inner: {m, c}, ms: ms}, key) do
    Process.sleep(ms)
    m.get(c, key)
  end

  @impl true
  def get_range(%{inner: {m, c}, ms: ms}, key, offset, length) do
    Process.sleep(ms)
    m.get_range(c, key, offset, length)
  end

  @impl true
  def head(%{inner: {m, c}, ms: ms}, key) do
    Process.sleep(ms)
    m.head(c, key)
  end

  @impl true
  def list(%{inner: {m, c}, ms: ms}, prefix, start_after, limit) do
    Process.sleep(ms)
    m.list(c, prefix, start_after, limit)
  end

  @impl true
  def delete(%{inner: {m, c}, ms: ms}, key) do
    Process.sleep(ms)
    m.delete(c, key)
  end
end
