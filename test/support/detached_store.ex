defmodule Alambique.DetachedStore do
  @moduledoc """
  Wraps a store so a put outlives its caller, as an HTTP request that was
  already sent does when the process that sent it is killed (design §6).

  The put runs in a process that nothing links to the caller; the caller
  waits for its result. Wrap a `GatedStore` to hold the request. When
  `notify` is a pid it gets `{:put_done, key, result}` as each put ends, even
  after its caller is gone.
  """
  @behaviour Alambique.Store

  @spec wrap({module, term}, pid | nil) :: {module, map}
  def wrap({_module, _config} = inner, notify \\ nil),
    do: {__MODULE__, %{inner: inner, notify: notify}}

  @impl Alambique.Store
  def put(%{inner: {m, c}, notify: notify}, key, body, condition, meta) do
    caller = self()
    ref = make_ref()

    spawn(fn ->
      result = m.put(c, key, body, condition, meta)
      if notify, do: send(notify, {:put_done, key, result})
      send(caller, {ref, result})
    end)

    receive do
      {^ref, result} -> result
    end
  end

  @impl Alambique.Store
  def get(%{inner: {m, c}}, key), do: m.get(c, key)

  @impl Alambique.Store
  def get_range(%{inner: {m, c}}, key, offset, length), do: m.get_range(c, key, offset, length)

  @impl Alambique.Store
  def head(%{inner: {m, c}}, key), do: m.head(c, key)

  @impl Alambique.Store
  def list(%{inner: {m, c}}, prefix, start_after, limit),
    do: m.list(c, prefix, start_after, limit)

  @impl Alambique.Store
  def delete(%{inner: {m, c}}, key), do: m.delete(c, key)

  @impl Alambique.Store
  def location(%{inner: inner}), do: Alambique.Store.location(inner)
end
