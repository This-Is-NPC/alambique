defmodule Alambique.Store.Guard do
  @moduledoc false
  # Wraps the store of every location, so a store that raises, throws or exits
  # answers `{:error, {:store_crashed, kind, reason}}` like any failed request
  # (design §3). The cell process then retries or stops by its own rules
  # instead of crashing. Config is the `{module, config}` it wraps.

  @behaviour Alambique.Store

  @impl true
  def put({store, config}, key, body, condition, meta),
    do: guard(fn -> store.put(config, key, body, condition, meta) end)

  @impl true
  def get({store, config}, key), do: guard(fn -> store.get(config, key) end)

  @impl true
  def head({store, config}, key), do: guard(fn -> store.head(config, key) end)

  @impl true
  def get_range({store, config}, key, offset, length),
    do: guard(fn -> store.get_range(config, key, offset, length) end)

  @impl true
  def list({store, config}, prefix, start_after, limit),
    do: guard(fn -> store.list(config, prefix, start_after, limit) end)

  @impl true
  def delete({store, config}, key), do: guard(fn -> store.delete(config, key) end)

  defp guard(fun) do
    fun.()
  catch
    kind, reason -> {:error, {:store_crashed, kind, reason}}
  end
end
