defmodule Alambique.AuditStore do
  @moduledoc """
  Wraps a store and logs, with the time of `now_fun`, every manifest put under
  `/trash/`, every put of another key and every delete of another key, so
  tests can check when objects were deleted against when they were marked, and
  that a reader never wrote.
  """
  @behaviour Alambique.Store

  @doc "Wraps `{module, config}`; `now_fun` returns the time the log stamps."
  def wrap({_module, _config} = inner, now_fun) do
    {__MODULE__, %{inner: inner, now: now_fun, log: :ets.new(:audit_store, [:public, :bag])}}
  end

  @doc "`{due_ms, put_ms, keys}` of every manifest put."
  def manifests(%{log: log}) do
    for {:manifest, put_ms, key, body} <- :ets.tab2list(log) do
      [_, rest] = String.split(key, "/trash/", parts: 2)
      {due, _} = Integer.parse(rest)
      {due, put_ms, Jason.decode!(body)["keys"]}
    end
  end

  @doc "`{time_ms, key}` of every put of an object that is not a manifest."
  def puts(%{log: log}), do: for({:put, at, key} <- :ets.tab2list(log), do: {at, key})

  @doc "`{time_ms, key}` of every delete of an object that is not a manifest."
  def deletes(%{log: log}), do: for({:delete, at, key} <- :ets.tab2list(log), do: {at, key})

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}, now: now, log: log}, key, body, condition, meta) do
    if String.contains?(key, "/trash/"),
      do: :ets.insert(log, {:manifest, now.(), key, IO.iodata_to_binary(body)}),
      else: :ets.insert(log, {:put, now.(), key})

    m.put(c, key, body, condition, meta)
  end

  @impl true
  def get(%{inner: {m, c}}, key), do: m.get(c, key)

  @impl true
  def get_range(%{inner: {m, c}}, key, offset, length), do: m.get_range(c, key, offset, length)

  @impl true
  def head(%{inner: {m, c}}, key), do: m.head(c, key)

  @impl true
  def list(%{inner: {m, c}}, prefix, start_after, limit),
    do: m.list(c, prefix, start_after, limit)

  @impl true
  def delete(%{inner: {m, c}, now: now, log: log}, key) do
    unless String.contains?(key, "/trash/"), do: :ets.insert(log, {:delete, now.(), key})
    m.delete(c, key)
  end
end
