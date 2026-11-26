defmodule Alambique.GateStore do
  @moduledoc """
  Wraps a store and holds every `get` of a key that matches a pattern until
  the test opens the gate. `started/1` says how many different keys were held
  so far (a request that is retried counts once), so a test can wait for the
  requests it expects to be under way together, then `open/1` them.
  """
  @behaviour Alambique.Store

  @doc "Wraps `{module, config}`; a `get` of a key that matches `pattern` waits for `open/1`."
  def wrap({_module, _config} = inner, %Regex{} = pattern) do
    {__MODULE__,
     %{
       inner: inner,
       pattern: pattern,
       started: :ets.new(:gate_started, [:set, :public]),
       open: :atomics.new(1, [])
     }}
  end

  @doc "How many different keys have a held `get`."
  def started(%{started: started}), do: :ets.info(started, :size)

  @doc "Lets the held `get`s, and the later ones, go."
  def open(%{open: open}), do: :atomics.put(open, 1, 1)

  defp hold(%{pattern: pattern, started: started, open: open}, key) do
    if Regex.match?(pattern, key) do
      :ets.insert(started, {key})
      wait(open)
    end
  end

  defp wait(open) do
    if :atomics.get(open, 1) == 0 do
      Process.sleep(1)
      wait(open)
    end
  end

  @impl true
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl true
  def put(%{inner: {m, c}}, key, body, condition, meta), do: m.put(c, key, body, condition, meta)

  @impl true
  def get(%{inner: {m, c}} = s, key) do
    hold(s, key)
    m.get(c, key)
  end

  @impl true
  def get_range(%{inner: {m, c}}, key, offset, length), do: m.get_range(c, key, offset, length)

  @impl true
  def head(%{inner: {m, c}}, key), do: m.head(c, key)

  @impl true
  def list(%{inner: {m, c}}, prefix, start_after, limit),
    do: m.list(c, prefix, start_after, limit)

  @impl true
  def delete(%{inner: {m, c}}, key), do: m.delete(c, key)
end
