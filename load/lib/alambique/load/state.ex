defmodule Alambique.Load.State do
  @moduledoc false
  # What the steps of one ramp share: how many cells exist, how many are awake.

  def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)
  def get(key, default \\ nil), do: Agent.get(__MODULE__, &Map.get(&1, key, default))
  def put(key, value), do: Agent.update(__MODULE__, &Map.put(&1, key, value))
end
