defmodule AlambiqueTestkit.ManualClock do
  @moduledoc """
  Clock that only moves when the test advances it. Many nodes in one VM share
  one timeline and each has its own offset:

      world = ManualClock.new(1_000_000)
      node_a = {ManualClock, ManualClock.config(world, 0)}
      node_b = {ManualClock, ManualClock.config(world, -500)}
      ManualClock.advance(world, 10_000)
  """
  @behaviour Alambique.Clock

  @type world :: :atomics.atomics_ref()

  @doc "Creates a timeline starting at `start_ms`."
  @spec new(integer) :: world
  def new(start_ms) do
    world = :atomics.new(1, signed: true)
    :atomics.put(world, 1, start_ms)
    world
  end

  @doc "Clock config for a node whose clock is `offset_ms` away from the timeline."
  @spec config(world, integer) :: %{world: world, offset: integer}
  def config(world, offset_ms), do: %{world: world, offset: offset_ms}

  @doc "Moves the timeline forward."
  @spec advance(world, non_neg_integer) :: :ok
  def advance(world, ms), do: :atomics.add(world, 1, ms)

  @doc "Timeline time, without any offset."
  @spec world_ms(world) :: integer
  def world_ms(world), do: :atomics.get(world, 1)

  @impl true
  def now_ms(%{world: world, offset: offset}), do: :atomics.get(world, 1) + offset
end
