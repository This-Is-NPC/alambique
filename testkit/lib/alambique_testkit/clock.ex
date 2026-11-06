defmodule AlambiqueTestkit.Clock do
  @moduledoc """
  Wall clock with an adjustable offset per VM (one VM is one node), for
  clock skew tests. Peer nodes set their own offset.
  """
  @behaviour Alambique.Clock

  @key {__MODULE__, :offset_ms}

  @impl true
  def now_ms(_config), do: System.os_time(:millisecond) + offset()

  @doc "Sets this node's offset in milliseconds."
  @spec set_offset(integer) :: :ok
  def set_offset(ms), do: :persistent_term.put(@key, ms)

  @doc "Returns this node's offset."
  @spec offset() :: integer
  def offset, do: :persistent_term.get(@key, 0)
end
