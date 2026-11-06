defmodule Alambique.Fault do
  @moduledoc """
  Named fault points in library code. A no-op unless a test installed a
  handler with `install/1`; the check is one `:persistent_term` read.
  """

  @key {__MODULE__, :handler}

  @doc "Called by library code at a crash point; `context` is any term."
  @spec point(atom, term) :: :ok
  def point(name, context \\ nil) do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      handler -> handler.(name, context)
    end
  end

  @doc false
  def install(handler) when is_function(handler, 2), do: :persistent_term.put(@key, handler)

  @doc false
  def uninstall, do: :persistent_term.erase(@key) && :ok
end
