defmodule Alambique.Fault do
  @moduledoc """
  Named fault points in library code. A no-op unless a test installed a
  handler with `install/1`; the check is one `:persistent_term` read.
  """

  @key {__MODULE__, :handler}

  @doc "Called by library code at a crash point; `context` is any term. The handler's result is dropped."
  @spec point(atom, term) :: :ok
  def point(name, context \\ nil) do
    case :persistent_term.get(@key, nil) do
      nil ->
        :ok

      handler ->
        _ = handler.(name, context)
        :ok
    end
  end

  @doc """
  A fault point that can fail the operation: `{:error, reason}` when the
  handler returns it, `:ok` otherwise.
  """
  @spec check(atom, term) :: :ok | {:error, term}
  def check(name, context \\ nil) do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      handler -> error_or_ok(handler.(name, context))
    end
  end

  defp error_or_ok({:error, _} = error), do: error
  defp error_or_ok(_), do: :ok

  @doc false
  @spec install((atom, term -> term)) :: :ok
  def install(handler) when is_function(handler, 2), do: :persistent_term.put(@key, handler)

  @doc false
  @spec uninstall() :: :ok
  def uninstall do
    _ = :persistent_term.erase(@key)
    :ok
  end
end
