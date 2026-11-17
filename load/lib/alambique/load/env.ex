defmodule Alambique.Load.Env do
  @moduledoc false
  # The wiring between scripts/load-case and the processes it starts: the ids
  # of the containers and the node name for the driver, the settings of the
  # service for the service container. The settings of a round are not here:
  # they come from the round file (`Alambique.Load.Config`).

  @spec get(String.t(), String.t() | nil) :: String.t() | nil
  def get(name, default \\ nil), do: System.get_env("ALAMBIQUE_LOAD_" <> name, default)

  @spec int(String.t(), integer) :: integer
  def int(name, default) do
    case get(name) do
      nil -> default
      "" -> default
      text -> String.to_integer(text)
    end
  end
end
