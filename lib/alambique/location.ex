defmodule Alambique.Location do
  @moduledoc """
  Where the data of a cell lives (design §18): a stable id, a store with its
  configuration (bucket, endpoint) and a key prefix in it.

  Every per-cell object is written under `<prefix>/cells/<cell>/` in `store`.
  The id names the location in leases, telemetry and feed positions, so it
  must not change while the location holds data.
  """

  @enforce_keys [:id, :store, :prefix]
  defstruct [:id, :store, :prefix]

  @type t :: %__MODULE__{id: String.t(), store: {module, term}, prefix: String.t()}

  @doc "Whether `location` is well formed."
  @spec valid?(term) :: boolean
  def valid?(%__MODULE__{id: id, store: {mod, _}, prefix: prefix}),
    do: is_binary(id) and id != "" and is_atom(mod) and is_binary(prefix) and prefix != ""

  def valid?(_), do: false
end
