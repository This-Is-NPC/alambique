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
    do: is_binary(id) and id != "" and is_atom(mod) and valid_prefix?(prefix)

  def valid?(_), do: false

  @doc """
  Whether `prefix` is a safe key prefix: slash-separated segments that are not
  empty, `.` or `..`, with no control characters. Nested prefixes are allowed,
  but no instance's prefix may be a path prefix of another instance's in the
  same store, since their keys would overlap.
  """
  @spec valid_prefix?(term) :: boolean
  def valid_prefix?(prefix) when is_binary(prefix) do
    String.valid?(prefix) and not String.contains?(prefix, control_chars()) and
      Enum.all?(String.split(prefix, "/"), &(&1 not in ["", ".", ".."]))
  end

  def valid_prefix?(_), do: false

  defp control_chars, do: [<<127>> | for(c <- 0..31, do: <<c>>)]
end
