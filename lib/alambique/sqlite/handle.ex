defmodule Alambique.Sqlite.Handle do
  @moduledoc false
  # A gated connection (`Alambique.Sqlite.open/2` with `gated: true`): the
  # reference and the secret of the connection. The secret is not printed.

  @enforce_keys [:ref, :key]
  @derive {Inspect, except: [:key]}
  defstruct [:ref, :key]

  @type t :: %__MODULE__{ref: reference, key: non_neg_integer}
end
