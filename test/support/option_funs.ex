defmodule Alambique.OptionFuns do
  @moduledoc "Compiled functions used by the option behaviour tests."

  @doc "Per-cell migrations with one version."
  def migrations(_cell), do: [{1, "CREATE TABLE IF NOT EXISTS opt (x INTEGER)"}]
end
