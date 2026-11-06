defmodule Alambique.Placement.Single do
  @moduledoc """
  The default placement: one store and one prefix hold every cell (design
  §18). Options: `:store` (`{module, config}`, required) and `:prefix`
  (default `"alambique"`). Its only location has the id `"default"`, and
  cells cannot be relocated.
  """

  @behaviour Alambique.Placement

  alias Alambique.Location

  @impl true
  def validate_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        "options must be a keyword list"

      not match?({mod, _} when is_atom(mod), opts[:store]) ->
        ":store must be {module, config}"

      not is_binary(location_prefix(opts)) or location_prefix(opts) == "" ->
        ":prefix must be a non-empty string"

      true ->
        :ok
    end
  end

  @impl true
  def locate(opts, _cell), do: {:ok, location(opts)}

  @impl true
  def locations(opts), do: {:ok, [location(opts)]}

  defp location(opts),
    do: %Location{id: "default", store: opts[:store], prefix: location_prefix(opts)}

  defp location_prefix(opts), do: Keyword.get(opts, :prefix, "alambique")
end
