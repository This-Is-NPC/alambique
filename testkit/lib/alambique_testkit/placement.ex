defmodule AlambiqueTestkit.Placement do
  @moduledoc """
  A placement over named locations for tests (design §18).

  Cells go to a location by a hash of their id unless `:initial` names one.
  The record of moved cells lives in a `AlambiqueTestkit.MemoryStore` of its
  own, so every node of a test cluster sees `commit_move/4` at once and
  store faults aimed at the locations do not reach it.

  Options:

    * `:locations` - list of `Alambique.Location` (required)
    * `:table` - `{AlambiqueTestkit.MemoryStore, config}` for the record
      (required)
    * `:initial` - map of cell id to location id, overriding the hash
  """

  @behaviour Alambique.Placement

  alias Alambique.Location

  @impl true
  def validate_options(opts) do
    if is_list(opts[:locations]) and opts[:locations] != [] and match?({_, _}, opts[:table]),
      do: :ok,
      else: ":locations and :table are required"
  end

  @impl true
  def locate(opts, cell) do
    {store, config} = opts[:table]

    case store.get(config, key(cell)) do
      {:ok, id, _etag, _meta} -> find(opts, id)
      {:error, :not_found} -> {:ok, initial(opts, cell)}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def locations(opts), do: {:ok, opts[:locations]}

  @impl true
  def commit_move(opts, cell, %Location{} = from, %Location{} = to) do
    {store, config} = opts[:table]

    condition =
      case store.get(config, key(cell)) do
        {:ok, id, etag, _} when id == from.id ->
          {:if_match, etag}

        {:ok, id, _, _} when id == to.id ->
          :done

        {:ok, id, _, _} ->
          {:conflict, id}

        {:error, :not_found} ->
          if initial(opts, cell).id == from.id, do: :if_none_match, else: {:conflict, nil}

        {:error, reason} ->
          {:error, reason}
      end

    case condition do
      :done -> :ok
      {:conflict, id} -> {:error, {:conflict, id}}
      {:error, _} = error -> error
      condition -> put(store, config, cell, to.id, condition)
    end
  end

  defp put(store, config, cell, id, condition) do
    case store.put(config, key(cell), id, condition, %{}) do
      {:ok, _etag} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp initial(opts, cell) do
    locations = opts[:locations]

    case opts[:initial][cell] && Enum.find(locations, &(&1.id == opts[:initial][cell])) do
      %Location{} = location -> location
      _ -> Enum.at(locations, :erlang.phash2(cell, length(locations)))
    end
  end

  defp find(opts, id) do
    case Enum.find(opts[:locations], &(&1.id == id)) do
      nil -> {:error, {:unknown_location, id}}
      location -> {:ok, location}
    end
  end

  defp key(cell), do: "placement/" <> cell
end
