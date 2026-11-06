defmodule Alambique.Placement do
  @moduledoc """
  Decides where cells live (design §18). The library knows no hierarchy: no
  regions, organizations or tenants. The caller supplies a module with this
  behaviour and its options, `placement: {module, opts}`; every callback
  receives `opts` first.

    * `c:validate_id/2` - how cell ids are structured (optional; the default
      accepts `[A-Za-z0-9_.-]`, 1 to 128 bytes). Whatever it accepts must also
      be usable in an object key and as a directory name: no `/`, no NUL, at
      most 200 bytes, and not `.` or `..` (a store or proxy may fold them in a
      key). The library refuses other ids.
    * `c:locate/2` - the `Alambique.Location` of a cell.
    * `c:locations/1` - every location, for whole-store work such as the
      change feed and cross-cell queries.
    * `c:commit_move/4` - records a completed move (optional; without it cells
      cannot be relocated).

  **Guarantee P1.** A cell id maps to exactly one location at a time, and only
  that location accepts its writes. `c:locate/2` answers may be cached by the
  library, so a node can act on an old answer for a while; the old location
  then refuses to wake the cell (`{:error, {:moved, location}}`) and the
  library asks the placement again. `c:locate/2` must therefore answer
  consistently once `c:commit_move/4` returned: the destination, on every node.
  """

  alias Alambique.{Config, Location}

  @type opts :: term
  @type cell :: String.t()

  @callback validate_id(opts, cell) :: :ok | {:error, term}
  @callback locate(opts, cell) :: {:ok, Location.t()} | {:error, term}
  @callback locations(opts) :: {:ok, [Location.t()]} | {:error, term}
  @callback commit_move(opts, cell, from :: Location.t(), to :: Location.t()) ::
              :ok | {:error, term}
  @callback validate_options(opts) :: :ok | {:error, String.t()}

  @optional_callbacks validate_id: 2, commit_move: 4, validate_options: 1

  @default_id ~r/\A[A-Za-z0-9_.\-]{1,128}\z/
  @max_id_bytes 200

  @doc "Whether `cell` follows the default id rule."
  @spec default_id?(term) :: boolean
  def default_id?(cell), do: is_binary(cell) and Regex.match?(@default_id, cell)

  @doc "Checks the id against the library's limits and the placement's rule."
  @spec validate_id(Config.t(), term) :: :ok | {:error, term}
  def validate_id(%{placement: {mod, opts}}, cell) do
    cond do
      not is_binary(cell) or cell in ["", ".", ".."] or byte_size(cell) > @max_id_bytes or
          String.contains?(cell, ["/", <<0>>]) ->
        {:error, :invalid_cell_id}

      function_exported?(mod, :validate_id, 2) ->
        mod.validate_id(opts, cell)

      default_id?(cell) ->
        :ok

      true ->
        {:error, :invalid_cell_id}
    end
  end

  @doc """
  The location of `cell`. Answers are cached per instance and dropped by
  `invalidate/2`; `fresh: true` asks the placement and refreshes the cache.
  """
  @spec locate(Config.t(), cell, keyword) :: {:ok, Location.t()} | {:error, term}
  def locate(%{placement: {mod, opts}} = config, cell, options \\ []) do
    with :ok <- validate_id(config, cell) do
      case not Keyword.get(options, :fresh, false) && :ets.lookup(config.locations, cell) do
        [{_, location}] -> {:ok, location}
        _ -> ask(config, mod, opts, cell)
      end
    end
  end

  defp ask(config, mod, opts, cell) do
    case mod.locate(opts, cell) do
      {:ok, location} ->
        if Location.valid?(location) do
          remember(config, cell, location)
          {:ok, location}
        else
          {:error, {:bad_location, location}}
        end

      {:error, reason} ->
        {:error, {:placement, reason}}
    end
  end

  defp remember(config, cell, location),
    do: Alambique.BoundedTable.put(config.locations, config.hint_limit, {cell, location})

  @doc "Forgets the cached location of `cell`."
  @spec invalidate(Config.t(), cell) :: true
  def invalidate(config, cell), do: :ets.delete(config.locations, cell)

  @doc "Every location of the placement."
  @spec locations(Config.t()) :: {:ok, [Location.t()]} | {:error, term}
  def locations(%{placement: {mod, opts}}) do
    case mod.locations(opts) do
      {:ok, list} when is_list(list) ->
        if Enum.all?(list, &Location.valid?/1),
          do: {:ok, list},
          else: {:error, {:bad_locations, list}}

      {:error, reason} ->
        {:error, {:placement, reason}}
    end
  end

  @doc "The location with `id`."
  @spec location(Config.t(), String.t()) :: {:ok, Location.t()} | {:error, term}
  def location(config, id) do
    with {:ok, list} <- locations(config) do
      case Enum.find(list, &(&1.id == id)) do
        nil -> {:error, {:unknown_location, id}}
        location -> {:ok, location}
      end
    end
  end

  @doc """
  The location to list: `nil` is the only location of the placement
  (`:location_required` when there are several), a string names one and a
  `Alambique.Location` stands for itself.
  """
  @spec listed_location(Config.t(), String.t() | Location.t() | nil) ::
          {:ok, Location.t()} | {:error, term}
  def listed_location(config, nil) do
    case locations(config) do
      {:ok, [location]} -> {:ok, location}
      {:ok, _} -> {:error, :location_required}
      {:error, _} = error -> error
    end
  end

  def listed_location(config, id) when is_binary(id), do: location(config, id)

  def listed_location(_config, %Location{} = location) do
    if Location.valid?(location),
      do: {:ok, location},
      else: raise(ArgumentError, ":location is not a valid location: #{inspect(location)}")
  end

  def listed_location(_config, other),
    do:
      raise(
        ArgumentError,
        ":location must be a location id or a location, got: #{inspect(other)}"
      )

  @doc "Records a completed move in the placement."
  @spec commit_move(Config.t(), cell, Location.t(), Location.t()) :: :ok | {:error, term}
  def commit_move(%{placement: {mod, opts}}, cell, from, to) do
    if function_exported?(mod, :commit_move, 4),
      do: mod.commit_move(opts, cell, from, to),
      else: {:error, :relocation_unsupported}
  end
end
