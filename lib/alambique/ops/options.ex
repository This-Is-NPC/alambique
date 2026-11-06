defmodule Alambique.Ops.Options do
  @moduledoc false
  # Command-line settings of the `mix alambique.*` tasks: where the cell lives.
  # Store flags or variables name the one location of a single store;
  # otherwise `config :alambique, :ops, placement: {module, opts}` locates the
  # cell, and `--location ID` picks a location of it. Precedence, highest
  # first: command-line flags, environment variables, `config :alambique, :ops`.

  @store_switches [
    location: :string,
    prefix: :string,
    endpoint: :string,
    bucket: :string,
    region: :string,
    access_key_id: :string,
    secret_access_key: :string,
    path_style: :boolean
  ]

  @env %{
    prefix: "ALAMBIQUE_PREFIX",
    endpoint: "ALAMBIQUE_S3_ENDPOINT",
    bucket: "ALAMBIQUE_S3_BUCKET",
    region: "ALAMBIQUE_S3_REGION",
    access_key_id: "ALAMBIQUE_S3_ACCESS_KEY_ID",
    secret_access_key: "ALAMBIQUE_S3_SECRET_ACCESS_KEY",
    path_style: "ALAMBIQUE_S3_PATH_STYLE"
  }

  @s3_keys [:endpoint, :bucket, :region, :access_key_id, :secret_access_key, :path_style]

  @doc "Switches for `OptionParser`, to be merged with a task's own."
  def switches, do: @store_switches

  @doc "Loads the application and starts its dependencies, without starting any instance."
  def boot do
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:alambique)
    :ok
  end

  @doc """
  Replication context for the location of `cell` from parsed switches
  `parsed`. Without a `cell`, a placement needs `--location`. Raises
  `Mix.Error` with a usage message when no store is configured.
  """
  @spec ctx(keyword, String.t() | nil) :: Alambique.Replication.ctx()
  def ctx(parsed, cell \\ nil) do
    configured = Application.get_env(:alambique, :ops, [])
    flags = Keyword.take(parsed, Map.keys(@env))
    settings = Keyword.merge(env_settings(), flags)
    location = location(placement(settings, configured), parsed[:location], cell)

    Alambique.Replication.ctx(
      store: location.store,
      prefix: location.prefix,
      node: "mix-" <> Atom.to_string(node()),
      timeout_ms: configured[:timeout_ms] || 30_000
    )
  end

  @doc """
  The placement the tasks work with, from the same flags, variables and
  `config :alambique, :ops` as `ctx/2`, for tasks that look at every location.
  """
  @spec placement(keyword) :: {module, term}
  def placement(parsed) do
    flags = Keyword.take(parsed, Map.keys(@env))
    placement(Keyword.merge(env_settings(), flags), Application.get_env(:alambique, :ops, []))
  end

  defp placement(settings, configured) do
    prefix = settings[:prefix] || configured[:prefix] || "alambique"

    cond do
      Enum.any?(@s3_keys, &Keyword.has_key?(settings, &1)) ->
        {Alambique.Placement.Single, [store: s3(settings), prefix: prefix]}

      configured[:placement] ->
        configured[:placement]

      configured[:store] ->
        {Alambique.Placement.Single, [store: configured[:store], prefix: prefix]}

      true ->
        Mix.raise(no_store())
    end
  end

  defp location({mod, opts}, id, cell) do
    result =
      cond do
        id -> pick(mod.locations(opts), id)
        cell -> mod.locate(opts, cell)
        true -> only(mod.locations(opts))
      end

    case result do
      {:ok, location} -> location
      {:error, reason} -> Mix.raise("could not place the cell: #{inspect(reason)}")
    end
  end

  defp only({:ok, [location]}), do: {:ok, location}
  defp only({:ok, _}), do: Mix.raise("this placement has several locations: give --location ID")
  defp only({:error, _} = error), do: error

  defp pick({:ok, locations}, id) do
    case Enum.find(locations, &(&1.id == id)) do
      nil -> {:error, {:unknown_location, id, Enum.map(locations, & &1.id)}}
      location -> {:ok, location}
    end
  end

  defp pick({:error, _} = error, _id), do: error

  defp env_settings do
    for {key, name} <- @env, value = System.get_env(name), value != "" do
      {key, if(key == :path_style, do: value not in ["false", "0"], else: value)}
    end
  end

  defp s3(settings) do
    missing =
      Enum.reject([:endpoint, :bucket, :access_key_id, :secret_access_key], &settings[&1])

    if missing != [],
      do: Mix.raise("missing store settings: " <> Enum.map_join(missing, ", ", &flag/1))

    {Alambique.Store.S3, settings |> Keyword.take(@s3_keys) |> Map.new()}
  end

  defp flag(key), do: "--" <> String.replace(Atom.to_string(key), "_", "-")

  defp no_store do
    """
    no store configured. Give the S3 settings as flags (--endpoint, --bucket,
    --access-key-id, --secret-access-key, optional --region, --no-path-style,
    --prefix), as environment variables (ALAMBIQUE_S3_ENDPOINT,
    ALAMBIQUE_S3_BUCKET, ALAMBIQUE_S3_ACCESS_KEY_ID,
    ALAMBIQUE_S3_SECRET_ACCESS_KEY, ALAMBIQUE_S3_REGION, ALAMBIQUE_S3_PATH_STYLE,
    ALAMBIQUE_PREFIX), or in the config:

        config :alambique, :ops, store: {Alambique.Store.S3, %{...}}, prefix: "myapp"
        config :alambique, :ops, placement: {MyApp.Placement, opts}
    """
  end
end
