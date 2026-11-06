defmodule Mix.Tasks.Alambique.CheckDeploy do
  @shortdoc "Fails when the code cannot read the oldest live schema version"

  @moduledoc """
  Checks that the code about to be deployed can read every live cell (design
  §17), before it runs.

      mix alambique.check_deploy [--migrations MODULE] [--window N] [--sample N] [store options]

  The task lists the schema index of every location of the placement: a few
  requests per location, whatever the number of cells. It compares the oldest
  live version of each location with the oldest version the code accepts, its
  schema version (the highest of its migrations) minus the migration window,
  and at least 1.

  When the migrations are chosen per cell, each cell has its own schema
  version, so the task lists every cell of every version and calls the
  function once per cell. It then names the cells whose version is below
  their own schema version minus the window, and at least 1.

    * `--migrations MODULE` - the module that exports `migrations/0`, as given
      to the instance, or `MODULE:FUNCTION` for migrations chosen per cell
      (`FUNCTION/1` receives the cell id). Extra arguments for the function
      cannot be given here: set `{module, function, extra_args}` in
      `config :alambique, :ops, migrations: ...`, which is also the default.
    * `--timeout MS` - time each listing request may take (default: the
      `:timeout_ms` of the `:ops` config, or 30000)
    * `--window N` - the migration window of the instance (default: the
      `:migration_window` of the `:ops` config, or 1)
    * `--sample N` - cells named per location after "and N more" (default 10)

  Run it against the same placement configuration as production, through the
  store options of `Mix.Tasks.Alambique.Restore` or `config :alambique, :ops`:
  another configuration lists other stores and passes for the wrong reason.

  Exit status:

    * 0 - every location holds only versions the code can read
    * 1 - the deploy is refused: a location holds a cell older than the code
      reads
    * 2 - the check could not run: options are missing or invalid, a location
      could not be listed, or the function of per-cell migrations failed

  A cell that code for the previous release left behind can be migrated now
  with the sweeper (the instance option `:sweeper`, or `Alambique.sweep/2`);
  run the check again afterwards.
  """

  use Mix.Task

  alias Alambique.Ops.Options

  @default_sample 10

  @impl Mix.Task
  def run(args) do
    {parsed, rest} =
      OptionParser.parse!(args,
        strict:
          [migrations: :string, window: :integer, timeout: :integer, sample: :integer] ++
            Options.switches()
      )

    if rest != [], do: fail("usage: mix alambique.check_deploy [options]")
    sample = sample(parsed)
    Options.boot()

    result =
      try do
        Alambique.check_deploy(
          [
            placement: Options.placement(parsed),
            migrations: migrations(parsed),
            migration_window: window(parsed)
          ],
          timeout_ms: parsed[:timeout] || ops()[:timeout_ms] || 30_000
        )
      rescue
        error in [ArgumentError, Mix.Error] -> fail(Exception.message(error))
      end

    case result do
      {:ok, report} ->
        print(report, sample)
        Mix.shell().info(allowed(report))

      {:error, {:too_old, report}} ->
        print(report, sample)
        Mix.shell().error(refusal(report, sample))
        exit({:shutdown, 1})

      {:error, reason} ->
        fail("could not check the deploy: #{inspect(reason)}")
    end
  end

  defp migrations(parsed) do
    case parsed[:migrations] || ops()[:migrations] do
      nil ->
        fail(
          "give the migrations: --migrations MODULE or config :alambique, :ops, migrations: ..."
        )

      name when is_binary(name) ->
        case String.split(name, ":") do
          [module] -> Module.concat([module])
          [module, function] -> {Module.concat([module]), String.to_atom(function)}
          _ -> fail("--migrations must be MODULE or MODULE:FUNCTION")
        end

      migrations ->
        migrations
    end
  end

  defp ops, do: Application.get_env(:alambique, :ops, [])

  defp window(parsed),
    do: parsed[:window] || ops()[:migration_window] || 1

  defp sample(parsed) do
    case parsed[:sample] || @default_sample do
      n when is_integer(n) and n > 0 -> n
      _ -> fail("--sample must be a positive integer")
    end
  end

  defp print(%{code_version: nil} = report, sample) do
    shell = Mix.shell()
    shell.info("Migrations are chosen per cell; each cell is compared with its own version.")

    for %{location: id, versions: versions, too_old_cells: cells} <- report.locations do
      shell.info("  location #{id}: #{versions(versions)}#{cells(cells, sample)}")
    end
  end

  defp print(report, _sample) do
    shell = Mix.shell()

    shell.info(
      "Code at schema version #{report.code_version} reads cells from version #{report.oldest_readable}."
    )

    for %{location: id, versions: versions, too_old: too_old} <- report.locations do
      shell.info(
        "  location #{id}: #{versions(versions)}#{if too_old, do: "  <- too old", else: ""}"
      )
    end
  end

  defp versions([]), do: "no cells"
  defp versions(versions), do: "versions " <> Enum.join(versions, ", ")

  defp cells([], _sample), do: ""
  defp cells(cells, sample), do: "  <- too old: " <> sampled(cells, sample)

  defp sampled(cells, sample) do
    {shown, rest} = Enum.split(cells, sample)
    Enum.join(shown, ", ") <> if(rest == [], do: "", else: " and #{length(rest)} more")
  end

  defp allowed(%{code_version: nil}), do: "Deploy allowed: every live cell is within its window."

  defp allowed(report),
    do: "Deploy allowed: every live cell is at version #{report.oldest_readable} or later."

  defp refusal(%{code_version: nil} = report, sample) do
    old =
      for %{too_old: true, location: id, too_old_cells: cells} <- report.locations,
          do: "#{id} (#{sampled(cells, sample)})"

    "Deploy refused: cells in #{Enum.join(old, ", ")} are older than their schema version " <>
      "minus the window. Migrate them with the previous release's sweeper " <>
      "or Alambique.sweep/2, then run the check again."
  end

  defp refusal(report, _sample) do
    old =
      for %{too_old: true, location: id, versions: [oldest | _]} <- report.locations,
          do: "#{id} (version #{oldest})"

    "Deploy refused: cells in #{Enum.join(old, ", ")} are older than version " <>
      "#{report.oldest_readable}. Migrate them with the previous release's sweeper " <>
      "or Alambique.sweep/2, then run the check again."
  end

  defp fail(message) do
    Mix.shell().error(message)
    exit({:shutdown, 2})
  end
end
