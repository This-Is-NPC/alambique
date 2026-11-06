defmodule Mix.Tasks.Alambique.Restore do
  @shortdoc "Restores a cell from the store into a local SQLite file"

  @moduledoc """
  Restores a cell into a local database file: the latest state, a TXID, or a
  point in time.

      mix alambique.restore CELL PATH [--at TARGET] [options]

  `TARGET` is `latest` (default), `txid:42`, an ISO 8601 UTC time such as
  `2026-10-26T12:00:00Z`, or `ms:1790000000000` (Unix milliseconds). The file
  is built next to `PATH` and renamed into place; an existing file at `PATH`
  is replaced. The cell is not touched, awake or not.

  ## Store options

  The task needs a store and a prefix. Flags win over environment variables,
  which win over `config :alambique, :ops`.

    * `--endpoint` / `ALAMBIQUE_S3_ENDPOINT` - base URL of the S3 service
    * `--bucket` / `ALAMBIQUE_S3_BUCKET`
    * `--access-key-id` / `ALAMBIQUE_S3_ACCESS_KEY_ID`
    * `--secret-access-key` / `ALAMBIQUE_S3_SECRET_ACCESS_KEY`
    * `--region` / `ALAMBIQUE_S3_REGION` - default `us-east-1`
    * `--no-path-style` / `ALAMBIQUE_S3_PATH_STYLE=false` - virtual-hosted
      buckets; path style is the default
    * `--prefix` / `ALAMBIQUE_PREFIX` - key prefix of the instance, default
      `alambique`
    * `--location ID` - the location of the cell when it is not the one the
      placement gives (see below)

  In the config, `store: {module, config}`, `prefix: "..."` and optionally
  `timeout_ms:` set the same things for any store module:

      config :alambique, :ops, store: {Alambique.Store.S3, %{...}}, prefix: "myapp"

  An instance with a placement (design §18) is described by the same module:
  the tasks ask it for the location of the cell, or take the one named by
  `--location`, which must be among its `locations/1`.

      config :alambique, :ops, placement: {MyApp.Placement, opts}
  """

  use Mix.Task

  alias Alambique.Ops
  alias Alambique.Ops.Options

  @impl Mix.Task
  def run(args) do
    {parsed, rest} = OptionParser.parse!(args, strict: [at: :string] ++ Options.switches())

    {cell, path} =
      case rest do
        [cell, path] -> {cell, path}
        _ -> Mix.raise("usage: mix alambique.restore CELL PATH [--at TARGET] [options]")
      end

    target =
      case Ops.parse_target(parsed[:at] || "latest") do
        {:ok, target} ->
          target

        :error ->
          Mix.raise("bad --at #{inspect(parsed[:at])}: use latest, txid:N, ms:N or ISO 8601")
      end

    Options.boot()
    ctx = Options.ctx(parsed, cell)

    case Ops.restore(ctx, cell, Path.expand(path), target) do
      {:ok, r} ->
        Mix.shell().info(
          "Restored #{cell} to #{Path.expand(path)}: TXID #{r.txid}, epoch #{r.epoch}, " <>
            "#{r.segments} files applied"
        )

      {:error, reason} ->
        Mix.raise("could not restore #{cell}: #{inspect(reason)}")
    end
  end
end
