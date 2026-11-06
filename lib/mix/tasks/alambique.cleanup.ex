defmodule Mix.Tasks.Alambique.Cleanup do
  @shortdoc "Removes what a moved cell left at its old location"

  @moduledoc """
  Asks a running cluster to remove the objects a cell left at the location it
  moved away from (design §18): segments, snapshots, the frozen marker and the
  schema index entry. The lease there stays, marked as moved. The objects are
  marked and deleted after the instance's `deletion_grace_ms` (design §9.1).

      mix alambique.cleanup CELL LOCATION_ID --instance NAME --connect NODE [options]

  The task joins the Erlang cluster like `mix alambique.move`; see
  `Mix.Tasks.Alambique.Move` for `--instance`, `--connect` and `--cookie`.
  `LOCATION_ID` is the location the cell left, not the one it lives in.

    * `--grace-ms MS` - time that must have passed since the move, default the
      instance's `moved_grace_ms`. A smaller value can delete files a reader
      that started before the move still needs.
    * `--timeout MS` - call timeout, default 60000

  The instance runs this after every move once the grace period is over, so the
  task is for a move whose cleanup was interrupted or that must free storage
  early. Running it again is safe. It refuses the location the cell lives in.
  """

  use Mix.Task

  alias Alambique.Ops
  alias Alambique.Ops.Cluster

  @impl Mix.Task
  def run(args) do
    {parsed, rest} =
      OptionParser.parse!(args,
        strict: Cluster.switches() ++ [timeout: :integer, grace_ms: :integer]
      )

    {cell, from} =
      case rest do
        [cell, from] ->
          {cell, from}

        _ ->
          Mix.raise(
            "usage: mix alambique.cleanup CELL LOCATION_ID --instance NAME --connect NODE"
          )
      end

    instance = Cluster.instance!(parsed)
    Mix.Task.run("app.config")
    via = Cluster.connect!(parsed)
    opts = Keyword.take(parsed, [:grace_ms])

    case Ops.cleanup_moved(via, instance, cell, from, opts, parsed[:timeout] || 60_000) do
      {:ok, %{result: :cleaned, objects: n}} ->
        Mix.shell().info(
          "Cleaned #{n} objects of #{cell} at location #{from} (deleted after deletion_grace_ms)"
        )

      {:ok, %{result: :waiting, ready_at_ms: at}} ->
        Mix.shell().info(
          "Not yet: location #{from} keeps the objects of #{cell} until " <>
            DateTime.to_iso8601(DateTime.from_unix!(at, :millisecond))
        )

      {:error, reason} ->
        Mix.raise("could not clean up #{cell} at #{from}: #{inspect(reason)}")
    end
  end
end
