defmodule Mix.Tasks.Alambique.Relocate do
  @shortdoc "Moves a cell to another location of the placement"

  @moduledoc """
  Asks a running cluster to move a cell to another location (design §18): the
  cell is hibernated and fenced at its location, copied to the destination, the
  old location is marked as moved, and the placement records the move. The
  objects the cell left at the old location are marked for deletion once
  `moved_grace_ms` passed (the reaper deletes them `deletion_grace_ms` later); the task reports whether that happened (see
  `Mix.Tasks.Alambique.Cleanup` to do it by hand). A cell can move back to a
  location it left after that cleanup.

      mix alambique.relocate CELL LOCATION_ID --instance NAME --connect NODE [options]

  The task joins the Erlang cluster like `mix alambique.move`; see
  `Mix.Tasks.Alambique.Move` for `--instance`, `--connect` and `--cookie`.
  `LOCATION_ID` is the id of one of the locations of the instance's
  placement.

    * `--timeout MS` - time the whole move may take, default 60000

  If the task fails or is interrupted, run it again with the same arguments:
  the move finishes from where it stopped. Calls to the cell during the move
  get `{:error, {:unavailable, "relocate", _}}`.
  """

  use Mix.Task

  alias Alambique.Ops
  alias Alambique.Ops.Cluster

  @impl Mix.Task
  def run(args) do
    {parsed, rest} = OptionParser.parse!(args, strict: Cluster.switches() ++ [timeout: :integer])

    {cell, to} =
      case rest do
        [cell, to] ->
          {cell, to}

        _ ->
          Mix.raise(
            "usage: mix alambique.relocate CELL LOCATION_ID --instance NAME --connect NODE"
          )
      end

    instance = Cluster.instance!(parsed)
    Mix.Task.run("app.config")
    via = Cluster.connect!(parsed)

    timeout = parsed[:timeout] || 60_000
    from = locate!(via, instance, cell)

    case Ops.relocate(via, instance, cell, to, timeout) do
      {:ok, location} ->
        Mix.shell().info("Moved #{cell} to location #{location.id}")
        if from != location.id, do: report_cleanup(via, instance, cell, from, timeout)

      {:error, reason} ->
        Mix.raise("could not relocate #{cell}: #{inspect(reason)}")
    end
  end

  defp locate!(via, instance, cell) do
    case Ops.locate(via, instance, cell) do
      {:ok, id} -> id
      {:error, reason} -> Mix.raise("could not locate #{cell}: #{inspect(reason)}")
    end
  end

  defp report_cleanup(via, instance, cell, from, timeout) do
    case Ops.cleanup_moved(via, instance, cell, from, [], timeout) do
      {:ok, %{result: :cleaned, objects: n}} ->
        Mix.shell().info(
          "Cleaned #{n} objects of #{cell} at location #{from} (deleted after deletion_grace_ms)"
        )

      {:ok, %{result: :waiting, ready_at_ms: at}} ->
        Mix.shell().info(
          "Location #{from} keeps the objects of #{cell} until " <>
            "#{DateTime.to_iso8601(DateTime.from_unix!(at, :millisecond))}; " <>
            "the instance deletes them then, or run: mix alambique.cleanup #{cell} #{from}"
        )

      {:error, reason} ->
        Mix.shell().info(
          "Could not delete the objects of #{cell} at location #{from} yet: " <>
            "#{inspect(reason)}; run: mix alambique.cleanup #{cell} #{from}"
        )
    end
  end
end
