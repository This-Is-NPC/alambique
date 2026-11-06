defmodule Mix.Tasks.Alambique.Freeze do
  @shortdoc "Freezes a cell: it refuses to wake and an awake one hibernates"

  @moduledoc """
  Freezes a cell by writing its freeze marker to the store.

      mix alambique.freeze CELL [options]

  A frozen cell refuses to wake (calls get `{:error, :frozen}`). A cell that
  is awake notices the marker at its next lease renewal, within a third of
  `:lease_ttl_ms`, and hibernates. With `--connect` the task also asks the
  running cluster to hibernate the cell now (see `Mix.Tasks.Alambique.Move`
  for the connection options, and `--instance`).

  Undo it with `mix alambique.unfreeze`. Store options are described in
  `Mix.Tasks.Alambique.Restore`.
  """

  use Mix.Task

  alias Alambique.Ops.{Cluster, Options}

  @impl Mix.Task
  def run(args) do
    {parsed, rest} =
      OptionParser.parse!(args, strict: Cluster.switches() ++ Options.switches())

    cell = cell!(rest)
    instance = if parsed[:connect], do: Cluster.instance!(parsed)
    Options.boot()
    ctx = Options.ctx(parsed, cell)

    case Alambique.Freeze.freeze(ctx, cell) do
      :ok -> Mix.shell().info("Froze #{cell}")
      {:error, reason} -> Mix.raise("could not freeze #{cell}: #{inspect(reason)}")
    end

    if parsed[:connect] do
      case Cluster.call(parsed, Alambique, :hibernate, [instance, cell]) do
        :ok ->
          Mix.shell().info("#{cell} is asleep")

        other ->
          Mix.shell().info("#{cell} will hibernate at its next lease renewal: #{inspect(other)}")
      end
    else
      Mix.shell().info("An awake #{cell} hibernates at its next lease renewal")
    end
  end

  defp cell!([cell]), do: cell
  defp cell!(_), do: Mix.raise("usage: mix alambique.freeze CELL [options]")
end
