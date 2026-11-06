defmodule Mix.Tasks.Alambique.Unfreeze do
  @shortdoc "Lets a frozen cell wake again"

  @moduledoc """
  Removes the freeze marker of a cell and forgets its recorded crashes.

      mix alambique.unfreeze CELL [options]

  Unfreezing a cell that is not frozen succeeds. The store options are
  described in `Mix.Tasks.Alambique.Restore`.
  """

  use Mix.Task

  alias Alambique.Ops.Options

  @impl Mix.Task
  def run(args) do
    {parsed, rest} = OptionParser.parse!(args, strict: Options.switches())

    cell =
      case rest do
        [cell] -> cell
        _ -> Mix.raise("usage: mix alambique.unfreeze CELL [options]")
      end

    Options.boot()

    case Alambique.Freeze.unfreeze(Options.ctx(parsed, cell), cell) do
      :ok -> Mix.shell().info("Unfroze #{cell}")
      {:error, reason} -> Mix.raise("could not unfreeze #{cell}: #{inspect(reason)}")
    end
  end
end
