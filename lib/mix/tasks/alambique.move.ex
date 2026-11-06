defmodule Mix.Tasks.Alambique.Move do
  @shortdoc "Moves a cell to another node of a running cluster"

  @moduledoc """
  Asks a running cluster to move a cell: its owner hibernates it and the
  target node wakes it.

      mix alambique.move CELL TARGET_NODE --instance NAME [options]

  Moving needs the nodes, not only the store, so the task joins the Erlang
  cluster. It starts distribution under a random name (unless the VM is
  already distributed), sets the cookie and connects to a node that runs the
  instance.

    * `--instance NAME` - the instance name given to `{Alambique, name: ...}`
      (required)
    * `--connect NODE` - the node to call through; default `TARGET_NODE`
    * `--cookie COOKIE` - the cluster cookie; default the variable
      `ALAMBIQUE_COOKIE`, then the cookie of the user (`~/.erlang.cookie`).
      A flag shows in the process list: prefer the variable
    * `--timeout MS` - call timeout, default 15000

  `TARGET_NODE` must run the instance. The task does not use the store, so it
  needs no store options. Nodes with long names (`app@host.example.com`) need
  the task to run with a host name that resolves for them; the task starts
  with `longnames` when the target name contains a dot after the `@`.
  """

  use Mix.Task

  alias Alambique.Ops
  alias Alambique.Ops.Cluster

  @impl Mix.Task
  def run(args) do
    {parsed, rest} = OptionParser.parse!(args, strict: Cluster.switches() ++ [timeout: :integer])

    {cell, target} =
      case rest do
        [cell, target] -> {cell, String.to_atom(target)}
        _ -> Mix.raise("usage: mix alambique.move CELL TARGET_NODE --instance NAME [options]")
      end

    instance = Cluster.instance!(parsed)
    Mix.Task.run("app.config")
    via = parsed |> Keyword.put_new(:connect, Atom.to_string(target)) |> Cluster.connect!()

    case Ops.move(via, instance, cell, target, parsed[:timeout] || 15_000) do
      {:ok, node} -> Mix.shell().info("Moved #{cell} to #{node}")
      {:error, reason} -> Mix.raise("could not move #{cell}: #{inspect(reason)}")
    end
  end
end
