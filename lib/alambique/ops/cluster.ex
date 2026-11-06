defmodule Alambique.Ops.Cluster do
  @moduledoc false
  # Joining a running cluster from a Mix task (design §10).

  @doc "Switches shared by the tasks that talk to nodes."
  def switches, do: [instance: :string, connect: :string, cookie: :string]

  @doc "The instance name, from `--instance`."
  @spec instance!(keyword) :: atom
  def instance!(parsed) do
    case parsed[:instance] do
      nil -> Mix.raise("--instance NAME is required to talk to the cluster")
      name -> String.to_atom(name)
    end
  end

  @doc """
  The cluster cookie: `--cookie`, else `ALAMBIQUE_COOKIE`, else nil (the cookie
  of the user). The flag shows in the process list; the variable does not.
  """
  @spec cookie(keyword) :: String.t() | nil
  def cookie(parsed), do: parsed[:cookie] || System.get_env("ALAMBIQUE_COOKIE")

  @doc """
  Makes this VM distributed if it is not, sets the cookie and connects to the
  node named by `--connect`. Returns that node.
  """
  @spec connect!(keyword) :: node
  def connect!(parsed) do
    via = String.to_atom(parsed[:connect] || Mix.raise("--connect NODE is required"))
    unless Node.alive?(), do: start_distribution!(via)
    if cookie = cookie(parsed), do: Node.set_cookie(String.to_atom(cookie))

    unless Node.connect(via) == true,
      do: Mix.raise("could not connect to #{via}: check the node name, the cookie and epmd")

    via
  end

  @doc "Runs `module.function(args)` on the connected node."
  @spec call(keyword, module, atom, list) :: term
  def call(parsed, module, function, args) do
    Alambique.Ops.call(connect!(parsed), module, function, args, 15_000)
  end

  defp start_distribution!(via) do
    name = String.to_atom("alambique_ops_#{System.unique_integer([:positive])}@#{host(via)}")
    domain = if String.contains?(host(via), "."), do: :longnames, else: :shortnames

    case Node.start(name, domain) do
      {:ok, _} -> :ok
      {:error, reason} -> Mix.raise("could not start distribution: #{inspect(reason)}")
    end
  end

  defp host(node), do: node |> Atom.to_string() |> String.split("@") |> List.last()
end
