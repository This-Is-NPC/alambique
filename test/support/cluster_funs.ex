defmodule Alambique.ClusterFuns do
  @moduledoc """
  Transaction functions for cluster tests. They live in a compiled module
  because peer nodes can only run code they can load from disk.
  """

  alias Alambique.Tx

  @doc "Inserts `n` into `log` and returns the TXID of the batch it joined and the node it ran on."
  def append(n) do
    fn tx ->
      {:ok, _} = Tx.execute(tx, "INSERT INTO log (n) VALUES (?)", [n])
      {Tx.txid(tx), node()}
    end
  end

  @schema [{1, "CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER NOT NULL)"}]
  def schema, do: @schema

  @doc "The schema plus a migration that writes a row, which the change feed must publish."
  def feed_schema, do: @schema ++ [{2, "INSERT INTO log (n) VALUES (0)"}]

  @doc """
  A database of `rows` rows, then one row rewritten in separate calls: the
  chain has one segment per call, each with the header page and the page of
  the row, while most pages stay in the snapshot.
  """
  def lazy_updates(instance, cell, rows, count) do
    {:ok, :ok} =
      Alambique.transaction(instance, cell, fn tx ->
        for n <- 1..rows do
          {:ok, _} = Tx.execute(tx, "INSERT INTO log (n, v) VALUES (?, ?)", [n, blob()])
        end

        :ok
      end)

    for _ <- 1..count do
      {:ok, _} = Alambique.execute(instance, cell, "UPDATE log SET v = ? WHERE id = 1", [blob()])
    end

    :ok
  end

  defp blob, do: :crypto.strong_rand_bytes(3_000)

  @doc "A crash-point action that sends {:hydrating, self()} and waits for :go."
  def hold(name) do
    fn _, _ ->
      send(:global.whereis_name(name), {:hydrating, self()})

      receive do
        :go -> :ok
      after
        10_000 -> :ok
      end
    end
  end

  @doc "A transaction that stops the whole VM while it runs in the cell, as a crashing NIF would."
  def poison, do: fn _tx -> System.halt(137) end

  @doc "Number of in-flight markers of the instance on this node that are busy."
  def busy_markers(instance) do
    instance
    |> Alambique.Config.fetch!()
    |> Map.fetch!(:data_dir)
    |> Alambique.CellHelpers.markers()
    |> Enum.count(&(&1.flag == :busy))
  end

  @doc "Crash point action: stops the whole VM, as a power cut would."
  def halt(_point, _context), do: System.halt(137)

  @doc """
  Crash point action for `:router_after_serve`: closes this node's links to
  every node but `keep`, so the caller of a served call sees `noconnection`.
  """
  def drop_links(keep) do
    fn _point, _context ->
      for peer <- Node.list() -- keep, do: Node.disconnect(peer)
      :ok
    end
  end

  @doc "Crash point guard: the context is a call of one of `kinds`, for `cell` when given."
  def serving(kinds, cell \\ nil) do
    fn context -> context.request in kinds and cell in [nil, context.cell] end
  end

  @doc """
  Crash point action for `:router_forward_lost`: the sender notices the lost
  connection late, when the target is reachable and in the group again.
  """
  def late_notice(ms) do
    fn _point, %{target: target} ->
      Node.connect(target)
      Process.sleep(ms)
    end
  end
end
