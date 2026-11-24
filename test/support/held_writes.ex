defmodule Alambique.HeldWrites do
  @moduledoc """
  Writes whose segment puts a `AlambiqueTestkit.GatedStore` holds, so a test
  sees the moment of the acknowledgement (design §15): no caller is answered
  while its put, or the put of an earlier batch, is held.

  The later put lands first; the callers stay unanswered until the earlier put
  is released. A write that is answered before any put reaches the gate (an
  error such as an expired lease) ends the hold early and its answer is
  returned like any other.
  """
  import ExUnit.Assertions
  import Alambique.CellHelpers, only: [eventually: 2]

  alias Alambique.ClusterFuns
  alias AlambiqueTestkit.{Cluster, GatedStore}

  @held_wait_ms 3_000

  @doc """
  Runs `write.(n)` for each of `ns` (one write per batch, concurrently) with the
  level-0 puts of `cell` held, releases them in reverse order and returns
  `[{n, answer}]` in the order of `ns`. `pid` is the cell process the writes go
  to; `depth` is its pipeline depth, which bounds the writes in flight.
  """
  def run(gate, cell, pid, depth, ns, write) do
    parent = self()
    ref = make_ref()
    GatedStore.hold(gate, Regex.compile!(Regex.escape("/cells/#{cell}/0000/")))

    {started, keys} =
      ns
      |> Enum.take(max(depth, 1))
      |> Enum.reduce_while({[], []}, fn n, {started, keys} ->
        spawn_link(fn -> send(parent, {ref, n, answer_of(fn -> write.(n) end)}) end)

        case wait_held(gate, ref, length(keys) + 1) do
          {:held, held} -> {:cont, {[n | started], keys ++ (held -- keys)}}
          :answered -> {:halt, {[n | started], keys}}
        end
      end)

    if length(keys) == length(started) and keys != [] do
      [first | rest] = keys

      for key <- Enum.reverse(rest), do: release_last(gate, pid, [key])

      assert answers(ref, 0) == [], "writes were answered with the put of #{first} held"
      GatedStore.release(gate, first)
    end

    GatedStore.open(gate)
    rest = Enum.reject(ns, &(&1 in started))

    for n <- rest,
        do: spawn_link(fn -> send(parent, {ref, n, answer_of(fn -> write.(n) end)}) end)

    collect(ref, ns)
  end

  @doc """
  `run/6` for a cell that a peer of `cluster` serves: finds the live peer that
  holds the lease of `cell`, asking `via`, and runs the writes against its
  process, with as many in flight as there are writes. `:none` when no live
  peer of `dead` (a `MapSet` of names) has the cell awake. The gate must
  be the one the instances' store goes through.
  """
  def run_on(cluster, gate, via, cell, dead, ns, write) do
    args = [Cluster.instance(cluster), cell]

    with {:ok, {:ok, owner}} <- Cluster.call(cluster, via, Alambique, :owner, args, 5_000),
         name when name != nil <-
           Enum.find(Cluster.names(cluster), &(Cluster.node(cluster, &1) == owner)),
         false <- name in dead,
         {:ok, pid} when is_pid(pid) <-
           Cluster.call(cluster, name, ClusterFuns, :cell_pid, args) do
      run(gate, cell, pid, length(ns), ns, write)
    else
      _ -> :none
    end
  end

  @doc """
  Releases the put of the last of `keys` and waits until the cell `pid` shows
  its batch landed or already acknowledged. A put that fails when released (a
  store fault) is put again and held: the wait ends after a bound.
  """
  def release_last(gate, pid, keys) do
    key = List.last(keys)
    GatedStore.release(gate, key)
    txid = key |> Path.basename() |> String.split("-") |> hd() |> String.to_integer(16)
    eventually(fn -> landed_or_acked?(pid, txid) end, @held_wait_ms)
  rescue
    RuntimeError -> :ok
  end

  defp answer_of(fun) do
    fun.()
  catch
    :exit, reason -> {:exit, reason}
  end

  # Waits until `count` keys are held or some write was answered. A write
  # that is neither (a wake that waits for its own puts, a backoff) ends the
  # hold: its answer is collected once the gate is open.
  defp wait_held(gate, ref, count) do
    eventually(
      fn ->
        held = GatedStore.held(gate)

        cond do
          length(held) >= count -> {:held, held}
          peek(ref) -> :answered
          true -> false
        end
      end,
      @held_wait_ms
    )
  rescue
    RuntimeError -> :answered
  end

  defp peek(ref) do
    {:messages, messages} = Process.info(self(), :messages)
    Enum.any?(messages, &match?({^ref, _, _}, &1))
  end

  defp answers(ref, timeout) do
    receive do
      {^ref, n, answer} -> [{n, answer} | answers(ref, timeout)]
    after
      timeout -> []
    end
  end

  defp collect(ref, ns) do
    answers = Map.new(wait_all(ref, length(ns)))
    for n <- ns, do: {n, Map.fetch!(answers, n)}
  end

  defp wait_all(_ref, 0), do: []

  defp wait_all(ref, count) do
    receive do
      {^ref, n, answer} -> [{n, answer} | wait_all(ref, count - 1)]
    after
      15_000 -> flunk("a write was never answered with every put released")
    end
  end

  # The cell shows the batch landed, or has already acknowledged it.
  defp landed_or_acked?(pid, txid) do
    case Enum.find(:sys.get_state(pid).pipe.queue, &(&1.txid == txid)) do
      nil -> true
      batch -> batch.landed == true
    end
  catch
    :exit, _ -> true
  end
end
