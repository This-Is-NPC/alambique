defmodule Alambique.FeedProbe do
  @moduledoc """
  Looks at the feed of a cell from outside and disturbs it, for the property
  tests of the feed: the TXID the cell knows to be published, a kill of the
  cell and a publication held while a write cleans up (design §13).
  """
  import Alambique.CellHelpers, only: [eventually: 1]

  alias Alambique.{Cell, Config}
  alias AlambiqueTestkit.GatedStore

  @doc "Kills the process of `cell` on `inst`, if it runs."
  def kill(inst, cell) do
    case Cell.whereis(Config.fetch!(inst), cell) do
      nil -> :ok
      pid -> Process.exit(pid, :kill)
    end
  end

  @doc "The TXID `cell` reports as published; `nil` when it does not run."
  def published(inst, cell) do
    case Cell.whereis(Config.fetch!(inst), cell) do
      nil -> nil
      pid -> :sys.get_state(pid).feed.published
    end
  catch
    :exit, _ -> nil
  end

  @doc """
  A batch is published and the next one is held on its put, which the cell has
  handed over but not published, when a write cleans up the cell: the rows of
  the held batch stay, whatever happens to the cell next.

  Holds the feed puts of `gate`, calls `write.(state)` twice, lets the first
  put land, waits until the cell reports it published (or stopped), calls
  `write.(state)` once more and returns its result. The puts stay held: the
  caller opens the gate.
  """
  def race(gate, inst, cell, state, write) do
    GatedStore.hold(gate, ~r{/feed/})
    state = state |> write.() |> write.()

    mine = fn -> Enum.filter(GatedStore.held(gate), &String.ends_with?(&1, cell)) end
    [first | _] = eventually(fn -> mine.() != [] and mine.() end)

    before = published(inst, cell) || 0
    GatedStore.release(gate, first)
    # A crash armed in the cleanup may have stopped the cell meanwhile.
    eventually(fn -> (now = published(inst, cell)) == nil or now > before end)
    write.(state)
  end
end
