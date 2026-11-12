defmodule Alambique.WalBudget do
  @moduledoc false
  # Node-wide budget for the WAL files of the awake cells (design §6). A
  # periodic round adds the sizes the cells published (`Cell.wal_bytes/1`);
  # at `wal.max_bytes` or more it asks the cell with the largest WAL for the
  # same checkpoint as `Alambique.checkpoint/3` (drain, then TRUNCATE), then
  # the next one, until the sum is under the budget. A cell that answers
  # `:busy` or `:disk_full` is skipped (counted as busy) until the next round.

  alias Alambique.{Cell, Config, Periodic, Telemetry}

  @spec children(Config.t()) :: [Supervisor.child_spec()]
  def children(%{wal: %{max_bytes: nil}}), do: []

  def children(%{wal: wal} = config) do
    round = fn -> run(config) end

    [
      %{
        id: __MODULE__,
        start: {Periodic, :start_link, ["wal budget", wal.budget_interval_ms, round]}
      }
    ]
  end

  @doc """
  One round now. `:ok` when the sum is under the budget and nothing ran;
  otherwise the sum before, the cells checkpointed and the cells that were busy.
  """
  @spec run(Config.t()) :: :ok | {:checkpointed, map}
  def run(%{wal: %{max_bytes: max}} = config) do
    started = System.monotonic_time()
    cells = sizes(config)
    total = cells |> Enum.map(&elem(&1, 0)) |> Enum.sum()

    if total >= max do
      {_sum, done, busy} = Enum.reduce_while(cells, {total, 0, 0}, &relieve(config, max, &1, &2))
      measures = %{bytes: total, cells: done, busy: busy}
      Telemetry.span_stop([:wal, :budget], started, measures, %{instance: config.name})
      {:checkpointed, measures}
    else
      :ok
    end
  end

  # The cells with a WAL, largest first.
  defp sizes(config) do
    config.registry
    |> Registry.select([{{:"$1", :_, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.flat_map(fn {id, load} ->
      case Cell.wal_bytes(load) do
        0 -> []
        size -> [{size, id, load}]
      end
    end)
    |> Enum.sort(:desc)
  end

  defp relieve(_config, max, _cell, {sum, _, _} = acc) when sum < max, do: {:halt, acc}

  defp relieve(config, _max, {size, id, load}, {sum, done, busy}) do
    case Alambique.checkpoint(config.name, id) do
      :ok -> {:cont, {sum - size + Cell.wal_bytes(load), done + 1, busy}}
      :asleep -> {:cont, {sum - size, done, busy}}
      {:error, _} -> {:cont, {sum, done, busy + 1}}
    end
  end
end
