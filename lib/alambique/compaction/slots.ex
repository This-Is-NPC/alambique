defmodule Alambique.Compaction.Slots do
  @moduledoc false
  # The release compactions of one instance that run at once (design §9,
  # "Release concurrency"). An ordered public ETS table, created with the
  # compactor, holds one row for every cell that compacts or waits to:
  #
  #   {{arrival, pid}}
  #
  # `arrival` is a unique increasing integer, so a row is a place in a queue:
  # the first `cap` rows run, the others wait, and a cell takes the place of
  # the one that ended in the order it came. A row whose process died is
  # dropped when anybody looks, so a cell that is killed holds nothing.

  @poll_ms 10

  @doc "Creates the table of an instance."
  @spec create(Alambique.Config.t()) :: :ok
  def create(config) do
    _ = :ets.new(config.release_slots, [:named_table, :public, :ordered_set])
    :ok
  end

  @doc """
  Takes a place among at most `cap` and waits for it until the monotonic time
  `deadline`. `{:ok, place}` is given back with `release/2`; `:busy` means the
  deadline passed first and the caller holds nothing. Without the table (no
  compactor) there is no cap.
  """
  @spec acquire(Alambique.Config.t(), pos_integer, integer) :: {:ok, term} | :busy
  def acquire(config, cap, deadline) do
    table = config.release_slots
    place = {System.unique_integer([:monotonic]), self()}
    :ets.insert(table, {place})
    wait(table, place, cap, deadline)
  rescue
    ArgumentError -> {:ok, nil}
  end

  @doc "Gives a place back."
  @spec release(Alambique.Config.t(), term) :: :ok
  def release(_config, nil), do: :ok

  def release(config, place) do
    :ets.delete(config.release_slots, place)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Cells that compact or wait to."
  @spec count(Alambique.Config.t()) :: non_neg_integer
  def count(config), do: :ets.info(config.release_slots, :size)

  defp wait(table, place, cap, deadline) do
    prune(table)

    cond do
      rank(table, place) <= cap ->
        {:ok, place}

      System.monotonic_time(:millisecond) >= deadline ->
        :ets.delete(table, place)
        :busy

      true ->
        Process.sleep(@poll_ms)
        wait(table, place, cap, deadline)
    end
  rescue
    # The table went with the compactor while the cell waited.
    ArgumentError -> {:ok, nil}
  end

  defp rank(table, place),
    do: :ets.select_count(table, [{{:"$1"}, [{:"=<", :"$1", {:const, place}}], [true]}])

  defp prune(table) do
    for {{_, pid} = place} <- :ets.tab2list(table),
        not Process.alive?(pid),
        do: :ets.delete(table, place)

    :ok
  end
end
