defmodule Alambique.Compaction.Hold do
  @moduledoc false
  # Bytes of replicated files kept in memory for the compactor (design §9,
  # "Held bytes"). A per-instance public ETS table, created with the compactor.
  #
  #   {{cell, level, min, max}, seq, meta, body, bytes}   a held file
  #   {{cell, :age, 0, 0}, {key, ts}}                     age of the snapshot
  #   {{:order, seq}, key}                                insertion order
  #   {:total, bytes}, {:seq, n}                          counters
  #
  # Rows are an optimisation: a missing row means the store is read, so any
  # row may vanish at any time (eviction, a stop, a restart). The compactor
  # uses a row only for a key in its fresh listing, and checks its epoch.

  alias Alambique.Replication.Objects

  @type key :: {String.t(), non_neg_integer, pos_integer, pos_integer}

  @doc "Creates the table of an instance."
  @spec create(Alambique.Config.t()) :: :ok
  def create(config) do
    :ets.new(config.holds, [
      :named_table,
      :public,
      :ordered_set,
      write_concurrency: true,
      read_concurrency: true
    ])

    :ets.insert(config.holds, [{:total, 0}, {:seq, 0}])
    :ok
  end

  @doc "Whether `config` holds bytes at all."
  @spec enabled?(Alambique.Config.t()) :: boolean
  def enabled?(%{compaction: %{hold_bytes: budget}}), do: budget > 0
  def enabled?(_config), do: false

  @doc "Holds the file `{cell, level, min, max}`, written with `info`, if it fits the limits."
  @spec put(Alambique.Config.t(), map, key, map, binary) :: :ok
  def put(config, ctx, key, info, body) do
    bytes = byte_size(body)

    if enabled?(config) and bytes <= config.compaction.hold_object_bytes and
         bytes <= config.compaction.hold_bytes do
      table = config.holds
      remove(table, key)
      seq = :ets.update_counter(table, :seq, 1)

      row = {key, seq, Objects.meta(ctx, info), body, bytes}

      if :ets.insert_new(table, row) do
        :ets.insert(table, {{:order, seq}, key})
        :ets.update_counter(table, :total, bytes)
        evict(table, config.compaction.hold_bytes)
      end
    end

    :ok
  rescue
    # The compactor is not running (yet): nothing to hold.
    ArgumentError -> :ok
  end

  @doc "Holds the level 0 file of an upload (`Replicator.encode/4`)."
  @spec put_upload(Alambique.Config.t(), map, map) :: :ok
  def put_upload(config, ctx, up),
    do: put(config, ctx, {up.cell, 0, up.txid, up.txid}, up.info, up.segment)

  @doc "The held `{meta, body}` of a file, or nil."
  @spec get(Alambique.Config.t(), key) :: {map, binary} | nil
  def get(config, key) do
    case :ets.lookup(config.holds, key) do
      [{^key, _seq, meta, body, _bytes}] -> {meta, body}
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "Forgets files."
  @spec drop(Alambique.Config.t(), [key]) :: :ok
  def drop(config, keys) do
    Enum.each(keys, &remove(config.holds, &1))
  rescue
    ArgumentError -> :ok
  end

  @doc "Forgets everything held for `cell`."
  @spec drop_cell(Alambique.Config.t(), String.t()) :: :ok
  def drop_cell(config, cell) do
    if enabled?(config) do
      spec = [
        {{{cell, :"$1", :"$2", :"$3"}, :_, :_, :_, :_}, [], [{{cell, :"$1", :"$2", :"$3"}}]}
      ]

      drop(config, :ets.select(config.holds, spec))
      :ets.delete(config.holds, {cell, :age, 0, 0})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "The cached time of the snapshot (or first file) `key`, or nil."
  @spec age(Alambique.Config.t(), String.t(), String.t()) :: integer | nil
  def age(config, cell, key) do
    case :ets.lookup(config.holds, {cell, :age, 0, 0}) do
      [{_, {^key, ts}}] -> ts
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "Caches the time of the snapshot (or first file) `key`."
  @spec put_age(Alambique.Config.t(), String.t(), String.t(), integer) :: :ok
  def put_age(config, cell, key, ts) do
    if enabled?(config), do: :ets.insert(config.holds, {{cell, :age, 0, 0}, {key, ts}})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Bytes held."
  @spec total(Alambique.Config.t()) :: non_neg_integer
  def total(config), do: :ets.lookup_element(config.holds, :total, 2)

  defp remove(table, key) do
    case :ets.take(table, key) do
      [{^key, seq, _meta, _body, bytes}] ->
        :ets.delete(table, {:order, seq})
        :ets.update_counter(table, :total, -bytes)

      [] ->
        :ok
    end
  end

  # FIFO: the oldest rows of any cell go first.
  defp evict(table, budget) do
    with true <- :ets.lookup_element(table, :total, 2) > budget,
         {:order, _} = first <- :ets.next(table, {:order, 0}),
         [{_, key}] <- :ets.lookup(table, first) do
      :ets.delete(table, first)
      remove(table, key)
      evict(table, budget)
    else
      _ -> :ok
    end
  end
end
