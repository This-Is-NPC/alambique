defmodule Alambique.Cell.Facts do
  @moduledoc false
  # The slots of the `:atomics` array every cell publishes as its registry
  # value, and how they are written and read. One table names every slot, so
  # the order of the array, its size and the encoding live in one place.
  #
  # Only the cell process writes (`publish/2`), after the call or message that
  # changed its state; any process reads without calling the cell, so a read
  # never wakes it and never touches its last use.
  #
  # Encoding: every slot is a signed 64-bit integer. A boolean is 0 or 1, an
  # enum is the position of the atom in its list, and `nil` is the smallest
  # integer, because a monotonic time may be negative or 0 and a TXID of 0 is
  # real. The `:raw` slots hold what the cell stores, with their own meaning:
  #
  #   * `callers`: callers the cell accepted and has not answered;
  #   * `admitted`: calls the router sent that the cell has not finished with;
  #   * `last_used`: monotonic ms of the last call, 0 until the wake ends (the
  #     wake writes it last, so a reader that finds it set finds the rest);
  #   * `wal_bytes`: size of the WAL after the last landed batch or checkpoint;
  #   * `stopped`: 1 once the cell stopped serving.

  alias Alambique.{Admission, Sqlite}
  alias Alambique.Cell.State

  @nil_value -9_223_372_036_854_775_808

  @slots [
    callers: :raw,
    admitted: :raw,
    last_used: :raw,
    wal_bytes: :raw,
    stopped: :raw,
    txid: :int,
    acked_txid: :int,
    batches_in_flight: :int,
    parked: :int,
    feed_published: :int,
    feed_sent: :int,
    feed_cleaned: :int,
    feed_backlog: :int,
    feed_capped: :bool,
    feed_reader_open: :bool,
    lease_epoch: :int,
    lease_expires_at: :int,
    cache: {:enum, [:full, :shrunk]},
    hydrating: :bool,
    cache_used: :int,
    schema_used: :int,
    stmt_used: :int,
    lookaside_used: :int,
    process_bytes: :int,
    wake_source: {:enum, [:local, :empty, :restore]},
    wake_started_at: :int,
    wake_started_at_mono: :int,
    awake_since: :int,
    awake_since_mono: :int
  ]

  @index @slots |> Keyword.keys() |> Enum.with_index(1) |> Map.new()

  # What the wake and `publish/2` write, in the order `read/2` returns it. A
  # reader reads the acknowledged TXID before the newest one, because the
  # writer stores the newest first: whatever acked it sees, txid is not below.
  @published [
    :acked_txid,
    :txid,
    :batches_in_flight,
    :parked,
    :feed_published,
    :feed_sent,
    :feed_cleaned,
    :feed_backlog,
    :feed_capped,
    :feed_reader_open,
    :lease_epoch,
    :lease_expires_at,
    :cache,
    :hydrating,
    :cache_used,
    :schema_used,
    :stmt_used,
    :lookaside_used,
    :process_bytes,
    :awake_since,
    :awake_since_mono
  ]

  # `slot/1` cannot be used in the module that defines it.
  @raw_callers Map.fetch!(@index, :callers)
  @raw_admitted Map.fetch!(@index, :admitted)
  @raw_last_used Map.fetch!(@index, :last_used)
  @raw_wal_bytes Map.fetch!(@index, :wal_bytes)
  @raw_stopped Map.fetch!(@index, :stopped)

  @doc """
  The index of the slot `name`, known at compile time, for the slots that
  hold what the cell stores itself (`:raw`).
  """
  defmacro slot(name) when is_atom(name), do: Map.fetch!(@index, name)

  @doc "A new array: the raw slots at 0, every other at nil."
  @spec new() :: :atomics.atomics_ref()
  def new do
    load = :atomics.new(map_size(@index), [])

    for {name, kind} <- @slots,
        kind != :raw,
        do: :atomics.put(load, Map.fetch!(@index, name), @nil_value)

    load
  end

  for {name, kind} <- @slots, kind != :raw do
    def put(load, unquote(name), value),
      do:
        :atomics.put(
          load,
          unquote(Map.fetch!(@index, name)),
          encode(unquote(Macro.escape(kind)), value)
        )
  end

  for {name, kind} <- @slots, kind != :raw do
    def get(load, unquote(name)),
      do:
        decode(unquote(Macro.escape(kind)), :atomics.get(load, unquote(Map.fetch!(@index, name))))
  end

  defp encode(_kind, nil), do: @nil_value
  defp encode(:int, value) when is_integer(value), do: value
  defp encode(:bool, true), do: 1
  defp encode(:bool, false), do: 0
  defp encode({:enum, atoms}, value), do: Enum.find_index(atoms, &(&1 == value))

  defp decode(_kind, @nil_value), do: nil
  defp decode(:int, value), do: value
  defp decode(:bool, value), do: value == 1
  defp decode({:enum, atoms}, value), do: Enum.at(atoms, value)

  @doc """
  Writes every slot taken from the state of a serving cell, in a fixed order:
  the newest TXID first, then the acknowledged one, the queue, the feed, the
  lease, the cache, the hydration and, with `measure?`, the memory. Reads go
  the other way (`read/2`), which keeps `acked_txid <= txid` for a reader.
  """
  @spec publish(State.t(), boolean) :: :ok
  def publish(%State{load: load} = state, measure? \\ false) do
    put(load, :txid, state.repl.txid)
    put(load, :acked_txid, state.pipe.acked.txid)
    put(load, :batches_in_flight, length(state.pipe.queue))
    put(load, :parked, length(state.parked))
    put(load, :feed_published, state.feed_published)
    put(load, :feed_sent, state.feed_sent)
    put(load, :feed_cleaned, state.feed_cleaned)
    put(load, :feed_backlog, state.feed_backlog)
    put(load, :feed_capped, state.feed_capped)
    put(load, :feed_reader_open, state.feed_reader != nil)
    put(load, :lease_epoch, state.lease.epoch)
    put(load, :lease_expires_at, state.lease.expires_at_ms)
    put(load, :cache, if(state.idle_cache == :shrunk, do: :shrunk, else: :full))
    put(load, :hydrating, state.hydrator != nil)
    if measure? and track_bytes?(state.config), do: measure(state)
    :ok
  end

  @doc "Whether the memory of the cells is measured: `awake.track_bytes`, or a budget."
  @spec track_bytes?(Alambique.Config.t()) :: boolean
  def track_bytes?(%{awake: awake}), do: awake.track_bytes or awake.max_bytes != nil

  # The bytes of the connection and, while the feed sender reads, of its
  # reader; a failed read leaves the last values. The sum the budget keeps is
  # the one admission has always used (no lookaside).
  defp measure(state) do
    memories = Enum.map([state.conn | List.wrap(state.feed_reader)], &Sqlite.memory/1)

    if Enum.all?(memories, &match?({:ok, _}, &1)) do
      sum = fn key -> Enum.sum(for {:ok, m} <- memories, do: Map.fetch!(m, key)) end
      cache = sum.(:cache_used)
      schema = sum.(:schema_used)
      stmt = sum.(:stmt_used)

      put(state.load, :cache_used, cache)
      put(state.load, :schema_used, schema)
      put(state.load, :stmt_used, stmt)
      put(state.load, :lookaside_used, sum.(:lookaside_used))
      Admission.record(state.config, self(), cache + schema + stmt)
    end

    {:memory, bytes} = Process.info(self(), :memory)
    put(state.load, :process_bytes, bytes)
  end

  @doc """
  The facts of the cell `pid` published in `load`, without a call into it.
  `pid`, `serving`, `pending`, `last_used`, `wal_bytes`, `wake_source` and the
  wake start are known from the start; every other fact is nil while the wake
  has not ended.
  """
  @spec read(:atomics.atomics_ref(), pid) :: map
  def read(load, pid) do
    last = :atomics.get(load, @raw_last_used)

    published =
      if last == 0,
        do: Map.new(@published, &{&1, nil}),
        else: Map.new(@published, &{&1, get(load, &1)})

    Map.merge(published, %{
      pid: pid,
      serving: :atomics.get(load, @raw_stopped) == 0,
      pending: :atomics.get(load, @raw_callers) + :atomics.get(load, @raw_admitted),
      last_used: if(last == 0, do: nil, else: last),
      wal_bytes: :atomics.get(load, @raw_wal_bytes),
      wake_source: get(load, :wake_source),
      wake_started_at: get(load, :wake_started_at),
      wake_started_at_mono: get(load, :wake_started_at_mono)
    })
  end
end
