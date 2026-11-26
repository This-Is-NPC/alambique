defmodule Alambique.Cell.Layout do
  @moduledoc false
  # The table of the slots of the `:atomics` array every cell publishes
  # (`Cell.Facts`): their names, their order and their encoding kind. It has no
  # dependency, so the modules that need the index of a slot at compile time
  # (`slot/1`) do not depend on `Facts`.

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
    cache: {:enum, [:full, :shrunk, :closed]},
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

  @doc "Every slot with its kind: `:raw`, `:int`, `:bool` or `{:enum, atoms}`."
  @spec table() :: keyword
  def table, do: @slots

  @doc "The index of every slot in the array, from 1."
  @spec index() :: %{atom => pos_integer}
  def index, do: @index

  @doc """
  The index of the slot `name`, known at compile time, for the slots that
  hold what the cell stores itself (`:raw`).
  """
  defmacro slot(name) when is_atom(name), do: Map.fetch!(@index, name)
end
