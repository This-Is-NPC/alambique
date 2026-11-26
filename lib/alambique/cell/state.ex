defmodule Alambique.Cell.State do
  @moduledoc false
  # State of one cell process. `mode` is `:waking` until the wake finishes,
  # `:serving` while it answers calls, and `:stopped` once it has given up the
  # lease or lost it, so that `terminate/2` has nothing left to do.

  alias Alambique.Cell.Options
  alias Alambique.Replication.Retry

  @enforce_keys [:config, :id, :path]
  defstruct [
    :config,
    :id,
    :path,
    :location,
    :lease_ctx,
    :repl_ctx,
    :conn,
    :lease,
    :repl,
    :source,
    # The process that fetches the pages of a lazily restored file, until the
    # file is complete (design §16). The marker stays busy for the whole
    # hydration, because its fetch tasks may run native code on any page.
    :hydrator,
    :hydrator_marker,
    # The process that writes the page-hash sidecar of the last landed batch.
    :sidecar,
    :last_active,
    # While the connection is closed: what `Sqlite.close_keep/1` kept, which
    # holds the file for this cell.
    :kept,
    # Marker of work in flight (design §10), `nil` when quarantine is off.
    :inflight,
    :pipe,
    # Counter of callers accepted and not answered yet, read by `Cell.load/2`.
    :load,
    # The options this cell set for itself (`Cell.Options`): only the keys it
    # changed. Read through `cell_opt/2`.
    cell_opts: %{},
    # How the cell came to wake: `takeover`, set by `Alambique.take_over/4` as
    # `%{reason, epoch}` and `previous` (the lease that was replaced) once the
    # wake took the lease; `lease_seen`, `%{location, answer, trips}`, the
    # lease object the router read just before the wake (`answer` is `:none`
    # or `{:ok, lease}`) and the counters of that read, which saves the wake
    # reading the lease again; `holder`, the process that woke the cell, whose
    # wake slot the cell takes over; `trips`, the counters of the store
    # requests of the wake (`Store.Metered`).
    origin: %{takeover: nil, lease_seen: nil, holder: nil, trips: nil},
    # The idle timers: `timer` is the ref of the `:idle` timer, a message with
    # another ref is stale. `cache` is the idle cache shrink: `nil` (off), the
    # ref of the `:idle_cache` timer, or `:shrunk` while the page cache holds
    # the smaller limit. `close` is the idle close: `nil` (off, or the
    # connection is closed), or the ref of the `:idle_close` timer.
    idle: %{timer: nil, cache: nil, close: nil},
    # The refresh of the options in progress (`Alambique.refresh_options/3`):
    # `%{gen, pid, mon, started, waiting, queued}`, or `nil`. One resolve runs
    # at a time, in a process of its own; the callers that asked meanwhile
    # (`queued`) are served by the next one.
    refresh: nil,
    # The memory admission counts for the cell while its connection is closed.
    reopen_floor: 0,
    mode: :waking,
    # Writes that wait for the end of the hydration (design §16), oldest last:
    # `{call, from}`.
    parked: [],
    # The migrations of this wake: the instance's, or those chosen for the cell.
    migrations: [],
    # Versions whose schema index entry this process wrote (design §17).
    indexed: [],
    # Whether the wake wrote the entry of the current version beside the fence.
    preindexed: false,
    # Commits not yet encoded, newest first: callers and reads that wait.
    pending: [],
    flush_queued: false,
    # Change feed (design §13): `floor` is the latest time a batch took,
    # `published` the newest TXID published and `cleaned` the newest one whose
    # rows the database no longer holds. `pid` is the sender process, `sent`
    # the newest TXID handed to it, `backlog` how many batches it holds, and
    # `capped` whether more wait in the file for room. `part` is the position
    # `{txid, seq}` of a batch whose rows are handed over in pieces
    # (`feed.hand_over_rows`), `tick` the ref of the message that continues it
    # (nil for none). `reader` is, with `hand_over: :sender`, the read-only
    # connection the sender reads with; only the cell closes it, before it
    # closes its own connection. `reading` is, with `reader_close:
    # :after_read`, whether a range is with the sender, which has not
    # reported it read yet. `refused` is, with `hand_over: :native`, whether a
    # table name that is not UTF-8 was refused and logged; cleared by the next
    # hand-over that sent rows. `cleanup` is the `:feed_cleanup` timer (`nil`
    # when none is armed) and `cleanup_failures` the failed cleanups since the
    # last one that committed. `unsent` is whether a commit that no caller
    # waits for (the feed cleanup) has a batch that is not encoded yet.
    feed: %{
      floor: 0,
      published: 0,
      cleaned: 0,
      pid: nil,
      sent: 0,
      backlog: 0,
      capped: false,
      part: nil,
      tick: nil,
      reader: nil,
      reading: false,
      refused: false,
      cleanup: nil,
      cleanup_failures: 0,
      unsent: false
    }
  ]

  @type t :: %__MODULE__{}

  @spec now_ms(t) :: integer
  def now_ms(%__MODULE__{config: %{clock: {clock, clock_config}}}) do
    case clock.now_ms(clock_config) do
      ms when is_integer(ms) -> ms
    end
  end

  @doc """
  An option a cell may set for itself: its own value (`nil` included) or the
  node's.
  """
  @spec cell_opt(
          t,
          :cache_kib
          | :idle_cache_after_ms
          | :idle_cache_kib
          | :idle_close_after_ms
          | :idle_gc
          | :idle_timeout_ms
          | :locking_mode
          | :lazy_restore_mode
          | :cleanup_after_ms
          | :track_bytes
          | :lookaside_bytes
          | :lookaside_count
        ) ::
          term
  def cell_opt(%__MODULE__{cell_opts: opts, config: config}, key) do
    case opts do
      %{^key => value} -> value
      _ -> Options.node_value(config, key)
    end
  end

  @doc "Whether the lease may still be used, by this node's clock and the skew margin."
  @spec lease_valid?(t) :: boolean
  def lease_valid?(%__MODULE__{lease: lease} = state),
    do: Alambique.Lease.valid?(state.lease_ctx, lease)

  @doc "Milliseconds until the lease may no longer be used, by this node's clock and the skew margin."
  @spec lease_left_ms(t) :: integer
  def lease_left_ms(%__MODULE__{config: config, lease: lease} = state),
    do: lease.expires_at_ms - config.skew_margin_ms - now_ms(state)

  @doc "Monotonic deadline for one store operation: the call budget, capped by the lease."
  @spec deadline(t) :: integer
  def deadline(%__MODULE__{config: config} = state) do
    Retry.deadline_in(min(config.op_timeout_ms, max(lease_left_ms(state), 0)))
  end
end
