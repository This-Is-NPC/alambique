defmodule Alambique.Cell.State do
  @moduledoc false
  # State of one cell process. `mode` is `:waking` until the wake finishes,
  # `:serving` while it answers calls, and `:stopped` once it has given up the
  # lease or lost it, so that `terminate/2` has nothing left to do.

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
    :last_active,
    # Marker of work in flight (design §10), `nil` when quarantine is off.
    :inflight,
    # Set by `Alambique.take_over/4`: `%{reason, epoch}`, and `previous` (the
    # lease that was replaced) once the wake took the lease.
    :takeover,
    # `%{location, answer, trips}`: the lease object the router read just before
    # the wake (`answer` is `:none` or `{:ok, lease}`) and the counters of that
    # read. It saves the wake reading the lease again.
    :lease_seen,
    # The process that woke the cell, whose wake slot the cell takes over.
    :holder,
    # Counters of the store requests of the wake (`Store.Metered`).
    :trips,
    :pipe,
    # Counter of callers accepted and not answered yet, read by `Cell.load/2`.
    :load,
    mode: :waking,
    # The migrations of this wake: the instance's, or those chosen for the cell.
    migrations: [],
    # Versions whose schema index entry this process wrote (design §17).
    indexed: [],
    # Whether the wake wrote the entry of the current version beside the fence.
    preindexed: false,
    # Commits not yet encoded, newest first: callers and reads that wait.
    pending: [],
    flush_queued: false,
    # Change feed (design §13): the latest time a batch took, the newest TXID
    # published and the newest one whose rows the database no longer holds.
    feed_floor: 0,
    feed_published: 0,
    feed_cleaned: 0,
    # The sender process, the newest TXID handed to it, how many batches it
    # holds, and whether more wait in the file for room.
    feed_pid: nil,
    feed_sent: 0,
    feed_backlog: 0,
    feed_capped: false
  ]

  @type t :: %__MODULE__{}

  @spec now_ms(t) :: integer
  def now_ms(%__MODULE__{config: %{clock: {clock, clock_config}}}), do: clock.now_ms(clock_config)

  @doc "Whether the lease may still be used, by this node's clock and the skew margin."
  @spec lease_valid?(t) :: boolean
  def lease_valid?(%__MODULE__{lease: lease} = state),
    do: Alambique.Lease.valid?(state.lease_ctx, lease)

  @doc "Monotonic deadline for one store operation: the call budget, capped by the lease."
  @spec deadline(t) :: integer
  def deadline(%__MODULE__{config: config, lease: lease} = state) do
    remaining = lease.expires_at_ms - config.skew_margin_ms - now_ms(state)
    Retry.deadline_in(min(config.op_timeout_ms, max(remaining, 0)))
  end
end
