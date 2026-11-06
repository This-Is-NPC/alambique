defmodule Alambique.Feed.Publisher do
  @moduledoc false
  # The cell's side of the change feed (design §13): stamps batches, hands the
  # captured rows of acknowledged batches to the cell's `Sender` process, which
  # publishes them as feed objects, and removes the published rows in a later
  # transaction.

  require Logger

  alias Alambique.Fault
  alias Alambique.Cell.State
  alias Alambique.Feed.{Capture, Sender}
  alias Alambique.Replication.Replicator

  # Batches

  @doc """
  Stamps the rows that the transactions of the coming batch wrote with its
  TXID and a time that never goes back for this cell. Called right before the
  batch is uploaded, so every row of a batch gets the same TXID and time.
  """
  @spec stamp(State.t()) :: State.t()
  def stamp(%State{config: %{feed_capture: false}} = state), do: state

  def stamp(state) do
    ts = max(State.now_ms(state), state.feed_floor)
    txid = Replicator.next_txid(state.repl)
    :ok = Capture.stamp(state.conn, txid, ts)
    %{state | feed_floor: ts}
  end

  @doc "Wraps a transaction function so it also removes the rows already published."
  @spec wrap(State.t(), (Alambique.Tx.t() -> term)) :: (Alambique.Tx.t() -> term)
  def wrap(%State{feed_published: same, feed_cleaned: same}, fun), do: fun

  def wrap(state, fun) do
    {published, floor} = {state.feed_published, state.feed_floor}

    fn tx ->
      Fault.point(:feed_before_cleanup, %{cell: state.id, txid: published})
      :ok = Capture.cleanup(tx.conn, published, floor)
      fun.(tx)
    end
  end

  @doc "Records that the transaction built by `wrap/2` committed."
  @spec committed(State.t()) :: State.t()
  def committed(state), do: %{state | feed_cleaned: state.feed_published}

  # Wake

  @doc "Restores the time floor from the file; before the first batch of a wake is stamped."
  @spec restore_floor(State.t()) :: State.t()
  def restore_floor(%State{config: %{feed_capture: false}} = state), do: state

  def restore_floor(state),
    do: %{state | feed_floor: max(state.feed_floor, Capture.last_time(state.conn))}

  @doc "Hands over what the previous owner left unpublished (design §7, step 5)."
  @spec start(State.t()) :: State.t()
  def start(state), do: after_ack(state)

  # Publication

  @doc """
  Hands the rows of every acknowledged batch not yet handed over to the
  sender, in TXID order, and returns at once. Rows of batches still in the
  pipeline wait: they are not durable. At most `feed.max_backlog_batches`
  batches are with the sender; the rest follows as it reports progress. The
  sender retries failures by itself; they never reach the callers.
  """
  @spec after_ack(State.t()) :: State.t()
  def after_ack(%State{config: %{feed_capture: false}} = state), do: state

  def after_ack(%State{feed_backlog: backlog} = state) do
    limit = state.config.feed.max_backlog_batches

    cond do
      backlog >= limit -> %{state | feed_capped: true}
      State.lease_valid?(state) -> hand_over(state, limit - backlog)
      true -> state
    end
  end

  @doc """
  Publishes what the cell holds before it stops with its lease (design §13):
  hands over every acknowledged batch, whatever the backlog cap, asks the
  sender to put its queue without lingering and waits for it, at most
  `feed.stop_publish_ms` and while the lease is valid. On a timeout, a lost
  lease or an ended sender it gives up and kills the sender; the rows stay in
  the file for the next owner. Nothing happens without a connection or a lease.
  """
  @spec flush(State.t()) :: State.t()
  def flush(%State{conn: conn, lease: lease, config: %{feed: %{stop_publish_ms: wait}}} = state)
      when conn != nil and lease != nil and wait > 0 do
    if State.lease_valid?(state) do
      state = hand_over(state, :all)

      if state.feed_backlog > 0 do
        Fault.point(:feed_before_stop_flush, %{cell: state.id, txid: state.pipe.acked.txid})
        ref = make_ref()
        send(state.feed_pid, {:flush, ref})
        await_flush(state, ref, System.monotonic_time(:millisecond) + wait)
      else
        state
      end
    else
      state
    end
  end

  def flush(state), do: state

  defp await_flush(state, ref, deadline) do
    lease_left = state.lease.expires_at_ms - state.config.skew_margin_ms - State.now_ms(state)
    wait = min(deadline - System.monotonic_time(:millisecond), lease_left)
    pid = state.feed_pid

    receive do
      {:feed_flushed, ^ref} ->
        state

      {:feed_published, ^pid, txid, time, count} ->
        state |> published(pid, txid, time, count) |> await_flush(ref, deadline)

      {:EXIT, ^pid, _reason} ->
        give_up(state)
    after
      max(wait, 0) -> give_up(state)
    end
  end

  defp give_up(state) do
    Logger.warning("Alambique stopped #{state.id} before its feed was published")
    if state.feed_pid, do: Process.exit(state.feed_pid, :kill)
    state
  end

  @doc "Handles an object of `count` batches the sender published."
  @spec published(State.t(), pid, pos_integer, integer, pos_integer) :: State.t()
  def published(%State{feed_pid: pid} = state, pid, txid, time, count) do
    state = %{
      state
      | feed_published: txid,
        feed_floor: max(state.feed_floor, time),
        feed_backlog: state.feed_backlog - count
    }

    if state.feed_capped, do: after_ack(state), else: state
  end

  # A message of a sender that was replaced.
  def published(state, _pid, _txid, _time, _count), do: state

  @doc "Handles the end of the sender; the batches it had are handed over again."
  @spec sender_down(State.t(), pid) :: State.t()
  def sender_down(%State{feed_pid: pid} = state, pid), do: after_ack(reset(state))
  def sender_down(state, _pid), do: state

  defp reset(state),
    do: %{state | feed_pid: nil, feed_sent: state.feed_published, feed_backlog: 0}

  defp hand_over(state, room) do
    state = if alive?(state.feed_pid), do: state, else: reset(state)

    case batches(state) do
      [] ->
        %{state | feed_capped: false}

      batches ->
        {now, later} = if room == :all, do: {batches, []}, else: Enum.split(batches, room)
        Fault.point(:feed_before_publish, %{cell: state.id, txid: hd(now).txid})
        state = if state.feed_pid, do: state, else: %{state | feed_pid: Sender.start(state)}
        send(state.feed_pid, {:batches, now})

        %{
          state
          | feed_sent: List.last(now).txid,
            feed_backlog: state.feed_backlog + length(now),
            feed_capped: later != []
        }
    end
  end

  defp alive?(nil), do: false
  defp alive?(pid), do: Process.alive?(pid)

  defp batches(state) do
    state.conn
    |> Capture.rows_after(state.feed_sent, state.pipe.acked.txid)
    |> Enum.chunk_by(&hd/1)
    |> Enum.map(fn [[txid, ts | _] | _] = rows ->
      %{
        txid: txid,
        ts: ts,
        changes: Enum.map(rows, fn [_, _ | change] -> List.to_tuple(change) end)
      }
    end)
  end
end
