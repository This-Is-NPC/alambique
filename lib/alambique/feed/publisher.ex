defmodule Alambique.Feed.Publisher do
  @moduledoc false
  # The cell's side of the change feed (design §13): stamps batches, hands the
  # captured rows of acknowledged batches to the cell's `Sender` process, which
  # publishes them as feed objects, and removes the published rows in a later
  # transaction.

  require Logger

  # A reader that could not be opened is tried again after this long.
  @reader_retry_ms 1_000

  alias Alambique.Fault
  alias Alambique.Cell.{Db, State}
  alias Alambique.Feed.{Capture, Pieces, Sender}
  alias Alambique.Replication.Replicator

  # Batches

  @doc """
  Stamps the rows that the transactions of the coming batch wrote with its
  TXID and a time that never goes back for this cell. Called right before the
  batch is uploaded, so every row of a batch gets the same TXID and time. The
  failure is a value, for the caller to stop on.
  """
  @spec try_stamp(State.t()) :: {:ok, State.t()} | {:error, term}
  def try_stamp(%State{config: %{feed_capture: false}} = state), do: {:ok, state}

  def try_stamp(state) do
    ts = max(State.now_ms(state), state.feed_floor)
    txid = Replicator.next_txid(state.repl)

    with :ok <- Capture.stamp(state.conn, txid, ts), do: {:ok, %{state | feed_floor: ts}}
  end

  # Cleanup (design §13): the rows of published batches are removed either in
  # the next user write (`:in_write`, `:idle_or_write`) or in a transaction of
  # the cell's own (`:idle`, `:after_publish`, `:idle_or_write`), which the cell
  # sends as a batch of its own.

  @doc """
  Wraps a transaction function so it also removes the rows already published,
  when the instance cleans in user writes.
  """
  @spec wrap(State.t(), (Alambique.Tx.t() -> term)) :: (Alambique.Tx.t() -> term)
  def wrap(%State{config: %{feed: %{cleanup: mode}}} = state, fun) do
    if mode in [:in_write, :idle_or_write] and state.feed_published != state.feed_cleaned do
      {published, floor} = {state.feed_published, state.feed_floor}

      fn tx ->
        Fault.point(:feed_before_cleanup, %{cell: state.id, txid: published})
        :ok = Capture.cleanup(tx.conn, published, floor)
        fun.(tx)
      end
    else
      fun
    end
  end

  @doc "Records that the transaction built by `wrap/2` committed."
  @spec committed(State.t()) :: State.t()
  def committed(%State{config: %{feed: %{cleanup: mode}}} = state)
      when mode in [:in_write, :idle_or_write],
      do: %{state | feed_cleaned: state.feed_published}

  def committed(state), do: state

  @doc "Whether published rows wait for a cleanup that runs outside user writes."
  @spec cleanup_due?(State.t()) :: boolean
  def cleanup_due?(%State{config: %{feed: %{cleanup: :in_write}}}), do: false
  def cleanup_due?(state), do: state.feed_published > state.feed_cleaned

  @doc """
  Removes the published rows in a transaction of its own, up to
  `feed.cleanup_rows` of them but whole batches only: a batch that kept some
  of its rows would be published again under other record ids after a crash.
  `feed_cleaned` advances only once the transaction committed. The cell must
  send a batch afterwards: the deletion is a change of the file.
  """
  @spec cleanup(State.t()) :: {:ok, State.t()} | {:error, State.t()} | {:stop, term, State.t()}
  def cleanup(state) do
    {from, published, floor} = {state.feed_cleaned, state.feed_published, state.feed_floor}
    rows = state.config.feed.cleanup_rows

    fun = fn tx ->
      Fault.point(:feed_before_cleanup, %{cell: state.id, txid: published})
      cut = Capture.cutoff(tx.conn, from, published, rows)
      :ok = Capture.cleanup(tx.conn, cut, floor)
      cut
    end

    case Db.transaction(state.conn, Replicator.next_txid(state.repl), fun,
           select: state.config.feed
         ) do
      {:ok, cut} -> {:ok, %{state | feed_cleaned: cut}}
      {:error, reason} -> if Db.fatal?(reason), do: {:stop, reason, state}, else: {:error, state}
    end
  end

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
  batches are with the sender; the rest follows as it reports progress. With
  `feed.hand_over_rows` the rows go in pieces of that many, the next one in a
  message of the cell to itself, so the calls in the mailbox are served
  between pieces. The sender retries failures by itself; they never reach the
  callers.
  """
  @spec after_ack(State.t()) :: State.t()
  def after_ack(%State{config: %{feed_capture: false}} = state), do: state

  # The rows come from pages that the checksum of a hydration has not covered
  # yet (design §16): `Cell` hands them over when it ends.
  def after_ack(
        %State{hydrator: hydrator, config: %{lazy_restore: %{verify_writes: true}}} = state
      )
      when hydrator != nil,
      do: state

  # The next piece is on its way.
  def after_ack(%State{feed_tick: tick} = state) when tick != nil, do: state

  # A batch that is partly with the sender takes no room: it must be finished.
  def after_ack(%State{feed_backlog: backlog, feed_part: part} = state) do
    limit = state.config.feed.max_backlog_batches

    cond do
      backlog >= limit and part == nil -> %{state | feed_capped: true}
      State.lease_valid?(state) -> state |> hand_over(max(limit - backlog, 0)) |> next_piece()
      true -> state
    end
  end

  @doc "Hands over the next piece, when `ref` is the message the cell sent itself for it."
  @spec continue(State.t(), reference) :: State.t()
  def continue(%State{feed_tick: ref} = state, ref), do: after_ack(%{state | feed_tick: nil})
  def continue(state, _stale), do: state

  defp next_piece({state, true}) do
    ref = make_ref()
    send(self(), {:feed_part, ref})
    %{state | feed_tick: ref}
  end

  defp next_piece({state, false}), do: state

  @doc """
  Publishes what the cell holds before it stops with its lease (design §13):
  hands over every acknowledged batch, whatever the backlog cap and without
  serving a call between the pieces, asks the sender to put its queue without
  lingering and waits for it, at most `feed.stop_publish_ms` and while the
  lease is valid. On a timeout, a lost lease or an ended sender it gives up
  and kills the sender; the rows stay in the file for the next owner. Nothing
  happens without a connection or a lease.
  """
  @spec flush(State.t()) :: State.t()
  def flush(%State{conn: conn, lease: lease, config: %{feed: %{stop_publish_ms: wait}}} = state)
      when conn != nil and lease != nil and wait > 0 do
    if State.lease_valid?(state) do
      deadline = System.monotonic_time(:millisecond) + wait

      case hand_over_all(%{state | feed_tick: nil}, deadline) do
        {:ok, %{feed_backlog: backlog} = state} when backlog > 0 ->
          Fault.point(:feed_before_stop_flush, %{cell: state.id, txid: state.pipe.acked.txid})
          ref = make_ref()
          send(state.feed_pid, {:flush, ref})
          await_flush(state, ref, deadline)

        {:ok, state} ->
          state

        {:late, state} ->
          give_up(state)
      end
    else
      state
    end
  end

  def flush(state), do: state

  defp hand_over_all(state, deadline) do
    case hand_over(state, :all) do
      {state, false} ->
        {:ok, state}

      {state, true} ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: {:late, state},
          else: hand_over_all(state, deadline)
    end
  end

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
        feed_backlog: backlog(state, txid, count)
    }

    if state.feed_capped, do: after_ack(state), else: state
  end

  # A message of a sender that was replaced.
  def published(state, _pid, _txid, _time, _count), do: state

  # With `hand_over: :sender` the cell does not know the batches: the backlog is
  # the TXIDs handed over and not published, empty ones included.
  defp backlog(%State{config: %{feed: %{hand_over: :sender}}} = state, txid, _count),
    do: max(state.feed_sent - txid, 0)

  defp backlog(state, _txid, count), do: state.feed_backlog - count

  @doc """
  Handles the report that the sender read up to `upto`: with nothing more
  handed over than that, the reader is closed.
  """
  @spec read_done(State.t(), pid, non_neg_integer) :: State.t()
  def read_done(%State{feed_pid: pid, feed_sent: sent} = state, pid, upto) when upto >= sent,
    do: close_reader(state)

  def read_done(state, _pid, _upto), do: state

  @doc """
  Closes the sender's reader. Only the cell closes it, and always before its
  own connection: a reader that outlived it would keep the file open after the
  checkpoint and the removal of `-wal` and `-shm`, and a statement of it would
  keep the checkpoint from finishing. Closing interrupts a running statement.
  """
  @spec close_reader(State.t()) :: State.t()
  def close_reader(%State{feed_reader: nil} = state), do: state

  def close_reader(%State{feed_reader: reader} = state) do
    Db.close(reader)
    %{state | feed_reader: nil}
  end

  @doc "Handles the end of the sender; the batches it had are handed over again."
  @spec sender_down(State.t(), pid) :: State.t()
  def sender_down(%State{feed_pid: pid} = state, pid), do: after_ack(reset(state))
  def sender_down(state, _pid), do: state

  defp reset(state),
    do: %{
      close_reader(state)
      | feed_pid: nil,
        feed_sent: state.feed_published,
        feed_backlog: 0,
        feed_part: nil,
        feed_tick: nil
    }

  # `hand_over: :sender`: the cell hands over the TXIDs, at most `room` of
  # them, and the sender reads the rows with the reader of the cell.
  defp hand_over(%State{config: %{feed: %{hand_over: :sender}}} = state, room) do
    state = if alive?(state.feed_pid), do: state, else: reset(state)
    acked = state.pipe.acked.txid
    upto = if room == :all, do: acked, else: min(acked, state.feed_sent + room)

    with true <- upto > state.feed_sent,
         {:ok, state} <- open_reader(state) do
      Fault.point(:feed_before_publish, %{cell: state.id, txid: upto, sender: state.feed_pid})
      state = if state.feed_pid, do: state, else: %{state | feed_pid: Sender.start(state)}
      send(state.feed_pid, {:range, state.feed_sent + 1, upto, state.feed_reader})

      {%{
         state
         | feed_sent: upto,
           feed_backlog: upto - state.feed_published,
           feed_capped: upto < acked
       }, false}
    else
      false ->
        {%{state | feed_capped: false}, false}

      {:error, reason} ->
        Logger.warning(
          "Alambique could not open the feed reader of #{state.id}: #{inspect(reason)}"
        )

        ref = make_ref()
        Process.send_after(self(), {:feed_part, ref}, @reader_retry_ms)
        {%{state | feed_tick: ref}, false}
    end
  end

  # One piece: at most `feed.hand_over_rows` rows from the position of the
  # hand-over: whole batches as far as they fit, and the start or the middle
  # of the one the bound cuts. Returns the state and whether more rows follow
  # at once. At most `room` batches are started; the others wait for the
  # sender (`feed_capped`).
  defp hand_over(state, room) do
    state = if alive?(state.feed_pid), do: state, else: reset(state)
    position = state.feed_part || {state.feed_sent + 1, -1}

    {:ok, pieces, more?} =
      Pieces.read(
        state.conn,
        position,
        state.pipe.acked.txid,
        state.config.feed.hand_over_rows,
        state.feed_part
      )

    {pieces, cut?} = take_room(pieces, room)

    case pieces do
      [] ->
        {%{state | feed_part: nil, feed_capped: false}, false}

      _ ->
        Fault.point(:feed_before_publish, %{
          cell: state.id,
          txid: hd(pieces).txid,
          sender: state.feed_pid
        })

        state = if state.feed_pid, do: state, else: %{state | feed_pid: Sender.start(state)}

        send(
          state.feed_pid,
          {:batches, Enum.map(pieces, &Map.take(&1, [:txid, :ts, :changes, :partial]))}
        )

        {sent(state, pieces, cut?), more? and not cut?}
    end
  end

  defp sent(state, pieces, cut?) do
    last = List.last(pieces)

    %{
      state
      | feed_part: if(last.partial, do: {last.txid, last.seq}),
        feed_sent: if(last.partial, do: state.feed_sent, else: last.txid),
        feed_backlog: state.feed_backlog + Enum.count(pieces, & &1.first),
        feed_capped: cut?
    }
  end

  defp open_reader(%State{feed_reader: nil} = state) do
    with {:ok, reader} <- Db.open_reader(state.path, state.config.feed.reader_cache_kib),
         do: {:ok, %{state | feed_reader: reader}}
  end

  defp open_reader(state), do: {:ok, state}

  defp alive?(nil), do: false
  defp alive?(pid), do: Process.alive?(pid)

  # The pieces of at most `room` batches: one that goes on a batch begun before
  # takes none. Whether others were left.
  defp take_room(pieces, :all), do: {pieces, false}

  defp take_room(pieces, room) do
    {resumed, fresh} = Enum.split_while(pieces, &(not &1.first))
    {kept, left} = Enum.split(fresh, room)

    case left do
      [] -> {pieces, false}
      _ -> {resumed ++ kept, true}
    end
  end
end
