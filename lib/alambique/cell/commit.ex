defmodule Alambique.Cell.Commit do
  @moduledoc false
  # Uploads the transactions committed since the last upload as one batch
  # (design §6) and tells the instance's after-ack callback.

  alias Alambique.Cell.{Pipe, State}
  alias Alambique.Compaction.{Compactor, Hold}
  alias Alambique.Replication.Replicator
  alias Alambique.Telemetry

  @doc """
  Replicates the pending local commits and waits for them, with an empty
  pipeline: the wake and the migrations use it. `size` is the number of
  transactions in the batch, for telemetry.

  `{:ok, state, txid}` carries the TXID of the new segment, or `nil` when
  there was nothing to upload. `{:error, :fenced, state}` means another owner
  holds the TXID; any other error means the upload did not complete in time.
  """
  @spec run(State.t(), non_neg_integer) ::
          {:ok, State.t(), pos_integer | nil} | {:error, term, State.t()}
  def run(%State{repl: repl} = state, size) do
    started = System.monotonic_time()

    meta = %{
      cell: state.id,
      location: state.location.id,
      epoch: state.lease.epoch,
      txid: Replicator.next_txid(repl)
    }

    case Replicator.sync(repl, state.repl_ctx,
           deadline: State.deadline(state),
           landed: &Hold.put_upload(state.config, state.repl_ctx, &1)
         ) do
      {:ok, ^repl} ->
        {:ok, state, nil}

      {:ok, next} ->
        Compactor.notify(state.config, state.id, next.txid)

        queued_ms =
          System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)

        emit(
          started,
          %{transactions: size, inflight: 1, queued_ms: queued_ms},
          Map.put(meta, :result, :ok)
        )

        state = %{state | repl: next, pipe: %{state.pipe | acked: Pipe.acked(next)}}
        {:ok, state, next.txid}

      {:error, reason} ->
        result = if reason == :fenced, do: :fenced, else: :error

        emit(
          started,
          %{transactions: size, inflight: 1},
          Map.merge(meta, %{result: result, reason: reason})
        )

        {:error, reason, state}
    end
  end

  @doc "Emits `[:cell, :commit]` for a batch that started at `started`."
  @spec emit(integer, map, map) :: :ok
  def emit(started, measurements, meta),
    do: Telemetry.span_stop([:cell, :commit], started, measurements, meta)

  @doc "Calls the instance's `:after_ack` callback; its failure never affects the cell."
  @spec after_ack(State.t(), pos_integer) :: :ok
  def after_ack(%State{config: %{after_ack: nil}}, _txid), do: :ok

  def after_ack(%State{config: %{after_ack: callback}, id: id}, txid) do
    callback.(id, txid)
    :ok
  rescue
    exception ->
      require Logger
      Logger.error("Alambique after_ack callback failed: " <> Exception.message(exception))
      :ok
  end
end
