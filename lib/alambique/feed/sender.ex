defmodule Alambique.Feed.Sender do
  @moduledoc false
  # The process that puts the feed objects of one cell (design §13), so the
  # store latency of the feed is not part of a commit. The cell reads the
  # captured rows of acknowledged batches and sends them here; the sender
  # puts them in TXID order, up to `object_batches` to an object, and reports
  # each object it published.
  #
  # A failed put is retried with a growing delay, indefinitely. The sender
  # keeps accepting batches meanwhile; the cell bounds how many it hands over.
  # It ends with the cell and hibernates while idle. What it did not publish
  # stays in the cell's file for the next owner.

  require Logger

  alias Alambique.{Fault, Telemetry}
  alias Alambique.Cell.State
  alias Alambique.Feed.{Keys, Record}
  alias Alambique.Replication.Retry

  @doc "Starts the sender of the calling cell, linked to it."
  @spec start(State.t()) :: pid
  def start(%State{} = state) do
    args = %{
      cell: self(),
      id: state.id,
      ctx: state.repl_ctx,
      config: state.config,
      prefix: state.location.prefix,
      epoch: state.lease.epoch,
      floor: state.feed_floor
    }

    spawn_link(fn -> init(args) end)
  end

  defp init(args) do
    Process.monitor(args.cell)

    loop(
      Map.merge(args, %{
        queue: :queue.new(),
        floor: args.floor,
        failures: 0,
        logged: nil,
        flush: nil,
        size: args.config.feed.object_batches
      })
    )
  end

  defp loop(w) do
    if :queue.is_empty(w.queue) do
      # The floor stays: keys never go back, however long the sender idles.
      :erlang.hibernate(__MODULE__, :resume, [flushed(w)])
    else
      w = linger(w)
      {batches, rest} = take(w.queue, w.size)

      case publish(w, batches) do
        {:ok, w} -> loop(%{w | queue: rest})
        {:error, w, delay} -> w |> receive_batches(delay) |> loop()
      end
    end
  end

  @doc false
  # Entry after hibernation: waits for the next batch.
  def resume(w), do: w |> receive_batches(:infinity) |> loop()

  defp receive_batches(w, :infinity) do
    receive do
      {:batches, batches} -> enqueue(w, batches)
      {:flush, ref} -> %{w | flush: ref}
      {:DOWN, _, :process, _, _} -> exit(:normal)
    end
  end

  defp receive_batches(w, delay),
    do: receive_until(w, System.monotonic_time(:millisecond) + delay)

  defp receive_until(w, deadline) do
    wait = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:batches, batches} -> receive_until(enqueue(w, batches), deadline)
      {:flush, ref} -> receive_until(%{w | flush: ref}, deadline)
      {:DOWN, _, :process, _, _} -> exit(:normal)
    after
      wait -> w
    end
  end

  # The cell stops and asks for everything now: it waits for this reply.
  defp flushed(%{flush: nil} = w), do: w

  defp flushed(w) do
    send(w.cell, {:feed_flushed, w.flush})
    %{w | flush: nil}
  end

  defp enqueue(w, batches) do
    arrived = System.monotonic_time(:millisecond)
    %{w | queue: Enum.reduce(batches, w.queue, &:queue.in(Map.put(&1, :arrived, arrived), &2))}
  end

  # While the group is short, waits up to `linger_ms` from the oldest queued
  # batch for more.
  defp linger(w) do
    {:value, oldest} = :queue.peek(w.queue)
    fill(w, oldest.arrived + w.config.feed.linger_ms)
  end

  # Never waits when the cell asked for a flush, when the queue is at the cap
  # of the cell, or when a rewind, which is always alone, follows the group.
  defp fill(w, deadline) do
    {batches, rest} = take(w.queue, w.size)
    wait = deadline - System.monotonic_time(:millisecond)

    if length(batches) < w.size and wait > 0 and w.flush == nil and
         not rewind_next?(rest) and
         :queue.len(w.queue) < w.config.feed.max_backlog_batches do
      receive do
        {:batches, batches} -> w |> enqueue(batches) |> fill(deadline)
        {:flush, ref} -> fill(%{w | flush: ref}, deadline)
        {:DOWN, _, :process, _, _} -> exit(:normal)
      after
        wait -> w
      end
    else
      w
    end
  end

  # The head and the batches after it that fit one object; a rewind is alone.
  defp take(queue, size) do
    {{:value, head}, rest} = :queue.out(queue)
    if rewind?(head), do: {[head], rest}, else: take(rest, size - 1, [head])
  end

  defp take(queue, 0, taken), do: {Enum.reverse(taken), queue}

  defp take(queue, left, taken) do
    case :queue.out(queue) do
      {{:value, batch}, rest} ->
        if rewind?(batch),
          do: {Enum.reverse(taken), queue},
          else: take(rest, left - 1, [batch | taken])

      {:empty, _} ->
        {Enum.reverse(taken), queue}
    end
  end

  defp rewind_next?(queue) do
    case :queue.peek(queue) do
      {:value, batch} -> rewind?(batch)
      :empty -> false
    end
  end

  defp rewind?(%{changes: [{"", "rewind", _, _}]}), do: true
  defp rewind?(_), do: false

  # `floor` is the latest time a key took; a new sender starts at the cell's
  # floor, which has the times of published objects and of stamped batches. A
  # key never takes a time below it, so keys keep the commit order of the cell
  # across late groups, an emptied queue and restarts (design §13).
  # A group is late when its oldest batch is; it then takes the time now. A
  # group whose time is not its last batch's own is "lifted": its object may
  # already exist under the own time, from a previous owner.
  defp publish(w, batches) do
    {clock, clock_config} = w.config.clock
    now = clock.now_ms(clock_config)
    {first, last} = {hd(batches), List.last(batches)}
    late? = now - first.ts > w.config.feed.window_ms
    time = Enum.max([last.ts, w.floor | if(late?, do: [now], else: [])])
    lifted? = time != last.ts
    started = System.monotonic_time()
    count = length(batches)
    Fault.point(:feed_before_group_put, %{cell: w.id, txid: last.txid, batches: count})

    case put(w, batches, last, time) do
      {:ok, result, put_time, bytes} ->
        Fault.point(:feed_after_put, %{cell: w.id, txid: last.txid, pid: w.cell})

        Telemetry.span_stop(
          [:feed, :publish],
          started,
          %{records: records(batches), bytes: bytes},
          meta(w, last, lifted?, result, count)
        )

        send(w.cell, {:feed_published, self(), last.txid, put_time, count})
        {:ok, %{w | floor: max(w.floor, put_time), failures: 0}}

      {:error, reason} ->
        failures = w.failures + 1
        delay = delay(w, failures)

        Telemetry.span_stop(
          [:feed, :publish],
          started,
          %{records: records(batches), bytes: 0},
          w
          |> meta(last, lifted?, :error, count)
          |> Map.merge(%{reason: reason, failures: failures, retry_in_ms: delay})
        )

        {:error, warn(%{w | floor: time, failures: failures}, reason), delay}
    end
  end

  defp records(batches), do: Enum.reduce(batches, 0, &(length(&1.changes) + &2))

  defp meta(w, last, lifted?, result, count),
    do: %{
      cell: w.id,
      txid: last.txid,
      epoch: w.epoch,
      result: result,
      late: lifted?,
      batches: count
    }

  # Exponential from `publish_retry_ms` up to `publish_retry_max_ms`.
  defp delay(w, failures) do
    %{publish_retry_ms: first, publish_retry_max_ms: max} = w.config.feed
    min(first * Integer.pow(2, min(failures - 1, 20)), max)
  end

  # At most one warning per `publish_retry_max_ms`, however often it fails.
  defp warn(w, reason) do
    now = System.monotonic_time(:millisecond)

    if w.logged == nil or now - w.logged >= w.config.feed.publish_retry_max_ms do
      Logger.warning(
        "Alambique could not publish the feed of #{w.id} (#{w.failures} failures): #{inspect(reason)}"
      )

      %{w | logged: now}
    else
      w
    end
  end

  # A group at its own time goes to the key of its last batch's time; a
  # duplicate (412) means an earlier attempt landed, and with it every batch
  # before that key, because the sender puts in order. A lifted one first looks
  # for that key, because the previous owner may have published it in time.
  defp put(w, batches, %{ts: time} = last, time),
    do: store_put(w, batches, object_key(w, time, last.txid), time)

  defp put(w, batches, last, time) do
    {store, config} = w.ctx.store
    original = object_key(w, last.ts, last.txid)

    found =
      Retry.run(w.ctx, deadline(w), fn ->
        case store.head(config, original) do
          {:ok, _etag, _meta} -> :found
          {:error, :not_found} -> :missing
          {:error, reason} -> {:retry, reason}
        end
      end)

    case found do
      :found -> {:ok, :exists, last.ts, 0}
      :missing -> store_put(w, batches, object_key(w, time, last.txid), time)
      {:error, _} = error -> error
    end
  end

  defp store_put(w, batches, key, time) do
    {store, config} = w.ctx.store
    body = Record.encode(w.id, Enum.map(batches, &{&1.txid, &1.changes}))
    meta = %{"epoch" => Integer.to_string(w.epoch), "node" => w.config.node_id}

    result =
      Retry.run(w.ctx, deadline(w), fn ->
        case store.put(config, key, body, :if_none_match, meta) do
          {:ok, _etag} -> {:ok, :ok}
          {:error, :precondition_failed} -> {:ok, :exists}
          {:error, reason} -> {:retry, reason}
        end
      end)

    case result do
      {:ok, outcome} -> {:ok, outcome, time, byte_size(body)}
      {:error, _} = error -> error
    end
  end

  defp deadline(w), do: Retry.deadline_in(w.config.op_timeout_ms)

  defp object_key(w, time, txid),
    do: Keys.object(w.prefix, w.config.feed.shards, w.id, time, txid)
end
