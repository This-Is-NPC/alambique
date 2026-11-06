# The producer exists only when the optional Broadway dependency is present.
if Code.ensure_loaded?(Broadway) do
  defmodule Alambique.Feed.Producer do
    @moduledoc """
    A Broadway producer for the change feed (design §13). Every message carries
    one `Alambique.Feed.Record`.

        Broadway.start_link(MyApp.FeedPipeline,
          name: MyApp.FeedPipeline,
          producer: [
            module:
              {Alambique.Feed.Producer,
               ctx: Alambique.Feed.ctx(MyApp.Cells),
               position: MyApp.Positions.load(),
               store_position: &MyApp.Positions.save/1},
            concurrency: 1
          ],
          processors: [default: [concurrency: 4]]
        )

    Use one producer per feed: it polls the feed, keeps the objects it read in
    order, and stores the position only after every message of every object up
    to it was acknowledged. After a restart, with the stored position, it
    delivers again whatever was not acknowledged, so delivery is at least
    once; deduplicate by `Alambique.Feed.Record.id`. A failed message keeps the
    position behind its object until the producer restarts.

    Options:

      * `:ctx` - the `Alambique.Feed.ctx/1` to read (required)
      * `:position` - where to start, as returned by `Alambique.Feed.read/3`
        (default: the beginning)
      * `:store_position` - function of one argument called with the new
        position each time it advances, in the producer process; keep it quick
      * `:poll_interval_ms` - wait after a read that found nothing (default 1000)
      * `:max_objects` - feed objects per read (default 20)
    """

    use GenStage

    require Logger

    alias Alambique.Feed
    alias Broadway.Message

    @behaviour Broadway.Acknowledger

    @impl GenStage
    def init(opts) do
      ctx =
        case Keyword.fetch(opts, :ctx) do
          {:ok, ctx} when is_map(ctx) -> ctx
          _ -> raise ArgumentError, "Alambique.Feed.Producer requires the :ctx option"
        end

      state = %{
        ctx: ctx,
        position: opts[:position],
        committed: opts[:position],
        store_position: opts[:store_position] || fn _ -> :ok end,
        interval: Keyword.get(opts, :poll_interval_ms, 1_000),
        max_objects: Keyword.get(opts, :max_objects, 20),
        demand: 0,
        buffer: :queue.new(),
        pending: [],
        timer: nil
      }

      {:producer, state}
    end

    @impl GenStage
    def handle_demand(demand, state), do: fill(%{state | demand: state.demand + demand}, [])

    @impl GenStage
    def handle_info(:poll, state), do: fill(%{state | timer: nil}, [])
    def handle_info(_message, state), do: {:noreply, [], state}

    @impl GenStage
    def handle_cast({:ack, succeeded, failed}, state) do
      {:noreply, [], acknowledge(state, succeeded, failed)}
    end

    # Emits what the buffer and the demand allow, reading more when the buffer
    # is empty. A read that finds objects is followed by another until the
    # demand is met or the feed is caught up.
    defp fill(%{demand: 0} = state, emitted), do: {:noreply, Enum.reverse(emitted), state}

    defp fill(state, emitted) do
      case take(state) do
        {[], state} -> read(state, emitted)
        {messages, state} -> fill(state, Enum.reverse(messages, emitted))
      end
    end

    defp take(%{buffer: buffer, demand: demand} = state) do
      {taken, rest} = :queue.split(min(demand, :queue.len(buffer)), buffer)
      messages = :queue.to_list(taken)
      {messages, %{state | buffer: rest, demand: demand - length(messages)}}
    end

    defp read(%{timer: timer} = state, emitted) when timer != nil,
      do: {:noreply, Enum.reverse(emitted), state}

    defp read(state, emitted) do
      case Feed.read_objects(state.ctx, state.position, state.max_objects) do
        {:ok, [], _position} ->
          {:noreply, Enum.reverse(emitted), wait(state)}

        {:ok, objects, position} ->
          fill(enqueue(state, objects, position), emitted)

        {:error, reason} ->
          Logger.warning("Alambique feed producer could not read: #{inspect(reason)}")
          {:noreply, Enum.reverse(emitted), wait(state)}
      end
    end

    defp wait(state), do: %{state | timer: Process.send_after(self(), :poll, state.interval)}

    defp enqueue(state, objects, position) do
      messages =
        for {key, records} <- objects, record <- records do
          %Message{data: record, acknowledger: {__MODULE__, self(), key}}
        end

      pending = for {key, records} <- objects, do: {key, length(records), false}

      %{
        state
        | position: position,
          buffer: Enum.reduce(messages, state.buffer, &:queue.in/2),
          pending: state.pending ++ pending
      }
    end

    @impl Broadway.Acknowledger
    def ack(producer, successful, failed) do
      GenStage.cast(producer, {:ack, count_by_key(successful), count_by_key(failed)})
    end

    defp count_by_key(messages) do
      Enum.frequencies_by(messages, fn %Message{acknowledger: {_, _, key}} -> key end)
    end

    # Objects are acknowledged out of order; the stored position moves over the
    # longest run of objects at the front whose messages are all acknowledged.
    defp acknowledge(state, succeeded, failed) do
      pending =
        Enum.map(state.pending, fn {key, left, bad?} ->
          {key, left - Map.get(succeeded, key, 0) - Map.get(failed, key, 0),
           bad? or Map.has_key?(failed, key)}
        end)

      {done, rest} = Enum.split_while(pending, fn {_, left, bad?} -> left == 0 and not bad? end)
      state = %{state | pending: rest}

      case List.last(done) do
        nil -> state
        {key, _, _} -> commit(state, key)
      end
    end

    defp commit(state, position) do
      state.store_position.(position)
      %{state | committed: position}
    end
  end
end
