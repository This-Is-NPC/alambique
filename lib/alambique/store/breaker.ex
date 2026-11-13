defmodule Alambique.Store.Breaker do
  @moduledoc false
  # Circuit breaker of one store (design §3). It wraps the guarded store of a
  # location, so every request of the instance that goes to it shares one state
  # in an `:atomics` array, read without a message:
  #
  #   1. `open_until`: monotonic ms until which requests fail fast; 0 is closed.
  #   2. consecutive failures while closed.
  #   3. `probe_until`: when the probe in flight is given up; 0 is none.
  #
  # Closed, `failures` failures in a row open it. Open, a request answers
  # `{:error, :store_unavailable}` without touching the store. When `open_until`
  # has passed the breaker is half-open: one request (the probe) is let through,
  # claimed with a compare-and-swap, so there is never a second; a success
  # closes it and a failure opens it again for another cool-down. A probe that
  # never reports (its process was killed) is given up after `probe_timeout_ms`
  # and the next request becomes the probe.
  #
  # A failure is an answer that says nothing about the request itself: a
  # timeout, a transport error, a crash of the store module or an HTTP 5xx.
  # `:not_found`, `:precondition_failed` (normal for leases and fences) and
  # HTTP 4xx are answers of a store that works.

  @behaviour Alambique.Store

  alias Alambique.Telemetry

  @jitter 0.2

  @type t :: %{
          ref: :atomics.atomics_ref(),
          failures: pos_integer,
          cooldown_ms: pos_integer,
          probe_timeout_ms: pos_integer,
          jitter: float,
          now: (-> integer),
          meta: map
        }

  @doc """
  A closed breaker from `config.breaker`; `meta` goes in its telemetry events.
  `:now` (a monotonic clock in ms) and `:jitter` are for tests.
  """
  @spec new(map, map, keyword) :: t
  def new(settings, meta, opts \\ []) do
    %{
      ref: :atomics.new(3, signed: true),
      failures: settings.failures,
      cooldown_ms: settings.cooldown_ms,
      probe_timeout_ms: settings.probe_timeout_ms,
      jitter: Keyword.get(opts, :jitter, @jitter),
      now: Keyword.get(opts, :now, &__MODULE__.monotonic_ms/0),
      meta: meta
    }
  end

  @doc """
  The guarded store of `location`, behind the breaker that every context of the
  instance shares for it (kept in `config.breakers`, created on first use). An
  instance without the table, such as a context built in a test, has none.
  """
  @spec guard(map, Alambique.Location.t()) :: {module, term}
  def guard(config, %{id: id, store: store}) do
    guarded = {Alambique.Store.Guard, store}

    case :ets.whereis(config.breakers) do
      :undefined -> guarded
      _ -> {__MODULE__, %{store: guarded, breaker: shared(config, id)}}
    end
  end

  defp shared(config, id) do
    case :ets.lookup(config.breakers, id) do
      [{_, breaker}] ->
        breaker

      [] ->
        fresh = new(config.breaker, %{instance: config.name, location: id})
        :ets.insert_new(config.breakers, {id, fresh})
        [{_, breaker}] = :ets.lookup(config.breakers, id)
        breaker
    end
  end

  @doc false
  def monotonic_ms, do: System.monotonic_time(:millisecond)

  @doc "Whether `breaker` is open or half-open."
  @spec open?(t) :: boolean
  def open?(%{ref: ref}), do: :atomics.get(ref, 1) != 0

  @doc """
  Records a request killed at its deadline, which never reported. Only a
  closed breaker counts it: a probe that hangs is given up by its own timeout.
  """
  @spec timed_out(term) :: :ok
  def timed_out({__MODULE__, %{breaker: breaker}}), do: failed(breaker, :closed)
  def timed_out({Alambique.Store.Metered, %{store: store}}), do: timed_out(store)
  def timed_out(_store), do: :ok

  @doc "`:store_unavailable` for a reason that is, or ends in, the breaker's answer."
  @spec classify(term) :: term
  def classify(:store_unavailable), do: :store_unavailable

  def classify(reason) when is_tuple(reason) and tuple_size(reason) > 1,
    do:
      if(classify(elem(reason, tuple_size(reason) - 1)) == :store_unavailable,
        do: :store_unavailable,
        else: reason
      )

  def classify(reason), do: reason

  @impl true
  def location(%{store: store}), do: Alambique.Store.location(store)

  @impl true
  def put(s, key, body, condition, meta),
    do: through(s, fn m, c -> m.put(c, key, body, condition, meta) end)

  @impl true
  def get(s, key), do: through(s, fn m, c -> m.get(c, key) end)

  @impl true
  def head(s, key), do: through(s, fn m, c -> m.head(c, key) end)

  @impl true
  def get_range(s, key, offset, length),
    do: through(s, fn m, c -> m.get_range(c, key, offset, length) end)

  @impl true
  def list(s, prefix, start_after, limit),
    do: through(s, fn m, c -> m.list(c, prefix, start_after, limit) end)

  @impl true
  def delete(s, key), do: through(s, fn m, c -> m.delete(c, key) end)

  defp through(%{store: {m, c}, breaker: breaker}, fun) do
    case admit(breaker) do
      :reject ->
        {:error, :store_unavailable}

      ticket ->
        result = fun.(m, c)
        record(breaker, ticket, result)
        result
    end
  end

  # `:closed` goes through; `{:probe, open_until}` is the one request of a
  # half-open breaker; the others are rejected.
  defp admit(%{ref: ref} = breaker) do
    case :atomics.get(ref, 1) do
      0 ->
        :closed

      open_until ->
        now = breaker.now.()

        with true <- now >= open_until,
             probe_until = :atomics.get(ref, 3),
             true <- probe_until == 0 or now >= probe_until,
             :ok <-
               :atomics.compare_exchange(
                 ref,
                 3,
                 probe_until,
                 nonzero(now + breaker.probe_timeout_ms)
               ) do
          {:probe, open_until}
        else
          _ -> :reject
        end
    end
  end

  defp record(breaker, ticket, result) do
    if failure?(result), do: failed(breaker, ticket), else: succeeded(breaker, ticket)
  end

  # Anything but a closed breaker ignores the requests that started before it
  # opened.
  defp succeeded(%{ref: ref}, :closed) do
    if :atomics.get(ref, 2) != 0, do: :atomics.put(ref, 2, 0)
    :ok
  end

  defp succeeded(%{ref: ref} = breaker, {:probe, open_until}) do
    if :atomics.compare_exchange(ref, 1, open_until, 0) == :ok do
      :atomics.put(ref, 2, 0)
      :atomics.put(ref, 3, 0)
      Telemetry.emit([:store, :breaker, :close], breaker.meta)
    end

    :ok
  end

  defp failed(%{ref: ref} = breaker, :closed) do
    if :atomics.get(ref, 1) == 0 and :atomics.add_get(ref, 2, 1) >= breaker.failures,
      do: open(breaker, 0, :failures)

    :ok
  end

  defp failed(breaker, {:probe, open_until}), do: open(breaker, open_until, :probe_failed)

  defp open(%{ref: ref} = breaker, from, reason) do
    if :atomics.compare_exchange(ref, 1, from, nonzero(breaker.now.() + cooldown(breaker))) == :ok do
      :atomics.put(ref, 2, 0)
      :atomics.put(ref, 3, 0)

      Telemetry.emit(
        [:store, :breaker, :open],
        %{cooldown_ms: breaker.cooldown_ms},
        Map.put(breaker.meta, :reason, reason)
      )
    end

    :ok
  end

  # Jitter keeps the nodes of a cluster from probing the store together.
  defp cooldown(%{cooldown_ms: ms, jitter: jitter}), do: ms + round(ms * jitter * :rand.uniform())

  defp failure?({:error, :not_found}), do: false
  defp failure?({:error, :precondition_failed}), do: false
  defp failure?({:error, {:http, status, _}}) when is_integer(status), do: status >= 500
  defp failure?({:error, _}), do: true
  defp failure?(_), do: false

  # 0 means none, and a monotonic time may be 0.
  defp nonzero(0), do: 1
  defp nonzero(ms), do: ms
end
