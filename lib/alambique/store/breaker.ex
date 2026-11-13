defmodule Alambique.Store.Breaker do
  @moduledoc false
  # Circuit breaker of one store (design §3). It wraps the guarded store of a
  # location, so every request of the instance that goes to it shares one state
  # in an `:atomics` array, read without a message:
  #
  #   1. `open_until`: monotonic ms until which requests fail fast; 0 is closed.
  #   2. consecutive failures while closed.
  #   3. `probe_until`: when the probe in flight is given up; 0 is none.
  #   4. wall-clock ms of the last change between closed and open, for `status/1`.
  #
  # The failures stay counted while it is open, until it closes, so `status/1`
  # shows what opened it.
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
      ref: new_ref(),
      failures: settings.failures,
      cooldown_ms: settings.cooldown_ms,
      probe_timeout_ms: settings.probe_timeout_ms,
      jitter: Keyword.get(opts, :jitter, @jitter),
      now: Keyword.get(opts, :now, &__MODULE__.monotonic_ms/0),
      meta: meta
    }
  end

  defp new_ref do
    ref = :atomics.new(4, signed: true)
    :atomics.put(ref, 4, wall_ms())
    ref
  end

  defp wall_ms, do: System.system_time(:millisecond)

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

  @doc "`store` without its breaker: a request of it neither waits for the breaker nor counts."
  @spec bypass({module, term}) :: {module, term}
  def bypass({__MODULE__, %{store: store}}), do: store

  def bypass({Alambique.Store.Metered, %{store: store} = metered}),
    do: {Alambique.Store.Metered, %{metered | store: bypass(store)}}

  def bypass(store), do: store

  @doc """
  The state of `breaker` from its atomics, without a message or a store
  request: `:closed`, `:open` (in its cool-down) or `:half_open` (the cool-down
  has passed and the next request is the probe, or it is in flight). `since_ms`
  is wall-clock Unix ms; a half-open breaker has been so since its cool-down
  ended. `failures` is the run that opened it, or the run so far while closed.
  """
  @spec status(t) :: %{
          state: :closed | :open | :half_open,
          since_ms: integer,
          failures: non_neg_integer
        }
  def status(%{ref: ref} = breaker) do
    open_until = :atomics.get(ref, 1)
    failures = :atomics.get(ref, 2)
    since = :atomics.get(ref, 4)
    elapsed = breaker.now.() - open_until

    cond do
      open_until == 0 -> %{state: :closed, since_ms: since, failures: failures}
      elapsed < 0 -> %{state: :open, since_ms: since, failures: failures}
      true -> %{state: :half_open, since_ms: wall_ms() - elapsed, failures: failures}
    end
  end

  @doc """
  `status/1` of every location of the instance: those with a breaker, and the
  locations of the placement that have none yet, which are closed.
  """
  @spec statuses(map) :: %{String.t() => map}
  def statuses(config) do
    known =
      case :ets.whereis(config.breakers) do
        :undefined -> []
        _ -> :ets.tab2list(config.breakers)
      end

    unused =
      case Alambique.Placement.locations(config) do
        {:ok, locations} -> Enum.map(locations, & &1.id)
        _ -> []
      end

    fresh = new(config.breaker, %{})

    Map.new(unused, &{&1, status(fresh)})
    |> Map.merge(Map.new(known, fn {id, breaker} -> {id, status(breaker)} end))
  end

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
    if :atomics.get(ref, 2) != 0 and :atomics.get(ref, 1) == 0, do: :atomics.put(ref, 2, 0)
    :ok
  end

  defp succeeded(%{ref: ref} = breaker, {:probe, open_until}) do
    if :atomics.compare_exchange(ref, 1, open_until, 0) == :ok do
      :atomics.put(ref, 2, 0)
      :atomics.put(ref, 3, 0)
      :atomics.put(ref, 4, wall_ms())
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
      :atomics.put(ref, 3, 0)
      :atomics.put(ref, 4, wall_ms())

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
