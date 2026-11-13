defmodule Alambique.Admission do
  @moduledoc false
  # Limits that protect a node from more cells than it can hold (design §7):
  # the cells waking at once, and a short memory of wakes that failed, for a
  # fixed time when a retry cannot change the reason and with an exponential
  # backoff when the store failed. The cap on awake cells is the
  # `max_children` of the cell supervisor, which the router keeps free by
  # hibernating the least recently used cells (`crowded?/1`), and the cap on a
  # cell's queue is taken by `Cell.admit/2` in the router.
  #
  # The wake slots and their queue belong to one process,
  # `Alambique.Admission.Wakes`, which monitors whoever holds a slot.
  #
  # One public table per instance, created by `Alambique.Membership`: the
  # entries `{{:failed, cell}, reason, until_ms, failures_in_a_row}` and, with `awake.max_bytes`,
  # the counter `:bytes` and one entry `{{:cell, pid}, bytes}` per awake cell.

  alias Alambique.{Config, Sqlite, Telemetry}

  @doc "Creates the table; called by the process that owns it."
  @spec init(Config.t()) :: :ok
  def init(config) do
    :ets.new(config.admission, [:named_table, :public, :set, write_concurrency: true])
    :ets.insert(config.admission, [{:bytes, 0}, {:owner, self()}])
    :ok
  end

  @doc """
  Reserves one of the `awake.max_wakes` slots for the calling process, which
  holds it until it passes it to a cell (`waking/3`) or gives it back
  (`release/1`); if it dies first the slot comes back. A wake that finds none
  waits in a FIFO queue of at most `awake.max_wake_queue` places, for up to
  `wait_ms` (never more than `awake.wake_queue_ms`), and gets the slot of the
  first wake that ends; past that, or with no wait, it is refused. See
  `Alambique.Admission.Wakes`.
  """
  @spec begin_wake(Config.t(), timeout) :: :ok | {:error, :overloaded}
  def begin_wake(config, wait_ms),
    do:
      GenServer.call(config.wakes, {:begin, min(wait_ms, config.awake.wake_queue_ms)}, :infinity)

  @doc "Gives back the slot the calling process holds, when no cell took it over."
  @spec release(Config.t()) :: :ok
  def release(config), do: end_wake(config, self())

  @doc """
  Called by the cell `pid` when it starts: `Membership` is asked to forget
  the cell's memory record if it dies.
  """
  @spec waking(Config.t(), pid) :: :ok
  def waking(config, pid) do
    [{:owner, owner}] = :ets.lookup(config.admission, :owner)
    GenServer.cast(owner, {:monitor_wake, pid})
  end

  @doc """
  Called by the cell, in its own process before any wake work, to hold a
  slot: the one of `holder`, the process that woke it, which is moved to the
  cell. If `holder` died, its slot was already freed and the cell takes one
  like any wake does, waiting in the queue; `{:error, :overloaded}` means the
  cell must not wake.
  """
  @spec take_over(Config.t(), pid | nil) :: :ok | {:error, :overloaded}
  def take_over(config, holder) do
    case GenServer.call(config.wakes, {:transfer, holder, self()}) do
      :ok -> :ok
      :lost -> begin_wake(config, config.awake.wake_queue_ms)
    end
  end

  @doc "Ends the wake of `pid`: gives its slot back once, however many times it is called."
  @spec end_wake(Config.t(), pid) :: :ok
  def end_wake(config, pid), do: GenServer.cast(config.wakes, {:end, pid})

  # Failures a retry reproduces at the cost of a full restore: remembered for
  # `wake.failure_backoff_ms`.
  @deterministic [:restore, :hydration, :local_file, :open, :migration, :schema_too_new]

  # What a wrapped reason says about its origin: the step that failed.
  @wrappers [:restore, :hydration, :lease, :lease_unreadable, :frozen_check, :placement, :lazy]

  @transport [
    :timeout,
    :closed,
    :store_unavailable,
    :econnrefused,
    :econnreset,
    :econnaborted,
    :etimedout,
    :ehostunreach,
    :enetunreach,
    :enetdown,
    :nxdomain,
    :epipe,
    :connect_timeout
  ]

  @doc """
  Whether `reason` is a store that failed or did not answer: a timeout, an
  HTTP answer, a crashed request, a refused or dropped connection, an open
  circuit breaker (`:store_unavailable`), alone or inside the reason of the
  step that met it (`{:lease_unreadable, _}`, `{:lease, _}`, `{:restore, _}`, `{kind, key, _}`...).
  Anything else is not: another node that owns the cell, a frozen or draining
  cell, a full disk, a damaged chain.
  """
  @spec store_error?(term) :: boolean
  def store_error?(reason) when reason in @transport, do: true
  def store_error?({tag, _, _}) when tag in [:http, :store_crashed, :request_crashed], do: true
  def store_error?({tag, reason}) when tag in @wrappers, do: store_error?(reason)

  def store_error?({kind, key, reason}) when is_atom(kind) and is_binary(key),
    do: store_error?(reason)

  def store_error?(_), do: false

  @doc """
  Remembers a failed wake of `cell`, if a retry would not do better.

  A store error is remembered with an exponential backoff: `wake.backoff_ms`,
  times `wake.backoff_factor` for each failure in a row, up to
  `wake.backoff_max_ms`, less a random share of up to `wake.backoff_jitter`.
  A deterministic failure is remembered for `wake.failure_backoff_ms`. The
  count starts over after a wake that succeeds (`recovered/2`) or a quiet time
  as long as the longest delay. Anything else is retried at once.
  """
  @spec failed(Config.t(), String.t(), term, integer) :: :ok
  def failed(config, cell, reason, now \\ System.monotonic_time(:millisecond)) do
    cond do
      store_error?(reason) ->
        attempts = attempts(config, cell, now) + 1
        delay = delay(config.wake, attempts)
        remember(config, cell, reason, now + delay, attempts, now)

        Telemetry.emit([:admission, :wake_backoff], %{delay_ms: delay, failures: attempts}, %{
          cell: cell,
          reason: reason
        })

      is_tuple(reason) and elem(reason, 0) in @deterministic ->
        remember(config, cell, reason, now + config.wake.failure_backoff_ms, 0, now)

      true ->
        :ok
    end
  end

  # The delay after the `attempts`-th failure in a row before the random
  # share is taken away.
  defp ceiling(wake, attempts),
    do:
      min(
        wake.backoff_max_ms,
        round(wake.backoff_ms * :math.pow(wake.backoff_factor, attempts - 1))
      )

  defp delay(wake, attempts) do
    top = ceiling(wake, attempts)
    max(1, round(top * (1 - wake.backoff_jitter * :rand.uniform())))
  end

  defp attempts(config, cell, now) do
    case :ets.lookup(config.admission, {:failed, cell}) do
      [{_, _, until, attempts}] when now <= until + config.wake.backoff_max_ms -> attempts
      _ -> 0
    end
  end

  defp remember(config, cell, reason, until, attempts, now) do
    if :ets.info(config.admission, :size) > config.hint_limit, do: purge(config, now)
    :ets.insert(config.admission, {{:failed, cell}, reason, until, attempts})
    :ok
  end

  # The entries whose backoff and quiet time ended are dropped first; the rest
  # only if that was not enough, which loses the counts of the cells in backoff.
  defp purge(config, now) do
    quiet = config.wake.backoff_max_ms

    :ets.select_delete(config.admission, [
      {{{:failed, :_}, :_, :"$1", :_}, [{:<, {:+, :"$1", quiet}, now}], [true]}
    ])

    if :ets.info(config.admission, :size) > config.hint_limit,
      do: :ets.match_delete(config.admission, {{:failed, :_}, :_, :_, :_})
  end

  @doc "Ends the backoff of `cell`, which woke: the next failure starts from the first delay."
  @spec recovered(Config.t(), String.t()) :: :ok
  def recovered(config, cell) do
    :ets.delete(config.admission, {:failed, cell})
    :ok
  end

  @doc """
  The remembered failure of `cell` if it is the store's, while its backoff
  lasts: the router asks it before reading the lease, which a store that
  failed would be asked for again by every call.
  """
  @spec store_failure(Config.t(), String.t()) :: {:error, term} | nil
  def store_failure(config, cell) do
    with {:error, reason} = error <- failure(config, cell),
         true <- store_error?(reason),
         do: error,
         else: (_ -> nil)
  end

  @doc "The remembered failure of `cell`, while its backoff lasts."
  @spec failure(Config.t(), String.t(), integer) :: {:error, term} | nil
  def failure(config, cell, now \\ System.monotonic_time(:millisecond)) do
    case :ets.lookup(config.admission, {:failed, cell}) do
      [{_, reason, until, _}] ->
        if now < until, do: {:error, reason}

      [] ->
        nil
    end
  end

  # The memory budget (`awake.max_bytes`, design §7). Each awake cell reports
  # what its connection holds after every call and at the end of its wake;
  # nothing is measured or kept without a budget.

  @doc "Records the memory of `conn` for the cell `pid`."
  @spec track(Config.t(), pid, Sqlite.conn()) :: :ok
  def track(%{awake: %{max_bytes: nil}}, _pid, _conn), do: :ok

  def track(config, pid, conn) do
    with {:ok, memory} <- Sqlite.memory(conn) do
      bytes = memory.cache_used + memory.schema_used + memory.stmt_used
      old = recorded(config, pid)
      :ets.insert(config.admission, {{:cell, pid}, bytes})
      if bytes != old, do: :ets.update_counter(config.admission, :bytes, bytes - old)
    end

    :ok
  end

  defp recorded(config, pid) do
    case :ets.lookup(config.admission, {:cell, pid}) do
      [{_, bytes}] -> bytes
      [] -> 0
    end
  end

  @doc """
  Drops the record of `pid`, once however many times it is called: by the cell
  when it ends and by `Membership` when it sees the cell die.
  """
  @spec forget(Config.t(), pid) :: :ok
  def forget(config, pid) do
    with [{_, bytes}] <- :ets.take(config.admission, {:cell, pid}),
         do: :ets.update_counter(config.admission, :bytes, -bytes)

    :ok
  end

  @doc """
  Whether a wake has to wait for a cell to hibernate: the awake cells fill
  `awake.max_cells`, or their memory reaches `awake.max_bytes`.
  """
  @spec crowded?(Config.t()) :: boolean
  def crowded?(config), do: full?(config) or over_budget?(config)

  # The registry count is the cheap test; the supervisor is asked only at the
  # ceiling, because it forgets an exited cell before the registry does.
  defp full?(config) do
    Registry.count(config.registry) >= config.awake.max_cells and
      DynamicSupervisor.count_children(config.supervisor).active >= config.awake.max_cells
  end

  defp over_budget?(%{awake: %{max_bytes: nil}}), do: false

  defp over_budget?(config),
    do: :ets.lookup_element(config.admission, :bytes, 2) >= config.awake.max_bytes

  @doc "The recorded bytes of the awake cells."
  @spec bytes(Config.t()) :: non_neg_integer
  def bytes(config), do: :ets.lookup_element(config.admission, :bytes, 2)

  @doc """
  The cells that finished waking, least recently used first, as
  `{pid, load, id}`. The last call is in the third slot of `load`.
  """
  @spec least_recently_used(Config.t()) :: [{pid, :atomics.atomics_ref(), String.t()}]
  def least_recently_used(config) do
    config.registry
    |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}])
    |> Enum.flat_map(fn {id, pid, load} ->
      case Alambique.Cell.last_used(load) do
        nil -> []
        last -> [{last, pid, load, id}]
      end
    end)
    |> Enum.sort()
    |> Enum.map(fn {_last, pid, load, id} -> {pid, load, id} end)
  end
end
