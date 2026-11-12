defmodule Alambique.Admission do
  @moduledoc false
  # Limits that protect a node from more cells than it can hold (design §7):
  # the cells waking at once, and a short memory of wakes that failed for a
  # reason a retry cannot change. The cap on awake cells is the
  # `max_children` of the cell supervisor, which the router keeps free by
  # hibernating the least recently used cells (`crowded?/1`), and the cap on a
  # cell's queue is taken by `Cell.admit/2` in the router.
  #
  # One public table per instance, created by `Alambique.Membership`: the
  # counters `:wakes` and `:queued` (wakes waiting for a slot, whose entries
  # `{seq, pid, ref}`, in the order they came, are in the ordered table
  # `wake_queue`), the entries `{{:waking, pid}}` of cells that have not
  # finished waking, the entries `{{:failed, cell}, reason, until_ms}`, and,
  # with `awake.max_bytes`, the counter `:bytes` and one entry
  # `{{:cell, pid}, bytes}` per awake cell.
  #
  # A wake slot is reserved by the router and becomes the `{:waking, pid}`
  # entry when the cell starts (`waking/2`). Whoever takes that entry first,
  # the cell at the end of its wake or `Membership` when it sees the cell die,
  # gives the slot back, so a cell killed mid-wake cannot leak it.

  alias Alambique.{Config, Sqlite, Telemetry}

  @doc "Creates the table; called by the process that owns it."
  @spec init(Config.t()) :: :ok
  def init(config) do
    :ets.new(config.admission, [:named_table, :public, :set, write_concurrency: true])
    :ets.new(config.wake_queue, [:named_table, :public, :ordered_set])
    :ets.insert(config.admission, [{:wakes, 0}, {:queued, 0}, {:bytes, 0}, {:owner, self()}])
    :ok
  end

  @doc """
  Reserves one of the `awake.max_wakes` slots. A wake that finds none waits
  in a FIFO queue of at most `awake.max_wake_queue` places, for up to
  `wait_ms` (and never past `awake.wake_queue_ms`), and takes over the slot
  of the first wake that ends; past that, or with no wait, it is refused. The
  caller gives the slot back with `release/1` when no cell was started; a
  started cell holds it as `waking/2` until `end_wake/2`.
  """
  @spec begin_wake(Config.t(), timeout) :: :ok | {:error, :overloaded}
  def begin_wake(config, wait_ms) do
    wait_ms = min(wait_ms, config.awake.wake_queue_ms)

    cond do
      queued(config) == 0 and take(config, :wakes, config.awake.max_wakes) -> :ok
      wait_ms <= 0 -> {:error, :overloaded}
      true -> enqueue(config, wait_ms)
    end
  end

  @doc "Gives back a slot that no cell used, or hands it to the first wake waiting for one."
  @spec release(Config.t()) :: :ok
  def release(config) do
    unless hand_over(config) do
      :ets.update_counter(config.admission, :wakes, -1)

      # A wake may have queued after the queue was seen empty.
      if queued(config) > 0 and take(config, :wakes, config.awake.max_wakes),
        do: release(config)
    end

    :ok
  end

  # `key` goes up by one unless it is at `limit`. One atomic step, and a
  # refusal leaves the counter as it was, so a refused caller never makes
  # another one fail.
  defp take(config, key, limit) do
    spec = [{{key, :"$1"}, [{:<, :"$1", limit}], [{{key, {:+, :"$1", 1}}}]}]
    :ets.select_replace(config.admission, spec) == 1
  end

  defp queued(config), do: :ets.lookup_element(config.admission, :queued, 2)

  defp enqueue(config, wait_ms) do
    started = System.monotonic_time()

    if take(config, :queued, config.awake.max_wake_queue) or
         (purge_dead(config) and take(config, :queued, config.awake.max_wake_queue)) do
      seq = :erlang.unique_integer([:monotonic])
      ref = make_ref()
      :ets.insert(config.wake_queue, {seq, self(), ref})
      granted? = wait(config, seq, ref, wait_ms)
      result = if granted?, do: :granted, else: :expired
      Telemetry.span_stop([:admission, :wake_queue], started, %{result: result})
      if granted?, do: :ok, else: {:error, :overloaded}
    else
      Telemetry.span_stop([:admission, :wake_queue], started, %{result: :full})
      {:error, :overloaded}
    end
  end

  # A slot may have been freed after the fast path looked, and its release
  # found nobody to hand it to: try once the place is in the queue.
  defp wait(config, seq, ref, wait_ms) do
    if take(config, :wakes, config.awake.max_wakes) do
      # If a release picked this wake in the meantime, it gave a slot too.
      if leave(config, seq) == :picked and granted?(config, ref), do: release(config)
      true
    else
      receive do
        {^ref, :granted} -> true
      after
        wait_ms -> leave(config, seq) == :picked and granted?(config, ref)
      end
    end
  end

  # Leaves the queue; the entry is gone already when a release picked it.
  defp leave(config, seq) do
    case :ets.take(config.wake_queue, seq) do
      [_] ->
        :ets.update_counter(config.admission, :queued, -1)
        :left

      [] ->
        :picked
    end
  end

  # The release that picked a wake sends the grant right after; wait for it
  # a bounded time, in case that process died in between.
  defp granted?(config, ref) do
    receive do
      {^ref, :granted} -> true
    after
      config.call_timeout_ms -> false
    end
  end

  # Gives the slot being released to the first wake in the queue that is
  # still alive (one whose caller gave up holds no place).
  defp hand_over(config) do
    with seq when seq != :"$end_of_table" <- :ets.first(config.wake_queue) do
      case :ets.take(config.wake_queue, seq) do
        [{^seq, pid, ref}] ->
          :ets.update_counter(config.admission, :queued, -1)

          if Process.alive?(pid) do
            send(pid, {ref, :granted})
            true
          else
            hand_over(config)
          end

        [] ->
          hand_over(config)
      end
    else
      _ -> false
    end
  end

  # Frees the places of wakes whose caller died; true if there was any.
  defp purge_dead(config) do
    config.wake_queue
    |> :ets.tab2list()
    |> Enum.reduce(false, fn {seq, pid, _}, purged ->
      if Process.alive?(pid), do: purged, else: leave(config, seq) == :left or purged
    end)
  end

  @doc """
  Called by the cell `pid` when it starts: the reserved slot is now held by
  it, and `Membership` is asked to give it back if the cell dies mid-wake.
  """
  @spec waking(Config.t(), pid) :: :ok
  def waking(config, pid) do
    :ets.insert(config.admission, {{:waking, pid}})
    [{:owner, owner}] = :ets.lookup(config.admission, :owner)
    GenServer.cast(owner, {:monitor_wake, pid})
  end

  @doc "Ends the wake of `pid`: gives its slot back once, however many times it is called."
  @spec end_wake(Config.t(), pid) :: :ok
  def end_wake(config, pid) do
    if :ets.take(config.admission, {:waking, pid}) != [], do: release(config)
    :ok
  end

  # Failures a retry reproduces at the cost of a full restore. Anything that
  # depends on the lease, the owner or the moment is retried at once.
  @deterministic [:restore, :hydration, :local_file, :open, :migration, :schema_too_new]

  @doc "Remembers a failed wake of `cell` for `wake.failure_backoff_ms`, if a retry would repeat it."
  @spec failed(Config.t(), String.t(), term) :: :ok
  def failed(config, cell, reason) do
    if is_tuple(reason) and elem(reason, 0) in @deterministic do
      if :ets.info(config.admission, :size) > config.hint_limit,
        do: :ets.match_delete(config.admission, {{:failed, :_}, :_, :_})

      until = System.monotonic_time(:millisecond) + config.wake.failure_backoff_ms
      :ets.insert(config.admission, {{:failed, cell}, reason, until})
    end

    :ok
  end

  @doc "The remembered failure of `cell`, while it lasts."
  @spec failure(Config.t(), String.t()) :: {:error, term} | nil
  def failure(config, cell) do
    case :ets.lookup(config.admission, {:failed, cell}) do
      [{_, reason, until}] ->
        if System.monotonic_time(:millisecond) < until do
          {:error, reason}
        else
          :ets.delete(config.admission, {:failed, cell})
          nil
        end

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
