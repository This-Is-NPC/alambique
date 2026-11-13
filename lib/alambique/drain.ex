defmodule Alambique.Drain do
  @moduledoc false
  # Graceful leave of a node (design §8): stop being a member so that other
  # nodes route elsewhere, refuse new wakes here, and hibernate every cell so
  # that its lease is released. `Alambique.drain/2` runs it on demand, and the
  # last child of the instance's supervision tree runs it when the instance
  # stops, so a stop releases the leases too.

  use GenServer

  alias Alambique.{Live, Membership, Router, Telemetry}

  # Stops after the cells' supervisor and before the registry and the group
  # (the supervisor stops its children in reverse order).
  def child_spec(config) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [config]},
      shutdown: config.drain.shutdown_ms
    }
  end

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)
    :ets.new(config.drain_status, [:named_table, :public, :set])
    # A restart of this child finds the state `terminate/2` restored.
    :ok = Membership.sync(config)
    {:ok, config}
  end

  @impl true
  def terminate(_reason, config) do
    # The leave of a stop or a crash is not a drain the host asked for: keep the
    # state, so a restart of this child puts the node back as it was.
    state = Membership.state(config)
    run(config, [])
    Membership.put_state(config, state)
    :ok
  end

  @doc """
  Drains this node (see `Alambique.drain/2`). The progress is in a table the
  child owns, so `status/1` can be read while the drain runs.
  """
  @spec run(Alambique.Config.t(), keyword) :: {:ok, %{hibernated: non_neg_integer, errors: list}}
  def run(config, opts) do
    defaults = Map.take(config.drain, [:concurrency, :timeout_ms, :deadline_ms])

    [concurrency: concurrency, deadline: deadline, timeout: timeout] =
      opts
      |> Keyword.validate!(
        concurrency: defaults.concurrency,
        timeout: defaults.timeout_ms,
        deadline: defaults.deadline_ms
      )
      |> Enum.sort()

    started = System.monotonic_time()
    :ok = Membership.leave(config)
    :ok = Alambique.NodeRegistry.deregister(config)
    cells = Registry.count(config.registry)
    begin(config, cells)
    meta = %{instance: config.name, node_id: config.node_id}
    Telemetry.emit([:drain, :start], %{cells: cells}, meta)

    limit = System.monotonic_time(:millisecond) + deadline
    summary = rounds(config, {concurrency, timeout, limit}, %{}, config.drain.rounds)
    finish(config, summary)

    errors = length(summary.errors)

    Telemetry.span_stop(
      [:drain, :stop],
      started,
      %{
        cells: cells,
        hibernated: summary.hibernated + released_failed(summary.errors),
        released: summary.hibernated,
        errors: errors
      },
      meta
    )

    {:ok, summary}
  end

  @doc """
  The state of the drain of this node: see `Alambique.drain_status/1`.
  """
  @spec status(Alambique.Config.t()) :: map
  def status(config) do
    table = config.drain_status

    row = fn key, default ->
      with [{_, v}] <- :ets.lookup(table, key), do: v, else: (_ -> default)
    end

    %{
      draining: Membership.draining?(config),
      running: row.(:running, false),
      started_at_ms: row.(:started_at_ms, nil),
      finished_at_ms: row.(:finished_at_ms, nil),
      cells_at_start: row.(:cells_at_start, 0),
      done: row.(:done, 0),
      errors:
        Enum.sort(
          for [cell, reason] <- :ets.match(table, {{:error, :"$1"}, :"$2"}), do: {cell, reason}
        ),
      left: Registry.count(config.registry)
    }
  end

  defp begin(config, cells) do
    table = config.drain_status
    :ets.delete_all_objects(table)

    :ets.insert(table, [
      {:running, true},
      {:started_at_ms, now_ms(config)},
      {:cells_at_start, cells},
      {:done, 0}
    ])
  end

  defp finish(config, summary) do
    table = config.drain_status
    :ets.match_delete(table, {{:error, :_}, :_})
    errors = for {cell, reason} <- summary.errors, do: {{:error, cell}, reason}

    :ets.insert(table, [
      {:running, false},
      {:finished_at_ms, now_ms(config)},
      {:done, summary.hibernated} | errors
    ])
  end

  defp now_ms(%{clock: {clock, clock_config}}), do: clock.now_ms(clock_config)

  # Cells whose lease could not be released did hibernate.
  defp released_failed(errors),
    do: Enum.count(errors, &match?({_, {:release_failed, _}}, &1))

  defp rounds(config, {_, _, limit} = bounds, results, left) do
    overdue? = System.monotonic_time(:millisecond) >= limit

    case cells(config, nil, config.drain.page_size) do
      [] ->
        summary(results, [], overdue?)

      awake when left == 0 or overdue? ->
        summary(results, awake, overdue?)

      awake ->
        results = hibernate(config, bounds, awake, results)
        rounds(config, bounds, results, left - 1)
    end
  end

  defp hibernate(config, {concurrency, timeout, limit}, awake, results) do
    awake
    |> Task.async_stream(&{&1, hibernate_cell(config, &1, timeout, limit)},
      max_concurrency: concurrency,
      timeout: 2 * timeout,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    )
    |> Enum.reduce(results, fn
      {:ok, {cell, result}}, acc -> record(config, acc, cell, result)
      {:exit, {cell, reason}}, acc -> record(config, acc, cell, {:error, reason})
    end)
  end

  # A cell that starts after the deadline is not touched; one in flight gets
  # what is left of it.
  defp hibernate_cell(config, cell, timeout, limit) do
    case limit - System.monotonic_time(:millisecond) do
      left when left <= 0 -> {:error, :deadline}
      left when left < timeout -> deadline_cut(Router.hibernate_local(config, cell, left))
      _left -> Router.hibernate_local(config, cell, timeout)
    end
  end

  # The call ran out of the time the deadline left it, not out of its own.
  defp deadline_cut({:error, :timeout}), do: {:error, :deadline}
  defp deadline_cut(result), do: result

  defp record(config, results, cell, result) do
    case result do
      :ok -> :ets.update_counter(config.drain_status, :done, 1)
      {:error, reason} -> :ets.insert(config.drain_status, {{:error, cell}, reason})
    end

    Map.put(results, cell, result)
  end

  defp summary(results, awake, overdue?) do
    left = if overdue?, do: :deadline, else: :awake

    results =
      Enum.reduce(awake, results, fn cell, acc ->
        Map.update(acc, cell, {:error, left}, fn
          :ok -> {:error, :awake}
          error -> error
        end)
      end)

    %{
      hibernated: Enum.count(results, &match?({_, :ok}, &1)),
      errors: for({cell, {:error, reason}} <- Enum.sort(results), do: {cell, reason})
    }
  end

  defp cells(config, after_id, page_size) do
    %{cells: ids, next: next} = Live.page(config, after_id, page_size)
    if next, do: ids ++ cells(config, next, page_size), else: ids
  end
end
