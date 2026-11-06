defmodule Alambique.Drain do
  @moduledoc false
  # Graceful leave of a node (design §8): stop being a member so that other
  # nodes route elsewhere, refuse new wakes here, and hibernate every cell so
  # that its lease is released. `Alambique.drain/2` runs it on demand, and the
  # last child of the instance's supervision tree runs it when the instance
  # stops, so a stop releases the leases too.

  use GenServer

  alias Alambique.{Live, Membership, Router}

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

  @spec run(Alambique.Config.t(), keyword) :: {:ok, %{hibernated: non_neg_integer, errors: list}}
  def run(config, opts) do
    defaults = Map.take(config.drain, [:concurrency, :timeout_ms])

    [concurrency: concurrency, timeout: timeout] =
      opts
      |> Keyword.validate!(concurrency: defaults.concurrency, timeout: defaults.timeout_ms)
      |> Enum.sort()

    :ok = Membership.leave(config)
    {:ok, rounds(config, concurrency, timeout, %{}, config.drain.rounds)}
  end

  defp rounds(config, concurrency, timeout, results, left) do
    case cells(config, nil, config.drain.page_size) do
      [] ->
        summary(results, [])

      awake when left == 0 ->
        summary(results, awake)

      awake ->
        results =
          awake
          |> Task.async_stream(&{&1, Router.hibernate_local(config, &1, timeout)},
            max_concurrency: concurrency,
            timeout: 2 * timeout,
            on_timeout: :kill_task,
            zip_input_on_exit: true
          )
          |> Enum.reduce(results, fn
            {:ok, {cell, result}}, acc -> Map.put(acc, cell, result)
            {:exit, {cell, reason}}, acc -> Map.put(acc, cell, {:error, reason})
          end)

        rounds(config, concurrency, timeout, results, left - 1)
    end
  end

  defp summary(results, awake) do
    results =
      Enum.reduce(awake, results, fn cell, acc ->
        Map.update(acc, cell, {:error, :awake}, fn
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
