defmodule Alambique.Freeze do
  @moduledoc """
  Freezing a cell (design §10).

  A frozen cell has an object at `<prefix>/cells/<cell>/frozen`. Waking a
  frozen cell fails with `{:error, :frozen}`, so nothing writes to it, and an
  awake cell finds the object at its next lease renewal (every `lease_ttl_ms /
  3`) and hibernates. Reads of the store, restores and compaction are not
  affected. `Alambique.freeze/3` also hibernates the cell at once.

  Options of all functions: `:timeout_ms`, or an absolute monotonic `:deadline`.

  These functions take an `Alambique.Replication.ctx/1`, so tools such as the
  `mix alambique.freeze` task can use them without a running instance.
  """

  alias Alambique.Quarantine
  alias Alambique.Replication.{Keys, Retry}
  alias Alambique.Telemetry

  @doc "Freezes `cell`. Freezing a frozen cell succeeds."
  @spec freeze(Alambique.Replication.ctx(), String.t(), keyword) :: :ok | {:error, term}
  def freeze(ctx, cell, opts \\ []) do
    started = System.monotonic_time()
    {store, config} = ctx.store
    key = Keys.frozen(ctx.prefix, cell)
    deadline = deadline(ctx, opts)
    {clock, clock_config} = ctx.clock
    body = Jason.encode!(%{cell: cell, frozen_at_ms: clock.now_ms(clock_config), by: ctx.node})

    result =
      Retry.run(ctx, deadline, fn ->
        case store.put(config, key, body, :none, %{}) do
          {:ok, _etag} -> :ok
          {:error, reason} -> {:retry, reason}
        end
      end)

    emit([:cell, :freeze], started, cell, result)
    result
  end

  @doc """
  Lets `cell` wake again and forgets its crashes (`Alambique.Quarantine`): the
  operator decided it is fine. Unfreezing a cell that is not frozen succeeds.
  """
  @spec unfreeze(Alambique.Replication.ctx(), String.t(), keyword) :: :ok | {:error, term}
  def unfreeze(ctx, cell, opts \\ []) do
    started = System.monotonic_time()
    deadline = deadline(ctx, opts)

    result =
      with :ok <- Quarantine.reset(ctx, cell, deadline: deadline) do
        delete_marker(ctx, Keys.frozen(ctx.prefix, cell), deadline)
      end

    emit([:cell, :unfreeze], started, cell, result)
    result
  end

  defp delete_marker(ctx, key, deadline) do
    {store, config} = ctx.store

    Retry.run(ctx, deadline, fn ->
      case store.delete(config, key) do
        :ok -> :ok
        {:error, :not_found} -> :ok
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc "Whether `cell` is frozen. Reads one object, with retries until the deadline."
  @spec frozen?(Alambique.Replication.ctx(), String.t(), keyword) ::
          {:ok, boolean} | {:error, term}
  def frozen?(ctx, cell, opts \\ []) do
    Retry.run(ctx, deadline(ctx, opts), fn -> probe(ctx, cell) end)
  end

  @doc """
  Whether `cell` is frozen, from one attempt that is killed after
  `ctx.check_timeout_ms` (`{:error, :timeout}`), for a caller that cannot wait
  for retries.
  """
  @spec check(Alambique.Replication.ctx(), String.t()) :: {:ok, boolean} | {:error, term}
  def check(ctx, cell) do
    Retry.once(ctx.store, ctx.check_timeout_ms, fn ->
      case probe(ctx, cell) do
        {:retry, reason} -> {:error, reason}
        result -> result
      end
    end)
  end

  defp probe(ctx, cell) do
    {store, config} = ctx.store

    case store.head(config, Keys.frozen(ctx.prefix, cell)) do
      {:ok, _etag, _meta} -> {:ok, true}
      {:error, :not_found} -> {:ok, false}
      {:error, reason} -> {:retry, reason}
    end
  end

  # `:deadline` is absolute (monotonic milliseconds), `:timeout_ms` relative.
  defp deadline(ctx, opts) do
    Keyword.get_lazy(opts, :deadline, fn ->
      Retry.deadline_in(Keyword.get(opts, :timeout_ms, ctx.timeout_ms))
    end)
  end

  defp emit(event, started, cell, result) do
    meta =
      case result do
        :ok -> %{cell: cell, result: :ok}
        {:error, reason} -> %{cell: cell, result: :error, reason: reason}
      end

    Telemetry.span_stop(event, started, meta)
  end
end
