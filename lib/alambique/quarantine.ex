defmodule Alambique.Quarantine do
  @moduledoc """
  The crash counter of a cell (design §10).

  A cell that crashed the VM leaves an in-flight marker (`Alambique.Inflight`);
  the next start of the instance adds the crash to the object at
  `<prefix>/cells/<cell>/crashes` and freezes the cell when the crashes within
  `:window_ms` reach `:threshold`. The object is JSON:

      {"total": 5,
       "recent": [{"id": "9f2c...", "at_ms": 1700000000000, "node": "n1", "kinds": ["cell"]}]}

  `total` counts every crash recorded. `recent` holds the newest crashes that
  are still inside the window, at most `:threshold` of them; `at_ms` is the
  time of the restart that recorded the crash, on the instance's clock.

  Writes are conditional (`If-Match`, `If-None-Match` for the first one) and
  repeated on a conflict, so nodes recording the same cell do not lose
  increments. Each write carries a random `id`: a retry after an unknown
  outcome finds its own entry and does not add it twice.

  These functions take an `Alambique.Replication.ctx/1`. Options of all
  functions: `:timeout_ms`, or an absolute monotonic `:deadline`.
  """

  alias Alambique.Replication.{Keys, Retry}

  @type crash :: %{id: String.t(), at_ms: integer, node: String.t(), kinds: [String.t()]}
  @type counter :: %{total: non_neg_integer, recent: [crash]}

  @doc """
  Adds one crash. `settings` has `:threshold` and `:window_ms`; `kinds` names
  the operations that were in flight. Returns the counter after the write.
  """
  @spec record(Alambique.Replication.ctx(), String.t(), map, [String.t()], keyword) ::
          {:ok, counter} | {:error, term}
  def record(ctx, cell, settings, kinds, opts \\ []) do
    key = Keys.crashes(ctx.prefix, cell)
    id = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    deadline = deadline(ctx, opts)

    Retry.run(ctx, deadline, fn -> attempt(ctx, key, settings, id, kinds, deadline) end)
  end

  # A lost race is read again at once: the winner changed the object, so there
  # is nothing to wait for.
  defp attempt(ctx, key, settings, id, kinds, deadline) do
    {store, config} = ctx.store

    with {:ok, counter, condition} <- load(store, config, key) do
      # A write whose outcome was unknown is read again: if the entry is
      # there, it was applied.
      if Enum.any?(counter.recent, &(&1.id == id)) do
        {:ok, counter}
      else
        updated = add(counter, ctx, settings, id, kinds)

        case store.put(config, key, Jason.encode!(updated), condition, %{}) do
          {:ok, _etag} ->
            {:ok, updated}

          {:error, :precondition_failed} ->
            if System.monotonic_time(:millisecond) < deadline,
              do: attempt(ctx, key, settings, id, kinds, deadline),
              else: {:retry, :precondition_failed}

          {:error, reason} ->
            {:retry, reason}
        end
      end
    end
  end

  defp load(store, config, key) do
    case store.get(config, key) do
      {:ok, body, etag, _meta} -> {:ok, decode(body), {:if_match, etag}}
      {:error, :not_found} -> {:ok, empty(), :if_none_match}
      {:error, reason} -> {:retry, reason}
    end
  end

  @doc "The counter of `cell`, `nil` when it never crashed or was reset."
  @spec read(Alambique.Replication.ctx(), String.t(), keyword) ::
          {:ok, counter | nil} | {:error, term}
  def read(ctx, cell, opts \\ []) do
    {store, config} = ctx.store
    key = Keys.crashes(ctx.prefix, cell)

    Retry.run(ctx, deadline(ctx, opts), fn ->
      case store.get(config, key) do
        {:ok, body, _etag, _meta} -> {:ok, decode(body)}
        {:error, :not_found} -> {:ok, nil}
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  @doc "Forgets the crashes of `cell`. `:ok` also when there were none."
  @spec reset(Alambique.Replication.ctx(), String.t(), keyword) :: :ok | {:error, term}
  def reset(ctx, cell, opts \\ []) do
    {store, config} = ctx.store
    key = Keys.crashes(ctx.prefix, cell)

    Retry.run(ctx, deadline(ctx, opts), fn ->
      case store.delete(config, key) do
        :ok -> :ok
        {:error, :not_found} -> :ok
        {:error, reason} -> {:retry, reason}
      end
    end)
  end

  defp add(counter, ctx, settings, id, kinds) do
    {clock, clock_config} = ctx.clock
    now = clock.now_ms(clock_config)
    crash = %{id: id, at_ms: now, node: ctx.node, kinds: kinds}

    recent =
      [crash | counter.recent]
      |> Enum.filter(&(&1.at_ms > now - settings.window_ms))
      |> Enum.sort_by(& &1.at_ms, :desc)
      |> Enum.take(settings.threshold)

    %{total: counter.total + 1, recent: recent}
  end

  defp empty, do: %{total: 0, recent: []}

  # An unreadable object counts as empty: the next write replaces it.
  defp decode(body) do
    with {:ok, %{"total" => total, "recent" => recent}} when is_integer(total) <-
           Jason.decode(body),
         true <- is_list(recent) do
      %{total: total, recent: Enum.flat_map(recent, &crash/1)}
    else
      _ -> empty()
    end
  end

  defp crash(%{"id" => id, "at_ms" => at, "node" => node, "kinds" => kinds})
       when is_binary(id) and is_integer(at) and is_binary(node) and is_list(kinds),
       do: [%{id: id, at_ms: at, node: node, kinds: kinds}]

  defp crash(_), do: []

  defp deadline(ctx, opts) do
    Keyword.get_lazy(opts, :deadline, fn ->
      Retry.deadline_in(Keyword.get(opts, :timeout_ms, ctx.timeout_ms))
    end)
  end
end
