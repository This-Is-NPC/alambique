defmodule Alambique.Load.OpenLoop do
  @moduledoc false
  # Runs a schedule of operations without waiting for the answers. Every
  # operation is due at a fixed time; its latency is measured from that time,
  # not from when the driver managed to issue it, so a slow service cannot hide
  # its queue (no coordinated omission). At most `:max_outstanding` operations
  # run at once; a due operation over the cap is dropped and counts as planned
  # but not completed.

  @doc """
  Runs `op.(index)` for each offset of `offsets` (microseconds from now).
  `op` returns `:ok` or `{:error, reason}`; a raise or exit is an error. The
  result counts `{:error, :busy}` refusals apart (`busy`) from the other
  `errors`.

  Options: `:max_outstanding` (default 5 000), `:drain_ms` (wait for the
  stragglers after the last one is due, default 20 000), `:abort` (a function
  polled about twice a second that returns `nil` or a reason that stops the
  step early).
  """
  @spec run([non_neg_integer], (non_neg_integer -> :ok | {:error, term}), keyword) :: map
  def run(offsets, op, opts \\ []) do
    ctx = %{
      op: op,
      max: Keyword.get(opts, :max_outstanding, 5_000),
      abort: Keyword.get(opts, :abort, fn -> nil end),
      results: :ets.new(:load_results, [:public, :duplicate_bag, write_concurrency: true]),
      live: :ets.new(:load_live, [:public, :set, write_concurrency: true]),
      # 1: outstanding, 2: dropped
      counters: :counters.new(2, []),
      t0: now() + 100_000
    }

    {issued, aborted} = issue(offsets, 0, ctx, 0)
    aborted = aborted || drain(ctx, now() + Keyword.get(opts, :drain_ms, 20_000) * 1000)
    summary = summarize(ctx, length(offsets), issued, aborted)
    for {pid} <- :ets.tab2list(ctx.live), do: Process.exit(pid, :kill)
    :ets.delete(ctx.results)
    :ets.delete(ctx.live)
    summary
  end

  defp issue([], _index, _ctx, issued), do: {issued, nil}

  defp issue([offset | rest] = offsets, index, ctx, issued) do
    wait = ctx.t0 + offset - now()

    if wait > 1_000 do
      case ctx.abort.() do
        nil ->
          Process.sleep(min(div(wait, 1000), 250))
          issue(offsets, index, ctx, issued)

        reason ->
          {issued, reason}
      end
    else
      issue(rest, index + 1, ctx, issued + start(ctx, offset, index))
    end
  end

  defp start(ctx, offset, index) do
    if :counters.get(ctx.counters, 1) >= ctx.max do
      :counters.add(ctx.counters, 2, 1)
      0
    else
      :counters.add(ctx.counters, 1, 1)
      due = ctx.t0 + offset

      pid =
        spawn(fn ->
          result =
            try do
              ctx.op.(index)
            catch
              kind, reason -> {:error, {kind, reason}}
            end

          :ets.insert(ctx.results, {now() - due, result})
          :ets.delete(ctx.live, self())
          :counters.sub(ctx.counters, 1, 1)
        end)

      :ets.insert(ctx.live, {pid})
      1
    end
  end

  defp drain(ctx, deadline) do
    cond do
      :counters.get(ctx.counters, 1) == 0 -> nil
      now() > deadline -> nil
      true -> Process.sleep(20) && drain(ctx, deadline)
    end
  end

  defp summarize(ctx, planned, issued, aborted) do
    rows = :ets.tab2list(ctx.results)
    ok = for {us, :ok} <- rows, do: us
    failed = for {_us, {:error, reason}} <- rows, do: reason
    {busy, other} = Enum.split_with(failed, &busy?/1)
    errors = Enum.map(other, &inspect(&1, limit: 10))

    %{
      planned: planned,
      issued: issued,
      dropped: :counters.get(ctx.counters, 2),
      completed: length(rows),
      ok: length(ok),
      busy: length(busy),
      errors: length(errors),
      error_samples:
        errors
        |> Enum.frequencies()
        |> Enum.sort_by(&elem(&1, 1), :desc)
        |> Enum.take(5)
        |> Enum.map(fn {error, count} -> %{error: error, count: count} end),
      latencies_us: Enum.sort(ok),
      aborted: aborted
    }
  end

  # The library's refusal when its statement slots are taken (a read), alone or
  # for one cell of a cross-cell query.
  defp busy?(:busy), do: true
  defp busy?({_cell, :busy}), do: true
  defp busy?(_), do: false

  defp now, do: System.monotonic_time(:microsecond)
end
