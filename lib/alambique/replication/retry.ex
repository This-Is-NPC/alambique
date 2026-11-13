defmodule Alambique.Replication.Retry do
  @moduledoc false
  # Retry with exponential backoff until an absolute deadline or
  # `ctx.max_attempts`. Every attempt runs in its own process, killed at the
  # deadline, so a store call that never returns ends the operation in time
  # (design §3). A killed attempt is reported to the circuit breaker of the
  # store, which cannot see it (`Store.Breaker`).

  alias Alambique.Store.Breaker

  @doc "Monotonic deadline `ms` from now."
  def deadline_in(ms), do: System.monotonic_time(:millisecond) + ms

  @doc """
  Calls `fun` until it returns something other than `{:retry, reason}`. A
  retry that would sleep past `deadline`, one after `ctx.max_attempts`
  attempts, and `{:retry, :store_unavailable}` (an open breaker: another
  attempt would fail the same way) end with `{:error, reason}`. An
  attempt still running at `deadline` is killed and ends with the reason of
  the last failure, or `{:error, :timeout}` if there was none. An attempt that
  starts at or past the deadline (`deadline_in(0)` asks for exactly one) gets
  `ctx.backoff_ms`, the first backoff, before it is killed. An attempt that
  raises, throws or exits does so in the caller.
  """
  @spec run(map, integer, (-> term)) :: term
  def run(ctx, deadline, fun),
    do: loop(ctx, ctx.backoff_ms, deadline, fun, 1, :timeout)

  @doc """
  One attempt of `fun`, killed after `timeout_ms` with `{:error, :timeout}`:
  for a request that must not be repeated by the caller's loop, such as the
  lease requests. `store` is the one `fun` calls, to report the kill to its
  breaker.
  """
  @spec once(term, pos_integer, (-> term)) :: term
  def once(store \\ nil, timeout_ms, fun) do
    case attempt(timeout_ms, fun) do
      :killed ->
        Breaker.timed_out(store)
        {:error, :timeout}

      result ->
        result
    end
  end

  defp loop(ctx, delay, deadline, fun, attempts, last) do
    # An attempt that starts at or past the deadline (`deadline_in(0)` asks for
    # exactly one) still gets `ctx.backoff_ms`, the first backoff.
    remaining = max(deadline - System.monotonic_time(:millisecond), ctx.backoff_ms)

    case attempt(remaining, fun) do
      # Killed at the deadline: the reason of the last failure says more.
      :killed ->
        Breaker.timed_out(ctx.store)
        {:error, last}

      {:retry, :store_unavailable} ->
        {:error, :store_unavailable}

      {:retry, reason} ->
        if attempts >= ctx.max_attempts or System.monotonic_time(:millisecond) + delay >= deadline do
          {:error, reason}
        else
          Process.sleep(delay)
          loop(ctx, min(delay * 2, ctx.max_backoff_ms), deadline, fun, attempts + 1, reason)
        end

      other ->
        other
    end
  end

  # The worker is linked, so it dies with the caller; the link is dropped
  # before the caller reads the result, so a caller that traps exits sees no
  # `:EXIT`.
  defp attempt(timeout, fun) do
    ref = make_ref()
    parent = self()

    {pid, mon} =
      :erlang.spawn_opt(
        fn ->
          send(parent, {ref, run_caught(fun)})
        end,
        [:link, :monitor]
      )

    receive do
      {^ref, result} ->
        release(pid, mon)
        unwrap(result)

      {:DOWN, ^mon, :process, ^pid, reason} ->
        # Killed from outside: the link may already have taken the caller.
        exit(reason)
    after
      timeout ->
        release(pid, mon)
        Process.exit(pid, :kill)

        receive do
          {^ref, result} -> unwrap(result)
        after
          0 -> :killed
        end
    end
  end

  defp run_caught(fun) do
    {:ok, fun.()}
  catch
    kind, reason -> {:raised, kind, reason, __STACKTRACE__}
  end

  defp unwrap({:ok, result}), do: result
  defp unwrap({:raised, kind, reason, stack}), do: :erlang.raise(kind, reason, stack)

  defp release(pid, mon) do
    Process.unlink(pid)
    Process.demonitor(mon, [:flush])

    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
    end
  end
end
