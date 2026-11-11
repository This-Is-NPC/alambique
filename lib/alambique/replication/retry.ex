defmodule Alambique.Replication.Retry do
  @moduledoc false
  # Retry with exponential backoff until an absolute deadline. Every attempt
  # runs in its own process, killed at the deadline, so a store call that never
  # returns ends the operation in time (design §3).

  # An attempt that starts at or past the deadline still gets this long.
  @late_ms 50

  @doc "Monotonic deadline `ms` from now."
  def deadline_in(ms), do: System.monotonic_time(:millisecond) + ms

  @doc """
  Calls `fun` until it returns something other than `{:retry, reason}`. A
  retry that would sleep past `deadline` ends with `{:error, reason}`, and an
  attempt still running at `deadline` is killed and ends with
  `{:error, :timeout}`. An attempt that raises, throws or exits does so in
  the caller.
  """
  @spec run(map, integer, (-> term)) :: term
  def run(ctx, deadline, fun), do: loop(ctx.backoff_ms, ctx.max_backoff_ms, deadline, fun)

  defp loop(delay, max, deadline, fun) do
    case attempt(deadline, fun) do
      {:retry, reason} ->
        if System.monotonic_time(:millisecond) + delay >= deadline do
          {:error, reason}
        else
          Process.sleep(delay)
          loop(min(delay * 2, max), max, deadline, fun)
        end

      other ->
        other
    end
  end

  # The worker is linked, so it dies with the caller; the link is dropped
  # before the caller reads the result, so a caller that traps exits sees no
  # `:EXIT`.
  defp attempt(deadline, fun) do
    ref = make_ref()
    parent = self()

    {pid, mon} =
      :erlang.spawn_opt(
        fn ->
          send(parent, {ref, run_caught(fun)})
        end,
        [:link, :monitor]
      )

    timeout = max(deadline - System.monotonic_time(:millisecond), @late_ms)

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
          0 -> {:error, :timeout}
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
