defmodule Alambique.Replication.Retry do
  @moduledoc false
  # Retry with exponential backoff until an absolute deadline.

  @doc "Monotonic deadline `ms` from now."
  def deadline_in(ms), do: System.monotonic_time(:millisecond) + ms

  @doc """
  Calls `fun` until it returns something other than `{:retry, reason}`. A
  retry that would sleep past `deadline` ends with `{:error, reason}`.
  """
  @spec run(map, integer, (-> term)) :: term
  def run(ctx, deadline, fun), do: loop(ctx.backoff_ms, ctx.max_backoff_ms, deadline, fun)

  defp loop(delay, max, deadline, fun) do
    case fun.() do
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
end
