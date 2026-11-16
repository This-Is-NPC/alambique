defmodule Alambique.Store.Conformance do
  @moduledoc false
  # Checks that a store honours the conditional writes the leases rest on
  # (design §3, §5): `If-None-Match: *` and `If-Match`, with a failed condition
  # answered `:precondition_failed`. A store that ignores them lets two nodes
  # hold one epoch and overwrite create-only segments, silently.
  #
  # The probe writes one scratch object under the prefix and deletes it: four
  # puts and a delete per store.

  @wrong_etag "alambique-store-check-no-such-etag"

  @type result ::
          :ok
          | {:error, {:not_conformant, [:if_none_match | :if_match]}}
          | {:inconclusive, term}

  @doc """
  Runs the probe on a scratch key under `prefix`; each request gets
  `timeout_ms`. `{:inconclusive, reason}` means a request failed for another
  reason (the store is unreachable, say), so the answer is not known.
  """
  @spec check({module, term}, String.t(), pos_integer) :: result
  def check({store, config}, prefix, timeout_ms) do
    key = "#{prefix}/store-check/#{Alambique.Conditional.token()}"
    call = fn op, args -> request(fn -> apply(store, op, [config | args]) end, timeout_ms) end

    try do
      probe(&call.(:put, [key, "x", &1, %{}]))
    after
      call.(:delete, [key])
    end
  end

  defp probe(put) do
    with {:ok, etag} <- put.(:if_none_match),
         {:ok, again} <- outcome(put.(:if_none_match)),
         {:ok, stale} <- outcome(put.({:if_match, @wrong_etag})),
         # A store that ignored the second create holds its object now.
         {:ok, fresh} <- outcome(put.({:if_match, current(again, etag)})) do
      violations =
        Enum.reject(
          [
            if(again != :precondition_failed, do: :if_none_match),
            if(stale != :precondition_failed or not match?({:applied, _}, fresh),
              do: :if_match
            )
          ],
          &is_nil/1
        )

      if violations == [], do: :ok, else: {:error, {:not_conformant, violations}}
    else
      {:error, reason} -> {:inconclusive, reason}
    end
  end

  defp current({:applied, etag}, _first), do: etag
  defp current(:precondition_failed, first), do: first

  # A put either applied, or the store refused its condition; anything else
  # says nothing about the conditions.
  defp outcome({:ok, etag}), do: {:ok, {:applied, etag}}
  defp outcome({:error, :precondition_failed}), do: {:ok, :precondition_failed}
  defp outcome({:error, reason}), do: {:error, reason}

  defp request(fun, timeout_ms) do
    task = Task.async(fn -> safely(fun) end)

    case Task.yield(task, timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :timeout}
      {:exit, reason} -> {:error, {:exit, reason}}
    end
  end

  defp safely(fun) do
    fun.()
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
