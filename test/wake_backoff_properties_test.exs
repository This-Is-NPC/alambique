defmodule Alambique.WakeBackoffPropertiesTest do
  @moduledoc """
  The backoff of a wake that failed because of the store (design §7) against a
  model, over random sequences of calls at random times: a call that finds the
  backoff active gets the remembered error and starts no wake; a wake that
  fails sets a delay that grows by `backoff_factor` with each failure in a row
  up to `backoff_max_ms`, less at most `backoff_jitter` of itself; a wake that
  succeeds, or a quiet time as long as `backoff_max_ms`, starts the count over.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Admission

  @store_reasons [:timeout, :store_unavailable, {:http, 503, "x"}, {:lease, :timeout}]

  defp config(wake) do
    %{
      admission: :"backoff_#{System.unique_integer([:positive])}",
      hint_limit: 1_000,
      wake: Map.merge(%{failure_backoff_ms: 5_000}, wake)
    }
  end

  defp wake_options do
    gen all(
          first <- integer(1..500),
          factor <- member_of([1, 2, 3, 1.5]),
          cap <- integer(1..8_000),
          jitter <- member_of([0.0, 0.2, 0.5, 0.9])
        ) do
      %{
        backoff_ms: first,
        backoff_factor: factor,
        backoff_max_ms: max(cap, first),
        backoff_jitter: jitter
      }
    end
  end

  # A call at `dt` ms after the previous one: the wake runs if no backoff is
  # active, and ends in a failure of the store or in success.
  defp calls,
    do: list_of({integer(0..6_000), member_of([:fail, :fail, :fail, :ok])}, max_length: 40)

  property "the delays follow the model and no wake starts while one is active" do
    check all(wake <- wake_options(), reason <- member_of(@store_reasons), calls <- calls()) do
      config = config(wake)
      :ok = Admission.init(config)

      try do
        # The model: when the backoff ends and how many failures are in a row.
        model = %{until: 0, failures: 0}

        Enum.reduce(calls, {1_000, model}, fn {dt, outcome}, {now, model} ->
          now = now + dt
          active? = now < model.until

          # A backoff that is active answers at once and starts no wake.
          assert Admission.failure(config, "cell", now) != nil == active?

          if active? do
            assert Admission.failure(config, "cell", now) == {:error, model.reason}
            {now, model}
          else
            wake_ended(config, wake, reason, outcome, now, model)
          end
        end)
      after
        :ets.delete(config.admission)
      end
    end
  end

  defp wake_ended(config, _wake, _reason, :ok, now, model) do
    :ok = Admission.recovered(config, "cell")
    {now, %{model | until: 0, failures: 0}}
  end

  defp wake_ended(config, wake, reason, :fail, now, model) do
    # A quiet time as long as the cap starts the count over.
    quiet? = model.failures > 0 and now > model.until + wake.backoff_max_ms
    failures = if quiet?, do: 1, else: model.failures + 1
    :ok = Admission.failed(config, "cell", reason, now)

    top = min(wake.backoff_max_ms, wake.backoff_ms * :math.pow(wake.backoff_factor, failures - 1))
    [{_, ^reason, until, ^failures}] = :ets.lookup(config.admission, {:failed, "cell"})
    delay = until - now

    assert delay >= 1
    assert delay <= top + 1
    assert delay >= top * (1 - wake.backoff_jitter) - 1

    {now, %{until: until, failures: failures, reason: reason}}
  end

  property "a failure that is not the store's is never remembered with a backoff" do
    check all(
            wake <- wake_options(),
            reason <-
              member_of([
                {:owned, "n", 1},
                :frozen,
                :draining,
                :disk_full,
                :unavailable,
                {:moved, :x},
                {:contended, "n", 1},
                {:restore, {:bad_snapshot, "k"}},
                {:lease, :lost},
                {:lease_unreadable, {:owned, "n", 1}},
                {:lease_unreadable, {:unavailable, "n", 1}}
              ])
          ) do
      config = config(wake)
      :ok = Admission.init(config)

      try do
        refute Admission.store_error?(reason)
        :ok = Admission.failed(config, "cell", reason, 1_000)
        # Only a deterministic failure leaves an entry, and it counts no failures.
        counts = :ets.match(config.admission, {{:failed, :_}, :_, :_, :"$1"})
        assert List.flatten(counts) -- [0] == []
      after
        :ets.delete(config.admission)
      end
    end
  end
end
