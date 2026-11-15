defmodule Alambique.FreezeCheckPropertiesTest do
  @moduledoc """
  `Alambique.Freeze.check/2` (design §3, "Hard deadline"): one attempt that
  answers what the store says when the store answers within
  `check_timeout_ms`, and `{:error, :timeout}` when it does not. A store that
  answers late is not a store that failed: the limit is the configured one,
  not the 50 ms backoff of a retry.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.{Freeze, Replication, SlowStore}
  alias AlambiqueTestkit.MemoryStore

  # The two regimes are far apart, so that a loaded scheduler cannot flip the
  # outcome. A fast store answers 100 to 300 ms late, which is more than the
  # 50 ms backoff of a retry (the limit this check must not use) and far under
  # a limit of 1.5 s or more. A slow store never answers: the check can only
  # return `{:error, :timeout}` by killing its attempt while it is blocked.
  @never_ms 60_000

  setup do
    pid = start_supervised!({MemoryStore, []})
    %{store: {MemoryStore, MemoryStore.config(pid)}}
  end

  property "the check answers when the store is faster than the limit and times out when slower",
           %{store: store} do
    check all(
            frozen? <- boolean(),
            regime <-
              one_of([
                tuple({constant(:fast), integer(1_500..2_500), integer(100..300)}),
                tuple({constant(:slow), integer(100..400), constant(@never_ms)})
              ]),
            max_runs: 25
          ) do
      {kind, limit, latency} = regime
      cell = "cell-#{System.unique_integer([:positive])}"
      {module, config} = store

      if frozen?,
        do:
          {:ok, _} =
            module.put(config, Alambique.Replication.Keys.frozen("p", cell), "{}", :none, %{})

      ctx =
        Replication.ctx(
          store: SlowStore.wrap(store, latency),
          prefix: "p",
          node: "n",
          check_timeout_ms: limit
        )

      case kind do
        :fast -> assert Freeze.check(ctx, cell) == {:ok, frozen?}
        :slow -> assert Freeze.check(ctx, cell) == {:error, :timeout}
      end
    end
  end
end
