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

  # Distance from the limit at which the outcome must be certain, so that a
  # loaded scheduler cannot flip it.
  @margin_ms 80

  setup do
    pid = start_supervised!({MemoryStore, []})
    %{store: {MemoryStore, MemoryStore.config(pid)}}
  end

  property "the check answers when the store is faster than the limit and times out when slower",
           %{store: store} do
    check all(
            limit <- integer(120..250),
            latency <- integer(0..400),
            latency + @margin_ms < limit or latency > limit + @margin_ms,
            frozen? <- boolean(),
            max_runs: 25
          ) do
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

      if latency < limit,
        do: assert(Freeze.check(ctx, cell) == {:ok, frozen?}),
        else: assert(Freeze.check(ctx, cell) == {:error, :timeout})
    end
  end
end
