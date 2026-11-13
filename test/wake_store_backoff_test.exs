defmodule Alambique.WakeStoreBackoffTest do
  @moduledoc """
  A wake that fails because the store does is remembered per cell with an
  exponential backoff, so a store that is down does not get a wake from every
  call (design §7).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.CountingStore
  alias AlambiqueTestkit.MemoryStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    env = setup_store()
    counting = CountingStore.wrap(env.store)
    {_, counts} = counting
    Map.merge(env, %{store: counting, counts: counts})
  end

  # The lease of a new cell is a PUT: a store that refuses every PUT fails the
  # wake at its first step, and each wake costs the same number of PUTs.
  defp puts(env), do: CountingStore.count(env.counts, :put)

  defp requests(env), do: puts(env) + CountingStore.count(env.counts, :get)

  defp start(env) do
    start_instance(env,
      migrations: @schema,
      compaction: false,
      wake: [backoff_ms: 400, backoff_max_ms: 400, backoff_jitter: 0.0]
    )
  end

  defp refuse_puts(env),
    do: MemoryStore.inject(env.store_config, kind: :error, reason: :timeout, ops: [:put])

  test "calls during the backoff do not start a wake, and the next one after it does", env do
    inst = start(env)
    cell = unique_cell()
    fault = refuse_puts(env)

    CountingStore.reset(env.counts)
    assert {:error, reason} = Alambique.query(inst, cell, "SELECT 1")
    one = puts(env)
    assert one > 0

    for _ <- 1..49, do: assert({:error, ^reason} = Alambique.query(inst, cell, "SELECT 1"))
    assert puts(env) == one

    Process.sleep(450)
    assert {:error, _} = Alambique.query(inst, cell, "SELECT 1")
    assert puts(env) == 2 * one

    # The store is back: the wake that follows the backoff succeeds.
    MemoryStore.remove(env.store_config, fault)
    Process.sleep(450)
    assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
  end

  test "a store that fails every request is not asked again by the calls during the backoff",
       env do
    inst = start(env)
    cell = unique_cell()

    MemoryStore.inject(env.store_config, kind: :error, reason: :timeout, ops: [:get, :put])

    CountingStore.reset(env.counts)
    assert {:error, {:lease_unreadable, _} = reason} = Alambique.query(inst, cell, "SELECT 1")
    one = requests(env)
    assert one > 0

    for _ <- 1..49, do: assert({:error, ^reason} = Alambique.query(inst, cell, "SELECT 1"))
    assert requests(env) == one

    Process.sleep(450)
    assert {:error, _} = Alambique.query(inst, cell, "SELECT 1")
    assert requests(env) == 2 * one
  end

  test "an open circuit breaker is a store error that is remembered", env do
    inst =
      start_instance(env,
        migrations: @schema,
        compaction: false,
        breaker: [failures: 1, cooldown_ms: 60_000],
        wake: [backoff_ms: 400, backoff_max_ms: 400, backoff_jitter: 0.0]
      )

    cell = unique_cell()
    MemoryStore.inject(env.store_config, kind: :error, reason: {:http, 500, ""}, ops: [:get])

    # The first failure opens the breaker; the next request is its answer.
    assert {:error, _} = Alambique.query(inst, cell, "SELECT 1")
    Process.sleep(450)
    assert {:error, :store_unavailable} = Alambique.query(inst, cell, "SELECT 1")

    config = Alambique.Config.fetch!(inst)
    assert {:error, :store_unavailable} = Alambique.Admission.store_failure(config, cell)
  end

  test "take_over wakes the cell at once, whatever the backoff remembered", env do
    inst = start(env)
    cell = unique_cell()
    fault = refuse_puts(env)
    assert {:error, _} = Alambique.query(inst, cell, "SELECT 1")
    MemoryStore.remove(env.store_config, fault)

    assert {:ok, _} = Alambique.take_over(inst, cell, :heal)
    assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
  end

  test "another cell is not affected by the backoff of one", env do
    inst = start(env)
    broken = unique_cell()
    fault = refuse_puts(env)
    assert {:error, _} = Alambique.query(inst, broken, "SELECT 1")
    MemoryStore.remove(env.store_config, fault)

    assert {:error, _} = Alambique.query(inst, broken, "SELECT 1")
    assert {:ok, _} = Alambique.query(inst, unique_cell(), "SELECT 1")
  end
end
