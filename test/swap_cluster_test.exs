defmodule Alambique.SwapClusterTest do
  @moduledoc """
  Swap one node for another through the store leases alone (design §8).

  Node A serves cells under load. Node B starts beside it as a separate
  instance over the same store, with no cluster between them. A small router
  sends every request to B; B refuses a cell A still holds with the lease
  error, and the client retries after a short Retry-After, as Atanor does with
  a 503. Then A drains and stops. Every acknowledged write must be present
  once, and the only errors seen are lease errors (`:unavailable`) and
  `:draining`. The swap runs twice: with two node ids and with one.

  The measurements are printed: the time from B's start to A's end, the drain,
  the number of refusals and the latency peak a client saw.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Config
  alias AlambiqueTestkit.MemoryStore

  @moduletag :cluster
  @moduletag timeout: 120_000
  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER NOT NULL)"}]
  @cells for n <- 1..40, do: "swap-#{n}"
  @clients 8
  @ttl_ms 5_000
  @skew_ms 500
  # What a 503 tells the client: wait this long at most before the retry.
  @retry_after_ms 250
  @give_up_ms 30_000
  @latency_ms 2

  setup do
    ctx = setup_store()
    MemoryStore.inject(ctx.store_config, kind: :latency, ms: @latency_ms)
    ctx
  end

  for {label, ids} <- [
        {"two node ids", {"swap-a", "swap-b"}},
        {"one node id", {"swap", "swap"}}
      ] do
    test "a swap under load loses and repeats nothing with #{label}", ctx do
      swap(ctx, unquote(Macro.escape(ids)))
    end
  end

  defp swap(ctx, {id_a, id_b}) do
    opts = [migrations: @schema, lease_ttl_ms: @ttl_ms, skew_margin_ms: @skew_ms]
    a = start_instance(ctx, [node_id: id_a] ++ opts)

    for cell <- @cells, n <- 1..3, do: {:ok, _} = write(a, cell, -n)

    env = %{
      a: a,
      b: nil,
      acked: :ets.new(:acked, [:public, :set]),
      writes: :ets.new(:writes, [:public, :bag]),
      stop: :atomics.new(1, []),
      b_up: :atomics.new(1, [])
    }

    for cell <- @cells, n <- 1..3, do: :ets.insert(env.writes, {cell, -n})
    for cell <- @cells, do: :ets.insert(env.acked, {cell, 3})

    # B is a separate instance with no link to A: its own groups, the same store.
    b = start_instance(ctx, [node_id: id_b, join: false] ++ opts)
    env = %{env | b: b}
    assert Config.fetch!(a).scope != Config.fetch!(b).scope

    clients = for id <- 1..@clients, do: Task.async(fn -> client(env, id) end)
    Process.sleep(1_000)

    # B is ready: the router sends every request to it from now on.
    :ok = Alambique.join(b)
    swap_started = System.monotonic_time(:millisecond)
    :atomics.put(env.b_up, 1, 1)
    Process.sleep(500)

    drain_started = System.monotonic_time(:millisecond)
    assert {:ok, %{errors: [], hibernated: hibernated}} = Alambique.drain(a)
    drained = System.monotonic_time(:millisecond)
    stop_supervised!(a)
    swap_ended = System.monotonic_time(:millisecond)
    assert hibernated > 0

    # Load continues on B until every cell has been served by it.
    Process.sleep(1_000)
    :atomics.put(env.stop, 1, 1)
    results = clients |> Task.await_many(60_000) |> List.flatten()

    report(label(id_a, id_b), results, %{
      swap_ms: swap_ended - swap_started,
      drain_ms: drained - drain_started,
      hibernated: hibernated
    })

    verify(ctx, env, results)
  end

  defp label(id, id), do: "one node id"
  defp label(_, _), do: "two node ids"

  # Clients

  defp client(env, id) do
    :rand.seed(:exsss, {id, 7, 11})
    loop(env, id, 1, [])
  end

  defp loop(env, id, seq, acc) do
    if :atomics.get(env.stop, 1) == 1 do
      acc
    else
      cell = Enum.random(@cells)
      op = if rem(seq, 3) == 0, do: :read, else: {:write, id * 1_000_000 + seq}
      started = System.monotonic_time(:millisecond)
      {outcome, refusals} = request(env, cell, op, 0, started)
      latency = System.monotonic_time(:millisecond) - started

      if match?({:acked, _}, outcome) do
        {:acked, n} = outcome
        :ets.insert(env.writes, {cell, n})
        :ets.update_counter(env.acked, cell, 1)
      end

      loop(env, id, seq + 1, [{cell, outcome, latency, refusals} | acc])
    end
  end

  # The router: B once it is up, A before. A refusal is a 503 with a
  # Retry-After; the client waits and sends the request again.
  defp request(env, cell, op, refusals, started) do
    inst = if :atomics.get(env.b_up, 1) == 1, do: env.b, else: env.a
    before = :ets.lookup_element(env.acked, cell, 2)

    case run(inst, cell, op, before) do
      {:error, _} = refused ->
        if System.monotonic_time(:millisecond) - started > @give_up_ms do
          {{:other, refused}, refusals}
        else
          Process.sleep(retry_after(env.b, cell))
          request(env, cell, op, refusals + 1, started)
        end

      outcome ->
        {outcome, refusals}
    end
  end

  defp run(inst, cell, {:write, n}, _before) do
    case write(inst, cell, n) do
      {:ok, _} -> {:acked, n}
      {:error, {:unavailable, _, _}} = refused -> refused
      {:error, :draining} = refused -> refused
      other -> {:other, other}
    end
  end

  defp run(inst, cell, :read, before) do
    case Alambique.query(inst, cell, "SELECT count(*) FROM t") do
      {:ok, %{rows: [[count]]}} when count >= before -> :read
      {:ok, %{rows: [[count]]}} -> {:other, {:read_missed_writes, cell, before, count}}
      {:error, {:unavailable, _, _}} = refused -> refused
      {:error, :draining} = refused -> refused
      other -> {:other, other}
    end
  end

  defp write(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (?)", [n])

  # `Alambique.lease/3` is one store read: the Retry-After comes from the
  # expiry, capped, and is short when the lease was released.
  defp retry_after(b, cell) do
    case Alambique.lease(b, cell) do
      {:ok, %{released: true}} ->
        10

      {:ok, %{expires_at_ms: expires}} when is_integer(expires) ->
        (expires + @skew_ms - System.os_time(:millisecond)) |> max(10) |> min(@retry_after_ms)

      _ ->
        @retry_after_ms
    end
  end

  # Checks and measurements

  defp verify(ctx, env, results) do
    others = for {_, {:other, error}, _, _} <- results, do: error
    assert others == [], "unexpected errors: #{inspect(Enum.take(others, 5))}"

    # Every cell holds exactly what was acknowledged: nothing lost, nothing twice.
    {:ok, _} = Alambique.drain(env.b)

    for cell <- @cells do
      expected = for {^cell, n} <- :ets.tab2list(env.writes), do: n
      rows = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY id") |> List.flatten()

      assert Enum.sort(rows) == Enum.sort(expected),
             "#{cell}: lost #{inspect(expected -- rows)}, repeated #{inspect(rows -- expected)}"

      assert rows == Enum.uniq(rows)
    end
  end

  defp report(label, results, swap) do
    latencies = for {_, _, ms, _} <- results, do: ms
    refused = for {_, _, _, n} <- results, n > 0, do: n
    sorted = Enum.sort(latencies)
    count = length(sorted)
    peak = List.last(sorted)

    IO.puts("""

    swap with #{label} (lease #{@ttl_ms} ms, skew #{@skew_ms} ms, Retry-After <= #{@retry_after_ms} ms, store latency #{@latency_ms} ms)
      swap (B ready to A stopped): #{swap.swap_ms} ms; drain of #{swap.hibernated} cells: #{swap.drain_ms} ms
      requests: #{count}, refused at least once: #{length(refused)}, refusals: #{Enum.sum(refused)}
      latency ms: median #{Enum.at(sorted, div(count, 2))}, p99 #{Enum.at(sorted, trunc(count * 0.99))}, peak #{peak}
    """)

    # A client that waited for the lease to expire would see about @ttl_ms.
    assert peak < @ttl_ms
    assert length(refused) > 0
  end
end
