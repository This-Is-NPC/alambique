defmodule Alambique.CompactionOverlapTest do
  @moduledoc "A merge does not wait for requests one at a time that do not depend on each other (design §9)."
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.{Compaction, TracingStore}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{GatedStore, MemoryStore}

  @latency 40

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  defp traced(env) do
    {TracingStore, tracing} = store = TracingStore.wrap(env.store)
    {%{env.ctx | store: store}, tracing}
  end

  defp request(trace, op, key) do
    case Enum.filter(trace, fn {o, k, _, _} -> o == op and k == key end) do
      [one] -> one
      other -> flunk("expected one #{op} of #{key}, got #{length(other)}")
    end
  end

  test "a cold merge reads the predecessor and every input at once, then writes", env do
    history(env, new_owner(env, "a.db"), 16)
    {ctx, tracing} = traced(env)
    MemoryStore.inject(env.cfg, kind: :latency, ms: @latency)

    # No held bytes: the second merge needs the epoch of TXID 8 (a HEAD) and
    # its eight inputs (8 GETs), then writes level 1.
    assert {:ok, %{merges: 2}} =
             Compaction.run(ctx, cell(), fanout: 8, max_level: 1, hold: nil)

    trace = TracingStore.trace(tracing)
    key = &Keys.segment(ctx.prefix, cell(), &1, &2, &3)

    head = request(trace, :head, key.(0, 8, 8))
    gets = for n <- 9..16, do: request(trace, :get, key.(0, n, n))
    {:put, _, put_from, _} = request(trace, :put, key.(1, 9, 16))

    requests = [head | gets]

    for a <- requests, b <- requests, a != b do
      assert TracingStore.overlap?(a, b),
             "#{inspect(a)} and #{inspect(b)} ran one after the other"
    end

    assert Enum.all?(requests, fn {_, _, _, to} -> to <= put_from end)
  end

  describe "merges of one level" do
    # Sixteen files and a fanout of 2 make eight merges to level 1: (1, 2),
    # (3, 4) and so on. The puts of level 1 wait at a gate, so the test sees
    # how many merges are in flight without timing anything.
    @opts [fanout: 2, max_level: 1, hold: nil]

    setup env do
      history(env, new_owner(env, "a.db"), 16)
      gate = start_supervised!(GatedStore)
      GatedStore.hold(gate, ~r{/0001/})
      ctx = %{env.ctx | store: GatedStore.wrap(gate, env.store)}
      %{gate: gate, ctx: ctx, key: &Keys.segment(ctx.prefix, cell(), 1, &1, &1 + 1)}
    end

    test "run together, up to merge_concurrency", %{ctx: ctx, gate: gate, key: key} do
      pass = Task.async(fn -> Compaction.run(ctx, cell(), [merge_concurrency: 4] ++ @opts) end)

      for min <- [1, 3, 5, 7], do: GatedStore.await(gate, key.(min))
      assert [_, _, _, _] = GatedStore.held(gate)

      GatedStore.open(gate)
      assert {:ok, %{merges: 8, more: false}} = Task.await(pass)
    end

    test "are one at a time with merge_concurrency 1", %{ctx: ctx, gate: gate, key: key} do
      pass = Task.async(fn -> Compaction.run(ctx, cell(), [merge_concurrency: 1] ++ @opts) end)

      GatedStore.await(gate, key.(1))
      assert [_] = GatedStore.held(gate)
      GatedStore.release(gate, key.(1))
      GatedStore.await(gate, key.(3))
      assert [_] = GatedStore.held(gate)

      GatedStore.open(gate)
      assert {:ok, %{merges: 8}} = Task.await(pass)
    end

    test "the merges in flight count against max_merges", env do
      %{ctx: ctx, gate: gate, key: key} = env
      opts = [merge_concurrency: 4, max_merges: 3] ++ @opts
      pass = Task.async(fn -> Compaction.run(ctx, cell(), opts) end)

      for min <- [1, 3, 5], do: GatedStore.await(gate, key.(min))
      GatedStore.open(gate)

      assert {:ok, %{merges: 3, more: true}} = Task.await(pass)
      assert Enum.sort(segments(env, 1)) == [{1, 2}, {3, 4}, {5, 6}]
    end

    test "a cut keeps the merges that published and drops the ones in flight", env do
      %{ctx: ctx, gate: gate, key: key} = env
      ctx = Map.put(ctx, :budget_cut, true)
      opts = [merge_concurrency: 4, timeout_ms: 500, min_tail: 1] ++ @opts
      pass = Task.async(fn -> Compaction.run(ctx, cell(), opts) end)

      for min <- [1, 3, 5, 7], do: GatedStore.await(gate, key.(min))
      for min <- [1, 3], do: GatedStore.release(gate, key.(min))

      # The two others, and the two merges that took the free slots, never
      # get an answer: the deadline kills them.
      assert {:ok, %{merges: 2, more: true, tail: 12}} = Task.await(pass, 10_000)
      assert Enum.sort(segments(env, 1)) == [{1, 2}, {3, 4}]
      assert {:ok, result, _dump} = restore(env, :latest)
      assert result.txid == 16
    end
  end
end
