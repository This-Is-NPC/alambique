defmodule Alambique.CompactionOverlapTest do
  @moduledoc "A merge does not wait for requests one at a time that do not depend on each other (design §9)."
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.{Compaction, TracingStore}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.MemoryStore

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
end
