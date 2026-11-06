defmodule AlambiqueTestkit.MemoryStoreFaultsTest do
  use ExUnit.Case, async: true

  alias AlambiqueTestkit.{Clock, CrashPoints, MemoryStore}

  setup do
    {:ok, pid} = start_supervised(MemoryStore)
    {:ok, c: MemoryStore.config(pid)}
  end

  test "etag changes on every put, even with identical content", %{c: c} do
    {:ok, e1} = MemoryStore.put(c, "k", "same", :none, %{})
    {:ok, e2} = MemoryStore.put(c, "k", "same", :none, %{})
    assert e1 != e2
  end

  test "error fails without applying, only for matching keys", %{c: c} do
    MemoryStore.inject(c, kind: :error, key: ~r/^bad/, reason: :boom)
    assert {:error, :boom} = MemoryStore.put(c, "bad/1", "x", :none, %{})
    assert {:error, :not_found} = MemoryStore.get(c, "bad/1") |> unfaulted(c, "bad/1")
    assert {:ok, _} = MemoryStore.put(c, "good/1", "x", :none, %{})
  end

  test "times limits how many requests are affected", %{c: c} do
    MemoryStore.inject(c, kind: :error, times: 2)
    assert {:error, :injected} = MemoryStore.get(c, "k")
    assert {:error, :injected} = MemoryStore.get(c, "k")
    assert {:error, :not_found} = MemoryStore.get(c, "k")
  end

  test "ops limits the affected operations", %{c: c} do
    MemoryStore.inject(c, kind: :error, ops: [:put])
    assert {:error, :injected} = MemoryStore.put(c, "k", "x", :none, %{})
    assert {:error, :not_found} = MemoryStore.get(c, "k")
  end

  test "timeout sleeps, returns :timeout and does not apply", %{c: c} do
    MemoryStore.inject(c, kind: :timeout, ms: 50)
    {us, result} = :timer.tc(fn -> MemoryStore.put(c, "k", "x", :none, %{}) end)
    assert result == {:error, :timeout}
    assert us >= 50_000
    MemoryStore.clear(c)
    assert {:error, :not_found} = MemoryStore.get(c, "k")
  end

  test "latency delays and then applies", %{c: c} do
    MemoryStore.inject(c, kind: :latency, ms: 50)
    {us, result} = :timer.tc(fn -> MemoryStore.put(c, "k", "x", :none, %{}) end)
    assert {:ok, _} = result
    assert us >= 50_000
    MemoryStore.clear(c)
    assert {:ok, "x", _, _} = MemoryStore.get(c, "k")
  end

  test "lost_ack applies the put and reports an error", %{c: c} do
    id = MemoryStore.inject(c, kind: :lost_ack, times: 1)
    assert {:error, :lost_ack} = MemoryStore.put(c, "k", "x", :if_none_match, %{})
    MemoryStore.remove(c, id)
    assert {:ok, "x", _, _} = MemoryStore.get(c, "k")
    assert {:error, :precondition_failed} = MemoryStore.put(c, "k", "y", :if_none_match, %{})
  end

  test "faults target the caller node", %{c: c} do
    MemoryStore.inject(c, kind: :error, node: :other@nowhere)
    assert {:ok, _} = MemoryStore.put(c, "k", "x", :none, %{})
    MemoryStore.inject(c, kind: :error, node: node())
    assert {:error, :injected} = MemoryStore.get(c, "k")
  end

  test "partition fails every request from the node until removed", %{c: c} do
    id = MemoryStore.partition(c, node())
    assert {:error, :partitioned} = MemoryStore.put(c, "k", "x", :none, %{})
    assert {:error, :partitioned} = MemoryStore.list(c, "", nil, 10)
    MemoryStore.remove(c, id)
    assert {:ok, _} = MemoryStore.put(c, "k", "x", :none, %{})
  end

  describe "crash points" do
    setup do
      on_exit(&CrashPoints.reset/0)
    end

    test "are no-ops until armed" do
      assert :ok = Alambique.Fault.point(:p, %{})
    end

    test "raise once by default" do
      CrashPoints.arm(:p)
      assert_raise CrashPoints.Crash, fn -> Alambique.Fault.point(:p) end
      assert :ok = Alambique.Fault.point(:p)
    end

    test "exit, guard and times" do
      CrashPoints.arm(:q, :exit, times: 2, when: &(&1 == :yes))
      assert :ok = Alambique.Fault.point(:q, :no)
      assert catch_exit(Alambique.Fault.point(:q, :yes)) == {:crash_point, :q}
      assert catch_exit(Alambique.Fault.point(:q, :yes)) == {:crash_point, :q}
      assert :ok = Alambique.Fault.point(:q, :yes)
    end

    test "kill terminates the calling process" do
      CrashPoints.arm(:k, :kill)
      {pid, ref} = spawn_monitor(fn -> Alambique.Fault.point(:k) end)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    end
  end

  test "clock offset shifts now_ms" do
    on_exit(fn -> Clock.set_offset(0) end)
    base = Alambique.Clock.System.now_ms(nil)
    Clock.set_offset(60_000)
    assert Clock.now_ms(nil) - base >= 60_000
  end

  defp unfaulted(_result, c, key) do
    MemoryStore.clear(c)
    MemoryStore.get(c, key)
  end
end
