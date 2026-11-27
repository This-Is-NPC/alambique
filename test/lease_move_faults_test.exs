defmodule Alambique.LeaseMoveFaultsTest do
  use ExUnit.Case, async: false

  import Alambique.LeaseHelpers

  alias Alambique.Lease
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @start 1_000_000_000

  # A store that calls `config.hook.(op)` before every request, so that a test
  # can change the stored lease between two requests of one operation.
  defmodule HookStore do
    alias AlambiqueTestkit.MemoryStore

    def put(c, key, body, condition, meta) do
      c.hook.(:put)
      MemoryStore.put(c.inner, key, body, condition, meta)
    end

    def get(c, key) do
      c.hook.(:get)
      MemoryStore.get(c.inner, key)
    end

    def head(c, key), do: MemoryStore.head(c.inner, key)
    def get_range(c, key, off, len), do: MemoryStore.get_range(c.inner, key, off, len)

    def get_range(c, key, off, len, etag),
      do: MemoryStore.get_range(c.inner, key, off, len, etag)

    def list(c, prefix, after_key, limit), do: MemoryStore.list(c.inner, prefix, after_key, limit)
    def delete(c, key), do: MemoryStore.delete(c.inner, key)
  end

  setup do
    on_exit(&CrashPoints.reset/0)
    {:ok, pid} = start_supervised(MemoryStore)
    config = MemoryStore.config(pid)
    world = ManualClock.new(@start)
    prefix = unique_prefix("move-faults")
    ctx = node_ctx({MemoryStore, config}, world, prefix)
    {:ok, store: config, world: world, prefix: prefix, ctx: ctx}
  end

  defp lease_key(prefix, cell), do: "#{prefix}/cells/#{cell}/lease"

  describe "mark_moved" do
    setup %{ctx: ctx} do
      {:ok, lease} = Lease.acquire(ctx, "c", "relocate")
      {:ok, lease: lease}
    end

    test "a lost acknowledgement is confirmed by re-reading", %{
      store: store,
      ctx: ctx,
      lease: lease
    } do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)

      assert :ok = Lease.mark_moved(ctx, lease, "b")
      assert {:ok, %Lease{moved_to: "b", released: true}} = Lease.read(ctx, "c")
    end

    test "a lease that vanished meanwhile is lost", %{
      store: store,
      prefix: prefix,
      ctx: ctx,
      lease: lease
    } do
      MemoryStore.inject(store, kind: :error, ops: [:put], times: 1)

      CrashPoints.arm(:lease_before_put, fn _, _ ->
        :ok = MemoryStore.delete(store, lease_key(prefix, "c"))
      end)

      assert {:error, :lost} = Lease.mark_moved(ctx, lease, "b")
    end

    test "an unreadable lease after a failed put is the put's error", %{
      store: store,
      ctx: ctx,
      lease: lease
    } do
      MemoryStore.inject(store, kind: :lost_ack, ops: [:put], times: 1)

      CrashPoints.arm(:lease_before_put, fn _, _ ->
        MemoryStore.inject(store, kind: :error, ops: [:get], times: 1)
      end)

      assert {:error, :lost_ack} = Lease.mark_moved(ctx, lease, "b")
    end
  end

  describe "install over a lease that exists" do
    setup %{store: store, world: world, prefix: prefix} do
      {:ok, counter} = Agent.start_link(fn -> %{puts: 0, on_put: nil} end)

      hook = fn
        :put ->
          n = Agent.get_and_update(counter, fn s -> {s, %{s | puts: s.puts + 1}} end)
          if n.on_put, do: n.on_put.(n.puts)

        :get ->
          :ok
      end

      hooked = {HookStore, %{inner: store, hook: hook}}
      ctx = node_ctx(hooked, world, prefix)
      {:ok, hooked_ctx: ctx, counter: counter}
    end

    test "a put that loses the race reports the precondition failure", %{
      hooked_ctx: ctx,
      store: store,
      prefix: prefix,
      counter: counter
    } do
      assert :ok = Lease.install(ctx, "c", 3, "a", "relocate")
      {:ok, before} = Lease.read(ctx, "c")

      # Of the next install, the first put finds the key and the second is the
      # replacement: a rival rewrites the object right before it.
      Agent.update(counter, fn s ->
        %{
          s
          | puts: 0,
            on_put: fn
              1 ->
                {:ok, body, _etag, _meta} = MemoryStore.get(store, lease_key(prefix, "c"))
                {:ok, _} = MemoryStore.put(store, lease_key(prefix, "c"), body, :none, %{})

              _ ->
                :ok
            end
        }
      end)

      assert {:error, :precondition_failed} = Lease.install(ctx, "c", 5, "a", "relocate")
      assert {:ok, %Lease{epoch: 3, seq: seq}} = Lease.read(ctx, "c")
      assert seq == before.seq
    end
  end

  describe "install over a cleaned moved lease" do
    setup %{ctx: ctx} do
      {:ok, lease} = Lease.acquire(ctx, "c", "relocate")
      :ok = Lease.mark_moved(ctx, lease, "b")
      {:ok, moved} = Lease.read(ctx, "c")
      {:ok, claimed} = Lease.claim_cleanup(ctx, moved)
      :ok = Lease.mark_cleaned(ctx, claimed)
      {:ok, stored} = Lease.read(ctx, "c")
      {:ok, stored: stored}
    end

    test "takes the epoch above the stored one and the next seq", %{ctx: ctx, stored: stored} do
      assert :ok = Lease.install(ctx, "c", 1, "x", "relocate")

      assert {:ok, %Lease{epoch: epoch, seq: seq, released: true, moved_to: nil, moved_from: "x"}} =
               Lease.read(ctx, "c")

      assert epoch == stored.epoch + 1
      assert seq == stored.seq + 1
    end

    test "keeps a higher requested epoch", %{ctx: ctx, stored: stored} do
      assert :ok = Lease.install(ctx, "c", 10, "x", "relocate")
      assert {:ok, %Lease{epoch: 10, seq: seq}} = Lease.read(ctx, "c")
      assert seq == stored.seq + 1
    end
  end

  # Every required field of a lease, with a value of the right type and one of
  # the wrong type.
  @fields [
    {"cell", "c", 1},
    {"epoch", 1, "1"},
    {"owner", "a", 1},
    {"token", "t", 1},
    {"seq", 1, "1"},
    {"expires_at_ms", 1, "1"},
    {"released", true, "true"}
  ]
  @valid Map.new(@fields, fn {key, good, _bad} -> {key, good} end)

  describe "a lease object that does not decode" do
    test "decodes with every field", %{store: store, prefix: prefix, ctx: ctx} do
      {:ok, _} = MemoryStore.put(store, lease_key(prefix, "c"), Jason.encode!(@valid), :none, %{})

      assert {:ok, %Lease{cell: "c", epoch: 1, seq: 1}} = Lease.read(ctx, "c")
    end

    for {name, body} <-
          [{"json that is not a map", "[1, 2]"}] ++
            for(
              {key, _good, bad} <- @fields,
              do: {"a #{key} of the wrong type", Jason.encode!(Map.put(@valid, key, bad))}
            ) ++
            for(
              {key, _good, _bad} <- @fields,
              do: {"a missing #{key}", Jason.encode!(Map.delete(@valid, key))}
            ) do
      test "#{name} is corrupt", %{store: store, prefix: prefix, ctx: ctx} do
        {:ok, _} = MemoryStore.put(store, lease_key(prefix, "c"), unquote(body), :none, %{})

        assert {:error, :corrupt_lease} = Lease.read(ctx, "c")
        assert {:error, :corrupt_lease} = Lease.acquire(ctx, "c", "a")
      end
    end
  end
end
