defmodule Alambique.NodesTest do
  @moduledoc "The node registry in the store (design §22): the API paths and the key layout."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [capture: 1]
  import Alambique.LeaseHelpers, only: [unique_prefix: 1]

  alias Alambique.Nodes
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @ttl 10_000
  @skew 1_000

  setup do
    {:ok, pid} = start_supervised(MemoryStore, restart: :temporary)
    store = {MemoryStore, MemoryStore.config(pid)}
    world = ManualClock.new(1_000_000_000)
    {:ok, store: store, world: world, prefix: unique_prefix("nodes")}
  end

  defp ctx(%{store: store, world: world, prefix: prefix}, opts \\ []) do
    Nodes.ctx(
      Keyword.merge(
        [
          store: store,
          clock: {ManualClock, ManualClock.config(world, 0)},
          prefix: prefix,
          ttl_ms: @ttl,
          skew_margin_ms: @skew
        ],
        opts
      )
    )
  end

  defp ids(ctx) do
    {:ok, entries} = Nodes.list(ctx)
    Enum.map(entries, & &1.node_id)
  end

  test "register writes <prefix>/nodes/<node_id>, outside cells/", %{store: {mod, conf}} = t do
    ctx = ctx(t)

    assert {:ok, %Nodes{} = entry} =
             Nodes.register(ctx, "n1", "i1", "10.0.0.1:4369", meta: %{"az" => "a"})

    assert %{node_id: "n1", incarnation: "i1", advertise: "10.0.0.1:4369", meta: %{"az" => "a"}} =
             entry

    assert entry.seq == 1 and entry.released == false
    assert entry.expires_at_ms == entry.started_at_ms + @ttl

    assert {:ok, body, _etag, _} = mod.get(conf, "#{t.prefix}/nodes/n1")

    assert %{"node_id" => "n1", "incarnation" => "i1", "advertise" => "10.0.0.1:4369", "seq" => 1} =
             Jason.decode!(body)

    assert {:ok, [], nil} = mod.list(conf, "#{t.prefix}/cells/", nil, 10)
  end

  test "no store listing of the library reaches nodes/", %{prefix: prefix} do
    key = "#{prefix}/nodes/n1"

    for dir <- [
          Alambique.Replication.Keys.files_prefix(prefix, ""),
          Alambique.Replication.Keys.moved_prefix(prefix),
          Alambique.Schema.prefix(prefix),
          Alambique.Trash.prefix(prefix),
          Alambique.Feed.Keys.prefix(prefix, 0),
          "#{prefix}/cells/"
        ] do
      refute String.starts_with?(key, dir), "#{dir} would list #{key}"
    end
  end

  test "list shows live nodes sorted, and drops expired and released ones", %{world: world} = t do
    ctx = ctx(t)
    {:ok, _} = Nodes.register(ctx, "b", "i", "adv-b")
    {:ok, _} = Nodes.register(ctx, "a", "i", "adv-a")
    {:ok, c} = Nodes.register(ctx, "c", "i", "adv-c")
    assert ids(ctx) == ["a", "b", "c"]

    assert :ok = Nodes.deregister(ctx, c)
    assert ids(ctx) == ["a", "b"]

    # Expired once the clock passes expires_at + skew; not before.
    ManualClock.advance(world, @ttl + @skew)
    assert ids(ctx) == ["a", "b"]
    ManualClock.advance(world, 1)
    assert ids(ctx) == []
  end

  test "list paginates the store listing", t do
    ctx = ctx(t, page_size: 2)
    for n <- 1..5, do: {:ok, _} = Nodes.register(ctx, "n#{n}", "i", "adv")
    assert ids(ctx) == ["n1", "n2", "n3", "n4", "n5"]
  end

  test "list skips an entry that is not a node", %{store: {mod, conf}} = t do
    ctx = ctx(t)
    {:ok, _} = Nodes.register(ctx, "ok", "i", "adv")
    {:ok, _} = mod.put(conf, "#{t.prefix}/nodes/junk", "not json", :none, %{})
    assert ids(ctx) == ["ok"]
  end

  test "register takes over an expired or released entry", %{world: world} = t do
    ctx = ctx(t)
    {:ok, first} = Nodes.register(ctx, "n", "i1", "adv")

    # Alive, another incarnation of the same id: a restart takes it over ...
    assert {:ok, second} = Nodes.register(ctx, "n", "i2", "adv2")
    assert second.seq == first.seq + 1 and second.incarnation == "i2"
    # ... unless the caller only wants a free entry.
    assert {:error, {:held, "i2", _}} = Nodes.register(ctx, "n", "i3", "adv3", takeover: :free)

    :ok = Nodes.deregister(ctx, second)
    assert {:ok, third} = Nodes.register(ctx, "n", "i3", "adv3", takeover: :free)

    ManualClock.advance(world, @ttl + @skew + 1)
    assert {:ok, fourth} = Nodes.register(ctx, "n", "i4", "adv4", takeover: :free)
    assert fourth.seq == third.seq + 1
  end

  test "renew extends the expiry and bumps seq", %{world: world} = t do
    ctx = ctx(t)
    {:ok, entry} = Nodes.register(ctx, "n", "i", "adv")
    ManualClock.advance(world, 4_000)
    assert {:ok, renewed} = Nodes.renew(ctx, entry)
    assert renewed.seq == 2 and renewed.expires_at_ms == entry.expires_at_ms + 4_000
    assert {:ok, again} = Nodes.renew(ctx, renewed)
    assert again.seq == 3
  end

  test "renew and deregister are lost once someone else wrote the key", t do
    ctx = ctx(t)
    {:ok, old} = Nodes.register(ctx, "n", "i1", "adv")
    {:ok, new} = Nodes.register(ctx, "n", "i2", "adv")
    capture([[:alambique, :nodes, :lost]])

    assert {:error, :lost} = Nodes.renew(ctx, old)
    assert_received {:telemetry, [:alambique, :nodes, :lost], _, %{node_id: "n", op: :renew}}
    assert {:error, :lost} = Nodes.deregister(ctx, old)
    assert_received {:telemetry, [:alambique, :nodes, :lost], _, %{op: :deregister}}

    # The new holder is untouched.
    assert ids(ctx) == ["n"]
    assert {:ok, _} = Nodes.renew(ctx, new)
  end

  test "renew of a deleted entry is lost", %{store: {mod, conf}} = t do
    ctx = ctx(t)
    {:ok, entry} = Nodes.register(ctx, "n", "i", "adv")
    :ok = mod.delete(conf, "#{t.prefix}/nodes/n")
    assert {:error, :lost} = Nodes.renew(ctx, entry)
  end

  test "a renewal whose answer was lost is adopted, not lost", %{store: {_, conf}} = t do
    ctx = ctx(t)
    {:ok, entry} = Nodes.register(ctx, "n", "i", "adv")
    MemoryStore.inject(conf, kind: :lost_ack, ops: [:put], times: 1)
    assert {:ok, renewed} = Nodes.renew(ctx, entry)
    assert renewed.seq == 2
    assert {:ok, _} = Nodes.renew(ctx, renewed)
  end

  test "renew reports a store error without losing the entry", %{store: {_, conf}} = t do
    ctx = ctx(t)
    {:ok, entry} = Nodes.register(ctx, "n", "i", "adv")
    MemoryStore.inject(conf, kind: :error, ops: [:put, :get], times: 2)
    assert {:error, :injected} = Nodes.renew(ctx, entry)
    assert {:ok, _} = Nodes.renew(ctx, entry)
  end

  test "sweep removes expired and released entries, never a live one", %{world: world} = t do
    ctx = ctx(t)
    {:ok, _} = Nodes.register(ctx, "dead", "i", "adv")
    ManualClock.advance(world, @ttl + @skew + 1)
    {:ok, _} = Nodes.register(ctx, "live", "i", "adv")
    {:ok, released} = Nodes.register(ctx, "released", "i", "adv")
    :ok = Nodes.deregister(ctx, released)
    capture([[:alambique, :nodes, :expired]])

    assert {:ok, %{removed: 2}} = Nodes.sweep(ctx, [])

    assert_received {:telemetry, [:alambique, :nodes, :expired], _,
                     %{node_id: "dead", by: :sweep}}

    refute_received {:telemetry, [:alambique, :nodes, :expired], _, %{node_id: "released"}}
    assert ids(ctx) == ["live"]
    assert {:ok, %{removed: 0}} = Nodes.sweep(ctx, [])
  end

  test "sweep honours its limit", %{world: world} = t do
    ctx = ctx(t)
    for n <- 1..3, do: {:ok, _} = Nodes.register(ctx, "n#{n}", "i", "adv")
    ManualClock.advance(world, @ttl + @skew + 1)
    assert {:ok, %{removed: 2}} = Nodes.sweep(ctx, limit: 2)
    assert {:ok, %{removed: 1}} = Nodes.sweep(ctx, limit: 2)
  end

  test "telemetry of register and renew", %{world: world} = t do
    ctx = ctx(t)

    capture([
      [:alambique, :nodes, :registered],
      [:alambique, :nodes, :renewed],
      [:alambique, :nodes, :expired]
    ])

    {:ok, e} = Nodes.register(ctx, "n", "i1", "adv")

    assert_received {:telemetry, [:alambique, :nodes, :registered], _,
                     %{node_id: "n", takeover: :new}}

    {:ok, _} = Nodes.renew(ctx, e)
    assert_received {:telemetry, [:alambique, :nodes, :renewed], _, %{node_id: "n", seq: 2}}

    {:ok, _} = Nodes.register(ctx, "n", "i2", "adv")
    assert_received {:telemetry, [:alambique, :nodes, :registered], _, %{takeover: :restart}}

    ManualClock.advance(world, @ttl + @skew + 1)
    {:ok, _} = Nodes.register(ctx, "n", "i3", "adv")

    assert_received {:telemetry, [:alambique, :nodes, :expired], _,
                     %{node_id: "n", by: :register}}
  end

  test "ctx rejects a ttl that is not above twice the skew", t do
    assert_raise ArgumentError, fn -> ctx(t, ttl_ms: 2_000, skew_margin_ms: 1_000) end
  end
end
