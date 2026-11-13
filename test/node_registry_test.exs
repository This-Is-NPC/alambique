defmodule Alambique.NodeRegistryTest do
  @moduledoc "The process that keeps a node in the registry (design §22), over a memory store."
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  import Alambique.CellHelpers

  alias Alambique.Nodes
  alias AlambiqueTestkit.MemoryStore

  @registry [advertise: "host:1", ttl_ms: 3_000, renew_ms: 50, refresh_ms: 50]

  setup do
    ctx = setup_store()

    reader =
      Nodes.ctx(
        store: ctx.store,
        clock: {Alambique.Clock.System, nil},
        prefix: "cells-test",
        ttl_ms: 3_000,
        skew_margin_ms: 500
      )

    capture([
      [:alambique, :registry, :peers_changed],
      [:alambique, :registry, :error],
      [:alambique, :nodes, :lost]
    ])

    {:ok, ctx: ctx, reader: reader}
  end

  defp node(ctx, name, registry \\ []) do
    start_instance(ctx,
      name: name,
      registry: Keyword.merge(@registry, [advertise: "#{name}:1"] ++ registry)
    )
  end

  defp entry(reader, id) do
    {:ok, entries} = Nodes.list(reader)
    Enum.find(entries, &(&1.node_id == id))
  end

  test "registers at start with its meta and renews", %{ctx: ctx, reader: reader} do
    node(ctx, :reg_a, meta: %{"role" => "web"})

    first = eventually(fn -> entry(reader, "reg_a") end)
    assert %{advertise: "reg_a:1", meta: %{"role" => "web"}} = first
    assert first.incarnation == Alambique.Config.fetch!(:reg_a).incarnation

    renewed = eventually(fn -> (e = entry(reader, "reg_a")) && e.seq > first.seq && e end)
    assert renewed.expires_at_ms > first.expires_at_ms
  end

  test "peers lists the other live nodes and reports joined and left", %{ctx: ctx} do
    a = node(ctx, :peer_a)
    assert Alambique.peers(a) == []
    b = node(ctx, :peer_b, meta: %{"n" => 2})

    eventually(fn -> match?([%{node_id: "peer_b"}], Alambique.peers(a)) end)
    eventually(fn -> match?([%{node_id: "peer_a"}], Alambique.peers(b)) end)

    assert [%{node_id: "peer_b", advertise: "peer_b:1", meta: %{"n" => 2}, incarnation: inc}] =
             Alambique.peers(a)

    assert inc == Alambique.Config.fetch!(b).incarnation

    assert_receive {:telemetry, [:alambique, :registry, :peers_changed], %{peers: 1},
                    %{instance: :peer_a, joined: [%{node_id: "peer_b"}], left: []}}

    stop_supervised!(b)
    eventually(fn -> Alambique.peers(a) == [] end)

    assert_receive {:telemetry, [:alambique, :registry, :peers_changed], %{peers: 0},
                    %{instance: :peer_a, joined: [], left: [%{node_id: "peer_b"}]}}
  end

  test "a lost entry is registered again once it is free", %{ctx: ctx, reader: reader} do
    node(ctx, :lost_a)
    first = eventually(fn -> entry(reader, "lost_a") end)

    {mod, conf} = ctx.store
    :ok = mod.delete(conf, "cells-test/nodes/lost_a")

    assert_receive {:telemetry, [:alambique, :nodes, :lost], _, %{node_id: "lost_a", op: :renew}},
                   5_000

    again = eventually(fn -> entry(reader, "lost_a") end)
    # The same instance: same incarnation, a new registration.
    assert again.incarnation == first.incarnation and again.token != first.token
  end

  test "an instance that lost the key to a live restart does not take it back", %{
    ctx: ctx,
    reader: reader
  } do
    a = node(ctx, :dup)
    eventually(fn -> entry(reader, "dup") end)

    {:ok, other} = Nodes.register(reader, "dup", "other-incarnation", "other:1")

    assert_receive {:telemetry, [:alambique, :nodes, :lost], _, %{node_id: "dup", op: :renew}},
                   5_000

    # Several rounds later the other incarnation still holds it.
    Process.sleep(300)
    assert %{incarnation: "other-incarnation"} = entry(reader, "dup")

    # When it leaves, the instance registers again.
    :ok = Nodes.deregister(reader, other)
    mine = Alambique.Config.fetch!(a).incarnation
    eventually(fn -> match?(%{incarnation: ^mine}, entry(reader, "dup")) end)
  end

  test "drain deregisters at once and join registers again", %{ctx: ctx, reader: reader} do
    a = node(ctx, :drain_a, renew_ms: 2_000, refresh_ms: 2_000)
    eventually(fn -> entry(reader, "drain_a") end)

    assert {:ok, _} = Alambique.drain(a)
    # Not after a round: the drain itself leaves the registry.
    assert entry(reader, "drain_a") == nil

    :ok = Alambique.join(a)
    eventually(fn -> entry(reader, "drain_a") end, 10_000)
  end

  test "stopping the instance deregisters", %{ctx: ctx, reader: reader} do
    node(ctx, :stop_a)
    eventually(fn -> entry(reader, "stop_a") end)

    stop_supervised!(:stop_a)
    assert entry(reader, "stop_a") == nil
    assert {:ok, body, _, _} = elem(ctx.store, 0).get(ctx.store_config, "cells-test/nodes/stop_a")
    assert %{"released" => true} = Jason.decode!(body)
  end

  test "a store failure does not crash the instance and the registry recovers", %{
    ctx: ctx,
    reader: reader
  } do
    a = node(ctx, :fail_a)
    eventually(fn -> entry(reader, "fail_a") end)
    pid = Process.whereis(Alambique.Config.fetch!(a).node_registry)

    fault = MemoryStore.inject(ctx.store_config, kind: :error, ops: [:put, :get, :list])

    assert_receive {:telemetry, [:alambique, :registry, :error], _,
                    %{instance: :fail_a, reason: :injected}},
                   5_000

    assert Process.alive?(pid) and Process.alive?(Process.whereis(a))
    MemoryStore.remove(ctx.store_config, fault)

    before = entry(reader, "fail_a")
    eventually(fn -> (e = entry(reader, "fail_a")) && e.seq > before.seq end)
    assert Process.whereis(Alambique.Config.fetch!(a).node_registry) == pid
  end

  test "the registry is off by default", %{ctx: ctx, reader: reader} do
    a = start_instance(ctx, name: :off_a)
    assert Alambique.peers(a) == []
    assert Process.whereis(Alambique.Config.fetch!(a).node_registry) == nil
    assert {:ok, []} = Nodes.list(reader)
  end
end
