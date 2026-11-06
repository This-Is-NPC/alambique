defmodule Alambique.StoreIdentityTest do
  @moduledoc "The store identity names where the data lives (design §8)."
  use ExUnit.Case, async: true

  alias Alambique.{CountingStore, Config}
  alias Alambique.Placement.Single
  alias AlambiqueTestkit.MemoryStore

  @s3 %{
    endpoint: "http://Seaweed.local:18333",
    bucket: "cells",
    access_key_id: "key-a",
    secret_access_key: "secret-a"
  }

  defp identity(config, prefix \\ "app"),
    do: Config.store_identity({Single, store: {Alambique.Store.S3, config}, prefix: prefix})

  test "credentials, timeouts and path style do not matter" do
    base = identity(@s3)

    assert base ==
             identity(%{
               @s3
               | access_key_id: "key-b",
                 secret_access_key: "secret-b"
             })

    assert base == identity(Map.merge(@s3, %{receive_timeout: 1_000, path_style: false}))
    assert base == identity(%{@s3 | endpoint: "http://seaweed.local:18333/"})
  end

  test "another bucket, host, port or prefix gives another identity" do
    base = identity(@s3)
    refute base == identity(%{@s3 | bucket: "other"})
    refute base == identity(%{@s3 | endpoint: "http://other.local:18333"})
    refute base == identity(%{@s3 | endpoint: "http://seaweed.local:9000"})
    refute base == identity(@s3, "other")
  end

  test "the order of the options does not matter" do
    store = {Alambique.Store.S3, @s3}

    assert Config.store_identity({Single, store: store, prefix: "app"}) ==
             Config.store_identity({Single, prefix: "app", store: store})
  end

  test "a wrapped store has the identity of its inner store" do
    pid = start_supervised!(MemoryStore)
    inner = {MemoryStore, MemoryStore.config(pid)}
    wrapped = CountingStore.wrap(inner)

    assert Config.store_identity({Single, store: inner, prefix: "app"}) ==
             Config.store_identity({Single, store: wrapped, prefix: "app"})

    other = start_supervised!(Supervisor.child_spec(MemoryStore, id: :other))

    refute Config.store_identity({Single, store: inner, prefix: "app"}) ==
             Config.store_identity(
               {Single, store: {MemoryStore, MemoryStore.config(other)}, prefix: "app"}
             )
  end

  test "a node joins its identity group before the members group" do
    inst = Alambique.CellHelpers.start_instance(Alambique.CellHelpers.setup_store())
    config = Config.fetch!(inst)
    identity_group = {:alambique, :store, config.store_identity}

    :ok = Supervisor.terminate_child(inst, Alambique.Membership)
    {_ref, _groups} = :pg.monitor_scope(config.scope)
    {:ok, _pid} = Supervisor.restart_child(inst, Alambique.Membership)
    Alambique.CellHelpers.eventually(fn -> node() in Alambique.Membership.nodes(config) end)

    joined = joins([])
    assert identity_group in joined
    assert :members in joined

    assert Enum.find_index(joined, &(&1 == identity_group)) <
             Enum.find_index(joined, &(&1 == :members))
  end

  defp joins(acc) do
    receive do
      {_ref, :join, group, _pids} -> joins(acc ++ [group])
      {_ref, _event, _group, _pids} -> joins(acc)
    after
      0 -> acc
    end
  end
end
