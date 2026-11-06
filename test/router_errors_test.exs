defmodule Alambique.RouterErrorsTest do
  @moduledoc "What the router answers on one node when the lease, the cell or the caller misbehave (design §8)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, Lease, Membership, Placement, Router}
  alias AlambiqueTestkit.MemoryStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    ctx = setup_store()
    inst = start_instance(ctx, migrations: @schema, compaction: false)
    config = Config.fetch!(inst)
    cell = unique_cell()
    {:ok, location} = Placement.locate(config, cell)

    Map.merge(ctx, %{
      inst: inst,
      config: config,
      cell: cell,
      lease_ctx: lease_ctx(config, location)
    })
  end

  defp lease_ctx(config, location), do: Config.lease_ctx(config, location)

  defp insert(inst, cell, n, opts \\ []),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (?)", [n], opts)

  describe "the owner of a cell nobody serves" do
    test "an unreadable lease is an error that says so", ctx do
      MemoryStore.inject(ctx.store_config, kind: :error, ops: [:get], key: ~r{/lease$})

      assert {:error, {:lease_unreadable, :injected}} = Alambique.owner(ctx.inst, ctx.cell)
    end

    test "a lease held by a node that is not a member is unavailable, not free", ctx do
      {:ok, lease} = Lease.acquire(ctx.lease_ctx, ctx.cell, "ghost")

      expected = {:error, {:unavailable, "ghost", lease.expires_at_ms}}
      assert expected == Alambique.owner(ctx.inst, ctx.cell)
      assert expected == insert(ctx.inst, ctx.cell, 1)
    end

    test "the answer carries the holder and the expiry, and the cell is served after the margin",
         ctx do
      # A short lease; the instance applies its own skew margin (500 ms).
      short = %{ctx.lease_ctx | ttl_ms: 1_200}
      {:ok, held} = Lease.acquire(short, ctx.cell, "ghost")
      expires = held.expires_at_ms
      margin = ctx.config.skew_margin_ms

      # Until `expires + margin` by this node's clock, every call is refused with
      # the same expiry; nothing is woken.
      assert {:error, {:unavailable, "ghost", ^expires}} = insert(ctx.inst, ctx.cell, 1)
      assert {:error, {:unavailable, "ghost", ^expires}} = Alambique.touch(ctx.inst, ctx.cell)
      assert Alambique.Cell.whereis(ctx.config, ctx.cell) == nil

      # `lease/3` gives the same expiry, and no node for the holder.
      assert {:ok, %{owner: nil, owner_id: "ghost", epoch: 1, expires_at_ms: ^expires} = view} =
               Alambique.lease(ctx.inst, ctx.cell)

      refute view.released

      eventually(fn -> match?({:ok, _}, insert(ctx.inst, ctx.cell, 1)) end, 5_000)
      assert System.os_time(:millisecond) > expires + margin
      assert {:ok, %{owner_id: owner_id, epoch: 2}} = Alambique.lease(ctx.inst, ctx.cell)
      assert owner_id == ctx.config.node_id
    end

    test "a lease the holder released is free at once, whoever held it", ctx do
      {:ok, held} = Lease.acquire(ctx.lease_ctx, ctx.cell, "ghost")
      assert {:error, {:unavailable, "ghost", _}} = insert(ctx.inst, ctx.cell, 1)

      assert :ok = Lease.release(ctx.lease_ctx, held)

      assert {:ok, %{released: true, expires_at_ms: expires}} =
               Alambique.lease(ctx.inst, ctx.cell)

      assert expires == held.expires_at_ms
      assert {:ok, _} = insert(ctx.inst, ctx.cell, 1)
    end

    test "a hint to a member answers without the store, a hint to a stranger is dropped", ctx do
      MemoryStore.inject(ctx.store_config, kind: :error, ops: [:get], key: ~r{/lease$})
      Membership.put_hint(ctx.config, ctx.cell, node())
      assert {:ok, node()} == Alambique.owner(ctx.inst, ctx.cell, hint: true)

      Membership.put_hint(ctx.config, ctx.cell, :stranger@nowhere)
      assert {:error, {:lease_unreadable, _}} = Alambique.owner(ctx.inst, ctx.cell, hint: true)
      assert Membership.hint(ctx.config, ctx.cell) == :error
    end
  end

  test "a call from a peer with an id that is not valid is refused, not served", ctx do
    assert {nil, {:error, :invalid_cell_id}} ==
             Router.remote(ctx.config.name, "a/b", :touch, 1_000, 0)
  end

  test "an asleep cell that never had data has position zero and is not woken", ctx do
    assert {:ok, %{txid: 0, epoch: 0, data: 0, awake: false}} ==
             Alambique.position(ctx.inst, ctx.cell)

    assert Alambique.Cell.whereis(ctx.config, ctx.cell) == nil
  end

  test "a write that may be repeated runs once on one node", ctx do
    assert {:ok, _} = insert(ctx.inst, ctx.cell, 1, idempotent: true)
    assert {:ok, %{rows: [[1]]}} = Alambique.query(ctx.inst, ctx.cell, "SELECT count(*) FROM t")
  end

  describe "a cell that is busy" do
    setup ctx do
      test = self()
      assert {:ok, _} = insert(ctx.inst, ctx.cell, 1)

      task =
        Task.async(fn ->
          Alambique.transaction(ctx.inst, ctx.cell, fn _ ->
            send(test, :holding)

            receive do
              :release -> :ok
            after
              3_000 -> :ok
            end
          end)
        end)

      assert_receive :holding

      {:ok, task: task}
    end

    test "a call that runs out of time answers a timeout and the cell stays usable", ctx do
      assert {:error, :timeout} = insert(ctx.inst, ctx.cell, 2, timeout: 50)

      send(Alambique.Cell.whereis(ctx.config, ctx.cell), :release)
      assert {:ok, :ok} = Task.await(ctx.task)
      assert {:ok, _} = insert(ctx.inst, ctx.cell, 3)
    end

    test "hibernating it locally times out too", ctx do
      assert {:error, :timeout} = Router.hibernate_local(ctx.config, ctx.cell, 50)

      send(Alambique.Cell.whereis(ctx.config, ctx.cell), :release)
      assert {:ok, :ok} = Task.await(ctx.task)
    end
  end

  test "hibernating a cell this node does not hold is done", ctx do
    assert :ok = Router.hibernate_local(ctx.config, ctx.cell, 1_000)
  end
end
