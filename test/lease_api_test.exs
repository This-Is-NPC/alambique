defmodule Alambique.LeaseApiTest do
  @moduledoc "The public lease/3 view: owner, epoch, expiry, released and moved."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Location, Tx}
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  defmodule Stale do
    @moduledoc false
    @behaviour Alambique.Placement

    def locate(opts, _cell), do: {:ok, hd(opts[:locations])}
    def locations(opts), do: {:ok, opts[:locations]}
  end

  setup do
    setup_store()
  end

  defp instance(ctx, opts \\ []),
    do: start_instance(ctx, Keyword.merge([migrations: @schema], opts))

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n])

  test "a cell that never had a lease", ctx do
    inst = instance(ctx)

    assert {:ok,
            %{
              owner: nil,
              owner_id: nil,
              epoch: 0,
              expires_at_ms: nil,
              released: true,
              moved_to: nil,
              moved_from: nil
            }} = Alambique.lease(inst, unique_cell())
  end

  test "an invalid id is an error", ctx do
    inst = instance(ctx)
    assert {:error, _reason} = Alambique.lease(inst, "bad/id")
  end

  test "an awake cell gives the epoch that take_over accepts", ctx do
    a = instance(ctx)
    b = instance(ctx)
    c = instance(ctx)
    cell = unique_cell()
    assert {:ok, _} = put(a, cell, 1)

    assert {:ok, %{owner: owner, owner_id: owner_id, epoch: epoch, released: false}} =
             Alambique.lease(a, cell)

    assert owner == node()
    assert is_binary(owner_id)

    assert {:ok, node} = Alambique.take_over(b, cell, :suspected, epoch: epoch)
    assert node == node()
    assert {:ok, %{epoch: newer, released: false}} = Alambique.lease(b, cell)
    assert newer == epoch + 1

    assert {:error, {:epoch_changed, ^newer}} =
             Alambique.take_over(c, cell, :late, epoch: epoch)
  end

  test "a released lease after hibernation", ctx do
    a = instance(ctx)
    cell = unique_cell()
    assert {:ok, _} = put(a, cell, 1)
    assert {:ok, %{epoch: epoch}} = Alambique.lease(a, cell)
    :ok = Alambique.hibernate(a, cell)

    assert {:ok, %{epoch: ^epoch, released: true, owner: owner, owner_id: owner_id}} =
             Alambique.lease(a, cell)

    assert owner == node()
    assert is_binary(owner_id)
  end

  test "a frozen cell still returns its lease", ctx do
    a = instance(ctx)
    cell = unique_cell()
    assert {:ok, _} = put(a, cell, 1)
    :ok = Alambique.freeze(a, cell)

    assert {:ok, %{released: true, epoch: _}} = Alambique.lease(a, cell)
  end

  test "a store failure is not wrapped", ctx do
    a = instance(ctx)
    cell = unique_cell()
    assert {:ok, _} = put(a, cell, 1)
    fault = MemoryStore.inject(ctx.store_config, kind: :raise, ops: [:get], times: 1)

    assert {:error, {:store_crashed, :error, %RuntimeError{}}} = Alambique.lease(a, cell)
    MemoryStore.remove(ctx.store_config, fault)
  end

  test "the timeout bounds the read", ctx do
    a = instance(ctx)
    cell = unique_cell()
    assert {:ok, _} = put(a, cell, 1)
    fault = MemoryStore.inject(ctx.store_config, kind: :latency, ms: 100, ops: [:get], times: 1)

    assert {:error, :timeout} = Alambique.lease(a, cell, timeout: 5)
    MemoryStore.remove(ctx.store_config, fault)
  end

  test "a moved cell reports the moved lease on both sides", ctx do
    a_store =
      start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: :lease_a))
      |> MemoryStore.config()

    b_store =
      start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: :lease_b))
      |> MemoryStore.config()

    table =
      start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: :lease_table))
      |> MemoryStore.config()

    locations = [
      %Location{id: "a", store: {MemoryStore, a_store}, prefix: "pa"},
      %Location{id: "b", store: {MemoryStore, b_store}, prefix: "pb"}
    ]

    placement =
      {AlambiqueTestkit.Placement,
       locations: locations, table: {MemoryStore, table}, initial: %{"move" => "a"}}

    inst = instance(ctx, placement: placement)

    stale =
      instance(ctx,
        placement: {Stale, locations: locations},
        name: :lease_stale,
        node_id: "stale"
      )

    assert {:ok, _} = put(inst, "move", 1)
    :ok = Alambique.hibernate(inst, "move")
    assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, "move", "b")

    assert {:ok, %{moved_to: "b", released: true, epoch: epoch}} =
             Alambique.lease(stale, "move")

    assert {:ok, %{moved_from: "a", released: true, epoch: newer}} =
             Alambique.lease(inst, "move")

    assert newer > epoch
  end

  test "a transaction runs through the public API", ctx do
    inst = instance(ctx)

    assert {:ok, :done} =
             Alambique.transaction(inst, "tx", fn tx ->
               assert {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (1)")
               :done
             end)
  end
end
