defmodule Alambique.SameNodeIdTest do
  @moduledoc """
  Two instances with one `node_id` over one store (design §5). They are not a
  cluster: the lease is the only thing they share. The holder is the node id
  and the incarnation of the instance, so the second one never takes the cell
  while the first holds it.
  """
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Lease, Placement}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @id "same-id"

  setup do
    ctx = setup_store()
    opts = [migrations: @schema, node_id: @id, lease_ttl_ms: 1_500, skew_margin_ms: 300]
    a = start_instance(ctx, opts)
    b = start_instance(ctx, opts)
    Map.merge(ctx, %{a: a, b: b, opts: opts, cell: unique_cell()})
  end

  defp insert(inst, cell, n), do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n])

  defp lease(inst, cell) do
    config = Config.fetch!(inst)
    {:ok, location} = Placement.locate(config, cell)
    {:ok, lease} = Lease.read(Config.lease_ctx(config, location), cell)
    lease
  end

  defp awake?(inst, cell), do: Cell.whereis(Config.fetch!(inst), cell) != nil

  defp kill_cell(inst, cell) do
    Process.exit(Cell.whereis(Config.fetch!(inst), cell), :kill)
    eventually(fn -> not awake?(inst, cell) end)
  end

  defp numbers(inst, cell) do
    {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end

  test "the instances have one node id and two incarnations", %{a: a, b: b} do
    [ca, cb] = [Config.fetch!(a), Config.fetch!(b)]
    assert ca.node_id == cb.node_id
    assert ca.incarnation != cb.incarnation
    assert byte_size(Base.url_decode64!(ca.incarnation, padding: false)) == 16
  end

  test "an instance does not take a cell another one holds", %{a: a, b: b, cell: cell} do
    assert {:ok, _} = insert(a, cell, 1)
    assert %{epoch: 1, owner: @id, incarnation: incarnation} = lease(a, cell)
    assert incarnation == Config.fetch!(a).incarnation

    assert {:error, {:unavailable, @id, _expires}} = insert(b, cell, 2)
    assert {:error, {:unavailable, @id, _expires}} = Alambique.query(b, cell, "SELECT 1")
    refute awake?(b, cell)

    # The holder keeps serving, and nothing replaced its lease.
    assert {:ok, _} = insert(a, cell, 3)
    assert %{epoch: 1} = lease(a, cell)
  end

  test "the second instance takes the cell once the first releases it", %{
    a: a,
    b: b,
    cell: cell
  } do
    assert {:ok, _} = insert(a, cell, 1)
    assert :ok = Alambique.hibernate(a, cell)

    assert {:ok, _} = insert(b, cell, 2)
    assert %{epoch: 2, incarnation: incarnation} = lease(b, cell)
    assert incarnation == Config.fetch!(b).incarnation
    assert numbers(b, cell) == [1, 2]
  end

  test "a lease left by a dead cell of the same instance is reclaimed at once", %{
    a: a,
    cell: cell
  } do
    assert {:ok, _} = insert(a, cell, 1)
    kill_cell(a, cell)

    assert {:ok, _} = insert(a, cell, 2)
    assert %{epoch: 2} = lease(a, cell)
  end

  test "a restart with the same node id waits for the old lease to expire",
       %{
         a: a,
         cell: cell,
         opts: opts
       } = ctx do
    assert {:ok, _} = insert(a, cell, 1)
    kill_cell(a, cell)

    restarted = start_instance(ctx, opts)
    assert {:error, {:unavailable, @id, expires}} = insert(restarted, cell, 2)
    assert expires > System.os_time(:millisecond)

    eventually(fn -> match?({:ok, _}, insert(restarted, cell, 2)) end, 10_000)
    assert System.os_time(:millisecond) > expires
    assert %{epoch: 2} = lease(restarted, cell)
    assert numbers(restarted, cell) == [1, 2]
  end

  test "a lease without an incarnation is never ours", %{b: b, cell: cell} do
    config = Config.fetch!(b)
    {:ok, location} = Placement.locate(config, cell)
    ctx = Config.lease_ctx(config, location)
    {:ok, held} = Lease.acquire(%{ctx | incarnation: nil}, cell, @id)

    assert held.incarnation == nil
    refute Lease.held_by?(held, @id, config.incarnation)
    refute Lease.held_by?(held, @id, nil)
    assert {:error, {:owned, @id, _}} = Lease.acquire(ctx, cell, @id)

    # The wake of the same node id does not reclaim it either.
    assert {:error, {:unavailable, @id, _}} = insert(b, cell, 1)
    assert %{epoch: 1} = lease(b, cell)
  end

  test "concurrent writers through both instances: one holder, each ack once",
       %{
         a: a,
         b: b,
         cell: cell
       } = ctx do
    assert {:ok, _} = insert(a, cell, 0)

    writer = fn inst, from ->
      Task.async(fn -> for n <- from..(from + 59), do: {n, insert(inst, cell, n)} end)
    end

    tasks = [writer.(a, 1000), writer.(b, 2000), writer.(a, 3000), writer.(b, 4000)]
    results = tasks |> Task.await_many(30_000) |> List.flatten()

    acked = for {n, {:ok, _}} <- results, do: n
    refused = for {n, {:error, reason}} <- results, do: {n, reason}

    # Only the holder's writes are acknowledged; the other instance is refused.
    assert Enum.sort(acked) == Enum.to_list(1000..1059) ++ Enum.to_list(3000..3059)
    assert length(refused) == 120
    assert Enum.all?(refused, &match?({_, {:unavailable, @id, _}}, &1))
    assert %{epoch: 1} = lease(a, cell)

    expected = Enum.sort([0 | acked])
    assert List.flatten(durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")) == expected
  end
end
