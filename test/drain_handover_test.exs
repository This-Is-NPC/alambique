defmodule Alambique.DrainHandoverTest do
  @moduledoc """
  `drain/2` hands the cells to a node that is not a member (design §8): two
  instances over one store whose groups are separate, as two nodes with no
  cluster between them. The only thing they share is the lease.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Config

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @cells for n <- 1..8, do: "handover-#{n}"

  setup do
    ctx = setup_store()
    opts = [migrations: @schema, lease_ttl_ms: 20_000, skew_margin_ms: 1_000]
    Map.merge(ctx, %{a: start_instance(ctx, opts), b: start_instance(ctx, opts)})
  end

  defp insert(inst, cell, n), do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n])

  defp numbers(inst, cell) do
    {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end

  test "the other node is refused until the drain, then serves every cell at once", %{
    a: a,
    b: b
  } do
    for cell <- @cells, n <- 1..3, do: assert({:ok, _} = insert(a, cell, n))

    # Not members of each other: B cannot route to A, and sees the lease only.
    assert Config.fetch!(a).scope != Config.fetch!(b).scope

    for cell <- @cells do
      assert {:ok, %{owner: nil, owner_id: owner, released: false, expires_at_ms: expires}} =
               Alambique.lease(b, cell)

      assert owner == Config.fetch!(a).node_id
      assert {:error, {:unavailable, ^owner, ^expires}} = insert(b, cell, 100)
    end

    # The lease is 20 s: a call that waited for it would not return in 5 s.
    began = System.monotonic_time(:millisecond)
    assert {:ok, %{hibernated: 8, errors: []}} = Alambique.drain(a)

    for cell <- @cells do
      assert {:ok, %{released: true}} = Alambique.lease(b, cell)
      assert {:ok, _} = insert(b, cell, 4)
      assert numbers(b, cell) == [1, 2, 3, 4]
      assert {:ok, %{epoch: 2}} = Alambique.lease(b, cell)
    end

    assert System.monotonic_time(:millisecond) - began < 5_000

    # The drained node serves nothing new and holds nothing: a cell B holds is
    # unavailable to it, and one nobody holds is refused.
    assert {:error, {:unavailable, _, _}} = Alambique.query(a, hd(@cells), "SELECT 1")
    assert {:error, :draining} = Alambique.query(a, "handover-new", "SELECT 1")
    assert %{cells: [], count: 0} = Alambique.cells(a)

    assert %{draining: true, running: false, done: 8, errors: [], left: 0} =
             Alambique.drain_status(a)
  end

  test "every acknowledged write of the old node is in the new one", %{a: a, b: b} = ctx do
    cell = hd(@cells)
    acked = for n <- 1..40, match?({:ok, _}, insert(a, cell, n)), do: n
    assert length(acked) == 40

    assert {:ok, %{errors: []}} = Alambique.drain(a)
    assert numbers(b, cell) == acked
    assert List.flatten(durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n")) == acked
  end
end
