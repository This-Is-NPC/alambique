defmodule Alambique.TakeoverTest do
  @moduledoc "Early takeover and checked reads on instances that share one store (design §5)."
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, CountingStore, Lease, Placement}
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    on_exit(&CrashPoints.reset/0)
    ctx = setup_store()
    a = start_instance(ctx, migrations: @schema)
    b = start_instance(ctx, migrations: @schema)
    Map.merge(ctx, %{a: a, b: b, cell: unique_cell()})
  end

  defp rows(inst, cell, opts \\ []) do
    with {:ok, %{rows: rows}} <-
           Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n", [], opts),
         do: List.flatten(rows)
  end

  defp insert(inst, cell, n), do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n])

  defp lease(inst, cell) do
    config = Config.fetch!(inst)
    {:ok, location} = Placement.locate(config, cell)
    {:ok, lease} = Lease.read(Config.lease_ctx(config, location), cell)
    lease
  end

  defp awake?(inst, cell), do: Cell.whereis(Config.fetch!(inst), cell) != nil

  describe "take_over" do
    test "replaces a live owner at once and fences it", %{a: a, b: b, cell: cell} = ctx do
      assert {:ok, _} = insert(a, cell, 1)
      assert %{epoch: 1, owner: owner_a} = lease(a, cell)

      assert {:ok, node()} == Alambique.take_over(b, cell, :suspected)
      assert %{epoch: 2, owner: owner_b, released: false} = lease(b, cell)
      assert owner_a != owner_b
      assert awake?(b, cell)

      # The old owner's write meets the fence; nothing of it is stored.
      assert {:error, :fenced} = insert(a, cell, 2)
      eventually(fn -> not awake?(a, cell) end)

      assert {:ok, _} = insert(b, cell, 3)
      assert rows(b, cell) == [1, 3]
      assert durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n") == [[1], [3]]
    end

    test "an owner that is already local has nothing to take over", %{a: a, cell: cell} do
      assert {:ok, _} = insert(a, cell, 1)
      assert {:ok, _} = Alambique.take_over(a, cell, :noop)
      assert %{epoch: 1} = lease(a, cell)
    end

    test "takes a cell that is asleep and one that never existed", %{a: a, b: b, cell: cell} do
      assert {:ok, _} = insert(a, cell, 1)
      assert :ok = Alambique.hibernate(a, cell)
      assert {:ok, _} = Alambique.take_over(b, cell, :why_not)
      assert %{epoch: 2} = lease(b, cell)
      assert rows(b, cell) == [1]

      fresh = unique_cell()
      assert {:ok, _} = Alambique.take_over(b, fresh, :new)
      assert %{epoch: 1} = lease(b, fresh)
    end

    test "reports the reason in telemetry and keeps it out of the lease", %{
      a: a,
      b: b,
      cell: cell,
      store_config: store_config
    } do
      capture([[:alambique, :lease, :take_over]])
      assert {:ok, _} = insert(a, cell, 1)
      %{owner: owner_a} = lease(a, cell)
      assert {:ok, _} = Alambique.take_over(b, cell, {:heartbeat_missed, "a"})

      {_, meta} = await_event([:alambique, :lease, :take_over], cell)
      assert %{cause: {:heartbeat_missed, "a"}, previous_epoch: 1, epoch: 2, result: :ok} = meta
      assert %{previous_owner: ^owner_a} = meta

      {:ok, body, _, _} = MemoryStore.get(store_config, "cells-test/cells/#{cell}/lease")
      refute body =~ "heartbeat_missed"
    end

    test "expected epoch: a newer epoch means someone else took the cell",
         %{a: a, b: b, cell: cell} = ctx do
      c = start_instance(ctx, migrations: @schema)
      assert {:ok, _} = insert(a, cell, 1)
      assert {:ok, _} = Alambique.take_over(c, cell, :first, epoch: 1)

      assert {:error, {:epoch_changed, 2}} = Alambique.take_over(b, cell, :late, epoch: 1)
      refute awake?(b, cell)
      assert %{epoch: 2} = lease(b, cell)
    end

    test "a lease that changes under the takeover is contended", %{a: a, b: b, cell: cell} do
      assert {:ok, _} = insert(a, cell, 1)
      c = Config.fetch!(a)
      ctx = Config.lease_ctx(c, elem(Placement.locate(c, cell), 1))

      CrashPoints.arm(:lease_before_put, fn _, %{op: :take_over} ->
        CrashPoints.disarm(:lease_before_put)
        Lease.take_over(ctx, cell, "rival")
      end)

      assert {:error, {:contended, "rival", 2}} = Alambique.take_over(b, cell, :race)
      refute awake?(b, cell)
    end

    test "refuses frozen cells and invalid ids", %{b: b, cell: cell} do
      assert :ok = Alambique.freeze(b, cell)
      assert {:error, :frozen} = Alambique.take_over(b, cell, :x)
      assert {:error, _} = Alambique.take_over(b, "bad/id", :x)
    end

    test "a notification of a newer epoch makes the old owner check its lease at once", %{
      a: a,
      b: b,
      cell: cell
    } do
      assert {:ok, _} = insert(a, cell, 1)
      # The instances here are not members of each other's groups, so the
      # message is sent by hand, as the new owner's node does in a cluster.
      assert {:ok, _} = Alambique.take_over(b, cell, :x)
      assert awake?(a, cell)
      :ok = Cell.superseded(a, cell, 2)
      # Well before its renewal, which is a third of the lease TTL (1 s) away.
      eventually(fn -> not awake?(a, cell) end, 500)
    end

    test "a notification for the epoch the cell holds is ignored", %{a: a, cell: cell} do
      assert {:ok, _} = insert(a, cell, 1)
      :ok = Cell.superseded(a, cell, 1)
      assert {:ok, _} = insert(a, cell, 2)
      assert awake?(a, cell)
      assert :ok = Cell.superseded(:no_such_instance, cell, 9)
    end
  end

  describe "reads on the old owner" do
    setup %{a: a, b: b, cell: cell} do
      assert {:ok, _} = insert(a, cell, 1)
      assert {:ok, _} = Alambique.take_over(b, cell, :suspected)
      assert {:ok, _} = insert(b, cell, 2)
      :ok
    end

    test "a plain read may be stale until the old owner learns the new epoch", %{a: a, cell: cell} do
      assert rows(a, cell) == [1]
    end

    test "a checked read is not answered by the old owner", %{a: a, cell: cell} do
      assert {:error, {:unavailable, _, _}} =
               Alambique.query(a, cell, "SELECT 1", [], verify_lease: true)

      eventually(fn -> not awake?(a, cell) end)
    end

    test "a checked read on the new owner is answered", %{b: b, cell: cell} do
      assert rows(b, cell, verify_lease: true) == [1, 2]
    end
  end

  describe "checked reads" do
    test "answer from the owner and wait behind unacknowledged writes", %{a: a, cell: cell} do
      assert {:ok, _} = insert(a, cell, 1)
      assert rows(a, cell, verify_lease: true) == [1]

      # Hold the upload of one write; a second write and a checked read queue behind it.
      test = self()

      CrashPoints.arm(
        :repl_after_encode,
        fn _, _ ->
          send(test, {:uploading, self()})

          receive do
            :go -> :ok
          after
            5_000 -> :ok
          end
        end,
        times: 1
      )

      first = Task.async(fn -> insert(a, cell, 2) end)
      assert_receive {:uploading, cell_pid}, 5_000
      second = Task.async(fn -> insert(a, cell, 3) end)
      eventually(fn -> queued(cell_pid) >= 1 end)
      read = Task.async(fn -> rows(a, cell, verify_lease: true) end)
      eventually(fn -> queued(cell_pid) >= 2 end)
      # It has read the rows but must not answer before the batch is durable.
      assert Task.yield(read, 0) == nil

      send(cell_pid, :go)
      assert {:ok, _} = Task.await(first)
      assert {:ok, _} = Task.await(second)
      assert Task.await(read) == [1, 2, 3]
    end

    test "waits behind a committed write before the flush", %{a: a, cell: cell} do
      assert {:ok, _} = insert(a, cell, 1)
      test = self()

      for point <- [:cell_after_commit, :repl_after_encode] do
        CrashPoints.arm(
          point,
          fn _, _ ->
            send(test, {:blocked, point, self()})

            receive do
              :go -> :ok
            after
              5_000 -> :ok
            end
          end,
          times: 1
        )
      end

      write = Task.async(fn -> insert(a, cell, 2) end)
      assert_receive {:blocked, :cell_after_commit, cell_pid}, 5_000

      read = Task.async(fn -> rows(a, cell, verify_lease: true) end)
      eventually(fn -> Process.info(cell_pid, :message_queue_len) |> elem(1) >= 1 end)
      send(cell_pid, :go)

      # The write is committed locally but not uploaded. A checked read must
      # not answer until the batch is durable; the broken version answers here.
      assert_receive {:blocked, :repl_after_encode, ^cell_pid}, 5_000
      assert Task.yield(read, 0) == nil

      send(cell_pid, :go)
      assert {:ok, _} = Task.await(write)
      assert Task.await(read) == [1, 2]
    end

    test "an unreadable lease is an error and the cell keeps serving", %{
      a: a,
      cell: cell,
      store_config: store_config
    } do
      assert {:ok, _} = insert(a, cell, 1)
      MemoryStore.inject(store_config, kind: :error, ops: [:get], times: 1)

      assert {:error, {:lease_unreadable, :injected}} =
               Alambique.query(a, cell, "SELECT 1", [], verify_lease: true)

      assert rows(a, cell, verify_lease: true) == [1]
    end

    test "cost one lease read each, none for plain reads", ctx do
      counting = CountingStore.wrap(ctx.store)
      {_, counts} = counting
      inst = start_instance(%{ctx | store: counting}, migrations: @schema)
      assert {:ok, _} = insert(inst, ctx.cell, 1)
      CountingStore.reset(counts)

      assert rows(inst, ctx.cell) == [1]
      assert CountingStore.count(counts, :get) == 0

      assert rows(inst, ctx.cell, verify_lease: true) == [1]
      assert CountingStore.count(counts, :get) == 1
    end
  end
end
