defmodule Alambique.PlacementTest do
  @moduledoc "Placement of cells and relocation between locations (design §18)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Feed, Location, Placement}
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  defmodule Strict do
    @moduledoc false
    @behaviour Alambique.Placement

    def validate_id(_opts, "t-" <> _ = id),
      do: if(byte_size(id) < 20, do: :ok, else: {:error, :too_long})

    def validate_id(_opts, _id), do: {:error, :not_a_tenant}
    def locate(opts, _id), do: {:ok, hd(opts[:locations])}
    def locations(opts), do: {:ok, opts[:locations]}
  end

  setup do
    on_exit(&CrashPoints.reset/0)
    base = setup_store()
    stores = for name <- [:a, :b, :table], into: %{}, do: {name, start_store(name)}

    locations = [
      %Location{id: "a", store: {MemoryStore, stores.a}, prefix: "pa"},
      %Location{id: "b", store: {MemoryStore, stores.b}, prefix: "pb"}
    ]

    placement =
      {AlambiqueTestkit.Placement,
       locations: locations, table: {MemoryStore, stores.table}, initial: %{"home-a" => "a"}}

    Map.merge(base, %{stores: stores, locations: locations, placement: placement})
  end

  defp start_store(name) do
    pid = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: name))
    MemoryStore.config(pid)
  end

  defp instance(ctx, opts \\ []),
    do: start_instance(ctx, Keyword.merge([placement: ctx.placement, migrations: @schema], opts))

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp numbers(inst, cell) do
    with {:ok, %{rows: rows}} <- Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n"),
         do: List.flatten(rows)
  end

  defp keys(store) do
    {:ok, keys, nil} = MemoryStore.list(store, "", nil, 100_000)
    keys
  end

  defp cell_of(key) do
    case String.split(key, "/") do
      [_, "cells", cell | _] -> cell
      [_, "schema", _version, cell] -> cell
      [_, "feed", _shard, object] -> object |> String.split("-", parts: 3) |> List.last()
    end
  end

  defp schema_index(store) do
    {:ok, keys, nil} = MemoryStore.list(store, "", nil, 100_000)
    Enum.filter(keys, &String.contains?(&1, "/schema/"))
  end

  defp location_of(inst, cell) do
    {:ok, location} = Placement.locate(Alambique.Config.fetch!(inst), cell, fresh: true)
    location.id
  end

  describe "routing" do
    test "every object of a cell lands only in its location", ctx do
      inst = instance(ctx)
      cells = for i <- 1..12, do: "cell-#{i}"
      for cell <- cells, do: assert({:ok, _} = put(inst, cell, 1))
      for cell <- cells, do: :ok = Alambique.hibernate(inst, cell)

      ids = Map.new(cells, &{&1, location_of(inst, &1)})
      assert ids |> Map.values() |> Enum.uniq() |> Enum.sort() == ["a", "b"]

      for {name, prefix} <- [a: "pa", b: "pb"], key <- keys(ctx.stores[name]) do
        assert String.starts_with?(key, prefix <> "/")
        assert ids[cell_of(key)] == Atom.to_string(name), "#{key} is in the wrong location"
      end

      for cell <- cells do
        store = ctx.stores[String.to_atom(ids[cell])]
        assert Enum.any?(keys(store), &String.contains?(&1, "/cells/#{cell}/lease"))
      end
    end

    test "the placement decides which ids are valid", ctx do
      location = hd(ctx.locations)
      inst = instance(ctx, placement: {Strict, locations: [location]})

      assert {:ok, _} = put(inst, "t-1", 1)
      assert {:error, :not_a_tenant} = put(inst, "x-1", 1)
      assert {:error, :too_long} = put(inst, "t-" <> String.duplicate("a", 30), 1)
      # The library's own limits hold for any rule.
      assert {:error, :invalid_cell_id} = put(inst, "t-a/b", 1)
    end

    test "a placement that cannot move cells says so", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      other = %Location{id: "other", store: ctx.store, prefix: "elsewhere"}
      assert {:error, :relocation_unsupported} = Alambique.relocate(inst, cell, other)
    end
  end

  describe "relocate" do
    test "moves the data and every later write to the destination", ctx do
      inst = instance(ctx)
      cell = "home-a"
      for n <- 1..3, do: assert({:ok, _} = put(inst, cell, n))
      assert location_of(inst, cell) == "a"
      :ok = Alambique.hibernate(inst, cell)
      before = keys(ctx.stores.a)

      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, cell, "b")
      assert location_of(inst, cell) == "b"
      assert numbers(inst, cell) == [1, 2, 3]
      assert {:ok, _} = put(inst, cell, 4)
      :ok = Alambique.hibernate(inst, cell)

      assert durable_rows(
               %{ctx | store: {MemoryStore, ctx.stores.b}},
               cell,
               "SELECT n FROM t",
               "pb"
             ) ==
               [[1], [2], [3], [4]]

      # The source got the fence and the moved mark, nothing from later writes.
      added = (keys(ctx.stores.a) -- before) -- ["pa/moved/home-a"]
      assert added |> Enum.all?(&String.contains?(&1, "/0000/"))
      {:ok, lease} = Alambique.Lease.read(lease_ctx(inst, hd(ctx.locations)), cell)
      assert lease.moved_to == "b"
      assert lease.released

      # Calling again is a no-op, and it cannot go back before the cleanup.
      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, cell, "b")
      assert {:error, :destination_not_cleaned} = Alambique.relocate(inst, cell, "a")
    end

    test "carries the schema index entry to the destination", ctx do
      v2 = {2, "ALTER TABLE t ADD COLUMN w INTEGER"}
      inst = instance(ctx, migrations: @schema ++ [v2])
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      assert schema_index(ctx.stores.a) == ["pa/schema/0000000002/#{cell}"]
      assert schema_index(ctx.stores.b) == []

      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      assert schema_index(ctx.stores.a) == []
      assert schema_index(ctx.stores.b) == ["pb/schema/0000000002/#{cell}"]
    end

    test "a relocation retried after the mark leaves the source entry to the sweeper", ctx do
      v2 = {2, "ALTER TABLE t ADD COLUMN w INTEGER"}
      inst = instance(ctx, migrations: @schema ++ [v2])
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)

      CrashPoints.arm(:relocate_after_mark, :raise)
      assert_raise CrashPoints.Crash, fn -> Alambique.relocate(inst, cell, "b") end
      CrashPoints.reset()
      assert schema_index(ctx.stores.b) == ["pb/schema/0000000002/#{cell}"]
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      assert schema_index(ctx.stores.b) == ["pb/schema/0000000002/#{cell}"]
      assert schema_index(ctx.stores.a) == ["pa/schema/0000000002/#{cell}"]

      # The sweeper of the next release recognises the entry as a leftover.
      v3 = {3, "ALTER TABLE t ADD COLUMN x INTEGER"}
      next = instance(ctx, migrations: @schema ++ [v2, v3])
      assert {:ok, %{stale: 1, migrated: 1}} = Alambique.sweep(next)
      assert schema_index(ctx.stores.a) == []
      assert schema_index(ctx.stores.b) == ["pb/schema/0000000003/#{cell}"]
    end

    test "a node with an old view of the placement is sent to the new location", ctx do
      first = instance(ctx)
      second = instance(ctx, node_id: "second-node")
      cell = "home-a"
      assert {:ok, _} = put(second, cell, 1)
      :ok = Alambique.hibernate(second, cell)
      assert location_of(second, cell) == "a"

      assert {:ok, _} = Alambique.relocate(first, cell, "b")

      # `second` still has the old answer cached and learns it from the source.
      assert {:ok, %{id: "a"}} = Placement.locate(Alambique.Config.fetch!(second), cell)
      assert {:ok, _} = put(second, cell, 2)
      assert {:ok, %{id: "b"}} = Placement.locate(Alambique.Config.fetch!(second), cell)
      :ok = Alambique.hibernate(second, cell)
      assert numbers(first, cell) == [1, 2]
    end

    test "a stale placement answer gives the moved error", ctx do
      inst = instance(ctx)
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      config = Alambique.Config.fetch!(inst)
      [a, _] = ctx.locations

      # Mark the source as moved without telling the placement.
      {:ok, lease} = Alambique.Lease.acquire(lease_ctx(inst, a), cell, "relocate")
      :ok = Alambique.Lease.mark_moved(lease_ctx(inst, a), lease, "b")

      assert {:error, {:wake_failed, _}} = put(inst, cell, 2) |> normalize()
      assert {:error, {:moved, %Location{id: "b"}}} = put(inst, cell, 2)
      assert {:ok, %{id: "a"}} = Placement.locate(config, cell)
    end

    for point <- [
          :relocate_after_acquire,
          :relocate_after_install,
          :relocate_after_snapshot,
          :relocate_before_mark,
          :relocate_after_mark,
          :relocate_after_commit
        ] do
      test "a crash at #{point} leaves one writable location and can be retried", ctx do
        inst = instance(ctx)
        cell = "home-a"
        for n <- 1..2, do: assert({:ok, _} = put(inst, cell, n))
        :ok = Alambique.hibernate(inst, cell)

        CrashPoints.arm(unquote(point), :raise)
        assert_raise CrashPoints.Crash, fn -> Alambique.relocate(inst, cell, "b") end
        CrashPoints.reset()

        # Before the mark the source is only held by the dead relocation; from
        # the mark on it refuses. The destination is reachable only once the
        # placement says so.
        committed? = unquote(point) == :relocate_after_commit
        assert location_of(inst, cell) == if(committed?, do: "b", else: "a")

        assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, cell, "b")
        assert numbers(inst, cell) == [1, 2]
        assert {:ok, _} = put(inst, cell, 3)
        assert numbers(inst, cell) == [1, 2, 3]
      end
    end

    test "the marker of a move that failed before the mark goes after its own deadline",
         ctx do
      world = ManualClock.new(1_700_000_000_000)

      inst =
        instance(ctx,
          clock: {ManualClock, ManualClock.config(world, 0)},
          relocate: [timeout_ms: 2_000],
          compaction: [interval_ms: 50, moved_scan_ms: 50]
        )

      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)

      # The call asks for more time than the instance default.
      CrashPoints.arm(:relocate_before_mark, :raise)

      assert_raise CrashPoints.Crash, fn ->
        Alambique.relocate(inst, cell, "b", timeout: 10_000)
      end

      CrashPoints.reset()

      # Scans keep finding it, and keep it, while the move could still own it.
      Process.sleep(400)
      ManualClock.advance(world, 2_000 + 500 + 1)
      Process.sleep(400)
      assert "pa/moved/home-a" in keys(ctx.stores.a)

      ManualClock.advance(world, 8_000)
      eventually(fn -> "pa/moved/home-a" not in keys(ctx.stores.a) end)
    end

    test "the frozen marker moves with the cell", ctx do
      inst = instance(ctx)
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      assert :ok = Alambique.freeze(inst, cell)
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      assert {:ok, true} = Alambique.frozen?(inst, cell)
      assert {:error, :frozen} = put(inst, cell, 2)
      assert :ok = Alambique.unfreeze(inst, cell)
      assert {:ok, _} = put(inst, cell, 2)
    end
  end

  describe "moving back and cleanup" do
    alias Alambique.Lease

    @grace_ms 5_000
    @settled_ms 30_000
    @scan [moved_scan_ms: 50]

    defmodule Stale do
      @moduledoc false
      # A placement that never learns of a move: the view of a stale node.
      @behaviour Alambique.Placement
      def locate(opts, _id), do: {:ok, hd(opts[:locations])}
      def locations(opts), do: {:ok, opts[:locations]}
    end

    setup ctx do
      world = ManualClock.new(1_700_000_000_000)
      clock = {ManualClock, ManualClock.config(world, 0)}
      {:ok, Map.merge(ctx, %{world: world, clock: clock})}
    end

    defp timed(ctx, opts \\ []),
      do:
        instance(
          ctx,
          Keyword.merge([clock: ctx.clock, moved_grace_ms: @grace_ms, deletion_grace_ms: 0], opts)
        )

    defp lease_at(inst, ctx, id) do
      location = Enum.find(ctx.locations, &(&1.id == id))
      {:ok, lease} = Lease.read(lease_ctx(inst, location), "home-a")
      lease
    end

    # Segments, snapshots and the frozen marker of the cell; not the lease.
    defp objects(ctx, name) do
      Enum.filter(
        keys(ctx.stores[name]),
        &(String.contains?(&1, "/cells/home-a/") and &1 != lease_key(name))
      )
    end

    defp lease_key(name), do: "p#{name}/cells/home-a/lease"

    defp elapse(ctx, ms), do: ManualClock.advance(ctx.world, ms)

    test "a cell moves a, b, a, b with writes in between and loses nothing", ctx do
      inst = timed(ctx)
      cell = "home-a"
      epochs = []

      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      epochs = [lease_at(inst, ctx, "a").epoch | epochs]
      assert {:error, :not_moved} = Alambique.cleanup_moved(inst, cell, "b", grace_ms: 0)

      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      assert {:ok, _} = put(inst, cell, 2)
      :ok = Alambique.hibernate(inst, cell)
      epochs = [lease_at(inst, ctx, "b").epoch | epochs]

      # The source keeps its objects for the grace period, then loses them.
      assert objects(ctx, :a) != []
      assert {:error, :destination_not_cleaned} = Alambique.relocate(inst, cell, "a")
      assert {:ok, %{result: :waiting}} = Alambique.cleanup_moved(inst, cell, "a")
      assert objects(ctx, :a) != []
      elapse(ctx, @grace_ms + 1_000)
      assert {:ok, %{result: :cleaned, objects: n}} = Alambique.cleanup_moved(inst, cell, "a")
      assert n > 0
      assert objects(ctx, :a) == []
      assert %{moved_to: "b", released: true, cleaned_at_ms: at} = lease_at(inst, ctx, "a")
      assert at != nil
      assert {:ok, %{result: :cleaned, objects: 0}} = Alambique.cleanup_moved(inst, cell, "a")

      # Back only after the cleanup settled.
      assert {:error, {:destination_settling, _}} = Alambique.relocate(inst, cell, "a")
      elapse(ctx, @settled_ms)
      assert {:ok, %Location{id: "a"}} = Alambique.relocate(inst, cell, "a")
      assert location_of(inst, cell) == "a"
      assert numbers(inst, cell) == [1, 2]
      assert {:ok, _} = put(inst, cell, 3)
      :ok = Alambique.hibernate(inst, cell)
      assert %{moved_to: nil} = lease_at(inst, ctx, "a")
      epochs = [lease_at(inst, ctx, "a").epoch | epochs]

      # The live location is never cleaned.
      assert {:error, :live_location} = Alambique.cleanup_moved(inst, cell, "a", grace_ms: 0)

      elapse(ctx, @grace_ms + 1_000)
      assert {:ok, %{result: :cleaned}} = Alambique.cleanup_moved(inst, cell, "b")
      assert objects(ctx, :b) == []
      elapse(ctx, @settled_ms)
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      assert {:ok, _} = put(inst, cell, 4)
      :ok = Alambique.hibernate(inst, cell)
      epochs = [lease_at(inst, ctx, "b").epoch | epochs]

      assert numbers(inst, cell) == [1, 2, 3, 4]

      assert durable_rows(
               %{ctx | store: {MemoryStore, ctx.stores.b}},
               cell,
               "SELECT n FROM t ORDER BY n",
               "pb"
             ) == [[1], [2], [3], [4]]

      assert epochs == Enum.sort(Enum.uniq(epochs), :desc)
      assert length(epochs) == 4
    end

    test "with a deletion grace the cleanup marks, and the cell moves back with them pending",
         ctx do
      deletion_ms = 3_600_000
      inst = timed(ctx, deletion_grace_ms: deletion_ms, trash: [reaper: false])
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      assert {:ok, _} = put(inst, cell, 2)
      :ok = Alambique.hibernate(inst, cell)

      # Cleaned means marked: the objects are still there and still restorable.
      old = objects(ctx, :a)
      elapse(ctx, @grace_ms + 1_000)
      assert {:ok, %{result: :cleaned, objects: n}} = Alambique.cleanup_moved(inst, cell, "a")
      assert n == length(old)
      assert objects(ctx, :a) == old
      assert %{cleaned_at_ms: at} = lease_at(inst, ctx, "a")
      assert at != nil

      assert {:ok, %{entries: [%{cell: ^cell, due_ms: due}]}} =
               Alambique.Trash.pending(inst, location: "a")

      assert due == at + deletion_ms

      # The marked objects no longer block the move back: the incoming
      # snapshot is at TXID 2, above every object left at the location.
      elapse(ctx, @settled_ms)
      assert {:ok, %Location{id: "a"}} = Alambique.relocate(inst, cell, "a")
      assert numbers(inst, cell) == [1, 2]
      assert {:ok, _} = put(inst, cell, 3)
      :ok = Alambique.hibernate(inst, cell)

      # The reaper still removes the old objects later; the new ones stay.
      elapse(ctx, deletion_ms)
      assert {:ok, %{objects: n, errors: 0}} = Alambique.Trash.reap(inst)
      assert n == length(old)
      assert Enum.all?(old, &(&1 not in objects(ctx, :a)))
      assert {:ok, %{entries: []}} = Alambique.Trash.pending(inst, location: "a")
      assert numbers(inst, cell) == [1, 2, 3]
    end

    test "the cleanup deletes fixed keys at once and marks only data", ctx do
      inst = timed(ctx, deletion_grace_ms: 3_600_000, trash: [reaper: false])
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      :ok = Alambique.hibernate(inst, cell)

      fixed = [
        "pa/cells/home-a/frozen",
        "pa/cells/home-a/trashed",
        "pa/cells/home-a/crashes"
      ]

      for key <- fixed do
        {:ok, _} = MemoryStore.put(ctx.stores.a, key, "x", :none, %{})
      end

      elapse(ctx, @grace_ms + 1_000)
      assert {:ok, %{result: :cleaned, objects: n}} = Alambique.cleanup_moved(inst, cell, "a")
      assert Enum.all?(fixed, &(&1 not in keys(ctx.stores.a)))

      data = objects(ctx, :a)
      assert data != []
      assert n == length(data) + length(fixed)

      assert {:ok, %{entries: [%{key: manifest}]}} =
               Alambique.Trash.pending(inst, location: "a")

      {:ok, body, _etag, _meta} = MemoryStore.get(ctx.stores.a, manifest)
      assert Enum.sort(Jason.decode!(body)["keys"]) == Enum.sort(data)
    end

    test "objects left at the destination refuse the move even with a cleaned marker", ctx do
      inst = timed(ctx)
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      elapse(ctx, @grace_ms + 1_000)
      assert {:ok, %{result: :cleaned}} = Alambique.cleanup_moved(inst, cell, "a")
      elapse(ctx, @settled_ms)

      # A straggler writes a segment beyond the restored TXID into the clean
      # source. It does not block the check on the lease, but the incoming
      # snapshot would not cover it, so the move refuses.
      stray = "pa/cells/home-a/0000/0000000000000009-0000000000000009.ltx"
      {:ok, _} = MemoryStore.put(ctx.stores.a, stray, "x", :none, %{})
      assert {:error, {:destination_ahead, 9}} = Alambique.relocate(inst, cell, "a")

      # Cleaning again removes it and restarts the settling time.
      assert {:ok, %{result: :cleaned, objects: 1}} = Alambique.cleanup_moved(inst, cell, "a")
      assert {:error, {:destination_settling, _}} = Alambique.relocate(inst, cell, "a")
      elapse(ctx, @settled_ms)
      assert {:ok, %Location{id: "a"}} = Alambique.relocate(inst, cell, "a")
      assert numbers(inst, cell) == [1]
    end

    for point <- [:trash_before_delete, :trash_after_delete, :cleanup_before_mark] do
      @tag :capture_log
      test "a crash at #{point} deletes nothing at the live location and can be retried", ctx do
        inst = timed(ctx, moved_grace_ms: 0)
        cell = "home-a"
        for n <- 1..3, do: assert({:ok, _} = put(inst, cell, n))
        :ok = Alambique.hibernate(inst, cell)
        assert {:ok, _} = Alambique.relocate(inst, cell, "b")
        live = keys(ctx.stores.b)
        elapse(ctx, 1_000)

        Process.flag(:trap_exit, true)
        CrashPoints.arm(unquote(point), :raise, times: 1)

        case unquote(point) do
          :cleanup_before_mark ->
            assert_raise CrashPoints.Crash, fn -> Alambique.cleanup_moved(inst, cell, "a") end

          _ ->
            assert {:error, {:task_exit, _}} = Alambique.cleanup_moved(inst, cell, "a")
        end

        CrashPoints.reset()

        # Whatever state the source is in, the live location is untouched.
        assert live -- keys(ctx.stores.b) == []
        assert numbers(inst, cell) == [1, 2, 3]
        assert %{moved_to: "b", cleaned_at_ms: nil} = lease_at(inst, ctx, "a")

        # Moving back says what to do: clean up (again).
        assert {:error, reason} = Alambique.relocate(inst, cell, "a")
        assert reason in [:destination_not_empty, :destination_not_cleaned]

        assert {:error, :contended} != Alambique.cleanup_moved(inst, cell, "a")
        assert objects(ctx, :a) == []
        assert %{cleaned_at_ms: at} = lease_at(inst, ctx, "a")
        assert at != nil
        assert {:error, {:destination_settling, _}} = Alambique.relocate(inst, cell, "a")
        assert live -- keys(ctx.stores.b) == []
        assert numbers(inst, cell) == [1, 2, 3]
      end
    end

    test "a move left half done must be finished before another is asked for", ctx do
      inst = timed(ctx)
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)

      CrashPoints.arm(:relocate_after_mark, :raise)
      assert_raise CrashPoints.Crash, fn -> Alambique.relocate(inst, cell, "b") end
      CrashPoints.reset()

      assert location_of(inst, cell) == "a"
      assert {:error, {:move_pending, "b"}} = Alambique.relocate(inst, cell, "a")
      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, cell, "b")
      assert numbers(inst, cell) == [1]
    end

    @tag :capture_log
    test "a crash before the last relocate step leaves the cleanup to a later call", ctx do
      inst = timed(ctx, moved_grace_ms: 0)
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)

      CrashPoints.arm(:relocate_before_cleanup, :raise)
      assert_raise CrashPoints.Crash, fn -> Alambique.relocate(inst, cell, "b") end
      CrashPoints.reset()

      assert location_of(inst, cell) == "b"
      assert objects(ctx, :a) != []
      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, cell, "b")
      elapse(ctx, 1_000)
      assert {:ok, %{result: :cleaned}} = Alambique.cleanup_moved(inst, cell, "a")
      assert numbers(inst, cell) == [1]
    end

    test "the compactor finishes the cleanup after the grace period", ctx do
      inst = timed(ctx, moved_grace_ms: 200, compaction: [interval_ms: 50])
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      capture([[:alambique, :cell, :relocate], [:alambique, :cell, :cleanup]])

      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      {_, %{cleanup: :waiting}} = await_event([:alambique, :cell, :relocate], cell)
      assert objects(ctx, :a) != []

      elapse(ctx, 2_000)
      eventually(fn -> objects(ctx, :a) == [] end)

      assert {_, %{result: :ok, location: "a"}} =
               await_event([:alambique, :cell, :cleanup], cell, 5_000, &(&1.result == :ok))

      assert numbers(inst, cell) == [1]
    end

    test "a restart before the grace ends keeps the pending cleanup", ctx do
      opts = [moved_grace_ms: 1_000, compaction: [interval_ms: 50] ++ @scan]
      inst = timed(ctx, opts)
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      before = objects(ctx, :a)
      assert before != []

      stop_supervised!(inst)
      capture([[:alambique, :cell, :cleanup]])
      timed(ctx, [name: inst] ++ opts)

      # The new node found the move and waits for the grace: nothing is deleted early.
      {_, %{result: :waiting}} = await_event([:alambique, :cell, :cleanup], cell)
      assert objects(ctx, :a) == before

      # Past the grace the new node deletes without anyone asking.
      elapse(ctx, 3_000)
      eventually(fn -> objects(ctx, :a) == [] end, 10_000)
      eventually(fn -> lease_at(inst, ctx, "a").cleaned_at_ms != nil end)

      # And the cell can move back once the cleanup settled.
      elapse(ctx, @settled_ms)
      assert {:ok, %Location{id: "a"}} = Alambique.relocate(inst, cell, "a")
      assert numbers(inst, cell) == [1]
    end

    @tag :capture_log
    test "a stale node is refused at the source before, during and after the cleanup", ctx do
      inst = timed(ctx)
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      stale = timed(ctx, placement: {Stale, locations: ctx.locations}, node_id: "stale")
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")

      refused = fn ->
        assert {:error, {:moved, %Location{id: "b"}}} = put(stale, cell, 99)

        assert {:error, {:moved, "b"}} =
                 Lease.acquire(lease_ctx(inst, hd(ctx.locations)), cell, "x")
      end

      refused.()
      elapse(ctx, @grace_ms + 1_000)

      Process.flag(:trap_exit, true)
      CrashPoints.arm(:trash_after_delete, :raise, times: 1)
      assert {:error, {:task_exit, _}} = Alambique.cleanup_moved(inst, cell, "a")
      CrashPoints.reset()
      refused.()

      assert {:ok, %{result: :cleaned}} = Alambique.cleanup_moved(inst, cell, "a")
      before = keys(ctx.stores.a)
      refused.()
      assert keys(ctx.stores.a) == before
      assert numbers(inst, cell) == [1]
    end

    test "a node with an old answer does not take the free lease of a cell coming back", ctx do
      inst = timed(ctx)
      other = timed(ctx, node_id: "other")
      cell = "home-a"
      assert {:ok, _} = put(inst, cell, 1)
      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = put(other, cell, 2)
      :ok = Alambique.hibernate(other, cell)
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      elapse(ctx, @grace_ms + 1_000)
      assert {:ok, %{result: :cleaned}} = Alambique.cleanup_moved(inst, cell, "a")
      elapse(ctx, @settled_ms)

      # The move back stops with the destination installed and the placement
      # still on the old location.
      CrashPoints.arm(:relocate_after_snapshot, :raise)
      assert_raise CrashPoints.Crash, fn -> Alambique.relocate(inst, cell, "a") end
      CrashPoints.reset()
      free = lease_at(inst, ctx, "a")
      assert %{released: true, moved_to: nil, moved_from: "b"} = free

      # `other` still remembers "a" but asks the placement, which says "b",
      # where the unfinished move holds the cell.
      assert {:error, {:unavailable, "relocate", _}} = put(other, cell, 3)
      assert lease_at(inst, ctx, "a") == free

      assert {:ok, %Location{id: "a"}} = Alambique.relocate(inst, cell, "a")
      assert {:ok, _} = put(other, cell, 3)
      :ok = Alambique.hibernate(other, cell)
      assert numbers(inst, cell) == [1, 2, 3]
    end
  end

  describe "relocation and cleanup failures" do
    alias Alambique.Lease

    @moduletag :capture_log

    defp relocated(ctx, opts \\ []) do
      inst = instance(ctx, Keyword.merge([moved_grace_ms: 0, skew_margin_ms: 0], opts))
      assert {:ok, _} = put(inst, "home-a", 1)
      :ok = Alambique.hibernate(inst, "home-a")
      inst
    end

    defp fail_deletes(ctx, name, regex),
      do: MemoryStore.inject(ctx.stores[name], kind: :error, ops: [:delete], key: regex)

    test "a destination that is neither a location nor its id is refused", ctx do
      inst = relocated(ctx)
      assert {:error, :bad_location} = Alambique.relocate(inst, "home-a", :b)
      bad = %Location{id: "", store: nil, prefix: nil}
      assert {:error, :bad_location} = Alambique.relocate(inst, "home-a", bad)
    end

    test "a source whose lease cannot be read fails the move, at rest or not", ctx do
      inst = relocated(ctx)
      MemoryStore.inject(ctx.stores.a, kind: :error, ops: [:get], key: ~r{/lease$})

      assert {:error, :injected} = Alambique.relocate(inst, "home-a", "a")
      assert {:error, :injected} = Alambique.relocate(inst, "home-a", "b")
    end

    test "a cleanup that cannot finish is left for later, the move stands", ctx do
      capture([[:alambique, :cell, :relocate]])
      inst = relocated(ctx)
      {:ok, _} = MemoryStore.put(ctx.stores.a, "pa/cells/home-a/crashes", "x", :none, %{})
      fault = fail_deletes(ctx, :a, ~r{/crashes$})

      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, "home-a", "b")
      {_, %{cleanup: :error}} = await_event([:alambique, :cell, :relocate], "home-a")

      MemoryStore.remove(ctx.stores.a, fault)
      assert {:ok, %{result: :cleaned}} = Alambique.cleanup_moved(inst, "home-a", "a")
      assert numbers(inst, "home-a") == [1]
    end

    test "a move whose grace has passed is cleaned at once", ctx do
      capture([[:alambique, :cell, :relocate]])
      inst = relocated(ctx)

      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, "home-a", "b")
      {_, %{cleanup: :cleaned}} = await_event([:alambique, :cell, :relocate], "home-a")
      assert %{cleaned_at_ms: at} = lease_at(inst, ctx, "a")
      assert at != nil
    end

    test "cleanup names the location it cannot use", ctx do
      inst = relocated(ctx)
      assert {:error, :bad_location} = Alambique.cleanup_moved(inst, "home-a", :a)
      assert {:error, {:unknown_location, "zzz"}} = Alambique.cleanup_moved(inst, "home-a", "zzz")
      assert {:error, :live_location} = Alambique.cleanup_moved(inst, "home-a", "a")
    end

    test "a lease at the location that never said moved is not cleaned", ctx do
      inst = relocated(ctx)
      b = Enum.find(ctx.locations, &(&1.id == "b"))
      {:ok, _} = Lease.acquire(lease_ctx(inst, b), "home-a", "someone")

      assert {:error, :not_moved} = Alambique.cleanup_moved(inst, "home-a", "b", grace_ms: 0)
    end

    test "a lease that cannot be read fails the cleanup", ctx do
      inst = relocated(ctx)
      assert {:ok, _} = Alambique.relocate(inst, "home-a", "b")
      MemoryStore.inject(ctx.stores.a, kind: :error, ops: [:get], key: ~r{/lease$})

      assert {:error, :injected} = Alambique.cleanup_moved(inst, "home-a", "a", grace_ms: 0)
    end

    test "a fixed key that cannot be deleted fails the cleanup and marks nothing", ctx do
      inst = relocated(ctx, moved_grace_ms: 600_000)
      assert {:ok, _} = Alambique.relocate(inst, "home-a", "b")
      {:ok, _} = MemoryStore.put(ctx.stores.a, "pa/cells/home-a/crashes", "x", :none, %{})
      fault = fail_deletes(ctx, :a, ~r{/crashes$})

      assert {:error, :injected} = Alambique.cleanup_moved(inst, "home-a", "a", grace_ms: 0)
      assert %{cleaned_at_ms: nil} = lease_at(inst, ctx, "a")

      MemoryStore.remove(ctx.stores.a, fault)

      assert {:ok, %{result: :cleaned}} =
               Alambique.cleanup_moved(inst, "home-a", "a", grace_ms: 0)

      assert %{cleaned_at_ms: at} = lease_at(inst, ctx, "a")
      assert at != nil
    end

    test "an index entry that cannot be dropped fails the cleanup", ctx do
      inst = relocated(ctx, moved_grace_ms: 600_000)
      assert {:ok, _} = Alambique.relocate(inst, "home-a", "b")

      {:ok, _} =
        MemoryStore.put(ctx.stores.a, Alambique.Schema.key("pa", 1, "home-a"), "", :none, %{})

      fault = fail_deletes(ctx, :a, ~r{/schema/})

      assert {:error, :injected} = Alambique.cleanup_moved(inst, "home-a", "a", grace_ms: 0)
      assert %{cleaned_at_ms: nil} = lease_at(inst, ctx, "a")

      MemoryStore.remove(ctx.stores.a, fault)

      assert {:ok, %{result: :cleaned}} =
               Alambique.cleanup_moved(inst, "home-a", "a", grace_ms: 0)

      refute Alambique.Schema.key("pa", 1, "home-a") in keys(ctx.stores.a)
    end
  end

  describe "cross-cell queries" do
    alias Alambique.Query

    test "read each cell where the placement puts it and scan every location", ctx do
      inst = instance(ctx)
      cells = for i <- 1..8, do: "q-#{i}"
      for cell <- cells, do: assert({:ok, _} = put(inst, cell, 1))
      for cell <- cells, do: :ok = Alambique.hibernate(inst, cell)
      ids = Map.new(cells, &{&1, location_of(inst, &1)})
      assert ids |> Map.values() |> Enum.uniq() |> length() == 2

      sql = "SELECT count(*) FROM t"
      assert %{rows: rows, errors: []} = Query.all(inst, cells, sql)
      assert rows |> Enum.map(& &1.cell) |> Enum.sort() == cells

      assert %{rows: all, errors: []} = Query.all(inst, :all, sql)
      assert all |> Enum.map(& &1.cell) |> Enum.sort() == cells
      assert inst |> Query.cells() |> Enum.sort() == cells

      in_b = for {cell, "b"} <- ids, do: cell
      assert %{rows: only_b} = Query.all(inst, :all, sql, [], location: "b")
      assert only_b |> Enum.map(& &1.cell) |> Enum.sort() == Enum.sort(in_b)

      # A cell listed under the wrong location is simply empty there.
      assert %{rows: [], errors: [{_, _}]} = Query.all(inst, [hd(in_b)], sql, [], location: "a")
    end

    test "a scan reads a relocated cell once, from its current location", ctx do
      inst = instance(ctx)
      cell = "home-a"
      for n <- 1..3, do: assert({:ok, _} = put(inst, cell, n))
      :ok = Alambique.hibernate(inst, cell)
      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, cell, "b")
      assert {:ok, _} = put(inst, cell, 4)

      # The old copy (3 rows) stays in "a" until its cleanup; the cell is read once.
      result = Query.all(inst, :all, "SELECT count(*) FROM t")
      assert [%{cell: ^cell, values: [4]}] = result.rows
      assert result.errors == []

      both = Query.all(inst, :all, "SELECT count(*) FROM t", [], moved: :include)
      assert both.rows |> Enum.map(& &1.values) |> Enum.sort() == [[3], [4]]
    end
  end

  describe "mix task options" do
    alias Alambique.Ops.Options

    setup ctx do
      old = Application.get_env(:alambique, :ops)
      Application.put_env(:alambique, :ops, placement: ctx.placement)
      on_exit(fn -> Application.put_env(:alambique, :ops, old || []) end)
    end

    test "the placement locates the cell, or --location names one", ctx do
      [a, b] = ctx.locations
      assert %{prefix: "pa", store: store} = Options.ctx([], "home-a")
      assert store == a.store
      assert %{prefix: "pb"} = Options.ctx([location: "b"], "home-a")
      assert Options.ctx(location: "b").store == b.store

      assert_raise Mix.Error, ~r/several locations/, fn -> Options.ctx([]) end
      assert_raise Mix.Error, ~r/unknown_location/, fn -> Options.ctx(location: "zz") end
    end
  end

  describe "moved leases" do
    test "install is repeatable, free to take, and never overwrites a foreign lease", ctx do
      inst = instance(ctx)
      [_, b] = ctx.locations
      lease = lease_ctx(inst, b)
      alias Alambique.Lease

      assert :ok = Lease.install(lease, "c", 3, "a", "relocate")
      assert :ok = Lease.install(lease, "c", 3, "a", "relocate")
      # A later attempt with a higher epoch raises it.
      assert :ok = Lease.install(lease, "c", 5, "a", "relocate")
      assert {:ok, %{epoch: 5, released: true, moved_from: "a"}} = Lease.read(lease, "c")
      assert {:ok, %{epoch: 6, moved_from: nil}} = Lease.acquire(lease, "c", "node")

      assert {:error, :destination_not_empty} = Lease.install(lease, "c", 7, "a", "relocate")
      assert {:ok, _} = Lease.acquire(lease, "other", "node")
      assert {:error, :destination_not_empty} = Lease.install(lease, "other", 7, "a", "relocate")
    end

    test "a moved lease is never acquired again and cannot be marked by a stale holder", ctx do
      inst = instance(ctx)
      [a, _] = ctx.locations
      lease = lease_ctx(inst, a)
      alias Alambique.Lease

      {:ok, held} = Lease.acquire(lease, "c", "relocate")
      {:ok, taken} = Lease.renew(lease, held)
      assert {:error, :lost} = Lease.mark_moved(lease, held, "b")
      assert :ok = Lease.mark_moved(lease, taken, "b")
      assert {:error, {:moved, "b"}} = Lease.acquire(lease, "c", "node")
      assert {:ok, %{moved_to: "b", released: true}} = Lease.read(lease, "c")
    end
  end

  describe "change feed" do
    test "reads every location and keeps the order of a cell across a move", ctx do
      inst = instance(ctx, feed: [window_ms: 100])
      cell = "home-a"
      for n <- 1..3, do: assert({:ok, _} = put(inst, cell, n))
      assert {:ok, _} = Alambique.relocate(inst, cell, "b")
      for n <- 4..6, do: assert({:ok, _} = put(inst, cell, n))
      :ok = Alambique.hibernate(inst, cell)

      feed = Feed.ctx(locations: ctx.locations, settle_ms: 0)
      {:ok, records, _} = Feed.read(feed, nil, 100)
      firsts = records |> Enum.filter(&(&1.table == "t")) |> Enum.uniq_by(& &1.id)
      assert Enum.map(firsts, & &1.values["n"]) == [1, 2, 3, 4, 5, 6]
      positions = Enum.map(firsts, & &1.position)
      assert positions == Enum.sort(positions)

      # Both locations hold feed objects of the cell.
      for name <- [:a, :b],
          do: assert(Enum.any?(keys(ctx.stores[name]), &String.contains?(&1, "/feed/")))

      assert %{locations: [_, _]} = Feed.ctx(inst)
    end
  end

  defp lease_ctx(inst, location),
    do: Alambique.Config.lease_ctx(Alambique.Config.fetch!(inst), location)

  defp normalize({:error, {:moved, _}}), do: {:error, {:wake_failed, :moved}}
  defp normalize(other), do: other
end
