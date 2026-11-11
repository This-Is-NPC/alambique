defmodule Alambique.DrainTest do
  @moduledoc "Graceful leave of a node: drain/2 and the drain at instance shutdown (design §8)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, Lease, Membership, Placement}
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    ctx = setup_store()

    %{
      ctx: ctx,
      a: start_instance(ctx, migrations: @schema),
      b: start_instance(ctx, migrations: @schema)
    }
  end

  defp insert(inst, cell, n), do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n])

  defp lease(inst, cell) do
    config = Config.fetch!(inst)
    {:ok, location} = Placement.locate(config, cell)
    {:ok, lease} = Lease.read(Config.lease_ctx(config, location), cell)
    lease
  end

  test "hibernates every cell and releases its lease", %{a: a, b: b} = %{ctx: ctx} do
    cells = for n <- 1..5, do: "drain-#{n}"
    for cell <- cells, do: assert({:ok, _} = insert(a, cell, 1))

    assert {:ok, %{hibernated: 5, errors: []}} = Alambique.drain(a, concurrency: 2)
    assert %{cells: [], count: 0} = Alambique.cells(a)

    for cell <- cells do
      assert %{released: true, epoch: 1} = lease(a, cell)
      assert durable_rows(ctx, cell, "SELECT n FROM t") == [[1]]
    end

    # Another node serves the cells at once, without waiting for the leases to expire.
    started = System.monotonic_time(:millisecond)
    assert {:ok, %{rows: [[1]]}} = Alambique.query(b, hd(cells), "SELECT n FROM t")
    assert System.monotonic_time(:millisecond) - started < 1_000
    assert %{epoch: 2} = lease(b, hd(cells))
  end

  @tag capture_log: true
  test "a lease that cannot be released is an error, not a hibernation", %{
    a: a,
    b: b,
    ctx: ctx
  } do
    cells = for n <- 1..3, do: "stuck-#{n}"
    for cell <- cells, do: assert({:ok, _} = insert(a, cell, 1))

    # Every release of the first cell fails; the writes and the others pass.
    fault =
      MemoryStore.inject(ctx.store_config, kind: :error, ops: [:put], key: ~r"/stuck-1/lease$")

    assert {:ok, %{hibernated: 2, errors: [{"stuck-1", {:release_failed, :injected}}]}} =
             Alambique.drain(a)

    # The cell is asleep and its data durable, but the lease is still held.
    assert %{cells: []} = Alambique.cells(a)
    assert durable_rows(ctx, "stuck-1", "SELECT n FROM t") == [[1]]
    assert %{released: false, epoch: 1} = lease(a, "stuck-1")
    assert %{released: true} = lease(a, "stuck-2")

    # Another node waits for the expiry, as after a crash.
    assert {:error, {:unavailable, _owner, _expires}} = insert(b, "stuck-1", 2)
    MemoryStore.remove(ctx.store_config, fault)
  end

  test "drain emits start and stop events with the counts", %{a: a} do
    capture([[:alambique, :drain, :start], [:alambique, :drain, :stop]])
    for n <- 1..4, do: assert({:ok, _} = insert(a, "event-#{n}", 1))

    assert {:ok, %{hibernated: 4, errors: []}} = Alambique.drain(a)

    node_id = Config.fetch!(a).node_id

    assert_receive {:telemetry, [:alambique, :drain, :start], %{cells: 4},
                    %{instance: ^a, node_id: ^node_id}}

    assert_receive {:telemetry, [:alambique, :drain, :stop],
                    %{cells: 4, hibernated: 4, released: 4, errors: 0, duration: duration},
                    %{instance: ^a}}

    assert duration > 0
  end

  @tag capture_log: true
  test "a release that fails counts as hibernated but not released", %{a: a, ctx: ctx} do
    capture([[:alambique, :drain, :stop]])
    for n <- 1..2, do: assert({:ok, _} = insert(a, "half-#{n}", 1))
    MemoryStore.inject(ctx.store_config, kind: :error, ops: [:put], key: ~r"/half-1/lease$")

    assert {:ok, %{hibernated: 1, errors: [{"half-1", {:release_failed, _}}]}} =
             Alambique.drain(a)

    assert_receive {:telemetry, [:alambique, :drain, :stop],
                    %{cells: 2, hibernated: 2, released: 1, errors: 1}, _}
  end

  test "drain_status follows a drain from start to end", %{a: a, ctx: ctx} do
    assert %{draining: false, running: false, started_at_ms: nil, finished_at_ms: nil, left: 0} =
             Alambique.drain_status(a)

    cells = for n <- 1..3, do: "status-#{n}"
    for cell <- cells, do: assert({:ok, _} = insert(a, cell, 1))
    assert %{draining: false, left: 3, done: 0, errors: []} = Alambique.drain_status(a)

    # A slow release keeps the drain running while the status is read.
    MemoryStore.inject(ctx.store_config,
      kind: :latency,
      ops: [:put],
      key: ~r"/status-3/lease$",
      ms: 600
    )

    began = System.os_time(:millisecond)
    drain = Task.async(fn -> Alambique.drain(a) end)

    running =
      eventually(fn ->
        status = Alambique.drain_status(a)
        status.running and status
      end)

    assert %{draining: true, cells_at_start: 3} = running
    assert running.started_at_ms >= began
    assert running.finished_at_ms == nil

    assert {:ok, %{hibernated: 3, errors: []}} = Task.await(drain)

    assert %{
             draining: true,
             running: false,
             cells_at_start: 3,
             done: 3,
             errors: [],
             left: 0,
             finished_at_ms: finished
           } = Alambique.drain_status(a)

    assert finished >= running.started_at_ms
  end

  @tag capture_log: true
  test "drain_status lists the cells that failed", %{a: a, ctx: ctx} do
    for n <- 1..2, do: assert({:ok, _} = insert(a, "bad-#{n}", 1))
    MemoryStore.inject(ctx.store_config, kind: :error, ops: [:put], key: ~r"/bad-2/lease$")

    assert {:ok, _} = Alambique.drain(a)

    # `left` counts the registered cells, and a cell whose release failed
    # leaves the registry when its process ends, a moment after the drain.
    status = eventually(fn -> (s = Alambique.drain_status(a)).left == 0 and s end)
    assert %{done: 1, errors: [{"bad-2", {:release_failed, _}}]} = status
  end

  test "the deadline ends the drain and leaves the rest awake", %{a: a, ctx: ctx} do
    cells = for n <- 1..6, do: "late-#{n}"
    for cell <- cells, do: assert({:ok, _} = insert(a, cell, 1))

    # Each hibernation takes at least 300 ms, one at a time: three fit in 1 s.
    fault =
      MemoryStore.inject(ctx.store_config, kind: :latency, ops: [:put], ms: 300, key: ~r"/lease$")

    assert {:ok, %{hibernated: done, errors: errors}} =
             Alambique.drain(a, concurrency: 1, deadline: 1_000)

    MemoryStore.remove(ctx.store_config, fault)

    assert done in 1..5
    assert length(errors) == 6 - done
    assert Enum.all?(errors, &match?({_, :deadline}, &1))

    # Nothing was killed: the cells that are awake were left. A cell cut off in
    # flight may still have gone to sleep since.
    left = for {cell, :deadline} <- errors, do: cell
    assert %{cells: awake} = Alambique.cells(a)
    assert awake != []
    assert awake -- left == []
    assert %{running: false, done: ^done} = Alambique.drain_status(a)

    # A second drain finishes what the first left.
    assert {:ok, %{errors: []}} = Alambique.drain(a)
    assert %{cells: []} = Alambique.cells(a)
  end

  test "a deadline longer than the work does not change the result", %{a: a} do
    for n <- 1..3, do: assert({:ok, _} = insert(a, "ontime-#{n}", 1))
    assert {:ok, %{hibernated: 3, errors: []}} = Alambique.drain(a, deadline: 30_000)
  end

  test "drain twice is safe", %{a: a} do
    assert {:ok, _} = insert(a, "once", 1)
    assert {:ok, %{hibernated: 1, errors: []}} = Alambique.drain(a)
    assert {:ok, %{hibernated: 0, errors: []}} = Alambique.drain(a)
  end

  test "a draining node leaves the members and refuses to wake cells", %{a: a} do
    config = Config.fetch!(a)
    assert {:ok, _} = insert(a, "awake", 1)
    refute Membership.draining?(config)

    assert {:ok, %{hibernated: 1}} = Alambique.drain(a)
    assert Membership.draining?(config)
    assert {:error, :draining} = Alambique.touch(a, "new")
    assert {:error, :draining} = Alambique.query(a, "awake", "SELECT 1")
    assert {:error, :draining} = Alambique.take_over(a, "new", :x)
    assert :none = Alambique.owner(a, "new")

    # Calls that need no wake still work.
    assert :ok = Alambique.hibernate(a, "awake")

    Membership.join(config)
    refute Membership.draining?(config)
    assert {:ok, _} = Alambique.touch(a, "new")
  end

  test "a node started with join: false serves its own calls until join/1", %{ctx: ctx} do
    c = start_instance(ctx, migrations: @schema, join: false, node_id: "not-joined")
    config = Config.fetch!(c)
    me = node()

    # Not a member, yet not draining: the call is served here.
    assert Membership.nodes(config) == [me]
    refute Membership.draining?(config)
    assert {:ok, _} = insert(c, "local", 1)
    assert {:ok, ^me} = Alambique.owner(c, "local")

    assert :ok = Alambique.join(c)
    assert :ok = Alambique.join(c)
    assert length(:pg.get_members(config.scope, :members)) == 1
    refute Membership.draining?(config)

    # Joined again after a drain.
    assert {:ok, _} = Alambique.drain(c)
    assert Membership.draining?(config)
    assert :ok = Alambique.join(c)
    refute Membership.draining?(config)
    assert {:ok, _} = insert(c, "again", 1)
  end

  defp child(inst, id) do
    {_, pid, _, _} = inst |> Supervisor.which_children() |> List.keyfind(id, 0)
    pid
  end

  defp crash(inst, id) do
    old = child(inst, id)
    Process.exit(old, :kill)
    eventually(fn -> is_pid(child(inst, id)) and child(inst, id) != old end)
  end

  defp member?(config), do: :pg.get_local_members(config.scope, :members) != []

  test "a joined node stays a member when the membership restarts", %{ctx: ctx} do
    c = start_instance(ctx, migrations: @schema, join: false, node_id: "restart-join")
    config = Config.fetch!(c)
    refute member?(config)

    :ok = Alambique.join(c)
    crash(c, Membership)
    eventually(fn -> member?(config) end)
    refute Membership.draining?(config)
  end

  test "a drained node stays drained when the membership restarts", %{ctx: ctx} do
    c = start_instance(ctx, migrations: @schema, node_id: "restart-drain")
    config = Config.fetch!(c)
    assert member?(config)

    {:ok, _} = Alambique.drain(c)
    crash(c, Membership)

    eventually(fn ->
      :pg.get_local_members(config.scope, {:node_id, "restart-drain", config.incarnation}) != []
    end)

    assert Membership.draining?(config)
    refute member?(config)
  end

  test "draining a node that never joined does not join it when the drain restarts", %{ctx: ctx} do
    c = start_instance(ctx, migrations: @schema, join: false, node_id: "restart-never")
    config = Config.fetch!(c)

    {:ok, _} = Alambique.drain(c)
    crash(c, Alambique.Drain)
    assert Membership.draining?(config)
    refute member?(config)
  end

  test "a restart of the drain with a clean stop keeps a member a member", %{ctx: ctx} do
    c = start_instance(ctx, migrations: @schema, node_id: "restart-stop")
    config = Config.fetch!(c)
    old = child(c, Alambique.Drain)
    :ok = Supervisor.terminate_child(c, Alambique.Drain)
    refute member?(config)
    {:ok, _} = Supervisor.restart_child(c, Alambique.Drain)
    assert child(c, Alambique.Drain) != old
    assert member?(config)
  end

  test "a new start of the instance begins from :join", %{ctx: ctx} do
    c = start_instance(ctx, migrations: @schema, join: false, node_id: "restart-fresh")
    :ok = Alambique.join(c)
    assert Membership.state(Config.fetch!(c)) == :member

    stop_supervised!(c)
    ^c = start_instance(ctx, migrations: @schema, join: false, node_id: "restart-fresh", name: c)
    assert Membership.state(Config.fetch!(c)) == :not_joined
  end

  test "stopping the instance drains it", %{ctx: ctx} do
    inst = start_instance(ctx, migrations: @schema)
    capture([[:alambique, :cell, :hibernate]])
    cells = for n <- 1..3, do: "stop-#{n}"
    for cell <- cells, do: assert({:ok, _} = insert(inst, cell, 1))

    :ok = stop_supervised(inst)

    for cell <- cells do
      {_, %{reason: reason}} = await_event([:alambique, :cell, :hibernate], cell)
      assert reason == :request
      assert durable_rows(ctx, cell, "SELECT n FROM t") == [[1]]
    end
  end

  test "the :drain option is validated and sets the defaults", %{ctx: ctx} do
    assert_raise ArgumentError, ~r/:drain :concurrency/, fn ->
      Alambique.Config.build(store: ctx.store, data_dir: ctx.dir, drain: [concurrency: 0])
    end

    config = Alambique.Config.build(store: ctx.store, data_dir: ctx.dir, drain: [rounds: 5])
    assert %{rounds: 5, concurrency: 64, timeout_ms: 15_000, deadline_ms: 60_000} = config.drain
  end
end
