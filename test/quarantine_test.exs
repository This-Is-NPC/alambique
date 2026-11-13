defmodule Alambique.QuarantineTest do
  @moduledoc "In-flight markers and the crash counter of a cell (design §10)."
  use ExUnit.Case, async: false

  @moduletag :capture_log

  import Alambique.CellHelpers

  alias Alambique.{Config, Inflight, Quarantine}
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @crash [:alambique, :cell, :crash_recorded]
  @quarantined [:alambique, :cell, :quarantined]

  setup do
    capture([@crash, @quarantined])
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # A fixed name keeps the data directory between starts of the same node.
  defp node_dir(ctx, name), do: Path.join(ctx.dir, Atom.to_string(name))

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp crashes(inst, cell) do
    {:ok, counter} = Alambique.crashes(inst, cell)
    counter
  end

  # The node crashed while working on the cells, then started again.
  defp crash_and_restart(ctx, name, cells, opts \\ []) do
    stop_supervised!(name)
    for cell <- cells, do: leave_busy_marker(node_dir(ctx, name), cell)
    start_instance(ctx, Keyword.merge([name: name, migrations: @schema], opts))
  end

  describe "markers" do
    test "an awake cell holds a marker that is busy only while it works", ctx do
      inst = start_instance(ctx, migrations: @schema)
      dir = Config.fetch!(inst).data_dir
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)

      assert [%{cell: ^cell, kind: "cell"}] = markers(dir)
      eventually(fn -> match?([%{flag: :idle}], markers(dir)) end)

      assert {:ok, [%{flag: :busy}]} = Alambique.transaction(inst, cell, fn _ -> markers(dir) end)
      eventually(fn -> match?([%{flag: :idle}], markers(dir)) end)
    end

    test "hibernation removes the marker", ctx do
      inst = start_instance(ctx, migrations: @schema)
      dir = Config.fetch!(inst).data_dir
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      assert :ok = Alambique.hibernate(inst, cell)
      eventually(fn -> markers(dir) == [] end)
    end

    test "a failed wake removes the marker", ctx do
      inst = start_instance(ctx, migrations: @schema)
      dir = Config.fetch!(inst).data_dir
      cell = unique_cell()
      assert :ok = Alambique.freeze(inst, cell)
      assert {:error, :frozen} = put(inst, cell, 1)
      eventually(fn -> markers(dir) == [] end)
    end

    test "a cell process that dies leaves no crash behind", ctx do
      inst = start_instance(ctx, name: :survivor, migrations: @schema)
      dir = Config.fetch!(inst).data_dir
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)

      # Killed while its marker is busy: an Elixir crash is not a VM crash.
      try do
        Alambique.transaction(inst, cell, fn _ -> Process.exit(self(), :kill) end)
      catch
        :exit, _ -> :ok
      end

      eventually(fn -> markers(dir) == [] end)

      restarted = crash_and_restart(ctx, :survivor, [])
      assert crashes(restarted, cell) == nil
      refute_received {:telemetry, @crash, _, _}
    end

    test "stopping the instance leaves no marker", ctx do
      inst = start_instance(ctx, name: :clean, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      stop_supervised!(:clean)
      assert markers(node_dir(ctx, :clean)) == []
    end

    test "work outside a cell process marks the cell and unmarks it, also on a raise", ctx do
      inst = start_instance(ctx, migrations: @schema)
      config = Config.fetch!(inst)

      assert [%{flag: :busy, cell: "c", kind: "query"}] =
               Inflight.run(config, "c", :query, fn -> markers(config.data_dir) end)

      assert markers(config.data_dir) == []
      assert_raise RuntimeError, fn -> Inflight.run(config, "c", :query, fn -> raise "x" end) end
      assert markers(config.data_dir) == []
    end

    test "a cell whose marker cannot be created works unprotected", ctx do
      inst = start_instance(ctx, migrations: @schema)
      dir = Config.fetch!(inst).data_dir
      cell = unique_cell()
      # A file where the directory of the markers should be.
      File.rm_rf!(Path.join(dir, "inflight"))
      File.write!(Path.join(dir, "inflight"), "")

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, _} = put(inst, cell, 1)
          assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t")
        end)

      assert log =~ "could not create the marker of #{cell}"
    end

    test "with quarantine off no marker is made, also while a lazy restore fills the file",
         ctx do
      inst =
        start_instance(ctx,
          migrations: @schema,
          quarantine: false,
          lazy_restore: [min_bytes: 0]
        )

      dir = Config.fetch!(inst).data_dir
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      assert :ok = Alambique.hibernate(inst, cell)
      File.rm_rf!(Path.dirname(Alambique.Cell.Local.db_path(Config.fetch!(inst), cell)))

      assert {:ok, _} = put(inst, cell, 2)

      assert {:ok, %{rows: [[1], [2]]}} =
               Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")

      refute File.exists?(Path.join(dir, "inflight"))
    end

    test "the long-running work of a cell is marked", ctx do
      test = self()

      CrashPoints.arm(
        :compact_before_put,
        fn _point, _context ->
          send(test, {:in_flight, markers(Config.fetch!(:marked).data_dir)})
        end,
        times: 1
      )

      inst =
        start_instance(ctx,
          name: :marked,
          migrations: @schema,
          compaction: [interval_ms: 50, fanout: 2]
        )

      cell = unique_cell()
      for n <- 1..4, do: assert({:ok, _} = put(inst, cell, n))

      assert_receive {:in_flight, seen}, 5_000
      assert %{flag: :busy, cell: ^cell} = Enum.find(seen, &(&1.kind == "compaction"))
    end

    test "a query is marked while it refreshes a replica", ctx do
      test = self()
      inst = start_instance(ctx, name: :reader, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      assert :ok = Alambique.hibernate(inst, cell)
      assert [_] = Alambique.Query.all(inst, [cell], "SELECT n FROM t").rows
      assert {:ok, _} = put(inst, cell, 2)
      assert :ok = Alambique.hibernate(inst, cell)

      CrashPoints.arm(:query_refresh_segment, fn _point, _context ->
        send(test, {:in_flight, markers(Config.fetch!(:reader).data_dir)})
      end)

      assert [_, _] = Alambique.Query.all(inst, [cell], "SELECT n FROM t").rows
      assert_receive {:in_flight, seen}
      assert [%{flag: :busy, cell: ^cell}] = Enum.filter(seen, &(&1.kind == "query"))
    end

    test "the hydration of a lazy restore is marked", ctx do
      test = self()
      inst = start_instance(ctx, name: :lazy, migrations: @schema, lazy_restore: [min_bytes: 0])
      cell = unique_cell()
      assert {:ok, _} = put(inst, cell, 1)
      assert :ok = Alambique.hibernate(inst, cell)
      File.rm_rf!(node_dir(ctx, :lazy) <> "/#{cell}.cell")

      CrashPoints.arm(:hydrate_before_fetch, fn _point, _context ->
        send(test, {:in_flight, markers(Config.fetch!(:lazy).data_dir)})
      end)

      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT n FROM t")
      assert_receive {:in_flight, seen}
      assert Enum.any?(seen, &match?(%{flag: :busy, kind: "hydrator", cell: ^cell}, &1))
    end
  end

  describe "recording crashes" do
    test "a marker left busy is recorded once and removed", ctx do
      inst = start_instance(ctx, name: :a, migrations: @schema)
      cell = unique_cell()
      assert crashes(inst, cell) == nil

      inst = crash_and_restart(ctx, :a, [cell])

      assert %{total: 1, recent: [%{node: "a", kinds: ["cell"]}]} = crashes(inst, cell)
      assert markers(node_dir(ctx, :a)) == []
      assert_received {:telemetry, @crash, %{count: 1, total: 1}, %{cell: ^cell, kinds: ["cell"]}}
      refute_received {:telemetry, @quarantined, _, _}
      assert {:ok, false} = Alambique.frozen?(inst, cell)
      assert {:ok, _} = put(inst, cell, 1)
    end

    test "several markers of one cell are one crash", ctx do
      start_instance(ctx, name: :a, migrations: @schema)
      cell = unique_cell()
      stop_supervised!(:a)
      leave_busy_marker(node_dir(ctx, :a), cell, :cell)
      leave_busy_marker(node_dir(ctx, :a), cell, :compaction)
      leave_busy_marker(node_dir(ctx, :a), cell, :cell)
      inst = start_instance(ctx, name: :a, migrations: @schema)

      assert %{total: 1, recent: [%{kinds: kinds}]} = crashes(inst, cell)
      assert Enum.sort(kinds) == ["cell", "compaction"]
    end

    test "idle markers prove nothing", ctx do
      start_instance(ctx, name: :a, migrations: @schema)
      stop_supervised!(:a)
      dir = node_dir(ctx, :a)

      idle =
        Inflight.open(%{quarantine: %{}, data_dir: dir, inflight: :no_tracker}, "idle", :cell)

      :ok = :file.close(idle.fd)
      assert [%{flag: :idle, cell: "idle"}] = markers(dir)

      inst = start_instance(ctx, name: :a, migrations: @schema)
      assert crashes(inst, "idle") == nil
      assert markers(dir) == []
    end

    test "the threshold freezes the cell, other cells keep working", ctx do
      start_instance(ctx, name: :a, migrations: @schema, quarantine: [threshold: 3])
      cell = unique_cell()
      other = unique_cell()

      crash_and_restart(ctx, :a, [cell, other], quarantine: [threshold: 3])
      inst = crash_and_restart(ctx, :a, [cell], quarantine: [threshold: 3])
      assert {:ok, false} = Alambique.frozen?(inst, cell)
      refute_received {:telemetry, @quarantined, _, _}

      inst = crash_and_restart(ctx, :a, [cell], quarantine: [threshold: 3])
      assert {:ok, true} = Alambique.frozen?(inst, cell)
      assert {:error, :frozen} = put(inst, cell, 1)

      assert_received {:telemetry, @quarantined, %{count: 3},
                       %{cell: ^cell, threshold: 3, window_ms: 3_600_000, crashes: times}}

      assert length(times) == 3
      assert %{total: 3, recent: [_, _, _]} = crashes(inst, cell)

      assert {:ok, false} = Alambique.frozen?(inst, other)
      assert {:ok, _} = put(inst, other, 1)
      assert %{total: 1} = crashes(inst, other)
    end

    test "crashes older than the window do not count", ctx do
      world = ManualClock.new(1_700_000_000_000)

      opts = [
        clock: {ManualClock, ManualClock.config(world, 0)},
        quarantine: [threshold: 2, window_ms: 10_000]
      ]

      start_instance(ctx, [name: :a, migrations: @schema] ++ opts)
      cell = unique_cell()

      crash_and_restart(ctx, :a, [cell], opts)
      ManualClock.advance(world, 10_001)
      inst = crash_and_restart(ctx, :a, [cell], opts)

      assert %{total: 2, recent: [_]} = crashes(inst, cell)
      assert {:ok, false} = Alambique.frozen?(inst, cell)

      ManualClock.advance(world, 9_000)
      inst = crash_and_restart(ctx, :a, [cell], opts)
      assert %{total: 3, recent: [_, _]} = crashes(inst, cell)
      assert {:ok, true} = Alambique.frozen?(inst, cell)
    end

    test "unfreezing forgets the crashes", ctx do
      start_instance(ctx, name: :a, migrations: @schema, quarantine: [threshold: 1])
      cell = unique_cell()
      inst = crash_and_restart(ctx, :a, [cell], quarantine: [threshold: 1])
      assert {:ok, true} = Alambique.frozen?(inst, cell)

      assert :ok = Alambique.unfreeze(inst, cell)
      assert crashes(inst, cell) == nil
      assert {:ok, _} = put(inst, cell, 1)
    end

    test "a crash recorded by a write whose answer was lost counts once", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      ctx_repl = repl_ctx(inst, cell)

      MemoryStore.inject(ctx.store_config,
        kind: :lost_ack,
        ops: [:put],
        key: ~r/crashes/,
        times: 2
      )

      settings = %{threshold: 10, window_ms: 60_000}
      assert {:ok, %{total: 1}} = Quarantine.record(ctx_repl, cell, settings, ["cell"])
      assert %{total: 1, recent: [_]} = crashes(inst, cell)
    end

    test "concurrent writers lose no increment", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      repl = %{repl_ctx(inst, cell) | timeout_ms: 10_000}
      settings = %{threshold: 100, window_ms: 3_600_000}

      results =
        1..25
        |> Task.async_stream(fn _ -> Quarantine.record(repl, cell, settings, ["cell"]) end,
          max_concurrency: 25,
          timeout: 20_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, _}, &1))
      assert %{total: 25, recent: recent} = crashes(inst, cell)
      assert length(recent) == 25
      assert length(Enum.uniq_by(recent, & &1.id)) == 25
    end

    test "the counter keeps at most the threshold of crashes", ctx do
      inst = start_instance(ctx, migrations: @schema)
      cell = unique_cell()
      repl = repl_ctx(inst, cell)
      settings = %{threshold: 3, window_ms: 3_600_000}
      for _ <- 1..5, do: assert({:ok, _} = Quarantine.record(repl, cell, settings, ["cell"]))
      assert %{total: 5, recent: [_, _, _]} = crashes(inst, cell)
    end
  end

  describe "the counter object" do
    setup ctx do
      # Each operation fails against the unreadable store, which would open a breaker.
      inst = start_instance(ctx, migrations: @schema, breaker: [failures: 1_000_000])
      cell = unique_cell()
      repl = %{repl_ctx(inst, cell) | timeout_ms: 150}
      {:ok, key: Alambique.Replication.Keys.crashes("cells-test", cell), cell: cell, repl: repl}
    end

    @settings %{threshold: 3, window_ms: 3_600_000}

    test "an unreadable store is an error for each operation", ctx do
      MemoryStore.inject(ctx.store_config, kind: :error, key: ~r/crashes/, reason: :down)

      assert {:error, :down} = Quarantine.read(ctx.repl, ctx.cell)
      assert {:error, :down} = Quarantine.reset(ctx.repl, ctx.cell)
      assert {:error, :down} = Quarantine.record(ctx.repl, ctx.cell, @settings, ["cell"])
    end

    test "an object that is not a counter reads as empty and the next crash replaces it", ctx do
      {mod, config} = ctx.store
      {:ok, _} = mod.put(config, ctx.key, "not json", :none, %{})
      assert {:ok, %{total: 0, recent: []}} = Quarantine.read(ctx.repl, ctx.cell)

      assert {:ok, %{total: 1, recent: [_]}} =
               Quarantine.record(ctx.repl, ctx.cell, @settings, ["cell"])

      assert {:ok, %{total: 1}} = Quarantine.read(ctx.repl, ctx.cell)
    end

    test "entries that are malformed are dropped, the others are kept", ctx do
      {mod, config} = ctx.store
      good = %{id: "a", at_ms: 5, node: "n", kinds: ["cell"]}
      body = Jason.encode!(%{total: 2, recent: [good, %{id: "b"}, "x"]})
      {:ok, _} = mod.put(config, ctx.key, body, :none, %{})

      assert {:ok, %{total: 2, recent: [%{id: "a", at_ms: 5}]}} =
               Quarantine.read(ctx.repl, ctx.cell)
    end
  end

  describe "the store is out of reach" do
    test "the start does not wait for it; the crash is recorded when it is back", ctx do
      start_instance(ctx, name: :a, migrations: @schema)
      cell = unique_cell()
      stop_supervised!(:a)
      leave_busy_marker(node_dir(ctx, :a), cell)

      fault = MemoryStore.inject(ctx.store_config, kind: :error, key: ~r/crashes/)
      opts = [quarantine: [startup_timeout_ms: 200, retry_ms: 50]]
      started = System.monotonic_time(:millisecond)
      inst = start_instance(ctx, [name: :a, migrations: @schema] ++ opts)
      assert System.monotonic_time(:millisecond) - started < 2_000

      assert [%{flag: :busy, cell: ^cell}] = markers(node_dir(ctx, :a))
      refute_received {:telemetry, @crash, _, _}

      MemoryStore.remove(ctx.store_config, fault)
      eventually(fn -> markers(node_dir(ctx, :a)) == [] end)
      assert %{total: 1} = crashes(inst, cell)
      assert_received {:telemetry, @crash, %{count: 1}, %{cell: ^cell}}
    end

    test "a freeze that failed is tried again without counting the crash again", ctx do
      start_instance(ctx, name: :a, migrations: @schema, quarantine: [threshold: 1])
      cell = unique_cell()
      stop_supervised!(:a)
      leave_busy_marker(node_dir(ctx, :a), cell)

      fault = MemoryStore.inject(ctx.store_config, kind: :error, key: ~r{/frozen$}, ops: [:put])
      opts = [quarantine: [threshold: 1, startup_timeout_ms: 300, retry_ms: 50]]
      inst = start_instance(ctx, [name: :a, migrations: @schema] ++ opts)
      assert %{total: 1} = crashes(inst, cell)
      assert {:ok, false} = Alambique.frozen?(inst, cell)

      MemoryStore.remove(ctx.store_config, fault)
      eventually(fn -> Alambique.frozen?(inst, cell) == {:ok, true} end)
      assert %{total: 1} = crashes(inst, cell)
      assert_receive {:telemetry, @quarantined, %{count: 1}, %{cell: ^cell}}
    end
  end

  describe "options" do
    test "false turns the quarantine off", ctx do
      start_instance(ctx, name: :a, migrations: @schema)
      cell = unique_cell()
      stop_supervised!(:a)
      leave_busy_marker(node_dir(ctx, :a), cell)

      inst = start_instance(ctx, name: :a, migrations: @schema, quarantine: false)
      assert {:ok, _} = put(inst, cell, 1)
      assert crashes(inst, cell) == nil
      # Nothing new is marked and nothing is read.
      assert [%{cell: ^cell}] = markers(node_dir(ctx, :a))
    end

    test "defaults and validation" do
      base = [store: {MemoryStore, nil}, data_dir: "/tmp/x"]

      assert %{threshold: 3, window_ms: 3_600_000, startup_timeout_ms: 5_000, retry_ms: 30_000} =
               Config.build(base).quarantine

      assert %{threshold: 1} = Config.build([quarantine: [threshold: 1]] ++ base).quarantine
      assert Config.build([quarantine: false] ++ base).quarantine == nil

      for bad <- [[threshold: 0], [window_ms: -1], [startup_timeout_ms: 1.5], [retry: 1], true] do
        assert_raise ArgumentError, ~r/quarantine/, fn ->
          Config.build([quarantine: bad] ++ base)
        end
      end
    end
  end
end
