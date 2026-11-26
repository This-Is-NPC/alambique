defmodule Alambique.CellOptionsRefreshTest do
  @moduledoc """
  `Alambique.refresh_options/3`: the options of an awake cell read again
  (design §7). Every per-cell key applies live; the cell never wakes for it;
  a refresh that cannot apply changes nothing.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Sqlite}
  alias Alambique.Cell.Facts
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  require Facts

  @moduletag :capture_log

  defmodule Callbacks do
    @moduledoc false
    # The callback runs in a process of its own: the test sets the function it
    # calls through a persistent term.
    @key {__MODULE__, :fun}
    def set(fun), do: :persistent_term.put(@key, fun)
    def clear, do: :persistent_term.erase(@key)
    def options(cell), do: :persistent_term.get(@key).(cell)
  end

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    on_exit(&Callbacks.clear/0)
    on_exit(&CrashPoints.reset/0)

    capture([
      [:alambique, :cell, :wake],
      [:alambique, :cell, :options, :refresh],
      [:alambique, :cell, :options, :invalid],
      [:alambique, :cell, :options, :repaired]
    ])

    Callbacks.set(fn _ -> [] end)
    setup_store()
  end

  defp start(ctx, opts \\ []) do
    start_instance(
      ctx,
      Keyword.merge(
        [
          cell_options: {Callbacks, :options},
          cell_options_timeout_ms: 400,
          migrations: @schema,
          feed: false,
          compaction: false,
          idle_timeout_ms: 300_000,
          lease_ttl_ms: 9_000
        ],
        opts
      )
    )
  end

  defp pid(inst, cell), do: Cell.whereis(Config.fetch!(inst), cell)
  defp wake(inst, cell), do: Alambique.query(inst, cell, "SELECT 1")
  defp state(inst, cell), do: in_cell(pid(inst, cell), & &1)
  defp refresh(inst, cell), do: Alambique.refresh_options(inst, cell)

  # Reads the connection from the process of the cell without a call: a call
  # would restore the cache.
  defp cache_size(inst, cell) do
    in_cell(pid(inst, cell), fn state ->
      {:ok, %{rows: [[kib]]}} = Sqlite.run(state.conn, "PRAGMA cache_size", [])
      kib
    end)
  end

  defp timer_alive?(inst, cell, ref),
    do: in_cell(pid(inst, cell), fn _ -> :erlang.read_timer(ref) end) != false

  defp admitted(inst, cell) do
    {_pid, load} = Cell.find(Config.fetch!(inst), cell)
    :atomics.get(load, Facts.slot(:admitted))
  end

  defp lease_keys(%{store_config: config}) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/", nil, 100_000)
    Enum.filter(keys, &String.contains?(&1, "lease"))
  end

  describe "the timers" do
    test "a message of the replaced shrink timer is ignored, and the timer is cancelled", ctx do
      inst = start(ctx, awake: [cache_kib: 128, idle_cache_kib: 8])
      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 60_000]] end)
      assert {:ok, _} = wake(inst, "c")
      assert %{idle_cache: old} = state(inst, "c")
      assert is_reference(old)

      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 70_000]] end)
      assert {:ok, :override, changed: [:idle_cache_after_ms]} = refresh(inst, "c")
      assert %{idle_cache: new} = state(inst, "c")
      assert is_reference(new) and new != old
      eventually(fn -> not timer_alive?(inst, "c", old) end)

      # The old message was queued before the cancel: it must do nothing.
      send(pid(inst, "c"), {:timeout, old, :idle_cache})
      assert %{idle_cache: ^new} = :sys.get_state(pid(inst, "c"))
      assert cache_size(inst, "c") == -128
    end

    test "after several refreshes one timer of each kind is alive", ctx do
      inst = start(ctx, awake: [cache_kib: 128, idle_cache_kib: 8])

      timings = fn i ->
        Callbacks.set(fn _ ->
          [
            idle_timeout_ms: 200_000 + i * 1_000,
            awake: [
              idle_close_after_ms: 100_000 + i * 1_000,
              idle_cache_after_ms: 50_000 + i * 1_000
            ]
          ]
        end)
      end

      timings.(0)
      assert {:ok, _} = wake(inst, "c")

      refs = fn ->
        s = state(inst, "c")
        [s.idle_timer, s.idle_cache, s.idle_close]
      end

      seen =
        for i <- 1..4, reduce: refs.() do
          acc ->
            timings.(i)
            assert {:ok, :override, changed: _} = refresh(inst, "c")
            acc ++ refs.()
        end

      latest = Enum.take(seen, -3)
      assert Enum.all?(latest, &is_reference/1)

      eventually(fn ->
        live = for ref <- seen, timer_alive?(inst, "c", ref), do: ref
        Enum.sort(live) == Enum.sort(latest)
      end)
    end

    test "an override of nil turns a timer off and a value turns it on", ctx do
      inst = start(ctx, awake: [idle_cache_after_ms: 60_000, idle_close_after_ms: 120_000])
      assert {:ok, _} = wake(inst, "c")
      assert %{idle_cache: cache, idle_close: close} = state(inst, "c")
      assert is_reference(cache) and is_reference(close)

      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: nil, idle_close_after_ms: nil]] end)

      assert {:ok, :override, changed: [:idle_cache_after_ms, :idle_close_after_ms]} =
               refresh(inst, "c")

      assert %{idle_cache: nil, idle_close: nil} = state(inst, "c")

      eventually(fn ->
        not timer_alive?(inst, "c", cache) and not timer_alive?(inst, "c", close)
      end)

      Callbacks.set(fn _ -> [] end)

      assert {:ok, :node, changed: [:idle_cache_after_ms, :idle_close_after_ms]} =
               refresh(inst, "c")

      assert %{idle_cache: cache, idle_close: close} = state(inst, "c")
      assert is_reference(cache) and is_reference(close)
    end

    test "a shorter idle timeout hibernates the cell by its own clock", ctx do
      inst = start(ctx)
      assert {:ok, _} = wake(inst, "c")
      Callbacks.set(fn _ -> [idle_timeout_ms: 1_500] end)
      assert {:ok, :override, changed: [:idle_timeout_ms]} = refresh(inst, "c")
      eventually(fn -> pid(inst, "c") == nil end)
    end

    test "the event reports what changed and how many timers moved", ctx do
      inst = start(ctx, awake: [cache_kib: 128])
      assert {:ok, _} = wake(inst, "c")
      Callbacks.set(fn _ -> [idle_timeout_ms: 200_000, awake: [cache_kib: 64]] end)
      assert {:ok, :override, changed: [:cache_kib, :idle_timeout_ms]} = refresh(inst, "c")

      {m, meta} = await_event([:alambique, :cell, :options, :refresh], "c")
      assert meta.result == :ok
      assert meta.options == :override
      assert meta.changed == [:cache_kib, :idle_timeout_ms]
      assert m.timers_rearmed == 1
      assert is_integer(m.options_time) and m.options_time > 0
      assert is_integer(m.duration)
    end
  end

  describe "the page cache" do
    test "a refresh while shrunk waits for the next call to restore the new limit", ctx do
      inst = start(ctx, awake: [cache_kib: 512, idle_cache_kib: 8])
      Callbacks.set(fn _ -> [awake: [cache_kib: 128, idle_cache_after_ms: 50]] end)
      assert {:ok, _} = wake(inst, "c")
      eventually(fn -> cache_size(inst, "c") == -8 end)

      Callbacks.set(fn _ -> [awake: [cache_kib: 256, idle_cache_after_ms: 60_000]] end)

      assert {:ok, :override, changed: [:cache_kib, :idle_cache_after_ms]} =
               refresh(inst, "c")

      assert %{idle_cache: :shrunk} = state(inst, "c")
      assert cache_size(inst, "c") == -8

      assert {:ok, _} = wake(inst, "c")
      assert cache_size(inst, "c") == -256
    end

    test "a refresh that turns the shrink off gives the cache back", ctx do
      inst = start(ctx, awake: [cache_kib: 128, idle_cache_kib: 8])
      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 50]] end)
      assert {:ok, _} = wake(inst, "c")
      eventually(fn -> cache_size(inst, "c") == -8 end)

      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: nil]] end)
      assert {:ok, :override, changed: [:idle_cache_after_ms]} = refresh(inst, "c")
      assert %{idle_cache: nil} = state(inst, "c")
      assert cache_size(inst, "c") == -128
    end

    test "a refresh of an awake cell sets its cache at once", ctx do
      inst = start(ctx, awake: [cache_kib: 512])
      assert {:ok, _} = wake(inst, "c")
      assert cache_size(inst, "c") == -512

      Callbacks.set(fn _ -> [awake: [cache_kib: 96]] end)
      assert {:ok, :override, changed: [:cache_kib]} = refresh(inst, "c")
      assert cache_size(inst, "c") == -96
    end

    test "a refresh while shrunk sets the new idle limit at once", ctx do
      inst = start(ctx, awake: [cache_kib: 128, idle_cache_kib: 8])
      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 50]] end)
      assert {:ok, _} = wake(inst, "c")
      eventually(fn -> cache_size(inst, "c") == -8 end)

      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 50, idle_cache_kib: 4]] end)
      assert {:ok, :override, changed: [:idle_cache_kib]} = refresh(inst, "c")
      assert %{idle_cache: :shrunk} = state(inst, "c")
      assert cache_size(inst, "c") == -4
    end

    test "a refresh is no activity: the cell stays shrunk and its clock is not reset", ctx do
      inst = start(ctx, awake: [cache_kib: 128, idle_cache_kib: 8])
      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 50]] end)
      assert {:ok, _} = wake(inst, "c")
      eventually(fn -> cache_size(inst, "c") == -8 end)
      %{last_active: last} = state(inst, "c")

      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 50, idle_gc: false]] end)
      assert {:ok, :override, changed: [:idle_gc]} = refresh(inst, "c")
      assert %{idle_cache: :shrunk, last_active: ^last} = state(inst, "c")
      assert cache_size(inst, "c") == -8
    end
  end

  describe "the idle shrink" do
    test "a pragma that fails leaves the cache full and is tried again", ctx do
      inst =
        start(ctx, awake: [cache_kib: 128, idle_cache_kib: 8, idle_cache_after_ms: 100])

      test = self()

      CrashPoints.arm(
        :cell_before_shrink_pragma,
        fn _, _ ->
          send(test, :shrink_attempt)
          {:error, :boom}
        end,
        times: :infinity
      )

      assert {:ok, _} = wake(inst, "c")
      assert_receive :shrink_attempt, 5_000
      assert_receive :shrink_attempt, 5_000

      # Not marked shrunk: the facts say full and the next call restores nothing.
      assert %{idle_cache: ref} = state(inst, "c")
      assert is_reference(ref)
      assert {:ok, %{cache: :full}} = Alambique.facts(inst, "c")
      assert cache_size(inst, "c") == -128

      CrashPoints.reset()
      eventually(fn -> state(inst, "c").idle_cache == :shrunk and cache_size(inst, "c") == -8 end)
    end
  end

  describe "a closed connection" do
    test "stays closed and takes the new options when it opens again", ctx do
      inst = start(ctx, awake: [idle_close_after_ms: 100, cache_kib: 128])
      assert {:ok, _} = wake(inst, "c")
      eventually(fn -> state(inst, "c").conn == nil end)

      Callbacks.set(fn _ -> [awake: [cache_kib: 256, idle_close_after_ms: 5_000]] end)

      assert {:ok, :override, changed: [:cache_kib, :idle_close_after_ms]} = refresh(inst, "c")
      assert %{conn: nil, idle_close: nil} = state(inst, "c")

      assert {:ok, _} = wake(inst, "c")
      assert %{conn: conn, idle_close: close} = state(inst, "c")
      assert conn != nil and is_reference(close)
      assert cache_size(inst, "c") == -256
    end
  end

  describe "a refresh that cannot apply" do
    setup ctx do
      inst = start(ctx, awake: [cache_kib: 128, idle_cache_kib: 8])
      Callbacks.set(fn _ -> [awake: [idle_cache_after_ms: 60_000]] end)
      assert {:ok, _} = wake(inst, "c")
      %{inst: inst, before: state(inst, "c")}
    end

    defp unchanged(inst, before) do
      now = state(inst, "c")
      assert now.cell_opts == before.cell_opts
      assert now.idle_cache == before.idle_cache
      assert now.idle_timer == before.idle_timer
      assert timer_alive?(inst, "c", before.idle_timer)
      assert timer_alive?(inst, "c", before.idle_cache)
      assert now.refresh == nil
      assert cache_size(inst, "c") == -128
      assert {:ok, _} = wake(inst, "c")
    end

    test "an invalid override keeps the options and reports why", %{inst: inst, before: before} do
      Callbacks.set(fn _ -> [bogus: 1] end)
      assert {:error, {:cell_options, message}} = refresh(inst, "c")
      assert message =~ "bogus"
      unchanged(inst, before)

      assert {_, %{phase: :refresh, action: :keep}} =
               await_event(
                 [:alambique, :cell, :options, :invalid],
                 "c",
                 5_000,
                 &(&1.phase == :refresh)
               )

      assert {_, %{result: :error, options: :kept}} =
               await_event([:alambique, :cell, :options, :refresh], "c")
    end

    test "a function that raises keeps the options", %{inst: inst, before: before} do
      Callbacks.set(fn _ -> raise "boom" end)
      assert {:error, {:cell_options, message}} = refresh(inst, "c")
      assert message =~ "boom"
      unchanged(inst, before)
    end

    test "a function that hangs is killed at the timeout and the options stay",
         %{inst: inst, before: before} do
      Callbacks.set(fn _ -> Process.sleep(:infinity) end)
      assert {:error, {:cell_options, message}} = refresh(inst, "c")
      assert message =~ "timed out after 400 ms"
      unchanged(inst, before)
    end

    test "a pragma that fails changes no timer and no option", %{inst: inst, before: before} do
      Callbacks.set(fn _ ->
        [
          idle_timeout_ms: 200_000,
          awake: [cache_kib: 256, idle_cache_after_ms: 70_000, idle_cache_kib: 8]
        ]
      end)

      CrashPoints.arm(:cell_before_refresh_pragma, fn _, _ -> {:error, :boom} end)
      assert {:error, {:cell_options, message}} = refresh(inst, "c")
      assert message =~ "boom"
      unchanged(inst, before)

      # The fault was armed once: the same refresh applies now.
      assert {:ok, :override, changed: [:cache_kib, :idle_cache_after_ms, :idle_timeout_ms]} =
               refresh(inst, "c")

      assert cache_size(inst, "c") == -256
    end
  end

  test "an invalid override keeps a serving cell also with cell_options_invalid: :refuse", ctx do
    inst = start(ctx, cell_options_invalid: :refuse, awake: [cache_kib: 128])
    assert {:ok, _} = wake(inst, "c")
    Callbacks.set(fn _ -> [bogus: 1] end)
    assert {:error, {:cell_options, _}} = refresh(inst, "c")
    assert {:ok, _} = wake(inst, "c")
    assert state(inst, "c").cell_opts == %{}
  end

  describe "track_bytes" do
    test "off clears the memory facts and on measures them again", ctx do
      inst = start(ctx)
      assert {:ok, _} = wake(inst, "c")
      assert {:ok, %{cache_used: used}} = Alambique.facts(inst, "c")
      assert is_integer(used)

      Callbacks.set(fn _ -> [awake: [track_bytes: false]] end)
      assert {:ok, :override, changed: [:track_bytes]} = refresh(inst, "c")

      assert {:ok, %{cache_used: nil, schema_used: nil}} =
               Alambique.facts(inst, "c")

      assert {:ok, _} = wake(inst, "c")
      assert {:ok, %{cache_used: nil}} = Alambique.facts(inst, "c")

      Callbacks.set(fn _ -> [] end)
      assert {:ok, :node, changed: [:track_bytes]} = refresh(inst, "c")
      assert {:ok, %{cache_used: used, process_bytes: bytes}} = Alambique.facts(inst, "c")
      assert is_integer(used) and is_integer(bytes)
    end

    test "off is a no-op under a memory budget", ctx do
      inst = start(ctx, awake: [max_bytes: 500_000_000])
      assert {:ok, _} = wake(inst, "c")
      Callbacks.set(fn _ -> [awake: [track_bytes: false]] end)
      assert {:ok, :override, changed: [:track_bytes]} = refresh(inst, "c")
      assert {:ok, _} = wake(inst, "c")
      assert {:ok, %{cache_used: used}} = Alambique.facts(inst, "c")
      assert is_integer(used)
    end
  end

  describe "locking_mode" do
    test "cannot change on an awake cell: the refresh is refused and nothing changes", ctx do
      inst = start(ctx, lazy_restore: false, awake: [cache_kib: 128])
      Callbacks.set(fn _ -> [sql: [locking_mode: :exclusive]] end)
      assert {:ok, _} = wake(inst, "c")
      before = state(inst, "c")
      assert before.cell_opts == %{locking_mode: :exclusive}

      Callbacks.set(fn _ -> [awake: [cache_kib: 64]] end)
      assert {:error, {:cell_options, message}} = refresh(inst, "c")
      assert message =~ "locking_mode"
      assert state(inst, "c").cell_opts == before.cell_opts
      assert cache_size(inst, "c") == -128

      # The same lock with another cache is a valid refresh.
      Callbacks.set(fn _ -> [sql: [locking_mode: :exclusive], awake: [cache_kib: 64]] end)
      assert {:ok, :override, changed: [:cache_kib]} = refresh(inst, "c")
      assert cache_size(inst, "c") == -64
    end
  end

  describe "cleanup_after_ms" do
    test "not below the idle timeout goes back to the node's, at a wake and at a refresh", ctx do
      inst = start(ctx)
      Callbacks.set(fn _ -> [idle_timeout_ms: 4_000, feed: [cleanup_after_ms: 5_000]] end)
      assert {:ok, _} = wake(inst, "c")
      assert state(inst, "c").cell_opts == %{idle_timeout_ms: 4_000}

      await_event(
        [:alambique, :cell, :options, :repaired],
        "c",
        5_000,
        &(&1.field == :cleanup_after_ms)
      )

      Callbacks.set(fn _ -> [feed: [cleanup_after_ms: 3_000]] end)
      assert {:ok, :override, changed: [:idle_timeout_ms, :cleanup_after_ms]} = refresh(inst, "c")
      assert state(inst, "c").cell_opts == %{cleanup_after_ms: 3_000}

      Callbacks.set(fn _ -> [idle_timeout_ms: 2_000, feed: [cleanup_after_ms: 3_000]] end)
      assert {:ok, _, changed: [:idle_timeout_ms, :cleanup_after_ms]} = refresh(inst, "c")
      assert state(inst, "c").cell_opts == %{idle_timeout_ms: 2_000}
    end

    test "a shorter wait applies to a cleanup that is already waiting", ctx do
      inst = start(ctx, feed: [cleanup_after_ms: 20_000])
      assert {:ok, _} = Alambique.execute(inst, "c", "INSERT INTO t VALUES (1)")

      eventually(fn ->
        s = state(inst, "c")
        s.feed_published > 0 and is_reference(s.feed_cleanup)
      end)

      assert %{feed_cleaned: 0} = state(inst, "c")
      Callbacks.set(fn _ -> [feed: [cleanup_after_ms: 150]] end)
      assert {:ok, :override, changed: [:cleanup_after_ms]} = refresh(inst, "c")

      eventually(
        fn ->
          s = state(inst, "c")
          s.feed_cleaned == s.feed_published
        end,
        5_000
      )
    end
  end

  describe "the node limits and rules" do
    test "a refreshed idle timeout not above the feed cleanup goes back to the node's", ctx do
      inst = start(ctx)
      Callbacks.set(fn _ -> [idle_timeout_ms: 5_000] end)
      assert {:ok, _} = wake(inst, "c")
      assert state(inst, "c").cell_opts == %{idle_timeout_ms: 5_000}

      # feed.cleanup_after_ms is 1 000.
      Callbacks.set(fn _ -> [idle_timeout_ms: 1_000] end)
      assert {:ok, _, changed: [:idle_timeout_ms]} = refresh(inst, "c")
      assert state(inst, "c").cell_opts == %{}

      await_event(
        [:alambique, :cell, :options, :repaired],
        "c",
        5_000,
        &(&1.field == :idle_timeout_ms)
      )
    end
  end

  describe "the call" do
    test "an instance without a callback answers the node's options", ctx do
      inst = start_instance(ctx, migrations: @schema)
      assert {:ok, _} = wake(inst, "c")
      assert {:ok, :node, changed: []} = refresh(inst, "c")
      assert admitted(inst, "c") == 0
    end

    test "gives its place in the admission count back, so the cell can sleep", ctx do
      inst = start(ctx)
      assert {:ok, _} = wake(inst, "c")
      Callbacks.set(fn _ -> [idle_timeout_ms: 1_500] end)
      assert {:ok, :override, changed: [:idle_timeout_ms]} = refresh(inst, "c")
      assert admitted(inst, "c") == 0
      eventually(fn -> pid(inst, "c") == nil end)
    end

    test "the cell serves and answers system messages while the callback runs", ctx do
      inst = start(ctx, cell_options_timeout_ms: 1_000)
      assert {:ok, _} = wake(inst, "c")
      test = self()

      Callbacks.set(fn _ ->
        send(test, {:called, self()})
        Process.sleep(:infinity)
      end)

      task = Task.async(fn -> refresh(inst, "c") end)
      assert_receive {:called, _}, 5_000
      assert {:ok, _} = wake(inst, "c")
      assert %{refresh: %{}} = :sys.get_state(pid(inst, "c"), 300)
      assert {:error, {:cell_options, _}} = await(task)
    end

    test "concurrent refreshes run one callback at a time and share the next one", ctx do
      inst = start(ctx, cell_options_timeout_ms: 400)
      assert {:ok, _} = wake(inst, "c")
      running = :atomics.new(2, [])
      started = :counters.new(1, [])

      Callbacks.set(fn _ ->
        :counters.add(started, 1, 1)
        now = :atomics.add_get(running, 1, 1)
        if now > :atomics.get(running, 2), do: :atomics.put(running, 2, now)
        Process.sleep(250)
        :atomics.sub(running, 1, 1)
        []
      end)

      tasks = for _ <- 1..4, do: Task.async(fn -> refresh(inst, "c") end)
      assert Enum.all?(await_all(tasks), &match?({:ok, :node, changed: []}, &1))
      assert :counters.get(started, 1) == 2
      assert :atomics.get(running, 2) == 1
    end

    test "the last request is the last applied", ctx do
      inst = start(ctx, cell_options_timeout_ms: 2_000, awake: [cache_kib: 128])
      assert {:ok, _} = wake(inst, "c")
      test = self()

      Callbacks.set(fn _ ->
        send(test, {:called, self()})
        receive do: (:go -> [awake: [cache_kib: 64]])
      end)

      first = Task.async(fn -> refresh(inst, "c") end)
      assert_receive {:called, worker}, 5_000

      # The second request is queued behind the first resolve, which still
      # holds the old answer; the new one must win.
      Callbacks.set(fn _ -> [awake: [cache_kib: 32]] end)
      second = Task.async(fn -> refresh(inst, "c") end)
      eventually(fn -> match?(%{refresh: %{queued: [_]}}, state(inst, "c")) end)
      send(worker, :go)

      assert {:ok, :override, changed: [:cache_kib]} = await(first)
      assert {:ok, :override, changed: [:cache_kib]} = await(second)
      assert cache_size(inst, "c") == -32
    end
  end

  describe "every cell awake on the node" do
    test "refreshes each one and counts the changed and the failed", ctx do
      inst = start(ctx, awake: [cache_kib: 128])
      cells = for i <- 1..5, do: "c#{i}"
      for cell <- cells, do: assert({:ok, _} = wake(inst, cell))

      Callbacks.set(fn
        "c1" -> raise "boom"
        "c2" -> []
        _ -> [awake: [cache_kib: 64]]
      end)

      assert {:ok, summary} = Alambique.refresh_options(inst, :all, concurrency: 2)

      assert %{cells: 5, refreshed: 4, changed: 3, asleep: 0, failed: 1} = summary
      assert [%{cell: "c1", reason: {:cell_options, message}}] = summary.failures
      assert message =~ "boom"
      for cell <- ["c3", "c4", "c5"], do: assert(cache_size(inst, cell) == -64)
      assert cache_size(inst, "c2") == -128
      # The place of each call in the admission count was given back.
      for cell <- cells, do: assert(admitted(inst, cell) == 0)
    end

    test "runs at most :concurrency callbacks at a time", ctx do
      inst = start(ctx)
      for i <- 1..6, do: assert({:ok, _} = wake(inst, "c#{i}"))
      running = :atomics.new(2, [])

      Callbacks.set(fn _ ->
        now = :atomics.add_get(running, 1, 1)
        if now > :atomics.get(running, 2), do: :atomics.put(running, 2, now)
        Process.sleep(100)
        :atomics.sub(running, 1, 1)
        []
      end)

      assert {:ok, %{cells: 6, refreshed: 6, failed: 0}} =
               Alambique.refresh_options(inst, :all, concurrency: 2)

      assert :atomics.get(running, 2) == 2
    end

    test "validates :concurrency", ctx do
      inst = start(ctx)

      for bad <- [0, -1, :many] do
        assert_raise ArgumentError, ~r/:concurrency must be a positive integer/, fn ->
          Alambique.refresh_options(inst, :all, concurrency: bad)
        end
      end
    end

    test "has nothing to do on a node with no awake cell", ctx do
      inst = start(ctx)

      assert {:ok, %{cells: 0, refreshed: 0, changed: 0, asleep: 0, failed: 0, failures: []}} =
               Alambique.refresh_options(inst, :all)
    end
  end

  describe "a cell that is not awake" do
    test "is not woken, and no lease is written", ctx do
      inst = start(ctx)
      assert {:ok, :asleep} = refresh(inst, "never")
      assert pid(inst, "never") == nil
      assert drain([:alambique, :cell, :wake], "never") == []
      assert lease_keys(ctx) == []
    end

    test "that hibernated stays asleep", ctx do
      inst = start(ctx)
      assert {:ok, _} = wake(inst, "c")
      assert :ok = Alambique.hibernate(inst, "c")
      assert {:ok, :asleep} = refresh(inst, "c")
      assert pid(inst, "c") == nil
      assert [_] = drain([:alambique, :cell, :wake], "c")
    end

    test "a refresh in flight when the cell hibernates answers asleep", ctx do
      inst = start(ctx, cell_options_timeout_ms: 400)
      assert {:ok, _} = wake(inst, "c")
      test = self()

      Callbacks.set(fn _ ->
        send(test, {:called, self()})
        Process.sleep(:infinity)
      end)

      task = Task.async(fn -> refresh(inst, "c") end)
      assert_receive {:called, worker}, 5_000
      assert :ok = Alambique.hibernate(inst, "c")
      assert {:ok, :asleep} = await(task)
      assert pid(inst, "c") == nil
      assert [_] = drain([:alambique, :cell, :wake], "c")
      eventually(fn -> not Process.alive?(worker) end)
    end
  end
end
