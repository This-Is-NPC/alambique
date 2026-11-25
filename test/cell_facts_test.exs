defmodule Alambique.CellFactsTest do
  @moduledoc """
  The facts a cell publishes in its load array are read from another process,
  through `cells(details: true)` and `facts/2`, without calling the cell
  (design §7.3).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Admission, Config}
  alias Alambique.Cell.{Facts, Local}
  alias AlambiqueTestkit.CrashPoints

  @moduletag :capture_log

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @write "INSERT INTO t (v) VALUES ('x')"
  @fill "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 4000) " <>
          "INSERT INTO t SELECT n, hex(randomblob(100)) FROM c"
  @scan "SELECT count(v) FROM t"

  # Every fact the cell writes when it starts serving.
  @slot_facts ~w(txid acked_txid batches_in_flight parked feed_published feed_sent
                 feed_cleaned feed_backlog feed_capped feed_reader_open lease_epoch
                 lease_expires_at cache hydrating cache_used schema_used stmt_used
                 lookaside_used process_bytes awake_since awake_since_mono)a

  @publish_order ~w(txid acked_txid batches_in_flight parked feed_published feed_sent
                    feed_cleaned feed_backlog feed_capped feed_reader_open lease_epoch
                    lease_expires_at cache hydrating cache_used schema_used stmt_used
                    lookaside_used process_bytes)a

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  defp facts!(inst, cell) do
    assert {:ok, facts} = Alambique.facts(inst, cell)
    facts
  end

  defp entry(inst, cell) do
    inst
    |> Alambique.cells(details: true, limit: 1_000)
    |> Map.fetch!(:cells)
    |> Enum.find(&(&1.id == cell))
  end

  # The listing follows the registry by a message.
  defp entry!(inst, cell), do: eventually(fn -> entry(inst, cell) end)

  defp pid(inst, cell), do: Alambique.Cell.whereis(Config.fetch!(inst), cell)
  defp repl_txid(inst, cell), do: in_cell(pid(inst, cell), & &1.repl.txid)

  # The facts once no batch is in flight.
  defp settled(inst, cell) do
    eventually(fn ->
      f = facts!(inst, cell)
      f[:batches_in_flight] == 0 and f.acked_txid == f.txid and f
    end)
  end

  # Holds the process that reaches `point` for `cell` until the returned
  # function is called.
  defp hold(point, cell) do
    gate = :atomics.new(1, [])

    CrashPoints.arm(point, fn _, _ -> await_gate(gate) end,
      times: :infinity,
      when: &match?(%{cell: ^cell}, &1)
    )

    fn -> :atomics.put(gate, 1, 1) end
  end

  defp await_gate(gate, left \\ 6_000) do
    if :atomics.get(gate, 1) == 0 and left > 0 do
      Process.sleep(5)
      await_gate(gate, left - 1)
    end
  end

  describe "the commit pipeline" do
    test "a write held in its upload shows in txid, acked_txid, batches_in_flight and pending",
         env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      base = settled(inst, cell)
      assert base.txid > 0

      release = hold(:repl_after_put, cell)
      write = Task.async(fn -> Alambique.execute(inst, cell, @write) end)

      held =
        eventually(fn ->
          f = facts!(inst, cell)
          f.batches_in_flight == 1 and f.txid > base.txid and f
        end)

      assert held.acked_txid == base.acked_txid
      assert held.txid == repl_txid(inst, cell)
      assert held.pending == 1
      assert entry!(inst, cell).pending == 1

      release.()
      assert {:ok, _} = await(write)
      done = settled(inst, cell)
      assert done.acked_txid == held.txid
      assert done.pending == 0
    end

    test "acked_txid is never above txid while writes and reads run together", env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      done = :atomics.new(1, [])

      pollers =
        for _ <- 1..2 do
          Task.async(fn -> poll_order(inst, cell, done, 0, 0) end)
        end

      1..8
      |> Task.async_stream(
        fn _ -> for _ <- 1..40, do: {:ok, _} = Alambique.execute(inst, cell, @write) end,
        timeout: 60_000
      )
      |> Stream.run()

      :atomics.put(done, 1, 1)

      for {reads, above} <- Enum.map(pollers, &await/1) do
        assert reads > 0
        assert above == 0
      end
    end

    # The window of a wrong order is a few instructions wide, so a run only
    # sometimes sees it; the two traces below pin the order itself.
    defp poll_order(inst, cell, done, reads, above) do
      if :atomics.get(done, 1) == 1 do
        {reads, above}
      else
        e = entry(inst, cell)
        bad = if e.txid != nil and e.acked_txid > e.txid, do: 1, else: 0
        poll_order(inst, cell, done, reads + 1, above + bad)
      end
    end

    test "the cell writes the newest TXID first and the memory last", env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      settled(inst, cell)
      cell_pid = pid(inst, cell)

      trace(Facts, :put, 3, cell_pid)
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      puts = traced(cell_pid, :put, fn names -> :process_bytes in names end)

      # The last group is the publish after the call, with the memory.
      last = puts |> Enum.with_index() |> Enum.filter(&(elem(&1, 0) == :txid)) |> List.last()
      assert Enum.drop(puts, elem(last, 1)) == @publish_order
    end

    test "a reader reads the acknowledged TXID before the newest one", env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      settled(inst, cell)

      reader =
        Task.async(fn ->
          receive do
            :go -> Alambique.cells(inst, details: true)
          end
        end)

      trace(Facts, :get, 2, reader.pid)
      send(reader.pid, :go)
      await(reader)

      gets = traced(reader.pid, :get, fn names -> :txid in names and :acked_txid in names end)
      assert Enum.find_index(gets, &(&1 == :acked_txid)) < Enum.find_index(gets, &(&1 == :txid))
    end

    # Calls of `module.fun/arity` made by `pid` reach this process as trace messages.
    defp trace(module, fun, arity, pid) do
      :erlang.trace_pattern({module, fun, arity}, true, [:local])
      :erlang.trace(pid, true, [:call])

      on_exit(fn ->
        :erlang.trace_pattern({module, fun, arity}, false, [:local])
      end)
    end

    # The first argument after the load of each traced call, in order, until
    # `done` accepts them.
    defp traced(pid, fun, done, acc \\ []) do
      receive do
        {:trace, ^pid, :call, {_module, ^fun, [_load, name | _]}} ->
          acc = acc ++ [name]
          if done.(acc), do: acc, else: traced(pid, fun, done, acc)
      after
        30_000 -> flunk("no trace of #{fun}")
      end
    end
  end

  describe "the txid" do
    test "a wake from a new cell, the local file and the store gives the source and the TXID",
         env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      config = Config.fetch!(inst)
      cell = unique_cell()

      before = System.system_time(:millisecond)
      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      first = settled(inst, cell)
      assert first.wake_source == :empty
      assert first.txid == repl_txid(inst, cell)
      assert first.wake_started_at >= before
      assert first.awake_since >= first.wake_started_at
      assert first.last_used >= first.awake_since_mono
      assert first.awake_since_mono >= first.wake_started_at_mono

      assert :ok = Alambique.hibernate(inst, cell)
      eventually(fn -> Alambique.owner(inst, cell) == :none end)
      assert {:ok, _} = Alambique.touch(inst, cell)
      local = facts!(inst, cell)
      assert local.wake_source == :local
      assert local.txid == repl_txid(inst, cell)
      assert local.txid >= first.txid
      assert local.awake_since >= first.awake_since

      assert :ok = Alambique.hibernate(inst, cell)
      eventually(fn -> Alambique.owner(inst, cell) == :none end)
      File.rm_rf!(Local.dir(config, cell))
      assert {:ok, _} = Alambique.touch(inst, cell)
      restored = facts!(inst, cell)
      assert restored.wake_source == :restore
      assert restored.txid == repl_txid(inst, cell)
      assert restored.txid >= local.txid
    end
  end

  describe "a waking cell" do
    test "has its start and its process but no other fact", env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      release = hold(:cell_before_take_over, cell)
      touch = Task.async(fn -> Alambique.touch(inst, cell) end)

      waking = eventually(fn -> entry(inst, cell) end)
      assert is_pid(waking.pid)
      assert waking.serving
      assert is_integer(waking.wake_started_at)
      assert is_integer(waking.wake_started_at_mono)
      assert waking.wake_source == nil
      assert waking.last_used == nil

      for key <- @slot_facts, do: assert(Map.fetch!(waking, key) == nil, "#{key} is not nil")

      assert {:ok, %{local_process: true, txid: nil, cache_used: nil}} =
               Alambique.facts(inst, cell)

      release.()
      assert {:ok, _} = await(touch)
      awake = facts!(inst, cell)
      assert is_integer(awake.txid) and is_integer(awake.awake_since)
    end
  end

  describe "serving" do
    test "is false from the moment the cell stops serving until it ends", env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      cell_pid = pid(inst, cell)
      assert entry!(inst, cell).serving
      assert Alambique.Cell.serving(Config.fetch!(inst), cell) == cell_pid

      id = {__MODULE__, make_ref()}
      test = self()

      :telemetry.attach(
        id,
        [:alambique, :cell, :hibernate],
        fn _event, _measurements, meta, _config ->
          if meta.cell == cell do
            send(test, {:stopping, self()})

            receive do
              :go -> :ok
            after
              30_000 -> :ok
            end
          end
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(id) end)

      # A shutdown hibernates the cell from `terminate/2`, after it stopped serving.
      stopper = Task.async(fn -> GenServer.stop(cell_pid, :shutdown, 30_000) end)
      assert_receive {:stopping, ^cell_pid}, 30_000

      refute entry(inst, cell).serving
      assert {:ok, %{serving: false, local_process: true}} = Alambique.facts(inst, cell)
      assert Alambique.Cell.serving(Config.fetch!(inst), cell) == nil

      send(cell_pid, :go)
      assert :ok = await(stopper)
    end
  end

  describe "the lease" do
    test "the epoch and the expiry follow the renewals", env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      assert {:ok, _} = Alambique.touch(inst, cell)
      first = facts!(inst, cell)
      assert {:ok, %{epoch: epoch}} = Alambique.lease(inst, cell)
      assert first.lease_epoch == epoch

      # The renewal is a timer message of the cell, not a call.
      renewed =
        eventually(fn ->
          (f = facts!(inst, cell)).lease_expires_at > first.lease_expires_at and f
        end)

      assert in_cell(pid(inst, cell), & &1.lease.expires_at_ms) >= renewed.lease_expires_at
    end
  end

  describe "the page cache" do
    test "the shrink and the restore show in cache and in the bytes", env do
      inst =
        start_instance(env,
          migrations: @schema,
          awake: [cache_kib: 512, idle_cache_after_ms: 1_000, idle_cache_kib: 16],
          idle_timeout_ms: 60_000
        )

      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @fill)
      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)
      full = facts!(inst, cell)
      assert full.cache == :full
      assert full.cache_used > 300_000

      shrunk = eventually(fn -> (f = facts!(inst, cell)).cache == :shrunk and f end)
      assert shrunk.cache_used < full.cache_used / 2

      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)
      restored = facts!(inst, cell)
      assert restored.cache == :full
      assert restored.cache_used > shrunk.cache_used
    end
  end

  describe "the memory" do
    test "is measured after a call, by component, and the process by itself", env do
      inst = start_instance(env, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @fill)
      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)

      f = facts!(inst, cell)
      assert f.cache_used > 100_000
      assert f.schema_used > 0
      assert is_integer(f.stmt_used) and f.stmt_used >= 0
      assert is_integer(f.lookaside_used) and f.lookaside_used >= 0
      assert f.process_bytes > 0
    end

    test "is nil with track_bytes: false", env do
      inst = start_instance(env, migrations: @schema, awake: [track_bytes: false])
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @fill)
      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)

      for f <- [facts!(inst, cell), entry!(inst, cell)] do
        for key <- ~w(cache_used schema_used stmt_used lookaside_used process_bytes)a,
            do: assert(Map.fetch!(f, key) == nil, "#{key} is not nil")

        # The other facts are not behind the option.
        assert is_integer(f.txid)
      end
    end

    test "a budget measures it whatever track_bytes says, and admission keeps the same sum",
         env do
      inst =
        start_instance(env,
          migrations: @schema,
          awake: [track_bytes: false, max_bytes: 1_000_000_000]
        )

      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @fill)
      assert {:ok, %{rows: [[4000]]}} = Alambique.query(inst, cell, @scan)

      eventually(fn ->
        f = facts!(inst, cell)

        is_integer(f.cache_used) and f.cache_used > 100_000 and
          Admission.bytes(Config.fetch!(inst)) == f.cache_used + f.schema_used + f.stmt_used
      end)
    end
  end

  describe "facts/2" do
    test "gives the entry of cells/2 and the local process", env do
      inst = start_instance(env, migrations: @schema)
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      f = settled(inst, cell)

      e = entry!(inst, cell)
      assert f.local_process
      assert f.pid == pid(inst, cell)
      assert f.id == cell
      assert f.serving and f.wake_backoff == nil
      assert f.local_file_bytes > 0
      assert is_integer(f.wal_bytes)
      assert Map.keys(e) -- Map.keys(f) == []
      assert e.pid == f.pid and e.serving
      assert {:ok, %{pending: pending}} = Alambique.load(inst, cell)
      assert e.pending == pending
    end

    test "for a cell with no process gives its file and its wake backoff", env do
      inst = start_instance(env, migrations: @schema, compaction: false)
      config = Config.fetch!(inst)
      cell = unique_cell()

      assert {:ok, %{id: ^cell, local_process: false} = f} = Alambique.facts(inst, cell)
      assert f.local_file_bytes == nil and f.wake_backoff == nil

      refute Map.has_key?(f, :txid)

      assert {:ok, _} = Alambique.execute(inst, cell, @write)
      assert :ok = Alambique.hibernate(inst, cell)
      eventually(fn -> Alambique.owner(inst, cell) == :none end)

      assert {:ok, %{local_process: false, local_file_bytes: size}} = Alambique.facts(inst, cell)
      assert size > 0
      assert size == File.stat!(Local.db_path(config, cell)).size

      # The failure a wake leaves behind, while its backoff lasts.
      until = System.monotonic_time(:millisecond) + 60_000
      :ets.insert(config.admission, {{:failed, cell}, :boom, until, 3})

      assert {:ok, %{wake_backoff: %{until: ^until, attempts: 3, reason: :boom}}} =
               Alambique.facts(inst, cell)

      :ets.insert(config.admission, {{:failed, cell}, :boom, until - 120_000, 3})
      assert {:ok, %{wake_backoff: nil}} = Alambique.facts(inst, cell)

      assert {:error, :invalid_cell_id} = Alambique.facts(inst, "a/b")
    end

    test "never wakes the cell", env do
      inst = start_instance(env, migrations: @schema)
      cell = unique_cell()
      assert {:ok, %{local_process: false}} = Alambique.facts(inst, cell)
      assert Alambique.cells(inst).count == 0
      assert Alambique.owner(inst, cell) == :none
    end
  end
end
