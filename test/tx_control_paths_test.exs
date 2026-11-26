defmodule Alambique.TxControlPathsTest do
  @moduledoc """
  Paths around a write (design §15): a transaction that never began, the
  paths of the cell that run transactions without a user write, a schema
  statement that starts with a byte order mark, a feed cleanup that runs
  behind a batch in flight, the connection once it is disarmed, and what a
  stopped cell leaves behind. The two tests tagged `:skip` assert behaviour
  that is not enforced yet.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Fault, Feed, Foreign, Sqlite, Tx}
  alias Alambique.Cell.Db
  alias Alambique.Replication.{Native, Replicator}
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @bom "﻿"
  @fail {__MODULE__, :fail}

  # Migrations of a cell that a test changes while the instance runs.
  defmodule Migrations do
    @moduledoc false
    @key {__MODULE__, :list}
    def set(list), do: :persistent_term.put(@key, list)
    def list(_cell), do: :persistent_term.get(@key)
  end

  # A store whose put is a request on the wire: once sent, it lands when the
  # gate lets it, whether or not the process that sent it is still alive.
  defmodule WireStore do
    @moduledoc false
    @behaviour Alambique.Store
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, :ok)
    def wrap(wire, {_m, _c} = inner), do: {__MODULE__, %{inner: inner, wire: wire}}
    def hold(wire), do: GenServer.call(wire, :hold)
    def held(wire), do: GenServer.call(wire, :held)
    def open(wire), do: GenServer.call(wire, :open)

    @impl Alambique.Store
    def put(%{wire: wire, inner: inner}, key, body, condition, meta),
      do: GenServer.call(wire, {:put, inner, key, body, condition, meta}, :infinity)

    @impl Alambique.Store
    def location(%{inner: inner}), do: Alambique.Store.location(inner)
    @impl Alambique.Store
    def get(%{inner: {m, c}}, key), do: m.get(c, key)
    @impl Alambique.Store
    def get_range(%{inner: {m, c}}, key, offset, length), do: m.get_range(c, key, offset, length)
    @impl Alambique.Store
    def head(%{inner: {m, c}}, key), do: m.head(c, key)
    @impl Alambique.Store
    def list(%{inner: {m, c}}, prefix, after_, limit), do: m.list(c, prefix, after_, limit)
    @impl Alambique.Store
    def delete(%{inner: {m, c}}, key), do: m.delete(c, key)

    @impl GenServer
    def init(:ok), do: {:ok, %{holding: false, held: []}}

    @impl GenServer
    def handle_call(:hold, _from, state), do: {:reply, :ok, %{state | holding: true}}
    def handle_call(:held, _from, state), do: {:reply, length(state.held), state}

    def handle_call(:open, _from, state) do
      Enum.each(state.held, &apply_put/1)
      {:reply, :ok, %{state | holding: false, held: []}}
    end

    def handle_call({:put, _, key, _, _, _} = put, from, state) do
      if state.holding and String.contains?(key, "/0000/") do
        {:noreply, %{state | held: state.held ++ [{from, put}]}}
      else
        apply_put({from, put})
        {:noreply, state}
      end
    end

    defp apply_put({from, {:put, {m, c}, key, body, condition, meta}}),
      do: GenServer.reply(from, m.put(c, key, body, condition, meta))
  end

  setup do
    install_commit_behind_back()

    on_exit(fn ->
      Fault.uninstall()
      :persistent_term.erase(@fail)
    end)

    setup_store()
  end

  # Faults of the transaction of the library, armed through `@fail`:
  # `{n, :query_only}` makes the pragma after the n-th commit fail, and
  # `{1, :rollback}` makes the next rollback fail. With `cleanup:` the feed
  # cleanup arms it itself, as its own transaction begins.
  defp install_faults(opts \\ []) do
    cleanup = Keyword.get(opts, :cleanup)
    notify = Keyword.get(opts, :notify)

    Fault.install(fn
      :feed_before_cleanup, _ when cleanup != nil ->
        :persistent_term.put(@fail, {1, cleanup})
        if cleanup == :rollback, do: raise("injected")
        :ok

      :db_before_commit, %{conn: conn} ->
        case :persistent_term.get(@fail, nil) do
          {1, :query_only} ->
            :persistent_term.erase(@fail)
            if notify, do: send(notify, :faulted)
            Native.fail_next_query_only(conn)

          {n, :query_only} when n > 1 ->
            :persistent_term.put(@fail, {n - 1, :query_only})

          _ ->
            :ok
        end

      :db_before_rollback, _conn ->
        if :persistent_term.get(@fail, nil) == {1, :rollback} do
          :persistent_term.erase(@fail)
          if notify, do: send(notify, :faulted)
          {:error, "injected"}
        else
          :ok
        end

      _, _ ->
        :ok
    end)
  end

  defp boot(env, mode, opts \\ []) do
    inst =
      start_instance(
        env,
        Keyword.merge([migrations: @schema, sql: [orphan_frames: mode], compaction: false], opts)
      )

    cell = unique_cell()
    for n <- [0, 1], do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (#{n})")
    %{inst: inst, cell: cell, env: env, mode: mode}
  end

  defp pid(%{inst: inst, cell: cell}), do: Cell.whereis(Config.fetch!(inst), cell)
  defp state(c), do: :sys.get_state(pid(c))
  defp path(c), do: state(c).repl.db_path
  defp unreplicated?(c), do: Replicator.unreplicated?(state(c).repl)

  defp rows(c, table \\ "t") do
    {:ok, %{rows: rows}} = Alambique.query(c.inst, c.cell, "SELECT n FROM #{table} ORDER BY n")
    List.flatten(rows)
  end

  defp store(c, table \\ "t"),
    do: c.env |> durable_rows(c.cell, "SELECT n FROM #{table} ORDER BY n") |> List.flatten()

  defp asleep?(c, bound \\ 30_000),
    do: eventually(fn -> Alambique.owner(c.inst, c.cell) == :none end, bound)

  defp feed_ctx(c), do: Feed.ctx(store: c.env.store, prefix: "cells-test", settle_ms: 0)

  # The feed rows of one table of the cell, as read now.
  defp feed_now(c, table \\ "t") do
    {:ok, records, _} = Feed.read(feed_ctx(c), nil, 10_000)
    for r <- records, r.cell == c.cell, r.table == table, do: r.key["n"]
  end

  # The feed rows of `table` once the feed holds `last` of `wait_table`.
  defp feed_after(c, wait_table, last, table, bound \\ 30_000) do
    eventually(fn -> if last in feed_now(c, wait_table), do: feed_now(c, table) end, bound)
  end

  # `eventually/2` as a boolean.
  defp reached?(fun, bound) do
    eventually(fun, bound)
    true
  rescue
    RuntimeError -> false
  end

  defp feed_keys(c, last, bound \\ 30_000), do: feed_after(c, "t", last, "t", bound)

  # Committed on the connection of the cell outside any write.
  defp orphan(c, n) do
    :ok = in_cell(pid(c), &Db.writable_script(&1.conn, "INSERT INTO t VALUES (#{n})"))
    assert unreplicated?(c)
  end

  describe "a write whose BEGIN fails" do
    for mode <- [:stop, :replicate] do
      test "is a plain error and the cell keeps serving (#{mode})", env do
        c = boot(env, unquote(mode))
        cell_pid = pid(c)
        {:ok, other} = Foreign.open(path(c))
        :ok = Sqlite.execute(other, "BEGIN IMMEDIATE")

        result = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (7)")

        :ok = Sqlite.execute(other, "ROLLBACK")
        Sqlite.close(other)

        assert {:error, reason} = result
        refute match?({:committed, _}, reason), inspect(result)
        assert reason != :orphan_frames, inspect(result)
        assert pid(c) == cell_pid, "the cell stopped: #{inspect(result)}"
        assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (8)")
        assert rows(c) == [0, 1, 8]
        assert store(c) == [0, 1, 8]
      end
    end
  end

  test "stamping the feed rows while another connection writes is an error, not a crash", env do
    c = boot(env, :stop)
    # Asleep: while the cell holds its file, a second connection of the
    # library on it is refused.
    file = path(c)
    assert Alambique.hibernate(c.inst, c.cell) == :ok
    assert asleep?(c)
    {:ok, mine} = Db.open(file)
    {:ok, other} = Foreign.open(file)
    :ok = Sqlite.execute(other, "BEGIN IMMEDIATE")

    result = Alambique.Feed.Capture.stamp(mine, 99, 0)

    :ok = Sqlite.execute(other, "ROLLBACK")
    Sqlite.close(other)
    Sqlite.close(mine)
    assert {:error, _} = result
  end

  test "a schema statement after SQLite ended the transaction is an error, not a raise", env do
    c = boot(env, :stop)
    test = self()

    result =
      Alambique.transaction(c.inst, c.cell, fn tx ->
        {:error, _} = Tx.execute(tx, "INSERT OR ROLLBACK INTO t VALUES (0)")
        send(test, {:inner, Tx.script(tx, "CREATE TABLE u (n)"), Tx.execute(tx, "DROP TABLE t")})
        :done
      end)

    assert_received {:inner, script, execute}
    assert script == {:error, :transaction_ended}
    assert execute == {:error, :transaction_ended}
    assert result == {:error, :transaction_ended}
    assert rows(c) == [0, 1]
  end

  # The cell must not go on serving with a connection that may write or a
  # transaction that stayed open: it stops at once. What a caller gets from a
  # cell that did not stop is reported.
  defp assert_stops(c, old, what) do
    # A call to a cell that is gone wakes another one, so the old one is asked.
    probe = if Process.alive?(old), do: probe(c)
    refute match?({:ok, _}, probe), "an INSERT through a read succeeded after #{what}"

    eventually(fn -> not Process.alive?(old) end, 3_000)
    :ok
  rescue
    error in RuntimeError ->
      flunk(
        "the cell kept serving after #{what} (#{error.message}). An INSERT through a read " <>
          "gave #{inspect(probe(c))} and a write gave #{inspect(probe_write(c))}"
      )
  end

  defp probe(c) do
    Alambique.query(c.inst, c.cell, "INSERT INTO t VALUES (99)")
  catch
    :exit, reason -> {:exit, reason}
  end

  defp probe_write(c) do
    Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (98)")
  catch
    :exit, reason -> {:exit, reason}
  end

  # What a stop leaves: the store as before the failed step, nothing durable
  # that the caller was not told, and a next wake that serves and feeds the
  # same rows.
  # The probe of `assert_stops/3` can reach a cell that is stopping, and the
  # router then wakes another one: a test that has no such race also asks for
  # a cell that is asleep.
  defp assert_clean_wake(c, asleep? \\ true) do
    if asleep?, do: assert(asleep?(c))
    assert store(c) == [0, 1]
    assert rows(c) == [0, 1]
    assert feed_keys(c, 1) == [0, 1]
  end

  describe "the feed cleanup transaction" do
    for mode <- [:stop, :replicate],
        {kind, what} <- [query_only: "a failed query_only pragma", rollback: "a failed rollback"] do
      test "stops the cell on #{what} (#{mode})", env do
        install_faults(cleanup: unquote(kind), notify: self())
        c = boot(env, unquote(mode), feed: [cleanup: :idle, cleanup_after_ms: 100])
        old = pid(c)
        assert_receive :faulted, 5_000
        assert_stops(c, old, unquote(what))
        Fault.uninstall()
        assert_clean_wake(c, false)
      end
    end
  end

  # DDL that bypasses the capture: the triggers are installed afterwards by a
  # transaction of their own.
  def raw_table(tx), do: :ok = Sqlite.execute(tx.conn, "CREATE TABLE u (n INTEGER PRIMARY KEY)")

  describe "a migration of a serving cell" do
    @v2_ok {2, "CREATE TABLE u (n INTEGER PRIMARY KEY)"}
    @v2_bad {2, "ALTER TABLE missing ADD COLUMN z INTEGER"}

    defp migrating(env, mode \\ :stop) do
      Migrations.set(@schema)
      on_exit(fn -> :persistent_term.erase({Migrations, :list}) end)
      boot(env, mode, migrations: {Migrations, :list, []})
    end

    for mode <- [:stop, :replicate] do
      test "stops the cell when its query_only pragma fails (#{mode})", env do
        install_faults()
        c = migrating(env, unquote(mode))
        Migrations.set(@schema ++ [@v2_ok])
        :persistent_term.put(@fail, {1, :query_only})

        old = pid(c)
        result = Alambique.migrate(c.inst, c.cell)
        assert match?({:error, _}, result), inspect(result)
        assert_stops(c, old, "a failed query_only pragma in a migration: #{inspect(result)}")
        Fault.uninstall()
        Migrations.set(@schema)
        assert_clean_wake(c)
      end

      test "stops the cell when the rollback of a failed migration fails (#{mode})", env do
        install_faults()
        c = migrating(env, unquote(mode))
        Migrations.set(@schema ++ [@v2_bad])
        :persistent_term.put(@fail, {1, :rollback})

        old = pid(c)
        result = Alambique.migrate(c.inst, c.cell)
        assert match?({:error, _}, result), inspect(result)
        assert_stops(c, old, "a failed rollback in a migration: #{inspect(result)}")
        Fault.uninstall()
        Migrations.set(@schema)
        assert_clean_wake(c)
      end
    end

    test "stops the cell when the query_only pragma of the trigger install fails", env do
      install_faults()
      c = migrating(env)
      Migrations.set(@schema ++ [{2, &raw_table/1}])
      # The second commit of the call: the first is the migration piece.
      :persistent_term.put(@fail, {2, :query_only})

      old = pid(c)
      result = Alambique.migrate(c.inst, c.cell)
      assert match?({:error, _}, result), inspect(result)

      assert_stops(
        c,
        old,
        "a failed query_only pragma in the trigger install: #{inspect(result)}"
      )
    end
  end

  describe "a wake that migrates" do
    for {name, fail, list} <- [
          {"the query_only pragma of the migration piece fails", {1, :query_only}, :good},
          {"the query_only pragma of the trigger install fails", {2, :query_only}, :raw},
          {"the rollback of a failed migration fails", {1, :rollback}, :bad}
        ] do
      test "leaves nothing serving when #{name}", env do
        install_faults()

        Migrations.set(
          case unquote(list) do
            :good -> @schema
            :raw -> [{1, &raw_table/1}]
            :bad -> [{1, "ALTER TABLE missing ADD COLUMN z INTEGER"}]
          end
        )

        on_exit(fn -> :persistent_term.erase({Migrations, :list}) end)
        inst = start_instance(env, migrations: {Migrations, :list, []}, compaction: false)
        cell = unique_cell()
        :persistent_term.put(@fail, unquote(fail))

        result = Alambique.execute(inst, cell, "SELECT 1")
        assert {:error, _} = result

        assert Alambique.owner(inst, cell) == :none,
               "serving after the wake gave #{inspect(result)}"
      end
    end
  end

  describe "a migration whose feed rows cannot be stamped" do
    # Holds the write lock of the file from a second connection once the
    # entry of `version` is in the schema index: the migration is committed
    # and the stamp comes next. The test releases it with `unlock/1`.
    defp lock_before_stamp(env, cell, version) do
      test = self()
      once = {__MODULE__, :locked}
      :persistent_term.erase(once)

      Fault.install(fn
        :schema_after_put, %{cell: ^cell, version: ^version} ->
          if :persistent_term.get(once, nil) == nil do
            :persistent_term.put(once, true)
            [path] = Path.wildcard(Path.join(env.dir, "instance_*/**/*.db"))
            parent = self()
            holder = spawn(fn -> hold(path, parent, test) end)
            assert_receive {:held, ^holder}, 5_000
            send(test, {:locked, holder})
          end

          :ok

        _, _ ->
          :ok
      end)

      on_exit(fn -> :persistent_term.erase(once) end)
    end

    # A second connection that holds the write lock until it is told to leave.
    defp hold(path, parent, test) do
      {:ok, other} = Foreign.open(path)
      :ok = Sqlite.execute(other, "BEGIN IMMEDIATE")
      send(parent, {:held, self()})

      receive do
        :unlock ->
          :ok = Sqlite.execute(other, "ROLLBACK")
          Sqlite.close(other)
          send(test, {:released, self()})
      end
    end

    defp unlock do
      assert_received {:locked, holder}
      send(holder, :unlock)
      assert_receive {:released, ^holder}, 5_000
      Fault.uninstall()
    end

    # A wake of another cell takes the only slot, so it proves the slot back.
    defp assert_released(inst, cell) do
      assert Alambique.owner(inst, cell) == :none
      assert {:ok, _} = Alambique.touch(inst, unique_cell(), timeout: 5_000)
      {t, result} = :timer.tc(fn -> Alambique.execute(inst, cell, "INSERT INTO t VALUES (5)") end)
      assert {:ok, _} = result
      assert t < 2_000_000, "the lease was not released: the wake waited #{div(t, 1000)} ms"
    end

    test "fails the first wake and releases the lease and the slot", env do
      cell = unique_cell()
      lock_before_stamp(env, cell, 1)
      inst = start_instance(env, migrations: @schema, compaction: false, awake: [max_wakes: 1])

      result = Alambique.execute(inst, cell, "INSERT INTO t VALUES (5)")
      unlock()

      assert {:error, {:stamp_failed, _}} = result
      assert_released(inst, cell)
      c = %{inst: inst, cell: cell, env: env}
      assert store(c) == [5]
      assert rows(c) == [5]
      assert feed_keys(c, 5) == [5]
    end

    test "stops a serving cell that migrates", env do
      Migrations.set(@schema)
      on_exit(fn -> :persistent_term.erase({Migrations, :list}) end)
      c = boot(env, :stop, migrations: {Migrations, :list, []}, awake: [max_wakes: 1])
      Migrations.set(@schema ++ [@v2_ok])
      lock_before_stamp(env, c.cell, 2)
      old = pid(c)

      result = Alambique.migrate(c.inst, c.cell)
      unlock()

      assert {:error, {:migrate_failed, {:stamp_failed, _}}} = result
      assert_stops(c, old, "a stamp that failed in a migration: #{inspect(result)}")
      assert asleep?(c)
      assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO u VALUES (1)")
      assert store(c) == [0, 1]
      assert rows(c) == [0, 1]
      assert feed_keys(c, 1) == [0, 1]
    end
  end

  describe "a stamp that fails on a batch" do
    # Holds the write lock of the file from a second connection at the fault
    # point `point`, once: the stamp that follows finds the file busy.
    defp lock_at(env, point) do
      test = self()

      Fault.install(fn
        ^point, _ ->
          if :persistent_term.get(@fail, nil) == nil do
            :persistent_term.put(@fail, :locked)
            [file] = Path.wildcard(Path.join(env.dir, "instance_*/**/*.db"))
            me = self()
            holder = spawn(fn -> hold(file, me, test) end)
            assert_receive {:held, ^holder}, 5_000
            send(test, {:locked, holder})
          end

          :ok

        _, _ ->
          :ok
      end)
    end

    for mode <- [:stop, :replicate] do
      test "an ordinary write answers :stamp_failed and stops the cell (#{mode})", env do
        c = boot(env, unquote(mode))
        old = pid(c)
        lock_at(env, :cell_after_commit)

        result = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (5)")
        unlock()

        assert {:error, {:stamp_failed, _}} = result
        assert_stops(c, old, "a stamp that failed on a batch: #{inspect(result)}")
        assert_clean_wake(c, false)
        assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (6)")
        assert store(c) == [0, 1, 6]
        assert feed_keys(c, 6) == [0, 1, 6]
      end
    end

    test "the batch of orphan frames answers :stamp_failed and stops the cell", env do
      c = boot(env, :replicate)
      old = pid(c)
      orphan(c, 5)
      [file] = Path.wildcard(Path.join(env.dir, "instance_*/**/*.db"))
      test = self()
      holder = spawn(fn -> hold(file, test, test) end)
      assert_receive {:held, ^holder}, 5_000
      send(test, {:locked, holder})

      result = Alambique.position(c.inst, c.cell)
      unlock()

      assert {:error, {:stamp_failed, _}} = result
      assert_stops(c, old, "a stamp that failed on the orphan batch: #{inspect(result)}")
      assert_clean_wake(c, false)
      assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (6)")
      assert store(c) == [0, 1, 6]
      assert feed_keys(c, 6) == [0, 1, 6]
    end
  end

  describe "a statement that starts with a byte order mark" do
    defp bom_feed(c, ddl_fun) do
      assert {:ok, _} = Alambique.transaction(c.inst, c.cell, ddl_fun)
      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO u VALUES (5)")
      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (6)")
      feed_after(c, "t", 6, "u")
    end

    test "that creates a table through Tx.execute keeps the table in the feed", env do
      c = boot(env, :stop)

      keys =
        bom_feed(c, fn tx ->
          {:ok, _} = Tx.execute(tx, @bom <> "CREATE TABLE u (n INTEGER PRIMARY KEY)")
        end)

      assert keys == [5]
    end

    test "that creates a table through Tx.script keeps the table in the feed", env do
      c = boot(env, :stop)

      keys =
        bom_feed(c, fn tx ->
          :ok = Tx.script(tx, @bom <> "CREATE TABLE u (n INTEGER PRIMARY KEY)")
        end)

      assert keys == [5]
    end

    test "that creates a table in a migration piece keeps the table in the feed", env do
      Migrations.set(@schema)
      on_exit(fn -> :persistent_term.erase({Migrations, :list}) end)
      c = boot(env, :stop, migrations: {Migrations, :list, []})
      Migrations.set(@schema ++ [{2, @bom <> "CREATE TABLE u (n INTEGER PRIMARY KEY)"}])
      assert {:ok, 2} = Alambique.migrate(c.inst, c.cell)
      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO u VALUES (5)")
      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (6)")
      assert feed_after(c, "t", 6, "u") == [5]
    end

    test "is recognised as schema by Sqlite.ddl?", _env do
      assert Sqlite.ddl?(@bom <> "CREATE TABLE u (n)")
      assert Sqlite.ddl?(@bom <> "-- c\nALTER TABLE u ADD COLUMN x")
      refute Sqlite.ddl?(@bom <> "INSERT INTO u VALUES (1)")
    end

    # Every prefix that SQLite skips, in each way a statement can reach the cell.
    for {name, prefix} <- [
          blanks: " \t\r\n\f ",
          line_comment: "-- note\n",
          block_comment: "/* note */",
          mixed: "\uFEFF /* a */ -- b\n\uFEFF\n"
        ] do
      test "after #{name} is captured through Tx.execute, Tx.script and a migration", env do
        prefix = unquote(prefix)
        c = boot(env, :stop)

        keys =
          bom_feed(c, fn tx ->
            {:ok, _} = Tx.execute(tx, prefix <> "CREATE TABLE u (n INTEGER PRIMARY KEY)")
          end)

        assert keys == [5]

        c = boot(env, :stop)

        keys =
          bom_feed(c, fn tx ->
            :ok = Tx.script(tx, prefix <> "CREATE TABLE u (n INTEGER PRIMARY KEY)")
          end)

        assert keys == [5]

        Migrations.set(@schema)
        on_exit(fn -> :persistent_term.erase({Migrations, :list}) end)
        c = boot(env, :stop, migrations: {Migrations, :list, []})
        Migrations.set(@schema ++ [{2, prefix <> "CREATE TABLE u (n INTEGER PRIMARY KEY)"}])
        assert {:ok, 2} = Alambique.migrate(c.inst, c.cell)
        {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO u VALUES (5)")
        {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (6)")
        assert feed_after(c, "t", 6, "u") == [5]
      end
    end
  end

  describe "a feed cleanup that follows a published batch" do
    # Rows of an acknowledged write wait on the gate on their way to the feed,
    # a second write is in flight with its batch held, and frames no write
    # made are in the WAL. Letting the rows reach the feed runs the cleanup
    # while the cell is not idle.
    defp cleanup_behind_batch(env, mode) do
      gate = start_supervised!(%{id: :gate, start: {GatedStore, :start_link, []}})
      store = GatedStore.wrap(gate, env.store)

      c = boot(%{env | store: store}, mode, feed: [cleanup: :after_publish])
      c = Map.merge(c, %{gate: gate, raw: env.store})

      eventually(fn ->
        s = state(c)

        s.feed.published > 0 and s.feed.published == s.feed.sent and s.feed.backlog == 0 and
          s.feed.cleaned == s.feed.published and s.pipe.queue == [] and s.pending == []
      end)

      published = state(c).feed.published
      GatedStore.hold(gate, ~r{/feed/})
      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
      GatedStore.hold(gate, ~r{/0000/})
      writer = Task.async(fn -> Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (3)") end)
      eventually(fn -> state(c).pipe.queue != [] end)
      orphan(c, 5)

      [feed_key] =
        eventually(fn ->
          if (held = GatedStore.held(gate)) != [], do: Enum.filter(held, &(&1 =~ "/feed/"))
        end)

      :ok = GatedStore.release(gate, feed_key)
      {Map.put(c, :published, published), writer}
    end

    for mode <- [:stop, :replicate] do
      test "judges frames no write made before its batch carries them (#{mode})", env do
        {c, writer} = cleanup_behind_batch(env, unquote(mode))

        # The cleanup has run, or the cell stopped before it.
        eventually(fn ->
          case pid(c) do
            nil ->
              true

            cell ->
              st = :sys.get_state(cell)
              st.feed.published > c.published and st.feed.cleaned == st.feed.published
          end
        end)

        # Before the batch in flight lands, the frames must not be in a batch
        # that nobody judged: under `:stop` the cell is already gone.
        if unquote(mode) == :stop do
          assert reached?(fn -> pid(c) == nil end, 2_000),
                 "the cell is still serving: the cleanup batch took the frames no write made"
        end

        GatedStore.open(c.gate)
        Task.shutdown(writer, 10_000)
        env = %{c.env | store: c.raw}
        c = %{c | env: env}

        case unquote(mode) do
          :stop ->
            assert asleep?(c, 5_000)
            refute 5 in store(c), "the cleanup batch made frames no write made durable"

          :replicate ->
            assert reached?(fn -> 5 in store(c) end, 5_000)

            assert reached?(fn -> 5 in feed_now(c) end, 5_000),
                   "row 5 is durable but never reaches the feed: #{inspect(feed_now(c))}"

            assert Enum.count(feed_now(c), &(&1 == 5)) == 1
        end
      end
    end
  end

  # What a caller can still do with the connection once the library has
  # disarmed it. Each route commits a row on the file of the cell that no
  # write carries.
  describe "after the protection ended" do
    defp write_by(c, :stale_handle) do
      {:ok, conn} = Alambique.transaction(c.inst, c.cell, fn tx -> tx.conn end)
      Sqlite.execute(conn, "PRAGMA query_only = OFF; INSERT INTO t VALUES (50)")
    end

    defp write_by(c, :second_connection) do
      {:ok, other} = Foreign.open(path(c))
      result = Sqlite.execute(other, "INSERT INTO t VALUES (50)")
      Sqlite.close(other)
      result
    end

    # A write, a checkpoint and a close with the handle of a finished write,
    # from the process of the cell itself, as a handler running there could.
    defp write_by(c, :inside) do
      {:ok, conn} = Alambique.transaction(c.inst, c.cell, fn tx -> tx.conn end)

      in_cell(pid(c), fn _state ->
        [
          Sqlite.execute(conn, "PRAGMA query_only = OFF; INSERT INTO t VALUES (50)"),
          Sqlite.checkpoint(conn),
          Sqlite.close(conn)
        ]
      end)
    end

    # A row that a second connection commits and checkpoints: a writer that
    # the library does not guard.
    defp write_by(c, :second_checkpoint) do
      {:ok, other} = Foreign.open(path(c))
      :ok = Sqlite.execute(other, "INSERT INTO t VALUES (50)")
      result = Sqlite.checkpoint(other)
      Sqlite.close(other)
      result
    end

    for mode <- [:stop, :replicate] do
      test "the handle of a write that returned cannot write later (#{mode})", env do
        c = boot(env, unquote(mode))
        assert {:error, _} = write_by(c, :stale_handle)
        assert rows(c) == [0, 1]
      end

      test "a checkpoint outside a write never leaves a row that the store lacks, answered as a cut (#{mode})",
           env do
        c = boot(env, unquote(mode))
        assert [{:error, _}, {:error, _}, {:error, _}] = write_by(c, :inside)
        position = Alambique.position(c.inst, c.cell)
        assert {:ok, _} = position
        assert rows(c) == store(c), "position gave #{inspect(position)} with the row visible"
        refute unreplicated?(c)
      end
    end

    # What the library does with a row that reached the file by a route it
    # does not guard: it is visible to reads, in no batch and no feed, until
    # the next cut or the idle backstop; after a checkpoint it is not even
    # found there.
    for route <- [:second_connection], mode <- [:stop, :replicate] do
      test "#{route}: the row is judged at the next cut (#{mode})", env do
        c = boot(env, unquote(mode))
        assert :ok = write_by(c, unquote(route))

        assert rows(c) == [0, 1, 50]
        assert store(c) == [0, 1]
        assert feed_now(c) == [0, 1]
        assert unreplicated?(c)

        position = Alambique.position(c.inst, c.cell)

        case unquote(mode) do
          :stop ->
            assert position == {:error, :orphan_frames}
            assert asleep?(c)
            assert store(c) == [0, 1]
            assert rows(c) == [0, 1]

          :replicate ->
            assert {:ok, _} = position
            assert store(c) == [0, 1, 50]
            assert feed_keys(c, 50) |> Enum.count(&(&1 == 50)) == 1
        end
      end
    end

    # The frames are gone from the WAL, so no batch can carry them: the cell
    # stops under both modes and the next wake restores what the store holds.
    for mode <- [:stop, :replicate] do
      test "checkpoint: the row is found at the next cut and gone at the next wake (#{mode})",
           env do
        c = boot(env, unquote(mode))
        :ok = write_by(c, :second_checkpoint)

        # The WAL is empty: only the wal-index header shows the writer.
        refute unreplicated?(c)
        assert rows(c) == [0, 1, 50]

        assert Alambique.position(c.inst, c.cell) == {:error, :orphan_frames}
        assert asleep?(c)
        assert store(c) == [0, 1]
        assert feed_now(c) == [0, 1]
        assert rows(c) == [0, 1]
      end
    end
  end

  describe "the token of the transaction" do
    # `Fault.install/1` is public and global, so what the point before the
    # commit hands to a handler is available to any code that installs one. It
    # must not hold the token that ends the transaction.
    for mode <- [:stop, :replicate] do
      test "the point before the commit hands out no token (#{mode})", env do
        c = boot(env, unquote(mode))
        test = self()

        Fault.install(fn
          :db_before_commit, context ->
            # Every integer of the context, and zero, tried as the token of the
            # transaction that is open now.
            guesses = [0 | for({_key, value} <- context, is_integer(value), do: value)]
            tried = for g <- guesses, do: Sqlite.finish_transaction(context.conn, g, :commit)
            send(test, {:tried, Map.keys(context), tried})
            :ok

          _, _ ->
            :ok
        end)

        assert {:ok, :done} =
                 Alambique.transaction(c.inst, c.cell, fn tx ->
                   {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (7)")
                   :done
                 end)

        Fault.uninstall()
        assert_received {:tried, keys, tried}
        assert keys == [:conn]
        assert tried == [{:error, :not_authorized}]
        assert rows(c) == [0, 1, 7]
        assert store(c) == [0, 1, 7]
      end
    end
  end

  describe "a page cache that spills" do
    # A small cache writes dirty pages to the WAL before the commit.
    @spill "INSERT INTO b(x) SELECT randomblob(2000) FROM " <>
             "(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 6000) SELECT x FROM c)"

    defp spill_boot(env, mode) do
      inst =
        start_instance(env,
          migrations: @schema ++ [{2, "CREATE TABLE b (id INTEGER PRIMARY KEY, x BLOB)"}],
          sql: [orphan_frames: mode],
          awake: [cache_kib: 64],
          op_timeout_ms: 20_000,
          compaction: false
        )

      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
      %{inst: inst, cell: cell, env: env, mode: mode}
    end

    for mode <- [:stop, :replicate], failure <- [:rollback, :raise] do
      test "a large write that fails is no orphan and does not stop the cell (#{failure}, #{mode})",
           env do
        c = spill_boot(env, unquote(mode))
        cell_pid = pid(c)
        wal = path(c) <> "-wal"
        before = File.stat!(wal).size

        result =
          Alambique.transaction(
            c.inst,
            c.cell,
            fn tx ->
              {:ok, _} = Tx.execute(tx, @spill)
              if unquote(failure) == :raise, do: raise("boom"), else: Tx.rollback(tx, :nope)
            end,
            timeout: 20_000
          )

        assert {:error, _} = result
        refute match?({:error, {:committed, _}}, result)
        # The pages reached the WAL, so the cache did spill.
        assert File.stat!(wal).size - before > 4_000_000
        refute in_cell(pid(c), &Sqlite.wal_advanced?(&1.conn))
        refute unreplicated?(c)

        # A cut is not judged an orphan, and the cell is the same one.
        assert {:ok, _} = Alambique.position(c.inst, c.cell)
        assert pid(c) == cell_pid
        assert {:ok, %{rows: [[0]]}} = Alambique.query(c.inst, c.cell, "SELECT count(*) FROM b")
        assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
        assert store(c) == [1, 2]
      end
    end

    test "a large write that commits is carried whole, spilled pages included", env do
      c = spill_boot(env, :stop)

      assert {:ok, _} =
               Alambique.execute(c.inst, c.cell, @spill, [], timeout: 20_000)

      count = durable_rows(c.env, c.cell, "SELECT count(*), sum(length(x)) FROM b")
      assert count == [[6000, 12_000_000]]
    end
  end

  describe "a cell stopped with a put in flight" do
    # The pipe kills the uploading processes. A put the process had not sent
    # does not land; a put already on the wire may, and the caller was told
    # that its outcome is unknown.
    test "a put the process still holds does not land after the stop", env do
      gate = start_supervised!(%{id: :gate, start: {GatedStore, :start_link, []}})
      c = boot(%{env | store: GatedStore.wrap(gate, env.store)}, :stop)
      GatedStore.hold(gate, ~r{/0000/})

      in_flight =
        Task.async(fn -> Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)") end)

      eventually(fn -> state(c).pipe.queue != [] end)

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (3)")
          commit_behind_back()
        end)

      assert result == {:error, :transaction_ended}
      assert Task.await(in_flight) == {:error, :outcome_unknown}
      assert asleep?(c)
      GatedStore.open(gate)
      assert store(%{c | env: env}) == [0, 1]
    end

    test "a put already on the wire lands after the stop, which the caller was told", env do
      wire = start_supervised!(%{id: :wire, start: {WireStore, :start_link, []}})
      c = boot(%{env | store: WireStore.wrap(wire, env.store)}, :stop)
      WireStore.hold(wire)

      in_flight =
        Task.async(fn -> Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)") end)

      eventually(fn -> WireStore.held(wire) == 1 end)

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (3)")
          commit_behind_back()
        end)

      assert result == {:error, :transaction_ended}
      assert Task.await(in_flight) == {:error, :outcome_unknown}
      assert asleep?(c)
      WireStore.open(wire)

      # The row of the write that was told "unknown" is durable; the wake serves it.
      assert store(%{c | env: env}) == [0, 1, 2]
      assert rows(c) == [0, 1, 2]
    end
  end

  describe "orphan_frames: :stop without a cleanup timer" do
    test "a write that ends in orphan frames stops the cell at once", env do
      c = boot(env, :stop, feed: [cleanup: :in_write])

      result =
        Alambique.transaction(c.inst, c.cell, fn tx ->
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (6)")
          commit_behind_back()
        end)

      assert result == {:error, :transaction_ended}
      assert reached?(fn -> Alambique.owner(c.inst, c.cell) == :none end, 3_000)
      assert store(c) == [0, 1]
    end
  end
end
