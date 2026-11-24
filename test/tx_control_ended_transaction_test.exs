defmodule Alambique.TxControlEndedTransactionTest do
  @moduledoc """
  A transaction function that ends its own transaction (COMMIT, ROLLBACK, a
  conflict or a timeout that rolls back, a smuggled script) must not leave
  committed frames that no batch carries: committed-but-unreplicated frames
  imply a write in flight (design §15). The store's data puts are held, so
  nothing can be acknowledged while a case is observed.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Feed, Tx}
  alias Alambique.Replication.{Native, Replicator}
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [
    {1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"},
    {2, "CREATE TABLE guard (x)"},
    {3, "CREATE TRIGGER g BEFORE INSERT ON guard BEGIN SELECT RAISE(ROLLBACK, 'no'); END"}
  ]
  @slow "INSERT INTO t SELECT x + 1000 FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 400000000) SELECT x FROM c)"
  @all "SELECT n FROM t ORDER BY n"

  setup ctx do
    env = setup_store()
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    install_commit_behind_back()
    Map.merge(env, %{gate: gate, tag: ctx.test |> Atom.to_string() |> :erlang.phash2()})
  end

  # An awake cell with row 0 acknowledged, then every level-0 put held.
  defp boot(env, opts \\ []) do
    store = GatedStore.wrap(env.gate, env.store)
    cell = "cell-#{env.tag}"

    inst =
      start_instance(
        %{env | store: store},
        [migrations: @schema, compaction: false] ++ opts
      )

    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (0)")
    GatedStore.hold(env.gate, ~r{/0000/})
    %{env | store: store} |> Map.merge(%{inst: inst, cell: cell, raw: env.store})
  end

  defp pid(%{inst: inst, cell: cell}), do: Cell.whereis(Config.fetch!(inst), cell)

  defp ins(tx, n), do: Tx.execute(tx, "INSERT INTO t VALUES (#{n})")

  # What a defect of the library would do: end the transaction behind its
  # back, when the function returns.
  defp lifted_commit(_tx), do: commit_behind_back()

  defp ids(rows), do: rows |> List.flatten() |> Enum.reject(&(&1 == 0))

  defp store_ids(env),
    do: env |> Map.put(:store, env.raw) |> durable_rows(env.cell, @all) |> ids()

  # `{ids, microseconds}` of a read from another process, or `:blocked`.
  defp read(env) do
    task =
      Task.async(fn ->
        started = System.monotonic_time(:microsecond)

        try do
          case Alambique.query(env.inst, env.cell, @all, [], timeout: 1_000) do
            {:ok, %{rows: rows}} -> {ids(rows), System.monotonic_time(:microsecond) - started}
            {:error, :timeout} -> :blocked
            other -> {:error, other}
          end
        catch
          # A call that does not answer in time is a block.
          :exit, {:timeout, _} -> :blocked
          :exit, reason -> {:error, {:exit, reason}}
        end
      end)

    case Task.yield(task, 3_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:error, _} = error} -> error
      {:ok, :blocked} -> :blocked
      {:ok, {ids, us}} -> {ids, us}
      _ -> :blocked
    end
  end

  # What the cell holds: committed WAL frames no batch carries, and whether a
  # write is in flight.
  defp wal(env) do
    s = :sys.get_state(pid(env))

    {:ok, pending?} =
      Native.wal_pending(s.repl.db_path <> "-wal", s.repl.page_size, s.repl.wal_pos)

    %{
      unreplicated: pending?,
      in_flight: s.pending != [] or s.pipe.queue != [],
      path: s.repl.db_path
    }
  end

  defp kill(env, wipe?) do
    %{path: path} = wal(env)
    Process.exit(pid(env), :kill)
    eventually(fn -> Alambique.owner(env.inst, env.cell) == :none end)
    if wipe?, do: File.rm_rf!(Path.dirname(path))
    GatedStore.open(env.gate)

    {:ok, %{rows: rows}} = Alambique.query(env.inst, env.cell, @all, [], timeout: 20_000)
    ids(rows)
  end

  defp hibernate(env, wipe?) do
    %{path: path} = wal(env)
    GatedStore.open(env.gate)
    result = Alambique.hibernate(env.inst, env.cell)
    eventually(fn -> Alambique.owner(env.inst, env.cell) == :none end)
    stored = store_ids(env)
    if wipe?, do: File.rm_rf!(Path.dirname(path))
    {:ok, %{rows: rows}} = Alambique.query(env.inst, env.cell, @all, [], timeout: 20_000)
    {result, stored, ids(rows)}
  end

  # Runs `fun` as the write while the puts are held. The call is given a
  # moment: an acknowledged write would stay blocked on the held put.
  defp write(env, fun) do
    task = Task.async(fn -> Alambique.transaction(env.inst, env.cell, fun) end)

    case Task.yield(task, 1_500) do
      {:ok, result} -> {task, result}
      nil -> {task, :blocked}
    end
  end

  defp finish(env, {task, _}) do
    GatedStore.open(env.gate)
    Task.shutdown(task, 5_000)
    :ok
  end

  # The checks of the invariant on one case. Nothing is acknowledged while the
  # puts are held, so a read that returns a row saw an undurable one.
  defp observe(env, fun, opts \\ []) do
    {_, result} = written = write(env, fun)
    reader = read(env)
    wal = wal(env)
    stored = store_ids(env)
    seen = if match?({ids, _} when is_list(ids), reader), do: elem(reader, 0), else: []

    wiped = kill_or_hibernate(env, opts)
    finish(env, written)
    local = if wiped.after_wake, do: wiped.after_wake, else: []
    final = store_ids(env)

    %{
      result: result,
      reader: reader,
      wal: wal,
      store_ids: stored,
      final_store: final,
      seen: seen,
      after_wake: local,
      hibernate: wiped[:hibernate]
    }
  end

  defp kill_or_hibernate(env, opts) do
    wipe? = Keyword.get(opts, :wipe, true)

    if Keyword.get(opts, :hibernate, false) do
      {result, stored, rows} = hibernate(env, wipe?)
      %{after_wake: rows, hibernate: %{result: result, stored: stored}}
    else
      %{after_wake: kill(env, wipe?)}
    end
  end

  defp violations(obs) do
    [
      if(obs.wal.unreplicated and not obs.wal.in_flight, do: :unreplicated_frames_no_write),
      if(obs.seen -- obs.store_ids != [], do: :reader_saw_unacknowledged_rows),
      if(obs.seen -- obs.after_wake != [], do: :reader_saw_rows_lost_after_wake),
      # An error answer means no row of the function exists, here or in the store.
      if(match?({:error, _}, obs.result) and (obs.after_wake != [] or obs.final_store != []),
        do: :error_but_row_present
      )
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp check(obs), do: assert(violations(obs) == [], inspect(obs, pretty: true))

  describe "COMMIT ends the transaction" do
    for {name, sql} <- [
          plain: "COMMIT",
          mixed_case: "cOmMiT",
          block_comment: "/* x */ commit",
          line_comment: "-- x\ncommit",
          leading_semicolon: ";commit",
          trailing_semicolon: "commit;",
          end: "END",
          end_transaction: "END TRANSACTION"
        ] do
      test "#{name}: no unreplicated frames, reader sees nothing undurable, nothing lost", env do
        env = boot(env)
        sql = unquote(sql)

        obs =
          observe(env, fn tx ->
            {:ok, _} = ins(tx, 1)
            Tx.execute(tx, sql)
          end)

        check(obs)
      end
    end

    test "COMMIT is refused, and the rows of the function stay a write in flight", env do
      env = boot(env)
      test = self()

      {task, result} =
        write(env, fn tx ->
          {:ok, _} = ins(tx, 1)
          send(test, {:commit, Tx.execute(tx, "COMMIT")})
          :done
        end)

      assert_received {:commit, {:error, :not_authorized}}
      reader = read(env)
      wal = wal(env)
      stored = store_ids(env)
      finish(env, {task, result})

      # Nothing is acknowledged or visible before the put; the rows are held
      # by the batch, not committed behind its back.
      assert result == :blocked and reader == :blocked and wal.in_flight and stored == [],
             inspect(%{result: result, reader: reader, wal: wal, stored: stored})
    end
  end

  describe "a transaction ended behind the library's back" do
    # The function returns and the transaction is committed behind the
    # library's back, as a defect of the library would.
    defp lifted(tx) do
      {:ok, _} = ins(tx, 1)
      lifted_commit(tx)
    end

    test "the cell stops and neither the store nor the next wake has the row", env do
      env = boot(env)
      {task, result} = written = write(env, &lifted/1)
      assert result == {:error, :transaction_ended}
      eventually(fn -> Alambique.owner(env.inst, env.cell) == :none end)
      finish(env, written)
      Task.shutdown(task, 100)

      {:ok, %{rows: rows}} = Alambique.query(env.inst, env.cell, @all, [], timeout: 20_000)
      assert ids(rows) == [] and store_ids(env) == []
    end

    test "a later acknowledged write does not carry the row to the store", env do
      env = boot(env)
      {task, _} = written = write(env, &lifted/1)

      GatedStore.open(env.gate)
      {:ok, _} = Alambique.execute(env.inst, env.cell, "INSERT INTO t VALUES (2)")
      stored = store_ids(env)
      finish(env, written)
      Task.shutdown(task, 100)

      # The write was answered with an error: its row is not durable.
      assert stored == [2]
    end

    test "a cut never includes a row of a write that failed", env do
      env = boot(env)
      written = write(env, &lifted/1)
      position = Alambique.position(env.inst, env.cell, timeout: 2_000)
      stored = store_ids(env)
      finish(env, written)

      # An answer is a cut of the state the store holds; the cell stopped, so
      # it is not an awake cell's.
      refute match?({:ok, %{awake: true}}, position), inspect(position)
      assert 1 not in stored
    end

    test "the row is not published; with :replicate it is, once", env do
      env = boot(env)
      written = write(env, &lifted/1)

      next =
        Task.async(fn -> Alambique.execute(env.inst, env.cell, "INSERT INTO t VALUES (2)") end)

      GatedStore.open(env.gate)
      {:ok, _} = Task.await(next, 10_000)
      records = feed(env, 2)
      finish(env, written)
      assert Enum.sort(for {n, op, _} <- records, n > 0, do: {n, op}) == [{2, :insert}]
    end

    test "with orphan_frames: :replicate the row is published once", env do
      env = boot(env, sql: [orphan_frames: :replicate])
      written = write(env, &lifted/1)

      next =
        Task.async(fn -> Alambique.execute(env.inst, env.cell, "INSERT INTO t VALUES (2)") end)

      GatedStore.open(env.gate)
      {:ok, _} = Task.await(next, 10_000)
      records = feed(env, 2)
      finish(env, written)

      assert Enum.sort(for {n, op, _} <- records, n > 0, do: {n, op}) == [
               {1, :insert},
               {2, :insert}
             ],
             inspect(records)
    end
  end

  # The function commits behind the library's back and the store holds its
  # puts. Under `:stop` the cell stops at once and answers the error; under
  # `:replicate` it parks the write, so the caller and the reads wait for the
  # batch, which `GatedStore.open/1` lets through.
  describe "a transaction committed behind the library's back, then the cell goes away" do
    for wipe <- [true, false] do
      kept = if wipe, do: "the local file deleted", else: "the local file kept"

      test "stop: the cell stops itself, #{kept}: no row anywhere", env do
        env = boot(env, sql: [orphan_frames: :stop])
        %{path: path} = wal(env)
        {task, result} = written = write(env, &lifted/1)

        # The caller is told it failed, at once.
        assert result == {:error, :transaction_ended}
        eventually(fn -> Alambique.owner(env.inst, env.cell) == :none end)
        if unquote(wipe), do: File.rm_rf!(Path.dirname(path))
        GatedStore.open(env.gate)

        # The next wake, a read from another process, and the store agree.
        assert {[], _us} = read(env)
        assert store_ids(env) == []
        {:ok, _} = Alambique.execute(env.inst, env.cell, "INSERT INTO t VALUES (2)")
        assert 1 not in Enum.map(feed(env, 2), &elem(&1, 0))
        Task.shutdown(task, 100)
        finish(env, written)
      end

      test "replicate, kill, #{kept}: nothing was told committed, wake serves the store", env do
        env = boot(env, sql: [orphan_frames: :replicate])
        {_, result} = written = write(env, &lifted/1)

        # Parked behind the held put: not answered, invisible to reads, in
        # flight, not in the store.
        assert result == :blocked
        assert read(env) == :blocked
        # The batch is encoded and its put is held.
        assert %{in_flight: true} = wal(env)
        assert store_ids(env) == []

        woken = kill(env, unquote(wipe))
        finish(env, written)
        stored = store_ids(env)

        # The caller got no answer. Whatever the store holds is what the wake
        # serves, and a row it holds is on the feed once.
        assert woken == stored
        {:ok, _} = Alambique.execute(env.inst, env.cell, "INSERT INTO t VALUES (2)")
        keys = feed(env, 2) |> Enum.map(&elem(&1, 0)) |> Enum.filter(&(&1 == 1))
        assert keys == if(1 in stored, do: [1], else: [])
      end

      test "replicate, hibernate, #{kept}: the row is durable and the caller is told so", env do
        env = boot(env, sql: [orphan_frames: :replicate])
        {task, result} = written = write(env, &lifted/1)
        assert result == :blocked
        assert read(env) == :blocked

        {hibernated, stored, woken} = hibernate(env, unquote(wipe))
        answer = Task.yield(task, 5_000)
        finish(env, written)

        assert hibernated == :ok
        assert answer == {:ok, {:error, {:committed, :transaction_ended}}}
        assert stored == [1] and woken == [1]
        assert [{1, :insert}] = for({n, op, _} <- feed(env, 1), n > 0, do: {n, op})
      end
    end
  end

  describe "other ways to end the transaction" do
    test "ROLLBACK, then an insert that autocommits", env do
      env = boot(env)

      obs =
        observe(env, fn tx ->
          {:ok, _} = ins(tx, 1)
          {:ok, _} = Tx.execute(tx, "ROLLBACK")
          ins(tx, 2)
        end)

      check(obs)
    end

    test "INSERT OR ROLLBACK on a conflict, then an insert", env do
      env = boot(env)

      obs =
        observe(env, fn tx ->
          {:ok, _} = ins(tx, 1)
          _ = Tx.execute(tx, "INSERT OR ROLLBACK INTO t VALUES (0)")
          ins(tx, 2)
        end)

      check(obs)
    end

    test "a trigger with RAISE(ROLLBACK), then an insert", env do
      env = boot(env)

      obs =
        observe(env, fn tx ->
          {:ok, _} = ins(tx, 1)
          _ = Tx.execute(tx, "INSERT INTO guard VALUES (1)")
          ins(tx, 2)
        end)

      check(obs)
    end

    test "a statement timeout on a writing statement, then an insert", env do
      env = boot(env, sql: [statement_timeout_ms: 300])

      obs =
        observe(env, fn tx ->
          {:ok, _} = ins(tx, 1)
          _ = Tx.execute(tx, @slow)
          ins(tx, 2)
        end)

      check(obs)
    end
  end

  describe "several statements in one Tx.execute" do
    test "none is silently dropped", env do
      env = boot(env)
      test = self()

      {task, result} =
        write(env, fn tx ->
          returned = Tx.execute(tx, "INSERT INTO t VALUES (1); COMMIT; INSERT INTO t VALUES (2)")
          send(test, {:execute, returned})
          returned
        end)

      returned =
        receive do
          {:execute, returned} -> returned
        after
          0 -> :none
        end

      wal = wal(env)
      GatedStore.open(env.gate)
      # The answer may come only once the puts are let through.
      result = if result == :blocked, do: Task.await(task, 10_000), else: result
      finish(env, {task, result})
      {:ok, %{rows: rows}} = Alambique.query(env.inst, env.cell, @all, [], timeout: 5_000)

      # Rejected as a whole, or run as a whole; never the first one only.
      ok? = match?({:ok, {:error, {:multiple_statements, _}}}, result) or ids(rows) == [1, 2]
      assert ok?, inspect(%{execute: returned, result: result, local_rows: rows, wal: wal})
    end
  end

  describe "Tx.script smuggles statements past the splitter" do
    # The script is refused as not DDL, so the INSERT never runs.
    for {name, ident} <- [dollar: "a$case", utf8: "écase"] do
      test "#{name} identifier containing case hides COMMIT and an INSERT", env do
        env = boot(env)
        ident = unquote(ident)

        script =
          "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1 AS #{ident}; END; " <>
            "COMMIT; INSERT INTO t VALUES (7); " <>
            "CREATE TRIGGER trg2 AFTER INSERT ON t BEGIN SELECT 1; END"

        obs = observe(env, fn tx -> Tx.script(tx, script) end)
        check(obs)
      end

      test "#{name} identifier: the script is refused as not DDL", env do
        env = boot(env)
        ident = unquote(ident)

        script =
          "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1 AS #{ident}; END; " <>
            "COMMIT; INSERT INTO t VALUES (7); " <>
            "CREATE TRIGGER trg2 AFTER INSERT ON t BEGIN SELECT 1; END"

        test = self()

        written =
          write(env, fn tx ->
            send(test, {:script, Tx.script(tx, script)})
            :done
          end)

        finish(env, written)

        # Refused before it runs: the COMMIT by the connection, an INSERT as not DDL.
        assert_received {:script, {:error, reason}}
        assert reason == :not_authorized or match?({:not_ddl, _}, reason)
        assert {:ok, %{rows: [[0]]}} = Alambique.query(env.inst, env.cell, "SELECT max(n) FROM t")
      end
    end

    test "no row of the store is missing from the change feed", env do
      env = boot(env)

      script =
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1 AS a$case; END; " <>
          "COMMIT; INSERT INTO t VALUES (7); " <>
          "CREATE TRIGGER trg2 AFTER INSERT ON t BEGIN SELECT 1; END"

      written = write(env, fn tx -> Tx.script(tx, script) end)

      next =
        Task.async(fn -> Alambique.execute(env.inst, env.cell, "INSERT INTO t VALUES (8)") end)

      GatedStore.open(env.gate)
      {:ok, _} = Task.await(next, 10_000)
      records = feed(env, 8)
      stored = store_ids(env)
      finish(env, written)
      keys = for {n, _, _} <- records, do: n

      # The script is refused, and every row the store holds is a feed record.
      assert 8 in keys and Enum.all?(stored, &(&1 in keys)) and 7 not in stored,
             inspect(%{feed: records, store: stored})
    end
  end

  describe "a migration function that issues COMMIT" do
    defmodule Mig do
      @moduledoc false
      def set(list), do: :persistent_term.put({__MODULE__, :list}, list)
      def clear, do: :persistent_term.erase({__MODULE__, :list})
      def migrations(_cell), do: :persistent_term.get({__MODULE__, :list})
    end

    setup do
      on_exit(&Mig.clear/0)
    end

    defp committing do
      {2,
       fn tx ->
         {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (100)")
         {:ok, _} = Tx.execute(tx, "COMMIT")
       end}
    end

    test "at wake: the cell does not serve a half-applied migration", env do
      Mig.set([hd(@schema), committing()])
      inst = start_instance(env, migrations: {Mig, :migrations}, compaction: false)
      cell = "cell-#{env.tag}"

      # Every attempt wakes the cell again, and the step fails every time.
      attempts =
        for _ <- 1..3,
            do: Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n", [], timeout: 5_000)

      assert Enum.all?(attempts, &match?({:error, {:migration, 2, _}}, &1)), inspect(attempts)
      assert Alambique.owner(inst, cell) == :none
    end

    test "a failed wake leaves nothing that a later wake serves or replicates", env do
      Mig.set([hd(@schema)])
      inst = start_instance(env, migrations: {Mig, :migrations}, compaction: false)
      cell = "cell-#{env.tag}"
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
      :ok = Alambique.hibernate(inst, cell)
      Mig.set([hd(@schema), committing()])
      for _ <- 1..2, do: {:error, {:migration, 2, _}} = Alambique.query(inst, cell, @all)
      assert Alambique.owner(inst, cell) == :none

      # A corrected list wakes the cell from the files the failed wakes left.
      Mig.set([hd(@schema), {2, "CREATE TABLE later (x)"}])
      # A failed wake is retried after a backoff.
      {:ok, %{rows: rows}} =
        eventually(fn ->
          case Alambique.query(inst, cell, @all, [], timeout: 10_000) do
            {:ok, _} = ok -> ok
            _ -> nil
          end
        end)

      refute 100 in List.flatten(rows)

      s = :sys.get_state(Cell.whereis(Config.fetch!(inst), cell))
      refute Replicator.unreplicated?(s.repl)
      assert List.flatten(durable_rows(env, cell, @all)) == List.flatten(rows)
      assert {:ok, %{rows: [[2]]}} = Alambique.query(inst, cell, "PRAGMA user_version")
    end

    test "remigrate on a serving cell: the step leaves no committed row", env do
      Mig.set([hd(@schema)])
      inst = start_instance(env, migrations: {Mig, :migrations}, compaction: false)
      cell = "cell-#{env.tag}"
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
      Mig.set([hd(@schema), committing()])

      migrate = Alambique.migrate(inst, cell)
      {:ok, %{rows: rows}} = Alambique.query(inst, cell, @all)
      version = Alambique.query(inst, cell, "PRAGMA user_version")
      stored = durable_rows(env, cell, @all)

      assert {:error, _} = migrate
      refute 100 in List.flatten(rows), inspect(%{migrate: migrate, rows: rows, version: version})

      assert version == {:ok, %{columns: ["user_version"], rows: [[1]]}} or
               match?({:ok, %{rows: [[1]]}}, version),
             inspect(%{migrate: migrate, version: version, stored: stored})
    end

    test "a failed step leaves no unreplicated frames without a write in flight", env do
      Mig.set([hd(@schema)])
      inst = start_instance(env, migrations: {Mig, :migrations}, compaction: false)
      cell = "cell-#{env.tag}"
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
      Mig.set([hd(@schema), committing()])
      assert {:error, {:migration, 2, _}} = Alambique.migrate(inst, cell)

      s = :sys.get_state(Cell.whereis(Config.fetch!(inst), cell))

      {:ok, unreplicated} =
        Native.wal_pending(s.repl.db_path <> "-wal", s.repl.page_size, s.repl.wal_pos)

      in_flight = s.pending != [] or s.pipe.queue != []

      assert not (unreplicated and not in_flight),
             inspect(%{unreplicated: unreplicated, in_flight: in_flight})
    end
  end

  describe "virtual tables" do
    test "FTS5 and rtree: a write through the transaction is a normal write", env do
      env = boot(env)

      script =
        "CREATE VIRTUAL TABLE doc USING fts5(body); CREATE VIRTUAL TABLE box USING rtree(id, x0, x1)"

      setup =
        Task.async(fn -> Alambique.transaction(env.inst, env.cell, &Tx.script(&1, script)) end)

      GatedStore.open(env.gate)
      assert {:ok, :ok} = Task.await(setup, 10_000)
      GatedStore.hold(env.gate, ~r{/0000/})

      obs =
        observe(env, fn tx ->
          {:ok, _} = Tx.execute(tx, "INSERT INTO doc VALUES ('hello')")
          {:ok, _} = Tx.execute(tx, "INSERT INTO box VALUES (1, 0, 1)")
          ins(tx, 1)
        end)

      assert obs.result == :blocked, inspect(obs)
      check(obs)
    end
  end

  describe "controls that keep working" do
    test "SAVEPOINT, ROLLBACK TO and RELEASE: answered only after the batch", env do
      env = boot(env)

      {task, result} =
        written =
        write(env, fn tx ->
          {:ok, _} = Tx.execute(tx, "SAVEPOINT s")
          {:ok, _} = ins(tx, 1)
          {:ok, _} = Tx.execute(tx, "ROLLBACK TO s")
          {:ok, _} = ins(tx, 2)
          {:ok, _} = Tx.execute(tx, "RELEASE s")
        end)

      assert result == :blocked
      assert read(env) == :blocked

      assert %{in_flight: true} = wal(env)

      assert store_ids(env) == []

      GatedStore.open(env.gate)
      assert {:ok, {:ok, _}} = Task.yield(task, 5_000)
      assert store_ids(env) == [2]
      finish(env, written)
    end

    test "a nested BEGIN is an SQLite error and the transaction survives", env do
      env = boot(env)

      {task, result} =
        written =
        write(env, fn tx ->
          {:error, _} = Tx.execute(tx, "BEGIN")
          ins(tx, 1)
        end)

      assert result == :blocked
      GatedStore.open(env.gate)
      assert {:ok, {:ok, _}} = Task.yield(task, 5_000)
      assert store_ids(env) == [1]
      finish(env, written)
    end

    test "the read-only path refuses COMMIT and BEGIN", env do
      env = boot(env)

      for sql <- ["COMMIT", "BEGIN", "END", "ROLLBACK"] do
        assert {:error, :read_only} = Alambique.query(env.inst, env.cell, sql), sql
      end
    end
  end

  # `{n, op, position}` of the records of this cell once the row `last` is published.
  defp feed(env, last) do
    ctx = Feed.ctx(store: env.raw, prefix: "cells-test", settle_ms: 0)

    eventually(fn ->
      {:ok, records, _} = Feed.read(ctx, nil, 1_000)

      mine =
        for r <- records, r.cell == env.cell, r.table == "t", do: {r.key["n"], r.op, r.position}

      if Enum.any?(mine, &(elem(&1, 0) == last)), do: mine
    end)
  end
end
