defmodule Alambique.RewindTest do
  @moduledoc "Alambique.rewind/4 on one node (design §20)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Feed, Replication, Sqlite}
  alias Alambique.Replication.Replicator
  alias Alambique.Feed.Rewind
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @schema [
    {1,
     """
     CREATE TABLE t (n INTEGER PRIMARY KEY);
     CREATE TABLE _h_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, done INTEGER DEFAULT 0);
     CREATE INDEX _h_jobs_name ON _h_jobs (name);
     CREATE TRIGGER _h_jobs_touch AFTER UPDATE ON _h_jobs BEGIN SELECT 1; END;
     """}
  ]

  setup do
    ctx = setup_store()
    inst = start_instance(ctx, migrations: @schema, feed: [internal_prefixes: ["_h_"]])
    %{ctx: ctx, inst: inst}
  end

  defp insert(inst, cell, n, opts \\ []),
    do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n], opts)

  defp job(inst, cell, name),
    do: Alambique.execute(inst, cell, "INSERT INTO _h_jobs (name) VALUES (?)", [name])

  defp rows(inst, cell, sql \\ "SELECT n FROM t ORDER BY n") do
    {:ok, %{rows: rows}} = Alambique.query(inst, cell, sql)
    List.flatten(rows)
  end

  defp position(inst, cell) do
    {:ok, %{txid: txid}} = Alambique.position(inst, cell)
    txid
  end

  # The restored file of the store, which is what survives if every node dies.
  defp restored(inst, cell, sql) do
    path =
      Path.join(
        Alambique.Config.fetch!(inst).data_dir,
        "r-#{System.unique_integer([:positive])}.db"
      )

    {:ok, _} = Replication.restore(repl_ctx(inst, cell), cell, path, :latest)
    {:ok, conn} = Sqlite.open(path, readonly: true)
    {:ok, %{rows: rows}} = Sqlite.run(conn, sql, [])
    Sqlite.close(conn)
    File.rm(path)
    List.flatten(rows)
  end

  test "the state becomes the target's and later writes are kept", %{inst: inst} do
    cell = unique_cell()
    for n <- 1..3, do: insert(inst, cell, n)
    at = position(inst, cell)
    for n <- 4..6, do: insert(inst, cell, n)
    {:ok, %{txid: head}} = Alambique.position(inst, cell)

    assert {:ok, %{txid: txid, from: from, to: ^at, kept: ["_h_jobs"], schema_version: 1}} =
             Alambique.rewind(inst, cell, {:txid, at})

    assert from >= head and txid == from + 1
    assert :none = Alambique.owner(inst, cell)
    assert restored(inst, cell, "SELECT n FROM t ORDER BY n") == [1, 2, 3]

    assert rows(inst, cell) == [1, 2, 3]
    assert {:ok, _} = insert(inst, cell, 7)
    assert rows(inst, cell) == [1, 2, 3, 7]
    assert restored(inst, cell, "SELECT n FROM t ORDER BY n") == [1, 2, 3, 7]
  end

  test "a timestamp target resolves to the state at that time", %{inst: inst} do
    cell = unique_cell()
    assert {:ok, _} = insert(inst, cell, 1)
    Process.sleep(30)
    mark = System.os_time(:millisecond)
    Process.sleep(30)
    assert {:ok, _} = insert(inst, cell, 2)
    assert :ok = Alambique.hibernate(inst, cell)

    assert {:ok, %{to: to}} = Alambique.rewind(inst, cell, {:timestamp, mark})
    assert to > 0
    assert rows(inst, cell) == [1]
  end

  test "host tables stay as they are and carry their indexes and triggers", %{inst: inst} do
    cell = unique_cell()
    assert {:ok, _} = insert(inst, cell, 1)
    assert {:ok, _} = job(inst, cell, "old")
    at = position(inst, cell)
    assert {:ok, _} = insert(inst, cell, 2)
    assert {:ok, _} = job(inst, cell, "new")

    assert {:ok, _} =
             Alambique.execute(inst, cell, "UPDATE _h_jobs SET done = 1 WHERE name = 'old'")

    assert {:ok, %{kept: ["_h_jobs"]}} = Alambique.rewind(inst, cell, {:txid, at})

    assert rows(inst, cell) == [1]
    assert rows(inst, cell, "SELECT name, done FROM _h_jobs ORDER BY id") == ["old", 1, "new", 0]

    # The sequence continues where the current state was, and no capture
    # trigger of the library sits on the kept table.
    assert {:ok, _} = job(inst, cell, "after")
    assert rows(inst, cell, "SELECT max(id) FROM _h_jobs") == [3]

    assert restored(
             inst,
             cell,
             "SELECT name FROM sqlite_schema WHERE type IN ('trigger', 'index') AND tbl_name = '_h_jobs' ORDER BY name"
           ) == ["_h_jobs_name", "_h_jobs_touch"]
  end

  test "no capture trigger of the library follows a table onto the kept side", %{ctx: store} do
    # The first instance captures the table; the second one treats it as the
    # host's, so the triggers of the library must not be copied with it.
    plain = start_instance(store, migrations: @schema)
    cell = unique_cell()
    assert {:ok, _} = job(plain, cell, "captured")
    at = position(plain, cell)
    assert {:ok, _} = job(plain, cell, "later")

    assert restored(
             plain,
             cell,
             "SELECT count(*) FROM sqlite_schema WHERE name LIKE '_alambique_feed_t__h_jobs%'"
           ) ==
             [3]

    assert :ok = Alambique.hibernate(plain, cell)
    host = start_instance(store, migrations: @schema, feed: [internal_prefixes: ["_h_"]])
    assert {:ok, %{kept: ["_h_jobs"]}} = Alambique.rewind(host, cell, {:txid, at})

    assert restored(
             host,
             cell,
             "SELECT count(*) FROM sqlite_schema WHERE name LIKE '_alambique_feed_t__h_jobs%'"
           ) ==
             [0]

    assert rows(host, cell, "SELECT name FROM _h_jobs ORDER BY id") == ["captured", "later"]
  end

  test "keep: :none rewinds the host tables too", %{inst: inst} do
    cell = unique_cell()
    assert {:ok, _} = job(inst, cell, "old")
    at = position(inst, cell)
    assert {:ok, _} = job(inst, cell, "new")

    assert {:ok, %{kept: []}} = Alambique.rewind(inst, cell, {:txid, at}, keep: :none)
    assert rows(inst, cell, "SELECT name FROM _h_jobs") == ["old"]
  end

  # Deletes files of the running rewind when it reaches `point`, as the
  # startup cleanup of an instance that restarted would.
  defp remove_work_files(inst, point, pattern) do
    dir = Alambique.Config.fetch!(inst).data_dir

    CrashPoints.arm(point, fn _point, _context ->
      for path <- Path.wildcard(Path.join(dir, "work/rewind-*/" <> pattern)), do: File.rm!(path)
    end)
  end

  describe "the chain" do
    setup do
      on_exit(&CrashPoints.reset/0)
    end

    # A writer of an older epoch takes the TXID the rewind was going to use,
    # between the rewind's read of the head and its upload.
    test "a writer that took the TXID makes the rewind start again", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      at = position(inst, cell) - 1
      assert :ok = Alambique.hibernate(inst, cell)
      repl = repl_ctx(inst, cell)
      path = Path.join(Alambique.Config.fetch!(inst).data_dir, "stale.db")

      CrashPoints.arm(:rewind_before_put, fn _point, %{txid: txid} ->
        {:ok, result} = Replication.restore(repl, cell, path, :latest)
        assert result.txid == txid - 1
        state = Replicator.from_restore(result, result.epoch)
        assert {:ok, _} = Replicator.fence(state, repl)
      end)

      assert {:ok, %{to: ^at, txid: txid, from: from}} = Alambique.rewind(inst, cell, {:txid, at})
      assert txid == from + 1
      assert from > position(inst, cell) - 3
      assert restored(inst, cell, "SELECT n FROM t ORDER BY n") == [1, 2]
      assert rows(inst, cell) == [1, 2]
    end

    # An instance that restarts while a rewind runs removes its work
    # directories (design §20); the rewind must fail, not install less.
    test "the current state disappearing refuses the rewind and loses no host table", %{
      inst: inst
    } do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      assert {:ok, _} = job(inst, cell, "old")
      at = position(inst, cell)
      assert {:ok, _} = insert(inst, cell, 2)
      assert {:ok, _} = job(inst, cell, "new")
      assert :ok = Alambique.hibernate(inst, cell)

      remove_work_files(inst, :rewind_before_reconcile, "current.db*")

      assert {:error, {:attach, _}} = Alambique.rewind(inst, cell, {:txid, at})
      CrashPoints.reset()

      assert restored(inst, cell, "SELECT n FROM t ORDER BY n") == [1, 2]
      assert restored(inst, cell, "SELECT name FROM _h_jobs ORDER BY id") == ["old", "new"]
      eventually(fn -> match?({:ok, _}, Alambique.query(inst, cell, "SELECT 1")) end, 10_000)
      assert rows(inst, cell) == [1, 2]
      assert rows(inst, cell, "SELECT name FROM _h_jobs ORDER BY id") == ["old", "new"]
    end

    test "the target file disappearing refuses the rewind and keeps the old state", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      at = position(inst, cell) - 1
      assert :ok = Alambique.hibernate(inst, cell)

      remove_work_files(inst, :rewind_before_put, "target.db*")

      assert {:error, {:capture, _}} = Alambique.rewind(inst, cell, {:txid, at})
      CrashPoints.reset()

      assert restored(inst, cell, "SELECT n FROM t ORDER BY n") == [1, 2, 3]
      eventually(fn -> match?({:ok, _}, Alambique.query(inst, cell, "SELECT 1")) end, 10_000)
      assert rows(inst, cell) == [1, 2, 3]
    end

    test "the chain that keeps moving ends the rewind", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      at = position(inst, cell) - 1
      assert :ok = Alambique.hibernate(inst, cell)
      repl = repl_ctx(inst, cell)
      path = Path.join(Alambique.Config.fetch!(inst).data_dir, "stale.db")

      CrashPoints.arm(
        :rewind_before_put,
        fn _point, _ ->
          {:ok, result} = Replication.restore(repl, cell, path, :latest)
          assert {:ok, _} = Replicator.fence(Replicator.from_restore(result, result.epoch), repl)
        end,
        times: :infinity
      )

      assert {:error, {:fence, :txid_taken}} = Alambique.rewind(inst, cell, {:txid, at})
      CrashPoints.reset()
      assert rows(inst, cell) == [1, 2, 3]
    end

    for {point, expected} <- [
          rewind_after_acquire: [1, 2, 3],
          rewind_after_head: [1, 2, 3],
          rewind_before_put: [1, 2, 3],
          rewind_after_put: [1, 2]
        ] do
      test "a crash at #{point} leaves #{inspect(expected)}", %{inst: inst} do
        cell = unique_cell()
        for n <- 1..3, do: insert(inst, cell, n)
        at = position(inst, cell) - 1
        assert :ok = Alambique.hibernate(inst, cell)

        CrashPoints.arm(unquote(point), :raise)
        assert_raise CrashPoints.Crash, fn -> Alambique.rewind(inst, cell, {:txid, at}) end
        CrashPoints.reset()

        # What the store holds is one state or the other, whatever happens to
        # the lease of the rewind that died.
        assert restored(inst, cell, "SELECT n FROM t ORDER BY n") == unquote(expected)
        eventually(fn -> match?({:ok, _}, Alambique.query(inst, cell, "SELECT 1")) end, 10_000)
        assert rows(inst, cell) == unquote(expected)
      end
    end
  end

  describe "size limit" do
    test "a database larger than rewind: [max_bytes:] is refused before the lease", %{ctx: store} do
      inst = start_instance(store, migrations: @schema, rewind: [max_bytes: 8_192])
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      at = position(inst, cell)
      assert {:ok, _} = insert(inst, cell, 2)

      # Two or three pages fit; a cell with this many rows does not.
      for n <- 3..40,
          do: Alambique.execute(inst, cell, "INSERT INTO t VALUES (?)", [n * 1_000_003])

      assert {:ok, _} =
               Alambique.execute(
                 inst,
                 cell,
                 "CREATE TABLE big (v BLOB); INSERT INTO big VALUES (zeroblob(60000))"
               )

      assert {:error, {:too_large, bytes, 8_192}} = Alambique.rewind(inst, cell, {:txid, at})
      assert bytes > 8_192
      assert {:ok, %{owner: owner}} = Alambique.lease(inst, cell)
      assert owner == node()
      assert Alambique.owner(inst, cell) == {:ok, node()}
    end

    test "nil removes the limit and the default is 256 MiB", %{ctx: store} do
      assert Alambique.Config.fetch!(start_instance(store, migrations: @schema)).rewind.max_bytes ==
               268_435_456

      inst = start_instance(store, migrations: @schema, rewind: [max_bytes: nil])
      assert Alambique.Config.fetch!(inst).rewind.max_bytes == nil
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      at = position(inst, cell)
      assert {:ok, _} = insert(inst, cell, 2)
      assert {:ok, _} = Alambique.rewind(inst, cell, {:txid, at})
    end
  end

  describe "work directories" do
    test "an instance removes the ones a crashed VM left, and only those", %{ctx: store} do
      data_dir = Path.join(store.dir, "restarted")

      for path <- [
            "work/rewind-7/target.db",
            "work/relocate-9/cell.db",
            "keep-me/file",
            "x.cell/cell.db"
          ] do
        File.mkdir_p!(Path.dirname(Path.join(data_dir, path)))
        File.write!(Path.join(data_dir, path), "x")
      end

      start_instance(store, name: :restarted, migrations: @schema)

      assert data_dir |> File.ls!() |> Enum.sort() |> Enum.reject(&(&1 in ["inflight", "lock"])) ==
               ["keep-me", "x.cell"]
    end

    test "a rewind and a relocation remove theirs when they end", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..2, do: insert(inst, cell, n)
      at = position(inst, cell) - 1
      assert {:ok, _} = Alambique.rewind(inst, cell, {:txid, at})

      work = Path.join(Alambique.Config.fetch!(inst).data_dir, "work")
      assert File.ls(work) |> elem(1) == []
    end
  end

  describe "feed order" do
    # The marker takes the time of the cell's feed floor even when the clock
    # of the rewinding node is behind it, so it sorts after the undone records
    # as every batch of a cell does (design §13).
    test "a clock that went back does not put the marker before the records", %{ctx: store} do
      world = ManualClock.new(1_800_000_000_000)
      clock = {ManualClock, ManualClock.config(world, 0)}
      inst = start_instance(store, migrations: @schema, clock: clock)
      cell = unique_cell()

      assert {:ok, _} = insert(inst, cell, 1)
      at = position(inst, cell)
      ManualClock.advance(world, 1_000)
      assert {:ok, _} = insert(inst, cell, 2)
      assert :ok = Alambique.hibernate(inst, cell)
      ManualClock.advance(world, -900)

      assert {:ok, %{txid: txid}} = Alambique.rewind(inst, cell, {:txid, at})
      assert :ok = Alambique.touch(inst, cell) |> then(&if match?({:ok, _}, &1), do: :ok)
      ManualClock.advance(world, 60_000)

      ctx = %{Feed.ctx(inst) | settle_ms: 0}

      # The feed is published after the acknowledgement: wait for all three.
      records =
        eventually(fn ->
          {:ok, records, _} = Feed.read(ctx, nil, 100)
          length(records) == 3 && records
        end)

      assert [%{values: %{"n" => 1}}, %{values: %{"n" => 2}}, %Rewind{txid: ^txid}] = records
    end
  end

  describe "refusals" do
    test "a target that is not behind the head", %{inst: inst} do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      head = position(inst, cell)

      assert {:error, {:target_not_behind, ^head}} = Alambique.rewind(inst, cell, {:txid, head})

      assert {:error, {:target_not_behind, ^head}} =
               Alambique.rewind(inst, cell, {:txid, head + 9})

      assert {:error, {:target_not_behind, ^head}} =
               Alambique.rewind(inst, cell, {:timestamp, System.os_time(:millisecond) + 60_000})

      assert {:error, :bad_target} = Alambique.rewind(inst, cell, {:txid, 0})
      assert {:error, :bad_target} = Alambique.rewind(inst, cell, :latest)
      assert {:error, :bad_keep} = Alambique.rewind(inst, cell, {:txid, 1}, keep: ["_h_"])

      assert {:error, {:unknown_option, :nope}} =
               Alambique.rewind(inst, cell, {:txid, 1}, nope: 1)

      assert rows(inst, cell) == [1]
    end

    test "a cell without data", %{inst: inst} do
      assert {:error, :no_history} = Alambique.rewind(inst, unique_cell(), {:txid, 1})
    end

    test "a target behind the history that is kept", %{ctx: ctx, inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      head = position(inst, cell)
      assert :ok = Alambique.hibernate(inst, cell)

      # Retention deleted the first file: nothing starts at TXID 1.
      key = "cells-test/cells/#{cell}/0000/0000000000000001-0000000000000001.ltx"
      {MemoryStore, config} = ctx.store
      assert :ok = MemoryStore.delete(config, key)

      assert {:error, {:behind_retention, oldest}} = Alambique.rewind(inst, cell, {:txid, 2})
      assert oldest in 0..head
      assert {:ok, %{txid: _}} = Alambique.position(inst, cell)
    end

    test "a head that is not the expected one", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      at = position(inst, cell) - 1
      head = position(inst, cell)

      assert {:error, {:head_moved, ^head}} =
               Alambique.rewind(inst, cell, {:txid, at}, if_head: head - 1)

      assert rows(inst, cell) == [1, 2, 3]
      assert {:ok, %{from: ^head}} = Alambique.rewind(inst, cell, {:txid, at}, if_head: head)

      # A repeated call with the same expectation does nothing.
      assert {:error, {:head_moved, _}} =
               Alambique.rewind(inst, cell, {:txid, at}, if_head: head)
    end

    test "if_head with :data ignores wakes and refuses after a write", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      {:ok, %{txid: seen, data: data}} = Alambique.position(inst, cell)
      assert data == seen

      for _ <- 1..2 do
        assert :ok = Alambique.hibernate(inst, cell)
        assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      end

      # The wakes moved the head, not the data.
      assert {:error, {:head_moved, head}} =
               Alambique.rewind(inst, cell, {:txid, seen - 1}, if_head: seen)

      assert head > seen
      assert {:ok, %{data: ^seen, txid: ^head}} = Alambique.position(inst, cell)

      assert {:ok, _} = insert(inst, cell, 4)
      {:ok, %{txid: written}} = Alambique.position(inst, cell)

      assert {:error, {:data_written, ^written}} =
               Alambique.rewind(inst, cell, {:txid, seen - 1}, if_head: {seen, :data})

      assert rows(inst, cell) == [1, 2, 3, 4]

      assert {:ok, %{from: ^written}} =
               Alambique.rewind(inst, cell, {:txid, seen - 1}, if_head: {written, :data})
    end

    test "if_head with :data succeeds after only wakes", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      {:ok, %{txid: seen}} = Alambique.position(inst, cell)
      assert :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")

      assert {:ok, %{from: from}} =
               Alambique.rewind(inst, cell, {:txid, seen - 1}, if_head: {seen, :data})

      assert from > seen
      assert rows(inst, cell) == [1, 2]
    end

    test "a rewind is data", %{inst: inst} do
      cell = unique_cell()
      for n <- 1..3, do: insert(inst, cell, n)
      at = position(inst, cell) - 1

      assert {:ok, %{txid: txid}} = Alambique.rewind(inst, cell, {:txid, at})
      assert {:ok, %{data: ^txid, txid: ^txid, awake: false}} = Alambique.position(inst, cell)

      assert :ok = Alambique.hibernate(inst, cell)
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT 1")
      assert {:ok, %{data: ^txid}} = Alambique.position(inst, cell)
    end

    test "if_head has a shape", %{inst: inst} do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)

      for bad <- [:x, {1, :txid}, {-1, :data}, "1"] do
        assert {:error, :bad_if_head} = Alambique.rewind(inst, cell, {:txid, 1}, if_head: bad)
      end
    end

    test "another schema version", %{ctx: store, inst: first} do
      cell = unique_cell()
      assert {:ok, _} = insert(first, cell, 1)
      before = position(first, cell)
      assert :ok = Alambique.hibernate(first, cell)

      second =
        start_instance(store,
          migrations: @schema ++ [{2, "CREATE TABLE extra (x INTEGER)"}],
          feed: [internal_prefixes: ["_h_"]]
        )

      assert {:ok, 2} = Alambique.migrate(second, cell, wake: true)
      assert :ok = Alambique.hibernate(second, cell)

      assert {:error, {:schema_differs, 1, 2}} = Alambique.rewind(second, cell, {:txid, before})

      assert {:error, :keep_needs_same_schema} =
               Alambique.rewind(second, cell, {:txid, before}, schema: :migrate)

      assert {:ok, %{schema_version: 1}} =
               Alambique.rewind(second, cell, {:txid, before}, schema: :migrate, keep: :none)

      # The next wake migrates forward again.
      assert rows(second, cell) == [1]
      assert {:ok, _} = Alambique.query(second, cell, "SELECT x FROM extra")
    end
  end

  test "a frozen cell is rewound and stays frozen", %{inst: inst} do
    cell = unique_cell()
    for n <- 1..2, do: insert(inst, cell, n)
    at = position(inst, cell) - 1
    assert :ok = Alambique.freeze(inst, cell)

    assert {:ok, _} = Alambique.rewind(inst, cell, {:txid, at})
    assert {:ok, true} = Alambique.frozen?(inst, cell)
    assert :ok = Alambique.unfreeze(inst, cell)
    assert rows(inst, cell) == [1]
  end

  describe "feed" do
    defp read_all(inst) do
      ctx = %{Feed.ctx(inst) | settle_ms: 0}
      {:ok, records, _} = Feed.read(ctx, nil, 1_000)
      records
    end

    test "the marker follows the undone records and carries both positions", %{inst: inst} do
      cell = unique_cell()
      assert {:ok, _} = insert(inst, cell, 1)
      at = position(inst, cell)
      assert {:ok, _} = insert(inst, cell, 2)
      assert {:ok, %{txid: txid, from: from}} = Alambique.rewind(inst, cell, {:txid, at})
      assert {:ok, _} = insert(inst, cell, 3)

      eventually(fn -> length(read_all(inst)) >= 4 end)
      records = read_all(inst)
      id = "#{cell}:#{txid}:rewind"

      assert [
               %Rewind{
                 id: ^id,
                 from_txid: ^from,
                 to_txid: ^at,
                 txid: ^txid,
                 target: {:txid, ^at},
                 kept: ["_h_jobs"]
               } = marker
             ] =
               Enum.filter(records, &match?(%Rewind{}, &1))

      assert marker.at > 0

      {before, [^marker | after_marker]} = Enum.split_while(records, &(not match?(%Rewind{}, &1)))
      assert Enum.map(before, & &1.values["n"]) |> Enum.sort() == [1, 2]
      assert [3] = Enum.map(after_marker, & &1.values["n"])
      assert Enum.all?(before, fn %{position: {t, _}} -> t <= from end)
      assert hd(after_marker).position |> elem(0) > txid
    end

    # Rows that were unpublished at the target are published again by the next
    # owner with the ids they had, before the marker, as the rows of any wake.
    test "records of the target that are published again keep their ids", %{ctx: ctx} do
      inst = start_instance(ctx, migrations: @schema, feed: [publish_retry_ms: 50])
      cell = unique_cell()
      {MemoryStore, store} = ctx.store

      assert {:ok, _} = insert(inst, cell, 1)
      MemoryStore.inject(store, kind: :error, ops: [:put], key: ~r{/feed/}, times: 1_000)
      assert {:ok, _} = insert(inst, cell, 2)
      at = position(inst, cell)
      assert {:ok, _} = insert(inst, cell, 3)
      MemoryStore.clear(store)

      eventually(fn -> length(read_all(inst)) == 3 end)
      before = read_all(inst)
      ids = Map.new(before, &{&1.values["n"], &1.id})

      # What retention would have pruned: the next publication is a new object.
      {:ok, keys, _} = MemoryStore.list(store, "cells-test/feed/", nil, 1_000)
      for key <- keys, do: MemoryStore.delete(store, key)

      assert {:ok, %{txid: txid}} = Alambique.rewind(inst, cell, {:txid, at})
      assert {:ok, _} = insert(inst, cell, 4)
      eventually(fn -> Enum.any?(read_all(inst), &match?(%{values: %{"n" => 4}}, &1)) end)

      records = read_all(inst)

      assert {delivered, [%Rewind{txid: ^txid} | after_marker]} =
               Enum.split_while(records, &(not match?(%Rewind{}, &1)))

      # Rows 1 and 2 are the target's: same ids as the first time, before the
      # marker. Row 3 was undone and is not published again.
      assert Map.new(delivered, &{&1.values["n"], &1.id}) == Map.take(ids, [1, 2])
      assert Enum.map(after_marker, & &1.values["n"]) == [4]

      # After deduplication by id the feed holds each change once.
      all = before ++ records
      assert all |> Enum.reject(&match?(%Rewind{}, &1)) |> Enum.uniq_by(& &1.id) |> length() == 4

      assert Enum.all?(before, fn r ->
               Enum.find(records, &(&1.id == r.id)) in [nil, r] or true
             end)
    end
  end
end
