defmodule Alambique.CellMigrationsTest do
  @moduledoc "Migrations chosen per cell at wake (design §17)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias AlambiqueTestkit.MemoryStore

  @moduletag :capture_log

  @v1 {1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}
  @v2 {2, "ALTER TABLE t ADD COLUMN w INTEGER"}
  @v3 {3, "ALTER TABLE t ADD COLUMN x INTEGER"}
  @v4 {4, "ALTER TABLE t ADD COLUMN y INTEGER"}

  defmodule Lists do
    @moduledoc false
    @key {__MODULE__, :lists}

    def set(lists), do: :persistent_term.put(@key, lists)
    def clear, do: :persistent_term.erase(@key)

    # The namespace of "a.1" is "a". A value `{:broken, kind}` fails in a way.
    def migrations(cell) do
      [namespace | _] = String.split(cell, ".")

      case Map.fetch!(:persistent_term.get(@key), namespace) do
        {:broken, kind} -> broken(kind)
        list -> list
      end
    end

    defp broken(:raising), do: raise("boom")
    defp broken(:throwing), do: throw(:nope)
    defp broken(:exiting), do: exit(:gone)
    defp broken(:not_a_list), do: :nope
    defp broken(:gap), do: [{1, "SELECT 1"}, {3, "SELECT 1"}]
    defp broken(:duplicate), do: [{1, "SELECT 1"}, {1, "SELECT 1"}]
    defp broken(:bad_entry), do: [{1, :sql}]
  end

  defmodule Named do
    @moduledoc false
    # Lists by namespace for each label, so that instances of one VM can
    # choose their lists with the extra argument.
    def set(label, lists), do: :persistent_term.put({__MODULE__, label}, lists)
    def clear(label), do: :persistent_term.erase({__MODULE__, label})

    def migrations(cell, label) do
      [namespace | _] = String.split(cell, ".")
      Map.fetch!(:persistent_term.get({__MODULE__, label}), namespace)
    end
  end

  setup do
    on_exit(fn ->
      Lists.clear()
      Enum.each([:one, :two], &Named.clear/1)
    end)

    setup_store()
  end

  defp start(ctx, lists, opts \\ []) do
    Lists.set(lists)
    start_instance(ctx, [migrations: {Lists, :migrations}] ++ opts)
  end

  defp put(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n, v) VALUES (?, 'x')", [n])

  defp version(inst, cell) do
    {:ok, %{rows: [[version]]}} = Alambique.query(inst, cell, "PRAGMA user_version")
    version
  end

  defp index(%{store_config: config}) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/schema/", nil, 100_000)

    for "cells-test/schema/" <> rest <- keys do
      [version, cell] = String.split(rest, "/")
      {String.to_integer(version), cell}
    end
    |> Enum.sort()
  end

  defp keys(%{store_config: config}) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/", nil, 100_000)
    keys
  end

  describe "options" do
    test "{module, function} must name an exported function/1", ctx do
      base = [name: :bad, store: ctx.store, data_dir: ctx.dir]

      for {migrations, message} <- [
            {{Nope, :migrations}, "Nope.migrations/1 must be exported"},
            {{Lists, :missing}, "missing/1 must be exported"},
            {{Lists, :clear}, "clear/1 must be exported"}
          ] do
        opts = base ++ [migrations: migrations]
        error = assert_raise ArgumentError, fn -> Alambique.child_spec(opts) end
        assert Exception.message(error) =~ message
      end

      assert %{} = Alambique.child_spec(base ++ [migrations: {Lists, :migrations}])
    end

    test "{module, function, extra_args} needs function/(1 + length(extra_args))", ctx do
      base = [name: :bad, store: ctx.store, data_dir: ctx.dir]

      for {migrations, message} <- [
            {{Lists, :migrations, [:one]}, "Lists.migrations/2 must be exported"},
            {{Named, :migrations, []}, "Named.migrations/1 must be exported"},
            {{Named, :migrations, [:a, :b]}, "Named.migrations/3 must be exported"},
            {{Named, :migrations, :one}, "must be a module, a {module, function"}
          ] do
        error =
          assert_raise ArgumentError, fn ->
            Alambique.child_spec(base ++ [migrations: migrations])
          end

        assert Exception.message(error) =~ message
      end

      assert %{} = Alambique.child_spec(base ++ [migrations: {Named, :migrations, [:one]}])
    end

    test "{module, function} is the same as extra_args []", ctx do
      base = [name: :same, store: ctx.store, data_dir: ctx.dir]
      short = Alambique.Config.build(base ++ [migrations: {Lists, :migrations}])
      long = Alambique.Config.build(base ++ [migrations: {Lists, :migrations, []}])
      assert short.migrations == {Lists, :migrations, []}
      assert long.migrations == short.migrations
    end
  end

  describe "wake" do
    test "each cell gets the list of its namespace, and an index entry at its version, 0 included",
         ctx do
      inst = start(ctx, %{"a" => [@v1, @v2], "b" => [@v1], "c" => []})

      assert {:ok, _} = put(inst, "a.1", 1)
      assert {:ok, _} = put(inst, "b.1", 1)
      assert version(inst, "a.1") == 2
      assert version(inst, "b.1") == 1
      assert version(inst, "c.1") == 0
      assert index(ctx) == [{0, "c.1"}, {1, "b.1"}, {2, "a.1"}]
    end

    test "a list that grows is applied at the next wake", ctx do
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)
      :ok = Alambique.hibernate(inst, "a.1")

      Lists.set(%{"a" => [@v1, @v2, @v3]})
      assert version(inst, "a.1") == 3
      assert index(ctx) == [{3, "a.1"}]
      assert durable_rows(ctx, "a.1", "PRAGMA user_version") == [[3]]
    end

    test "the window is relative to the list of the cell", ctx do
      inst = start(ctx, %{"a" => [@v1, @v2, @v3, @v4], "b" => [@v1, @v2, @v3, @v4]})

      for cell <- ["a.1", "b.1"] do
        assert {:ok, _} = put(inst, cell, 1)
        :ok = Alambique.hibernate(inst, cell)
      end

      # Both cells are at 4: "a" still has four migrations, "b" has two.
      Lists.set(%{"a" => [@v1, @v2, @v3, @v4], "b" => [@v1, @v2]})
      assert version(inst, "a.1") == 4
      assert {:error, {:schema_too_new, 4}} = put(inst, "b.1", 2)
      assert :none = Alambique.owner(inst, "b.1")
      assert durable_rows(ctx, "b.1", "SELECT n FROM t") == [[1]]
    end

    test "the extra arguments choose the list of each instance", ctx do
      Named.set(:one, %{"a" => [@v1]})
      Named.set(:two, %{"a" => [@v1, @v2, @v3]})
      one = start_instance(ctx, migrations: {Named, :migrations, [:one]}, prefix: "one")
      two = start_instance(ctx, migrations: {Named, :migrations, [:two]}, prefix: "two")

      assert {:ok, _} = put(one, "a.1", 1)
      assert {:ok, _} = put(two, "a.1", 1)
      assert version(one, "a.1") == 1
      assert version(two, "a.1") == 3
    end

    test "a failing function with extra arguments names its arity", ctx do
      Named.set(:one, %{})
      inst = start_instance(ctx, migrations: {Named, :migrations, [:one]})

      assert {:error, {:bad_migrations, message}} = put(inst, "a.1", 1)
      assert message =~ "Named.migrations/2 for a.1"
    end

    for kind <- [:raising, :throwing, :exiting, :not_a_list, :gap, :duplicate, :bad_entry] do
      test "a #{kind} function fails the wake and writes nothing", ctx do
        inst = start(ctx, %{"a" => {:broken, unquote(kind)}})

        assert {:error, {:bad_migrations, message}} = put(inst, "a.1", 1)
        assert message =~ "Lists.migrations/1 for a.1"
        assert :none = Alambique.owner(inst, "a.1")
        assert Enum.reject(keys(ctx), &String.contains?(&1, "lease")) == []
      end
    end
  end

  describe "sweeper" do
    test "wakes only the cells below their own version", ctx do
      old = start(ctx, %{"a" => [@v1], "b" => [@v1]})
      for cell <- ["a.1", "a.2", "b.1"], do: {:ok, _} = put(old, cell, 1)
      for cell <- ["a.1", "a.2", "b.1"], do: :ok = Alambique.hibernate(old, cell)

      # Only namespace "a" is deployed.
      Lists.set(%{"a" => [@v1, @v2], "b" => [@v1]})
      assert {:ok, %{cells: 2, migrated: 2, errors: 0}} = Alambique.sweep(old)
      assert index(ctx) == [{1, "b.1"}, {2, "a.1"}, {2, "a.2"}]
      assert {:ok, %{cells: 0}} = Alambique.sweep(old)
    end

    test "a failing function is an error for its cell and the round goes on", ctx do
      old = start(ctx, %{"a" => [@v1], "b" => [@v1]})
      for cell <- ["a.1", "b.1"], do: {:ok, _} = put(old, cell, 1)
      for cell <- ["a.1", "b.1"], do: :ok = Alambique.hibernate(old, cell)

      Lists.set(%{"a" => {:broken, :raising}, "b" => [@v1, @v2]})
      assert {:ok, %{cells: 2, migrated: 1, errors: 1}} = Alambique.sweep(old)
      assert index(ctx) == [{1, "a.1"}, {2, "b.1"}]
    end
  end

  describe "sweeper and check_deploy with extra arguments" do
    test "use the 3-tuple of the instance", ctx do
      Named.set(:one, %{"a" => [@v1]})
      opts = [migrations: {Named, :migrations, [:one]}]
      inst = start_instance(ctx, opts)
      {:ok, _} = put(inst, "a.1", 1)
      :ok = Alambique.hibernate(inst, "a.1")

      Named.set(:one, %{"a" => [@v1, @v2, @v3]})

      assert {:error, {:too_old, %{locations: [%{too_old_cells: ["a.1"]}]}}} =
               Alambique.check_deploy(inst)

      options = [store: ctx.store, prefix: "cells-test", migration_window: 1] ++ opts
      assert {:error, {:too_old, _}} = Alambique.check_deploy(options)

      assert {:ok, %{cells: 1, migrated: 1, errors: 0}} = Alambique.sweep(inst)
      assert index(ctx) == [{3, "a.1"}]
      assert {:ok, _} = Alambique.check_deploy(inst)
    end
  end

  describe "check_deploy" do
    defp written(ctx, lists, cells) do
      inst = start(ctx, lists)

      for cell <- cells do
        {:ok, _} = put(inst, cell, 1)
        :ok = Alambique.hibernate(inst, cell)
      end

      inst
    end

    test "compares each cell with the window of its own list", ctx do
      inst = written(ctx, %{"a" => [@v1], "b" => [@v1, @v2, @v3]}, ["a.1", "b.1"])
      # "a.1" is at 1 of 1, "b.1" at 3 of 3.
      assert {:ok, %{code_version: nil, oldest_readable: nil} = report} =
               Alambique.check_deploy(inst)

      assert [%{versions: [1, 3], too_old: false, too_old_cells: []}] = report.locations

      # Release "a" to 3 migrations: "a.1" is now two behind.
      Lists.set(%{"a" => [@v1, @v2, @v3], "b" => [@v1, @v2, @v3]})

      assert {:error, {:too_old, report}} = Alambique.check_deploy(inst)
      assert [%{versions: [1, 3], too_old: true, too_old_cells: ["a.1"]}] = report.locations
    end

    test "works from keyword options", ctx do
      written(ctx, %{"a" => [@v1]}, ["a.1"])

      options = [
        store: ctx.store,
        prefix: "cells-test",
        migrations: {Lists, :migrations},
        migration_window: 1
      ]

      Lists.set(%{"a" => [@v1, @v2, @v3]})

      assert {:error, {:too_old, %{locations: [%{too_old_cells: ["a.1"]}]}}} =
               Alambique.check_deploy(options)

      assert {:ok, _} = Alambique.check_deploy(Keyword.put(options, :migration_window, 2))
    end

    test "a failing function is an error", ctx do
      inst = written(ctx, %{"a" => [@v1]}, ["a.1"])
      Lists.set(%{"a" => {:broken, :raising}})

      assert {:error, {:cell, "a.1", {:bad_migrations, _}}} = Alambique.check_deploy(inst)
    end

    test "static instances keep the report without per-cell keys", ctx do
      inst = start_instance(ctx, migrations: [@v1])
      assert {:ok, report} = Alambique.check_deploy(inst)
      assert [location] = report.locations
      refute Map.has_key?(location, :too_old_cells)
    end
  end

  describe "migrate" do
    defp pid_of(inst, cell), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

    test "migrates an awake cell in place when its list grew", ctx do
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)
      pid = pid_of(inst, "a.1")
      assert index(ctx) == [{1, "a.1"}]

      Lists.set(%{"a" => [@v1, @v2, @v3]})
      assert {:ok, 3} = Alambique.migrate(inst, "a.1")

      assert pid_of(inst, "a.1") == pid
      assert version(inst, "a.1") == 3
      assert index(ctx) == [{3, "a.1"}]
      assert durable_rows(ctx, "a.1", "PRAGMA user_version") == [[3]]
      assert durable_rows(ctx, "a.1", "SELECT n FROM t") == [[1]]
      assert {:ok, _} = put(inst, "a.1", 2)

      # Nothing pending: the version comes back and nothing is uploaded.
      assert {:ok, 3} = Alambique.migrate(inst, "a.1")
      assert index(ctx) == [{3, "a.1"}]
    end

    test "answers after the writes before it are durable and serves the ones behind it", ctx do
      test = self()
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)
      pid = pid_of(inst, "a.1")

      slow = {2, fn tx -> block(test, tx) end}
      Lists.set(%{"a" => [@v1, slow]})

      # The write and the migration are both in the mailbox when the cell runs.
      :sys.suspend(pid)
      write = Task.async(fn -> put(inst, "a.1", 2) end)
      eventually(fn -> queued(pid) == 1 end)
      migrate = Task.async(fn -> Alambique.migrate(inst, "a.1") end)
      eventually(fn -> queued(pid) == 2 end)
      :sys.resume(pid)

      assert_receive {:in_migration, tx_pid}, 5_000
      # The write was acknowledged before the migration started.
      assert {:ok, _} = Task.await(write, 5_000)
      assert durable_rows(ctx, "a.1", "SELECT n FROM t ORDER BY n") == [[1], [2]]

      behind = Task.async(fn -> put(inst, "a.1", 3) end)
      eventually(fn -> queued(pid) == 1 end)
      send(tx_pid, :go)

      assert {:ok, 2} = Task.await(migrate, 5_000)
      assert {:ok, _} = Task.await(behind, 5_000)
      assert pid_of(inst, "a.1") == pid
      assert durable_rows(ctx, "a.1", "SELECT n FROM t ORDER BY n") == [[1], [2], [3]]
    end

    defp block(test, tx) do
      send(test, {:in_migration, self()})

      receive do
        :go -> {:ok, _} = Alambique.Tx.execute(tx, "ALTER TABLE t ADD COLUMN w INTEGER")
      end
    end

    test "rows a migration writes reach the feed once", ctx do
      seed = {2, "INSERT INTO t (n, v) VALUES (100, 'seed')"}
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)

      Lists.set(%{"a" => [@v1, seed]})
      assert {:ok, 2} = Alambique.migrate(inst, "a.1")
      assert {:ok, 2} = Alambique.migrate(inst, "a.1")

      feed = Alambique.Feed.ctx(store: ctx.store, prefix: "cells-test", settle_ms: 0)

      seeded = fn ->
        {:ok, records, _} = Alambique.Feed.read(feed, nil, 1000)
        for r <- records, r.key == %{"n" => 100}, do: r.op
      end

      eventually(fn -> seeded.() == [:insert] end)
      :ok = Alambique.hibernate(inst, "a.1")
      assert version(inst, "a.1") == 2
      assert seeded.() == [:insert]
    end

    test "a cell nobody owns is not woken unless asked", ctx do
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)
      :ok = Alambique.hibernate(inst, "a.1")

      Lists.set(%{"a" => [@v1, @v2]})
      assert {:ok, :asleep} = Alambique.migrate(inst, "a.1")
      assert :none = Alambique.owner(inst, "a.1")
      assert durable_rows(ctx, "a.1", "PRAGMA user_version") == [[1]]

      assert {:ok, 2} = Alambique.migrate(inst, "a.1", wake: true)
      assert {:ok, _} = Alambique.owner(inst, "a.1")
      assert durable_rows(ctx, "a.1", "PRAGMA user_version") == [[2]]
      assert index(ctx) == [{2, "a.1"}]
    end

    test "a cell too far ahead refuses and keeps serving", ctx do
      inst = start(ctx, %{"a" => [@v1, @v2, @v3, @v4]})
      assert {:ok, _} = put(inst, "a.1", 1)
      pid = pid_of(inst, "a.1")

      Lists.set(%{"a" => [@v1, @v2]})
      assert {:error, {:schema_too_new, 4}} = Alambique.migrate(inst, "a.1")
      assert pid_of(inst, "a.1") == pid
      assert {:ok, _} = put(inst, "a.1", 2)
      assert index(ctx) == [{4, "a.1"}]
    end

    test "a bad function leaves the cell serving", ctx do
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)
      pid = pid_of(inst, "a.1")

      Lists.set(%{"a" => {:broken, :raising}})
      assert {:error, {:bad_migrations, message}} = Alambique.migrate(inst, "a.1")
      assert message =~ "Lists.migrations/1 for a.1"
      assert pid_of(inst, "a.1") == pid
      assert {:ok, _} = put(inst, "a.1", 2)
    end

    test "a failing migration keeps the ones before it and the cell serving", ctx do
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)
      pid = pid_of(inst, "a.1")

      Lists.set(%{"a" => [@v1, @v2, {3, "ALTER TABLE missing ADD COLUMN z INTEGER"}, @v4]})
      assert {:error, {:migration, 3, _}} = Alambique.migrate(inst, "a.1")

      assert pid_of(inst, "a.1") == pid
      assert version(inst, "a.1") == 2
      assert durable_rows(ctx, "a.1", "PRAGMA user_version") == [[2]]
      assert index(ctx) == [{2, "a.1"}]
      assert {:ok, _} = put(inst, "a.1", 2)

      # A fixed list picks up from there.
      Lists.set(%{"a" => [@v1, @v2, @v3, @v4]})
      assert {:ok, 4} = Alambique.migrate(inst, "a.1")
      assert index(ctx) == [{4, "a.1"}]
    end

    test "a failed upload stops the cell", ctx do
      inst = start(ctx, %{"a" => [@v1]})
      assert {:ok, _} = put(inst, "a.1", 1)

      Lists.set(%{"a" => [@v1, @v2]})
      AlambiqueTestkit.MemoryStore.inject(ctx.store_config, kind: :error, times: 1_000, ms: 1)
      assert {:error, _} = Alambique.migrate(inst, "a.1", timeout: 10_000)
      AlambiqueTestkit.MemoryStore.clear(ctx.store_config)

      eventually(fn -> pid_of(inst, "a.1") == nil end)
    end

    test "options are checked", ctx do
      inst = start(ctx, %{"a" => []})
      assert_raise ArgumentError, fn -> Alambique.migrate(inst, "a.1", wake: :yes) end
      assert_raise ArgumentError, fn -> Alambique.migrate(inst, "a.1", other: 1) end
    end
  end
end
