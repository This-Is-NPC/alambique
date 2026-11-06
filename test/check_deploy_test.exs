defmodule Alambique.CheckDeployTest do
  @moduledoc "The deploy check, as a function and as a mix task (design §17)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.CountingStore
  alias Alambique.Location
  alias AlambiqueTestkit.MemoryStore

  @moduletag :capture_log

  @v1 {1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}
  @v2 {2, "ALTER TABLE t ADD COLUMN w INTEGER"}
  @v3 {3, "ALTER TABLE t ADD COLUMN x INTEGER"}

  defmodule Release3 do
    @moduledoc false
    def migrations,
      do: [
        {1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"},
        {2, "ALTER TABLE t ADD COLUMN w INTEGER"},
        {3, "ALTER TABLE t ADD COLUMN x INTEGER"}
      ]
  end

  defmodule PerCell do
    @moduledoc false
    def for_cell(_cell),
      do: [
        {1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"},
        {2, "ALTER TABLE t ADD COLUMN w INTEGER"},
        {3, "ALTER TABLE t ADD COLUMN x INTEGER"}
      ]
  end

  setup do
    base = setup_store()
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    old = Application.get_env(:alambique, :ops)
    on_exit(fn -> restore_env(old, shell) end)
    base
  end

  defp restore_env(old, shell) do
    Mix.shell(shell)

    if old,
      do: Application.put_env(:alambique, :ops, old),
      else: Application.delete_env(:alambique, :ops)
  end

  defp cells(ctx, migrations, count) do
    inst = start_instance(ctx, migrations: migrations)

    for _ <- 1..count do
      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (1)")
      :ok = Alambique.hibernate(inst, cell)
      cell
    end
  end

  defp options(ctx, migrations),
    do: [store: ctx.store, prefix: "cells-test", migrations: migrations]

  describe "Alambique.check_deploy/2" do
    test "passes when every live cell is within the window", ctx do
      cells(ctx, [@v1, @v2], 3)

      assert {:ok, report} = Alambique.check_deploy(options(ctx, [@v1, @v2, @v3]))
      assert report.code_version == 3
      assert report.oldest_readable == 2
      assert [%{location: "default", versions: [2], too_old: false}] = report.locations
    end

    test "fails when a cell is older than the oldest readable version", ctx do
      cells(ctx, [@v1], 2)
      cells(ctx, [@v1, @v2], 1)

      assert {:error, {:too_old, report}} = Alambique.check_deploy(options(ctx, [@v1, @v2, @v3]))
      assert [%{versions: [1, 2], too_old: true}] = report.locations
      assert {:ok, _} = Alambique.check_deploy(options(ctx, [@v1, @v2]))
    end

    test "the migration window moves the limit", ctx do
      cells(ctx, [@v1], 1)
      assert {:error, {:too_old, _}} = Alambique.check_deploy(options(ctx, [@v1, @v2, @v3]))

      assert {:ok, %{oldest_readable: 1}} =
               Alambique.check_deploy(options(ctx, [@v1, @v2, @v3]) ++ [migration_window: 2])
    end

    test "passes after the sweeper drained the old versions", ctx do
      cells(ctx, [@v1], 4)
      new = start_instance(ctx, migrations: [@v1, @v2, @v3])
      assert {:error, {:too_old, _}} = Alambique.check_deploy(new)

      assert {:ok, %{migrated: 4}} = Alambique.sweep(new)
      assert {:ok, %{locations: [%{versions: [3]}]}} = Alambique.check_deploy(new)
    end

    test "an empty store passes", ctx do
      assert {:ok, %{locations: [%{versions: [], too_old: false}]}} =
               Alambique.check_deploy(options(ctx, [@v1, @v2, @v3]))
    end

    test "costs a few requests, not one per cell", ctx do
      cells(ctx, [@v1], 10)
      counting = CountingStore.wrap(ctx.store)
      {_, counts} = counting

      assert {:error, {:too_old, _}} =
               Alambique.check_deploy(
                 store: counting,
                 prefix: "cells-test",
                 migrations: [@v1, @v2, @v3]
               )

      assert CountingStore.count(counts, :list) == 2
      assert CountingStore.count(counts, :get) == 0
      assert CountingStore.count(counts, :head) == 0
    end

    test "a location that cannot be listed is an error, not a pass", ctx do
      cells(ctx, [@v1], 1)
      MemoryStore.inject(ctx.store_config, kind: :error, times: 1_000, ops: [:list])

      assert {:error, {:location, "default", _}} =
               Alambique.check_deploy(options(ctx, [@v1, @v2, @v3]), timeout_ms: 200)
    end

    test "checks every location of the placement", ctx do
      store_b = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: :b))

      locations = [
        %Location{id: "a", store: ctx.store, prefix: "pa"},
        %Location{id: "b", store: {MemoryStore, MemoryStore.config(store_b)}, prefix: "pb"}
      ]

      placement =
        {AlambiqueTestkit.Placement,
         locations: locations, table: ctx.store, initial: %{"in-a" => "a", "in-b" => "b"}}

      old = start_instance(ctx, placement: placement, migrations: [@v1])
      new = start_instance(ctx, placement: placement, migrations: [@v1, @v2, @v3])
      assert {:ok, _} = Alambique.execute(old, "in-a", "CREATE TABLE IF NOT EXISTS x (a)")
      assert {:ok, _} = Alambique.execute(old, "in-b", "CREATE TABLE IF NOT EXISTS x (a)")
      :ok = Alambique.hibernate(old, "in-a")
      :ok = Alambique.hibernate(old, "in-b")

      assert {:error, {:too_old, %{locations: found}}} = Alambique.check_deploy(new)
      assert [%{location: "a", too_old: true}, %{location: "b", too_old: true}] = found

      assert {:ok, _} = Alambique.sweep(new)

      assert {:ok, %{locations: [%{versions: [3]}, %{versions: [3]}]}} =
               Alambique.check_deploy(new)
    end
  end

  describe "mix alambique.check_deploy" do
    defp task(args) do
      Mix.Task.rerun("alambique.check_deploy", args)
    end

    defp output(acc \\ []) do
      receive do
        {:mix_shell, level, [line]} -> output([{level, line} | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end

    test "exits 0 and prints the versions found", ctx do
      cells(ctx, [@v1, @v2], 2)
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "cells-test")

      task(["--migrations", inspect(Release3)])

      assert [
               info: "Code at schema version 3 reads cells from version 2.",
               info: "  location default: versions 2",
               info: "Deploy allowed" <> _
             ] = output()
    end

    test "names the cells that are too old when migrations are chosen per cell", ctx do
      [cell] = cells(ctx, [@v1], 1)
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "cells-test")

      assert {:shutdown, 1} = catch_exit(task(["--migrations", inspect(PerCell) <> ":for_cell"]))

      lines = output()
      assert {:info, "  location default: versions 1  <- too old: #{cell}"} in lines
      assert {:error, "Deploy refused: cells in default (" <> _} = List.last(lines)
    end

    test "exits 1 and names the location when the deploy is refused", ctx do
      cells(ctx, [@v1], 1)
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "cells-test")

      assert {:shutdown, 1} = catch_exit(task(["--migrations", inspect(Release3)]))

      lines = output()
      assert {:info, "  location default: versions 1  <- too old"} in lines
      assert {:error, "Deploy refused: cells in default (version 1)" <> _} = List.last(lines)
    end

    test "the window comes from a flag or the ops config", ctx do
      cells(ctx, [@v1], 1)
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "cells-test")
      task(["--migrations", inspect(Release3), "--window", "2"])
      assert {:info, "Code at schema version 3 reads cells from version 1."} in output()

      Application.put_env(:alambique, :ops,
        store: ctx.store,
        prefix: "cells-test",
        migrations: Release3
      )

      assert {:shutdown, 1} = catch_exit(task([]))
    end

    test "exits 2 when it cannot run", ctx do
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "cells-test")
      assert {:shutdown, 2} = catch_exit(task([]))
      assert {:error, "give the migrations" <> _} = List.last(output())

      MemoryStore.inject(ctx.store_config, kind: :error, times: 1_000, ops: [:list])

      Application.put_env(:alambique, :ops,
        store: ctx.store,
        prefix: "cells-test",
        timeout_ms: 200
      )

      assert {:shutdown, 2} = catch_exit(task(["--migrations", inspect(Release3)]))
      assert {:error, "could not check the deploy" <> _} = List.last(output())
    end

    test "bad arguments exit 2 with the reason, before the store is read", ctx do
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "cells-test")
      release = inspect(Release3)

      for {args, message} <- [
            {["extra"], "usage: mix alambique.check_deploy [options]"},
            {["--migrations", release, "--sample", "0"], "--sample must be a positive integer"},
            {["--migrations", "A:b:c"], "--migrations must be MODULE or MODULE:FUNCTION"},
            {["--migrations", "No.Such.Release"], "No.Such.Release"}
          ] do
        assert {:shutdown, 2} = catch_exit(task(args))
        assert {:error, text} = List.last(output())
        assert text =~ message
      end
    end

    test "lists as many cells as --sample and counts the rest", ctx do
      cells = cells(ctx, [@v1], 3)
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "cells-test")

      assert {:shutdown, 1} =
               catch_exit(
                 task(["--migrations", inspect(PerCell) <> ":for_cell", "--sample", "2"])
               )

      lines = output()
      shown = Enum.sort(cells) |> Enum.take(2) |> Enum.join(", ")
      assert {:info, "  location default: versions 1  <- too old: #{shown} and 1 more"} in lines
      assert {:error, "Deploy refused: cells in default (" <> refusal} = List.last(lines)
      assert refusal =~ "#{shown} and 1 more)"
    end
  end
end
