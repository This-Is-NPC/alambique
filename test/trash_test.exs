defmodule Alambique.TrashTest do
  @moduledoc "Delayed deletions (design §9.1): marking, the reaper and the options."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers,
    only: [setup_store: 0, start_instance: 2, unique_cell: 0, repl_ctx: 2]

  import Alambique.CompactionHelpers

  alias Alambique.{Retention, Trash}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{CrashPoints, ManualClock, MemoryStore}

  @day 86_400_000

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    on_exit(&CrashPoints.reset/0)
    env
  end

  defp retain(env, opts \\ []) do
    Retention.run(
      env.ctx,
      cell(),
      Keyword.merge([pitr_window_ms: @day, grace_ms: 0, deletion_grace_ms: @day], opts)
    )
  end

  defp reap(env), do: {:ok, Trash.reap_ctx(env.ctx, Map.new(Trash.defaults()))}

  # Keys of the manifests, soonest first.
  defp manifests(env) do
    {:ok, keys, nil} = env.mod.list(env.cfg, Trash.prefix(env.ctx.prefix), nil, 10_000)
    keys
  end

  defp manifest_keys(env, key) do
    {:ok, body, _, _} = env.mod.get(env.cfg, key)
    Jason.decode!(body)["keys"]
  end

  # The objects of the cell, without the markers retention and compaction leave.
  defp objects(env), do: files(env) -- ["trashed", "chain"]

  describe "retention" do
    test "records the covered files and deletes nothing", env do
      daily_history(env)
      before = Enum.sort(objects(env))

      assert {:ok, %{anchor: 15, snapshots: 2, segments: segments}} = retain(env)
      assert segments > 0
      assert Enum.sort(objects(env)) == before
      assert [manifest] = manifests(env)
      assert length(manifest_keys(env, manifest)) == 2 + segments
    end

    test "points before the anchor stay restorable until the files go", env do
      owner = daily_history(env)
      assert {:ok, %{anchor: 15}} = retain(env)

      for txid <- [3, 7, 12, 15] do
        assert {:ok, _, dump} = restore(env, {:txid, txid})
        assert dump == owner.dumps[txid]
      end

      ManualClock.advance(env.world, @day)
      assert {:ok, %{manifests: 1, errors: 0}} = reap(env)
      assert snapshots(env) == [15]
      assert {:error, _} = restore(env, {:txid, 7})
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]
    end

    test "a file is deleted when its time is due and not before", env do
      daily_history(env)
      assert {:ok, %{anchor: 15}} = retain(env)
      before = Enum.sort(objects(env))

      assert {:ok, %{manifests: 0, objects: 0}} = reap(env)
      ManualClock.advance(env.world, @day - 1)
      assert {:ok, %{manifests: 0, objects: 0}} = reap(env)
      assert Enum.sort(objects(env)) == before

      ManualClock.advance(env.world, 1)
      assert {:ok, %{manifests: 1, objects: n, errors: 0}} = reap(env)
      assert n == length(before) - 1
      assert manifests(env) == []
      assert snapshots(env) == [15]
    end

    test "a zero grace deletes at once and records nothing", env do
      daily_history(env)
      assert {:ok, %{anchor: 15, snapshots: 2}} = retain(env, deletion_grace_ms: 0)
      assert snapshots(env) == [15]
      assert manifests(env) == []
      assert "trashed" not in files(env)
    end

    test "records a file once however often it runs", env do
      daily_history(env)
      assert {:ok, %{snapshots: 2}} = retain(env)
      assert {:ok, %{anchor: 15, snapshots: 0, segments: 0}} = retain(env)
      assert {:ok, %{snapshots: 0, segments: 0}} = retain(env)
      assert length(manifests(env)) == 1
    end

    test "a newer anchor records only what is new", env do
      owner = daily_history(env)
      assert {:ok, %{anchor: 15}} = retain(env)
      [first] = manifests(env)

      owner = history(env, owner, 5, 1)
      {:ok, %{snapshot: 20}} = compact_pass(env, snapshot_interval_txids: 5)
      ManualClock.advance(env.world, 2 * @day)
      assert {:ok, %{anchor: 20, snapshots: 1}} = retain(env)

      assert [^first, second] = manifests(env)
      assert manifest_keys(env, first) -- manifest_keys(env, second) == manifest_keys(env, first)
      assert Keys.snapshot(env.ctx.prefix, cell(), 15) in manifest_keys(env, second)
      assert owner.dumps[20] != nil
    end
  end

  describe "the reaper" do
    test "skips what is gone and finishes the rest", env do
      daily_history(env)
      assert {:ok, %{anchor: 15}} = retain(env)
      [manifest] = manifests(env)
      [gone | _] = manifest_keys(env, manifest)
      env.mod.delete(env.cfg, gone)

      ManualClock.advance(env.world, @day)
      assert {:ok, %{manifests: 1, errors: 0}} = reap(env)
      assert manifests(env) == []
      assert snapshots(env) == [15]
    end

    test "pages through many manifests", env do
      for n <- 1..7 do
        key = Keys.segment(env.ctx.prefix, cell(), 0, n, n)
        {:ok, _} = env.mod.put(env.cfg, key, "x", :none, %{})
        opts = [grace_ms: 1_000 * n, batch_keys: 1]
        assert {:ok, 1} = Trash.mark(env.ctx, cell(), [key], opts, deadline())
      end

      ManualClock.advance(env.world, 5_000)
      settings = Map.merge(Map.new(Trash.defaults()), %{page_size: 2})
      assert %{manifests: 5, objects: 5, errors: 0} = Trash.reap_ctx(env.ctx, settings)
      assert length(manifests(env)) == 2
    end

    @tag :capture_log
    test "never deletes outside the cell a manifest names", env do
      owner = history(env, new_owner(env, "a.db"), 3)
      stranger = Keys.segment(env.ctx.prefix, "other", 0, 1, 1)
      {:ok, _} = env.mod.put(env.cfg, stranger, "x", :none, %{})
      key = "#{Trash.prefix(env.ctx.prefix)}0000000000000001/#{cell()}/forged"
      body = Jason.encode!(%{cell: cell(), keys: [stranger]})
      {:ok, _} = env.mod.put(env.cfg, key, body, :none, %{})

      assert {:ok, %{manifests: 0, errors: 1}} = reap(env)
      assert {:ok, _, _, _} = env.mod.get(env.cfg, stranger)
      assert manifests(env) == [key]
      assert owner.state.txid == 3
    end

    test "deletes only segment and snapshot keys from an old manifest", env do
      data = Keys.segment(env.ctx.prefix, cell(), 0, 1, 1)
      frozen = Keys.frozen(env.ctx.prefix, cell())
      {:ok, _} = env.mod.put(env.cfg, data, "x", :none, %{})
      {:ok, _} = env.mod.put(env.cfg, frozen, "x", :none, %{})

      key = "#{Trash.prefix(env.ctx.prefix)}0000000000000000/#{cell()}/old"
      body = Jason.encode!(%{cell: cell(), keys: [data, frozen]})
      {:ok, _} = env.mod.put(env.cfg, key, body, :none, %{})

      assert {:ok, %{manifests: 1, objects: 1, errors: 0}} = reap(env)
      assert {:error, :not_found} = env.mod.get(env.cfg, data)
      assert {:ok, _, _, _} = env.mod.get(env.cfg, frozen)
      assert manifests(env) == []
    end

    test "marking drops fixed keys", env do
      frozen = Keys.frozen(env.ctx.prefix, cell())
      assert {:ok, 0} = Trash.mark(env.ctx, cell(), [frozen], [grace_ms: @day], deadline())
      assert manifests(env) == []
    end

    test "honours the rate", env do
      keys =
        for n <- 1..6 do
          key = Keys.segment(env.ctx.prefix, cell(), 0, n, n)
          {:ok, _} = env.mod.put(env.cfg, key, "x", :none, %{})
          key
        end

      assert {:ok, 6} = Trash.mark(env.ctx, cell(), keys, [grace_ms: 1], deadline())
      ManualClock.advance(env.world, 1)
      settings = Map.merge(Map.new(Trash.defaults()), %{rate: 20, concurrency: 2})

      {us, %{objects: 6}} = :timer.tc(fn -> Trash.reap_ctx(env.ctx, settings) end)
      # Three chunks of two at 20 objects per second: 100 ms each.
      assert us >= 280_000
    end
  end

  describe "faults" do
    @points [
      :trash_before_mark,
      :trash_after_mark,
      :retain_before_marker,
      :trash_before_delete,
      :trash_after_delete,
      :trash_before_entry_delete
    ]

    for point <- @points do
      @tag :capture_log
      test "a crash at #{point} leaves every point of the window restorable", env do
        owner = daily_history(env)
        CrashPoints.arm(unquote(point), :raise, times: 1)
        Process.flag(:trap_exit, true)

        for _stage <- 1..2 do
          attempt(fn -> retain(env) end)
          assert window_ok?(env, owner)
          ManualClock.advance(env.world, @day)
          attempt(fn -> reap(env) end)
          assert window_ok?(env, owner)
        end

        CrashPoints.reset()
        assert {:ok, %{}} = retain(env)
        ManualClock.advance(env.world, @day)
        assert {:ok, %{errors: 0}} = reap(env)
        assert snapshots(env) == [15]
        assert manifests(env) == []
      end
    end

    @tag :capture_log
    test "an interrupted reap leaves nothing for compaction to rebuild", env do
      daily_history(env)
      assert {:ok, %{anchor: 15}} = retain(env)
      ManualClock.advance(env.world, @day)

      # Stops when the first merged file is about to go: lower levels are gone.
      CrashPoints.arm(:trash_before_delete, :raise, when: &String.contains?(&1.key, "/0001/"))
      Process.flag(:trap_exit, true)
      assert {:ok, %{errors: 1}} = reap(env)
      CrashPoints.reset()

      assert segments(env, 0) == []
      before = Enum.sort(objects(env))
      assert {:ok, %{merges: 0}} = compact_pass(env, snapshot_interval_txids: 5)
      assert Enum.sort(objects(env)) == before
    end

    test "store faults while marking and reaping change nothing in the end", env do
      owner = daily_history(env)
      MemoryStore.inject(env.cfg, kind: :lost_ack, ops: [:put, :delete], times: 4)
      assert {:ok, %{anchor: 15}} = retain(env)
      ManualClock.advance(env.world, @day)
      assert {:ok, %{errors: 0}} = reap(env)
      assert {:ok, _, dump} = restore(env, :latest)
      assert dump == owner.dumps[15]
      assert snapshots(env) == [15]
    end
  end

  defp attempt(fun) do
    fun.()
  rescue
    _ -> :crashed
  catch
    _, _ -> :crashed
  end

  # The anchor and the newest state restore to what was acknowledged.
  defp window_ok?(env, owner) do
    Enum.all?([{:txid, 15}, :latest], fn target ->
      {:ok, _, dump} = restore(env, target)
      dump == owner.dumps[15]
    end)
  end

  defp deadline, do: System.monotonic_time(:millisecond) + 5_000

  describe "operators" do
    setup do
      ctx = setup_store()
      {:ok, Map.put(ctx, :inst, start_instance(ctx, migrations: [{1, "CREATE TABLE t (n)"}]))}
    end

    test "pending lists the manifests and cancel keeps the objects", %{inst: inst} = ctx do
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
      :ok = Alambique.hibernate(inst, cell)
      repl = repl_ctx(inst, cell)

      {:ok, keys} =
        Alambique.Replication.Objects.list(repl, Keys.files_prefix(repl.prefix, cell), 5_000)

      segment = Enum.find(keys, &String.contains?(&1, "/0000/"))

      assert {:ok, %{entries: []}} = Trash.pending(inst)
      assert {:ok, 1} = Trash.mark(repl, cell, [segment], [grace_ms: 0 + 50], deadline())

      assert {:ok, %{entries: [%{key: key, cell: ^cell, due_ms: due}], next: nil}} =
               Trash.pending(inst)

      assert due > 0

      assert :ok = Trash.cancel(inst, key)
      assert {:ok, %{entries: []}} = Trash.pending(inst)
      assert {:error, :not_a_manifest} = Trash.cancel(inst, segment)
      assert {:ok, %{manifests: 0}} = Trash.reap(inst)
      assert {:ok, _, _, _} = MemoryStore.get(ctx.store_config, segment)
    end
  end
end
