defmodule Alambique.PageSidecarDeletionTest do
  @moduledoc "A page-hash sidecar goes with its object, and one that does not is swept (design §16)."
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.{Compaction, Retention, Trash}
  alias Alambique.Replication.Keys

  @day 86_400_000

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    %{env | ctx: Map.put(env.ctx, :page_sidecar, 0)}
  end

  defp retain(env, opts \\ []),
    do:
      Retention.run(
        env.ctx,
        cell(),
        Keyword.merge([pitr_window_ms: @day, grace_ms: 0, deletion_grace_ms: 0], opts)
      )

  defp stored(env), do: Enum.map(keys(env), &(Keys.files_prefix(env.ctx.prefix, cell()) <> &1))

  # Sidecars whose object is not there, and objects without a sidecar.
  defp orphans(env) do
    all = stored(env)

    for key <- all,
        String.ends_with?(key, ".pages"),
        String.replace_suffix(key, ".pages", "") not in all,
        do: key
  end

  defp uncovered(env) do
    all = stored(env)
    for key <- all, String.ends_with?(key, ".ltx"), Keys.sidecar(key) not in all, do: key
  end

  describe "keys" do
    @prefix "p"
    @object "p/cells/c/0000/0000000000000001-0000000000000001.ltx"

    test "parse never returns a sidecar" do
      assert {[%{key: @object}], []} = Keys.parse(@prefix, "c", [@object, Keys.sidecar(@object)])
      assert {[], []} = Keys.parse(@prefix, "c", [Keys.sidecar(@object)])
    end

    test "data_keys returns the sidecars with the objects, and not the fixed keys" do
      lease = "p/cells/c/lease"

      assert Enum.sort(Keys.data_keys(@prefix, "c", [lease, Keys.sidecar(@object), @object])) ==
               [@object, Keys.sidecar(@object)]

      assert Keys.data_keys(@prefix, "c", [lease, "p/cells/c/0000/x.ltx.pages"]) == []
    end

    test "sidecars lists the objects that have one" do
      other = "p/cells/c/0000/0000000000000002-0000000000000002.ltx"

      assert Keys.sidecars(@prefix, "c", [@object, other, Keys.sidecar(@object)]) ==
               MapSet.new([@object])

      assert Keys.sidecars(@prefix, "c", ["p/cells/c/lease.pages"]) == MapSet.new()
    end

    test "file_info reads the level and the range of a key" do
      assert Keys.file_info(@object) == %{level: 0, min: 1, max: 1}
      assert Keys.file_info(Keys.sidecar(@object)) == nil
      assert Keys.file_info("p/cells/c/lease") == nil
    end
  end

  test "marking every key of a cell deletes the sidecars too", env do
    history(env, new_owner(env, "a.db"), 3)
    assert length(stored(env)) == 6

    assert {:ok, 6} =
             Trash.mark(
               env.ctx,
               cell(),
               stored(env),
               [grace_ms: 0],
               System.monotonic_time(:millisecond) + 5_000
             )

    assert stored(env) == []
  end

  test "retention deletes the sidecars of the files it deletes", env do
    daily_history(env)
    assert uncovered(env) == []

    assert {:ok, %{snapshots: 2}} = retain(env)
    assert snapshots(env) == [15]
    assert orphans(env) == []
    assert uncovered(env) == []
  end

  test "compaction deletes the sidecars of the files it supersedes", env do
    history(env, new_owner(env, "a.db"), 8)

    assert {:ok, %{merges: 7}} =
             Compaction.run(env.ctx, cell(), fanout: 2, max_level: 3, deletion_grace_ms: 0)

    assert segments(env, 1) == []
    assert orphans(env) == []
    assert uncovered(env) == []
  end

  describe "orphan sidecars" do
    setup env do
      history(env, new_owner(env, "a.db"), 3)
      object = Keys.segment(env.ctx.prefix, cell(), 0, 2, 2)
      :ok = env.mod.delete(env.cfg, object)
      %{object: object}
    end

    test "are deleted by the next retention run", %{object: object} = env do
      assert orphans(env) == [Keys.sidecar(object)]
      assert {:ok, %{anchor: nil}} = retain(env)
      assert orphans(env) == []
      # The others keep theirs.
      assert length(stored(env)) == 4
    end

    test "wait for the deletion grace", %{object: object} = env do
      assert {:ok, _} = retain(env, deletion_grace_ms: @day)
      assert orphans(env) == [Keys.sidecar(object)]

      AlambiqueTestkit.ManualClock.advance(env.world, @day + 1)
      Trash.reap_ctx(env.ctx, Map.new(Trash.defaults()))
      assert orphans(env) == []
    end
  end
end
