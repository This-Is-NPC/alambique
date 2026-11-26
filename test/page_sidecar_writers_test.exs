defmodule Alambique.PageSidecarWritersTest do
  @moduledoc "Every writer of a segment or snapshot writes its page-hash sidecar after it (design §16)."
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Native, Replicator, Sidecar}
  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    on_exit(&CrashPoints.reset/0)
    %{env | ctx: Map.put(env.ctx, :page_sidecar, 0)}
  end

  defp key(env, 9, _min, max), do: Keys.snapshot(env.ctx.prefix, cell(), max)
  defp key(env, level, min, max), do: Keys.segment(env.ctx.prefix, cell(), level, min, max)

  defp stored(env, key) do
    case env.mod.get(env.cfg, key) do
      {:ok, body, _etag, _meta} -> body
      {:error, :not_found} -> nil
    end
  end

  # The sidecar of the stored object at `key`, or nil.
  defp sidecar(env, key), do: stored(env, Keys.sidecar(key))

  defp expected(env, level, key) do
    {:ok, side} = Native.page_sidecar(level, stored(env, key))
    side
  end

  test "a segment gets the sidecar of its bytes", env do
    history(env, new_owner(env, "a.db"), 2)
    k = key(env, 0, 1, 1)
    assert sidecar(env, k) == expected(env, 0, k)
    assert sidecar(env, key(env, 0, 2, 2)) == expected(env, 0, key(env, 0, 2, 2))
  end

  test "a context without the setting gets none", env do
    off = %{env | ctx: Map.put(env.ctx, :page_sidecar, nil)}
    history(off, new_owner(off, "a.db"), 1)
    assert stored(env, key(env, 0, 1, 1)) != nil
    assert sidecar(env, key(env, 0, 1, 1)) == nil
  end

  test "a segment that is not larger than the size gets none", env do
    small = %{env | ctx: Map.put(env.ctx, :page_sidecar, 100_000_000)}
    history(small, new_owner(small, "a.db"), 1)
    assert stored(env, key(env, 0, 1, 1)) != nil
    assert sidecar(env, key(env, 0, 1, 1)) == nil
  end

  test "a crash between the object and its sidecar leaves the object alone", env do
    owner = new_owner(env, "a.db")
    CrashPoints.arm(:repl_before_sidecar, :raise)
    assert_raise CrashPoints.Crash, fn -> history(env, owner, 1) end
    k = key(env, 0, 1, 1)
    assert stored(env, k) != nil
    assert sidecar(env, k) == nil
    CrashPoints.reset()

    # The chain is whole without the sidecar.
    assert {:ok, %{txid: 1}, _dump} = restore(env, :latest)
  end

  test "a sidecar that cannot be written does not fail the sync", env do
    MemoryStore.inject(env.cfg, kind: :error, ops: [:put], key: ~r/\.pages$/)
    owner = history(env, new_owner(env, "a.db"), 1)
    assert owner.state.txid == 1
    assert sidecar(env, key(env, 0, 1, 1)) == nil
  end

  test "a stale sidecar is replaced, not kept", env do
    history(env, new_owner(env, "a.db"), 1)
    k = key(env, 0, 1, 1)
    other = stored(env, Keys.sidecar(k))
    stale = binary_part(other, 0, byte_size(other) - 1) <> <<0>>
    {:ok, _} = env.mod.put(env.cfg, Keys.sidecar(k), stale, :none, %{})

    object = stored(env, k)
    deadline = System.monotonic_time(:millisecond) + 2_000
    assert :ok = Sidecar.write(env.ctx, k, 0, object, %{epoch: 1, data: 1}, deadline, :ours)
    assert sidecar(env, k) == other
  end

  test "an existing key gets a sidecar only when its file checksum is the same", env do
    history(env, new_owner(env, "a.db"), 2)
    k = key(env, 0, 1, 1)
    mine = stored(env, k)
    other = stored(env, key(env, 0, 2, 2))
    deadline = System.monotonic_time(:millisecond) + 2_000
    info = %{epoch: 1, data: 1}
    :ok = env.mod.delete(env.cfg, Keys.sidecar(k))

    assert :skipped = Sidecar.write(env.ctx, k, 0, other, info, deadline, :verify)
    assert sidecar(env, k) == nil
    assert :ok = Sidecar.write(env.ctx, k, 0, mine, info, deadline, :verify)
    assert sidecar(env, k) == expected(env, 0, k)
  end

  test "a snapshot gets its sidecar at level 9, and a crash before it leaves the snapshot", env do
    owner = history(env, new_owner(env, "a.db"), 2)
    CrashPoints.arm(:repl_before_sidecar, :raise)
    assert_raise CrashPoints.Crash, fn -> Replicator.snapshot(owner.state, env.ctx) end
    k = key(env, 9, 1, 2)
    assert stored(env, k) != nil
    assert sidecar(env, k) == nil
    CrashPoints.reset()

    assert {:ok, ^k} = Replicator.snapshot(owner.state, env.ctx)
    assert sidecar(env, k) == expected(env, 9, k)
  end

  test "a replace (rewind) gets its sidecar", env do
    owner = history(env, new_owner(env, "a.db"), 1)
    {:ok, head} = Replication.head(env.ctx, cell())

    assert {:ok, 2} =
             Replicator.replace(env.ctx, cell(), owner.state.db_path, 4096, head, 1,
               pages: head.commit
             )

    k = key(env, 0, 2, 2)
    assert sidecar(env, k) == expected(env, 0, k)
  end

  test "compaction writes the sidecars of merged files and snapshots", env do
    history(env, new_owner(env, "a.db"), 4)
    assert {:ok, %{merges: 3}} = compact_pass(env, snapshot_interval_txids: 1)

    for {level, min, max} <- [{1, 1, 2}, {1, 3, 4}, {2, 1, 4}] do
      k = key(env, level, min, max)
      assert sidecar(env, k) == expected(env, level, k), "level #{level} #{min}-#{max}"
    end

    [txid | _] = snapshots(env)
    k = key(env, 9, 1, txid)
    assert sidecar(env, k) == expected(env, 9, k)
  end
end
