defmodule Alambique.RestoreHistoryTest do
  @moduledoc """
  Restore refuses to return an older state as if it were the target when the
  history has holes, which retention and interrupted runs can leave (design §6, §9).
  """
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.Compaction
  alias Alambique.Replication.Keys

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  defp delete_segment(env, level, min, max),
    do: env.mod.delete(env.cfg, Keys.segment(env.ctx.prefix, cell(), level, min, max))

  defp stamp(env, txid) do
    {:ok, _, _, meta} = env.mod.get(env.cfg, Keys.segment(env.ctx.prefix, cell(), 0, txid, txid))
    String.to_integer(meta["ts"])
  end

  # More than the tail window (`Chain.window/0`) of files follows the hole.
  test "a missing file in the middle is an error, not a shorter chain", env do
    history(env, new_owner(env, "a.db"), 20)
    delete_segment(env, 0, 3, 3)

    assert {:error, {:gap, 3}} = restore(env, :latest)
    assert {:error, {:gap, 3}} = restore(env, {:timestamp, stamp(env, 20)})
    # Targets before the hole are still served.
    assert {:ok, %{txid: 2}, _} = restore(env, {:txid, 2})
    assert {:ok, %{txid: 1}, _} = restore(env, {:timestamp, stamp(env, 1)})
  end

  test "a timestamp before the first state is still an empty database", env do
    history(env, new_owner(env, "a.db"), 3)
    assert {:ok, %{txid: 0}, {[], %{}}} = restore(env, {:timestamp, stamp(env, 1) - 1})
  end

  test "merged files alone cannot place a timestamp", env do
    owner = history(env, new_owner(env, "a.db"), 4)
    {:ok, _} = Compaction.run(env.ctx, cell(), fanout: 2, max_level: 1)
    target = stamp(env, 1)

    assert {:ok, %{txid: 1}, dump} = restore(env, {:timestamp, target})
    assert dump == owner.dumps[1]

    delete_segment(env, 0, 1, 1)
    assert {:error, {:history_coarse, 1}} = restore(env, {:timestamp, target})
    # The latest state does not need the finer file.
    assert {:ok, %{txid: 4}, _} = restore(env, :latest)
  end

  test "a store whose early history is gone does not restore as empty", env do
    owner = history(env, new_owner(env, "a.db"), 6)

    {:ok, _} =
      Compaction.run(env.ctx, cell(), fanout: 2, max_level: 0, snapshot_interval_txids: 4)

    for t <- 1..4, do: delete_segment(env, 0, t, t)
    assert snapshots(env) == [6]

    assert {:ok, %{txid: 6}, dump} = restore(env, :latest)
    assert dump == owner.dumps[6]

    assert {:error, :history_truncated} = restore(env, {:txid, 3})
    assert {:error, {:snapshot_after_target, _}} = restore(env, {:timestamp, stamp(env, 5)})
  end
end
