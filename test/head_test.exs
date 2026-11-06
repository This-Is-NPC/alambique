defmodule Alambique.HeadTest do
  @moduledoc "Finding the head of a chain when the store fails or holds a bad file (design §6)."
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.Replication
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.MemoryStore

  setup do
    env = setup_world(timeout_ms: 100)
    on_exit(fn -> File.rm_rf(env.dir) end)
    history(env, new_owner(env, "a.db"), 3)
    env
  end

  test "an unreadable listing is an error", env do
    MemoryStore.inject(env.cfg, kind: :error, ops: [:list], reason: :down)

    assert {:error, :down} = Replication.head(env.ctx, cell())
  end

  test "an epoch that cannot be read is an error, not a guess", env do
    MemoryStore.inject(env.cfg, kind: :error, ops: [:head], reason: :down)

    assert {:error, :down} = Replication.head(env.ctx, cell())
  end

  test "a last file that is not a segment is reported with its key", env do
    key = Keys.segment(env.ctx.prefix, cell(), 0, 3, 3)
    {:ok, _} = env.mod.put(env.cfg, key, String.duplicate("x", 200), :none, %{"epoch" => "1"})

    assert {:error, {:segment, ^key, _reason}} = Replication.head(env.ctx, cell())
  end
end
