defmodule Alambique.LazyRestoreHostileTest do
  @moduledoc """
  A lazy restore treats the objects of the store as untrusted input: a
  corrupt or crafted first object is an error, never a crash, and no header
  field sizes an allocation (design §16, docs/segment-format.md).
  """
  use ExUnit.Case, async: false

  import Alambique.CompactionHelpers

  alias Alambique.LtxExample
  alias Alambique.Replication
  alias Alambique.Replication.Keys

  @lazy %{min_bytes: 0, wait_ms: 1_000}

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  # The example snapshot (6 pages, TXIDs 1 to 4), changed by `patch`, as the
  # only object of the cell.
  defp put_snapshot(env, patch) do
    key = Keys.snapshot(env.ctx.prefix, cell(), 4)
    body = patch.(LtxExample.snapshot())
    {:ok, _} = env.mod.put(env.cfg, key, body, :none, %{"ts" => "1000"})
  end

  defp restore_lazy(env) do
    path = Path.join(env.dir, "lazy-#{System.unique_integer([:positive])}.db")
    Replication.restore(env.ctx, cell(), path, :latest, lazy: @lazy)
  end

  defp put_u32(bin, offset, value) do
    <<head::binary-size(^offset), _::32, rest::binary>> = bin
    <<head::binary, value::32, rest::binary>>
  end

  test "a first object with an invalid page size is an error", env do
    put_snapshot(env, &put_u32(&1, 8, 0))
    assert {:error, {:snapshot, _, _}} = restore_lazy(env)
    put_snapshot(env, &put_u32(&1, 8, 3))
    assert {:error, {:snapshot, _, _}} = restore_lazy(env)
  end

  test "a first object shorter than its header is an error", env do
    put_snapshot(env, &binary_part(&1, 0, 5))
    assert {:error, {:snapshot, _, _}} = restore_lazy(env)
  end

  test "a commit the object has no pages for is an error", env do
    put_snapshot(env, &put_u32(&1, 12, 0xFFFFFFFF))
    assert {:error, {:snapshot, _, _}} = restore_lazy(env)
  end

  test "an index size beyond what the commit allows is an error", env do
    put_snapshot(env, fn bin ->
      at = byte_size(bin) - 16 - 8
      <<head::binary-size(^at), _::64, rest::binary>> = bin
      <<head::binary, byte_size(bin) - 24::64, rest::binary>>
    end)

    assert {:error, {:snapshot, _, _}} = restore_lazy(env)
  end
end
