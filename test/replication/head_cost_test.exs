defmodule Alambique.Replication.HeadCostTest do
  @moduledoc "Finding the head of a chain reads the edges of its last file, not the file (design §6)."
  use ExUnit.Case, async: false

  import Alambique.ReplicationHelpers

  alias Alambique.CountingStore
  alias Alambique.Replication
  alias Alambique.Replication.Replicator

  setup do
    {_pid, store} = start_store()
    dir = tmp_dir()
    on_exit(fn -> File.rm_rf(dir) end)
    {_, counting} = counted = CountingStore.wrap(store)
    {:ok, dir: dir, counting: counting, ctx: ctx(counted, new_world())}
  end

  defp sync(ctx, dir, ops) do
    {conn, state} = new_replicator(dir, "src.db")
    Enum.each(ops, &safe_op(conn, &1))
    assert {:ok, state} = Replicator.sync(state, ctx)
    state
  end

  test "a large file is read by range", %{ctx: ctx, dir: dir, counting: counting} do
    # 400 pages: the first file holds the whole database.
    state = sync(ctx, dir, for(k <- 1..400, do: {:put, k, 4000}))
    CountingStore.reset(counting)

    assert {:ok, %{txid: 1, checksum: checksum, page_size: 4096}} = Replication.head(ctx, "cell")
    assert checksum == state.checksum
    assert CountingStore.count(counting, :get) == 0
    assert CountingStore.count(counting, :get_range) == 2
  end

  test "a small file is read in one request", %{ctx: ctx, dir: dir, counting: counting} do
    sync(ctx, dir, [{:put, 1, 100}])
    CountingStore.reset(counting)

    assert {:ok, %{txid: 1}} = Replication.head(ctx, "cell")
    assert CountingStore.count(counting, :get) + CountingStore.count(counting, :get_range) == 1
  end
end
