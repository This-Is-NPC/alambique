defmodule Alambique.ReplicationSeaweedFSTest do
  @moduledoc "Replicate and restore against the container from `mise run seaweedfs:up`."
  use ExUnit.Case, async: false

  @moduletag :seaweedfs

  import Alambique.ReplicationHelpers

  alias Alambique.Replication
  alias Alambique.Replication.Replicator

  # Unique per run so concurrent runs never share keys.
  @bucket "repl-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

  setup_all do
    {:ok, resp} =
      Req.put(
        url: "#{config().endpoint}/#{@bucket}",
        aws_sigv4: [
          access_key_id: config().access_key_id,
          secret_access_key: config().secret_access_key,
          service: "s3",
          region: "us-east-1"
        ],
        retry: false
      )

    assert resp.status in [200, 409]
    :ok
  end

  test "segments, fence, snapshot and restore round trip" do
    store = {Alambique.Store.S3, config()}
    ctx = ctx(store, new_world(), prefix: unique_prefix("rt"), timeout_ms: 20_000)
    dir = tmp_dir()
    on_exit(fn -> File.rm_rf(dir) end)

    {conn, state} = new_replicator(dir, "src.db")

    commit = fn state, ops ->
      run_ops(conn, ops)
      assert {:ok, state} = Replicator.sync(state, ctx)
      state
    end

    state = commit.(state, [{:put, 1, 5000}, {:put, 2, 100}])
    state = commit.(state, [{:put, 3, 9000}, {:del, 1}])
    assert {:ok, state} = Replicator.checkpoint(state, conn)
    state = commit.(state, [{:put, 4, 700}])
    assert {:ok, _key} = Replicator.snapshot(state, ctx)
    state = commit.(state, [{:put, 5, 700}])

    path = Path.join(dir, "restored.db")
    assert {:ok, result} = Replication.restore(ctx, "cell", path, :latest)
    assert result.txid == state.txid
    assert result.checksum == state.checksum
    assert dump_file(path) == dump(conn)

    # A second owner takes over and fences; the old owner is rejected.
    {:ok, b} = Replication.restore(ctx, "cell", Path.join(dir, "b.db"), :latest)
    b = Replicator.from_restore(b, 2)
    assert {:ok, _} = Replicator.fence(b, ctx)
    run_ops(conn, [{:put, 6, 100}])
    assert {:error, :fenced} = Replicator.sync(state, ctx)
  end

  defp config do
    %{
      endpoint: "http://127.0.0.1:18333",
      bucket: @bucket,
      access_key_id: "alambique",
      secret_access_key: "alambique-secret",
      path_style: true,
      receive_timeout: 30_000
    }
  end
end
