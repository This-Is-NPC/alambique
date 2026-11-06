defmodule Alambique.CompactionSeaweedFSTest do
  @moduledoc "Compaction, retention and freezing against the container from `mise run seaweedfs:up`."
  use ExUnit.Case, async: false

  @moduletag :seaweedfs

  import Alambique.CellHelpers, only: [eventually: 2, unique_cell: 0]
  import Alambique.CompactionHelpers

  alias Alambique.{Compaction, Retention}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.ManualClock

  @bucket "compact-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
  @day 86_400_000

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

  setup do
    store = {Alambique.Store.S3, config()}
    {mod, cfg} = store
    world = Alambique.ReplicationHelpers.new_world()
    dir = Alambique.ReplicationHelpers.tmp_dir()
    prefix = Alambique.ReplicationHelpers.unique_prefix("rt")
    ctx = Alambique.ReplicationHelpers.ctx(store, world, prefix: prefix, timeout_ms: 30_000)
    on_exit(fn -> File.rm_rf(dir) end)
    %{store: store, mod: mod, cfg: cfg, world: world, dir: dir, ctx: ctx}
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

  test "merge, snapshot, retain and restore round trip", env do
    owner = history(env, new_owner(env, "a.db"), 24)
    ManualClock.advance(env.world, 2 * @day)

    assert {:ok, %{merges: merges, snapshot: 24}} =
             Compaction.run(env.ctx, cell(),
               fanout: 4,
               max_level: 2,
               snapshot_interval_txids: 24,
               timeout_ms: 60_000
             )

    assert merges == 6 + 1
    assert Enum.sort(segments(env, 1)) == for(i <- 0..5, do: {4 * i + 1, 4 * i + 4})
    assert Enum.sort(segments(env, 2)) == [{1, 16}]
    assert snapshots(env) == [24]

    assert {:ok, %{segments: 1}, dump} = restore(env, :latest)
    assert dump == owner.dumps[24]

    # More writes after the snapshot, then retention at a one day window.
    owner = history(env, owner, 3)

    assert {:ok, %{anchor: 24, snapshots: 0, segments: segments}} =
             Retention.run(env.ctx, cell(),
               pitr_window_ms: @day,
               grace_ms: 0,
               deletion_grace_ms: 0
             )

    assert segments == 24 + 6 + 1
    assert segments(env, 0) |> Enum.map(&elem(&1, 0)) == [25, 26, 27]
    assert segments(env, 1) == [] and segments(env, 2) == []

    for txid <- [24, 25, 27] do
      assert {:ok, %{txid: ^txid}, dump} = restore(env, {:txid, txid})
      assert dump == owner.dumps[txid]
    end

    assert {:error, _} = restore(env, {:txid, 10})
  end

  test "a zombie of a fenced-out epoch is ignored and overwritten on S3", env do
    a = history(env, new_owner(env, "a.db"), 3)
    b = take_over(env, "b.db", 2, a.dumps)
    Alambique.ReplicationHelpers.safe_op(a.conn, {:put, 9, 100})
    {:ok, _} = Alambique.Replication.Replicator.sync(%{a.state | txid: b.state.txid}, env.ctx)

    assert {:ok, %{txid: 4}, _} = restore(env, :latest)
    assert {:ok, %{anchor: nil}} = Retention.run(env.ctx, cell(), grace_ms: 0)

    b = write!(env, b, [{:put, 1, 500}])
    assert {:ok, _, dump} = restore(env, :latest)
    assert dump == b.dumps[b.state.txid]
  end

  test "freeze markers, inspection and the background compactor on S3", env do
    store = env.store
    prefix = env.ctx.prefix
    dir = env.dir
    name = :"compact_s3_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Alambique,
       name: name,
       store: store,
       prefix: prefix,
       data_dir: Path.join(dir, "cells"),
       migrations: [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}],
       compaction: [interval_ms: 50, fanout: 2, max_level: 1, snapshot_interval_txids: 1_000],
       retention: false}
    )

    id = unique_cell()

    for n <- 1..6,
        do: assert({:ok, _} = Alambique.execute(name, id, "INSERT INTO t VALUES (?)", [n]))

    eventually(
      fn ->
        {:ok, %{levels: levels}} = Alambique.Ops.describe(env.ctx, id, sizes: false)
        Enum.any?(levels, &(&1.level == 1 and &1.files >= 3))
      end,
      30_000
    )

    assert :ok = Alambique.freeze(name, id)

    assert {:ok, %{frozen: true, lease: %{state: :released}}} =
             Alambique.Ops.describe(env.ctx, id)

    assert {:error, :frozen} = Alambique.query(name, id, "SELECT n FROM t")
    assert :ok = Alambique.unfreeze(name, id)
    assert {:ok, %{rows: rows}} = Alambique.query(name, id, "SELECT n FROM t ORDER BY n")
    assert rows == Enum.map(1..6, &[&1])

    key = Keys.frozen(prefix, id)
    assert {:error, :not_found} = env.mod.head(env.cfg, key)
  end
end
