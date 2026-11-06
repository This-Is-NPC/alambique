defmodule Alambique.VersioningSeaweedFSTest do
  @moduledoc "Trash, scrub and move-back with a versioned SeaweedFS bucket."
  use ExUnit.Case, async: false

  @moduletag :seaweedfs
  @moduletag timeout: 180_000

  import Alambique.CellHelpers, only: [start_instance: 2]
  import Alambique.CompactionHelpers
  import Alambique.ReplicationHelpers, only: [ctx: 3, new_world: 0, tmp_dir: 0, unique_prefix: 1]

  alias Alambique.{Retention, Trash}
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @bucket "versioning-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
  @day 86_400_000
  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup_all do
    config = config()

    {:ok, _} =
      Req.put(
        url: "#{config.endpoint}/#{@bucket}",
        aws_sigv4: sigv4(config),
        retry: false
      )

    {:ok, _} =
      Req.put(
        url: "#{config.endpoint}/#{@bucket}?versioning",
        body: "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
        aws_sigv4: sigv4(config),
        retry: false
      )

    :ok
  end

  setup do
    world = new_world()
    dir = tmp_dir()
    store = {Alambique.Store.S3, config()}
    ctx = ctx(store, world, prefix: unique_prefix("versioning"), timeout_ms: 30_000)
    on_exit(fn -> File.rm_rf(dir) end)
    %{store: store, mod: Alambique.Store.S3, cfg: config(), world: world, dir: dir, ctx: ctx}
  end

  test "trash deletes leave delete markers and listings ignore them", env do
    owner = history(env, new_owner(env, "a.db"), 6)
    assert {:ok, %{snapshot: 6}} = compact_pass(env, snapshot_interval_txids: 6)
    ManualClock.advance(env.world, 2 * @day)

    assert snapshots(env) == [6]
    before = keys(env)
    assert {:ok, %{anchor: 6, segments: segments}} = retain(env)
    assert segments > 0

    ManualClock.advance(env.world, @day)
    assert %{objects: n, errors: 0} = reap(env)
    assert n == segments

    after_reap = keys(env)
    assert "trashed" in after_reap
    assert Enum.any?(after_reap, &String.starts_with?(&1, "0009/"))
    assert restore(env, :latest) |> elem(2) == owner.dumps[6]

    versions = versions(env.ctx)
    assert versions =~ "<DeleteMarker>"

    for key <- before -- after_reap do
      assert String.contains?(versions, key)
      refute key in after_reap
    end
  end

  test "scrub validates a cell stored in the versioned bucket", env do
    prefix = unique_prefix("scrub")
    inst = start_instance(%{store: env.store, dir: env.dir}, migrations: @schema, prefix: prefix)
    assert {:ok, _} = Alambique.execute(inst, "scrub-cell", "INSERT INTO t VALUES (1)")
    :ok = Alambique.hibernate(inst, "scrub-cell")

    assert {:ok, %{cells: 1, ok: 1, failed: 0}} =
             Alambique.scrub(inst, cells: 1, attempts: 1, timeout_ms: 30_000)
  end

  test "a cell moves back while its marked objects wait for the reaper", env do
    table = start_supervised!({MemoryStore, []})
    table = MemoryStore.config(table)
    prefix = unique_prefix("move")

    locations = [
      %Alambique.Location{id: "a", store: env.store, prefix: prefix <> "-a"},
      %Alambique.Location{id: "b", store: env.store, prefix: prefix <> "-b"}
    ]

    placement =
      {AlambiqueTestkit.Placement,
       locations: locations, table: {MemoryStore, table}, initial: %{"move-cell" => "a"}}

    inst =
      start_instance(%{store: env.store, dir: env.dir},
        placement: placement,
        migrations: @schema,
        clock: {ManualClock, ManualClock.config(env.world, 0)},
        moved_grace_ms: 0,
        deletion_grace_ms: @day,
        trash: [reaper: false]
      )

    assert {:ok, _} = Alambique.execute(inst, "move-cell", "INSERT INTO t VALUES (1)")
    :ok = Alambique.hibernate(inst, "move-cell")
    assert {:ok, %Alambique.Location{id: "b"}} = Alambique.relocate(inst, "move-cell", "b")
    assert {:ok, _} = Alambique.execute(inst, "move-cell", "INSERT INTO t VALUES (2)")
    :ok = Alambique.hibernate(inst, "move-cell")

    ManualClock.advance(env.world, 1_000)
    assert {:ok, %{result: :cleaned}} = Alambique.cleanup_moved(inst, "move-cell", "a")
    assert {:ok, %{entries: [_]}} = Trash.pending(inst, location: "a")
    :ok = Alambique.hibernate(inst, "move-cell")
    ManualClock.advance(env.world, 20_000)

    assert {:ok, %Alambique.Location{id: "a"}} = Alambique.relocate(inst, "move-cell", "a")

    assert {:ok, %{rows: [[1], [2]]}} =
             Alambique.query(inst, "move-cell", "SELECT n FROM t ORDER BY n")

    :ok = Alambique.hibernate(inst, "move-cell")
    ManualClock.advance(env.world, @day)
    assert {:ok, %{errors: 0}} = Trash.reap(inst)

    assert {:ok, %{rows: [[1], [2]]}} =
             Alambique.query(inst, "move-cell", "SELECT n FROM t ORDER BY n")
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

  defp sigv4(config) do
    [
      access_key_id: config.access_key_id,
      secret_access_key: config.secret_access_key,
      service: "s3",
      region: "us-east-1"
    ]
  end

  defp retain(env) do
    Retention.run(env.ctx, cell(),
      pitr_window_ms: @day,
      grace_ms: 0,
      deletion_grace_ms: @day
    )
  end

  defp reap(env), do: Trash.reap_ctx(env.ctx, Map.new(Trash.defaults()))

  defp versions(ctx) do
    prefix = URI.encode_www_form(ctx.prefix)
    config = config()

    {:ok, resp} =
      Req.get(
        url: "#{config.endpoint}/#{@bucket}?versions&prefix=#{prefix}",
        aws_sigv4: sigv4(config),
        retry: false
      )

    resp.body
  end
end
