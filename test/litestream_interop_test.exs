defmodule Alambique.LitestreamInteropTest do
  @moduledoc """
  Litestream restores replicas that Alambique wrote. Needs the pinned
  `litestream` (mise.toml) and the container from `mise run seaweedfs:up`;
  run it with `mise run test:litestream`.
  """
  use ExUnit.Case, async: false

  @moduletag :litestream
  @moduletag timeout: 180_000

  import Alambique.ReplicationHelpers

  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Replicator}

  @bucket "ls-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

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
    assert System.find_executable("litestream"), "litestream is not on the PATH; use mise run"
    :ok
  end

  setup do
    store = {Alambique.Store.S3, config()}
    prefix = unique_prefix("ls")
    ctx = ctx(store, new_world(), prefix: prefix, timeout_ms: 20_000)
    dir = tmp_dir()
    on_exit(fn -> File.rm_rf(dir) end)
    {conn, state} = new_replicator(dir, "src.db")
    %{ctx: ctx, store: store, prefix: prefix, dir: dir, conn: conn, state: state}
  end

  # One level-0 file per step, with a checkpoint and a snapshot in the middle.
  defp replicate(t) do
    commit = fn state, ops ->
      run_ops(t.conn, ops)
      assert {:ok, state} = Replicator.sync(state, t.ctx)
      state
    end

    state = commit.(t.state, [{:put, 1, 5000}, {:put, 2, 100}])
    state = commit.(state, [{:put, 3, 9000}, {:del, 1}])
    assert {:ok, state} = Replicator.checkpoint(state, t.conn)
    state = commit.(state, [{:put, 4, 700}])
    assert {:ok, _key} = Replicator.snapshot(state, t.ctx)
    state = commit.(state, [{:put, 5, 700}])
    commit.(state, [{:put, 6, 300}])
  end

  defp litestream_restore(t, out, extra \\ []) do
    url =
      "s3://#{@bucket}/#{Keys.cell_prefix(t.prefix, "cell")}" <>
        "?endpoint=#{URI.encode_www_form(config().endpoint)}&force-path-style=true"

    System.cmd("litestream", ["restore", "-o", out] ++ extra ++ [url],
      env: [
        {"LITESTREAM_ACCESS_KEY_ID", config().access_key_id},
        {"LITESTREAM_SECRET_ACCESS_KEY", config().secret_access_key}
      ],
      stderr_to_stdout: true
    )
  end

  defp integrity(path) do
    {:ok, conn} = Alambique.Sqlite.open(path, readonly: true)
    result = rows(conn, "PRAGMA integrity_check")
    Alambique.Sqlite.close(conn)
    result
  end

  defp alambique_restore(t, name, target) do
    path = Path.join(t.dir, name)
    assert {:ok, result} = Replication.restore(t.ctx, "cell", path, target)
    {path, result}
  end

  test "Litestream restores the latest state as Alambique does", t do
    state = replicate(t)
    out = Path.join(t.dir, "litestream.db")

    assert {"", 0} = litestream_restore(t, out)
    assert integrity(out) == [["ok"]]
    assert dump_file(out) == dump(t.conn)

    {mine, result} = alambique_restore(t, "alambique.db", :latest)
    assert result.txid == state.txid
    assert dump_file(mine) == dump_file(out)
  end

  # TXID 2 is before the snapshot, so Litestream applies level-0 files only.
  test "Litestream restores a TXID as Alambique does", t do
    replicate(t)

    for txid <- [2, 4] do
      out = Path.join(t.dir, "litestream-#{txid}.db")
      assert {"", 0} = litestream_restore(t, out, ["-txid", Keys.hex(txid)])
      assert integrity(out) == [["ok"]]

      {mine, _} = alambique_restore(t, "alambique-#{txid}.db", {:txid, txid})
      assert dump_file(mine) == dump_file(out)
    end
  end

  # Level-0 files 1 to 5 exist; the gap at 4 leaves 5 as the tail (design §6).
  test "a tail past a gap ends Alambique's restore and fails Litestream's", t do
    replicate(t)
    {mod, config} = t.store
    :ok = mod.delete(config, Keys.segment(t.prefix, "cell", 0, 4, 4))

    {mine, result} = alambique_restore(t, "alambique.db", :latest)
    assert result.txid == 3

    {output, status} = litestream_restore(t, Path.join(t.dir, "litestream.db"))
    assert status != 0
    assert output =~ "non-contiguous ltx files"

    out = Path.join(t.dir, "litestream-3.db")
    assert {"", 0} = litestream_restore(t, out, ["-txid", Keys.hex(3)])
    assert dump_file(out) == dump_file(mine)
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
