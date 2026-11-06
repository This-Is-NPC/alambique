defmodule Alambique.LazyRestoreSeaweedFSTest do
  @moduledoc "A lazy wake round trip against the container from `mise run seaweedfs:up`."
  use ExUnit.Case, async: false

  @moduletag :seaweedfs
  @moduletag timeout: 120_000

  import Alambique.CellHelpers

  alias Alambique.Tx

  @bucket "lazy-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]

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
    capture([[:alambique, :replication, :hydrate]])
    dir = Alambique.TestDirs.new("lazy")
    on_exit(fn -> File.rm_rf(dir) end)
    %{store: {Alambique.Store.S3, config()}, dir: dir}
  end

  test "a cell woken lazily serves, takes writes and ends up complete", ctx do
    prefix = "lazy-#{System.unique_integer([:positive])}"

    seeder =
      start_instance(ctx,
        migrations: @schema,
        prefix: prefix,
        lazy_restore: false,
        op_timeout_ms: 20_000
      )

    cell = unique_cell()

    {:ok, :ok} =
      Alambique.transaction(seeder, cell, fn tx ->
        for n <- 1..300,
            do: {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (?, randomblob(1024))", [n])

        :ok
      end)

    # More files in the chain: a second batch, then hibernation.
    {:ok, _} = Alambique.execute(seeder, cell, "UPDATE t SET v = zeroblob(8) WHERE n = 7")
    :ok = Alambique.hibernate(seeder, cell)

    inst =
      start_instance(ctx,
        migrations: @schema,
        prefix: prefix,
        op_timeout_ms: 20_000,
        lazy_restore: [min_bytes: 0, concurrency: 2, batch_bytes: 8_192]
      )

    assert {:ok, %{rows: [[8]]}} =
             Alambique.query(inst, cell, "SELECT length(v) FROM t WHERE n = 7")

    assert {:ok, _} = Alambique.execute(inst, cell, "DELETE FROM t WHERE n > 290")

    await_event([:alambique, :replication, :hydrate], cell, 60_000)
    assert {:ok, %{rows: [[290]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    assert durable_rows(ctx, cell, "SELECT count(*) FROM t", prefix) == [[290]]
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
