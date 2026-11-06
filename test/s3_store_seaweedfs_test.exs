defmodule Alambique.Store.S3SeaweedFSTest do
  @moduledoc "Contract tests against the container from `mise run seaweedfs:up`."
  use Alambique.StoreContract, store: Alambique.Store.S3, tags: :seaweedfs

  @bucket "alambique-test"

  setup_all do
    config = base_config()
    # Bucket creation is idempotent: 200 when new, 409 when ours already exists.
    {:ok, resp} = request_bucket(config)
    assert resp.status in [200, 409]
    :ok
  end

  def new_config, do: base_config()

  defp base_config do
    %{
      endpoint: "http://127.0.0.1:18333",
      bucket: @bucket,
      access_key_id: "alambique",
      secret_access_key: "alambique-secret",
      path_style: true,
      receive_timeout: 30_000
    }
  end

  defp request_bucket(c) do
    Req.put(
      url: "#{c.endpoint}/#{c.bucket}",
      aws_sigv4: [
        access_key_id: c.access_key_id,
        secret_access_key: c.secret_access_key,
        service: "s3",
        region: "us-east-1"
      ],
      retry: false
    )
  end
end
