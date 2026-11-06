defmodule Alambique.LeaseSeaweedFSTest do
  @moduledoc "Lease tests against the container from `mise run seaweedfs:up`."
  use Alambique.LeaseContract, tags: :seaweedfs

  # Unique per run so concurrent test runs never share lease keys.
  @bucket "lease-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

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

  def new_store, do: {Alambique.Store.S3, config()}

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
