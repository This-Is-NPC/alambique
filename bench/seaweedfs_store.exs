# SeaweedFS for the benches that restore through a store with latency: a
# bucket in the container of `mise run seaweedfs:up`, and the delay proxy
# (`bench/delay_proxy.exs`) in front of it. Loaded with `Code.require_file/2`.
#
#   ALAMBIQUE_BENCH_PROXY_CPU  CPU the proxy is pinned to  (default "5")

defmodule BenchSeaweedfs do
  @moduledoc false

  @endpoint "http://127.0.0.1:18333"

  @doc "The S3 config of a bucket, created if it does not exist (`name` defaults to a random one)."
  def bucket(name \\ nil) do
    config = %{
      endpoint: @endpoint,
      bucket: name || "bench-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower),
      access_key_id: "alambique",
      secret_access_key: "alambique-secret",
      path_style: true,
      receive_timeout: 30_000
    }

    created =
      Req.put(
        url: "#{config.endpoint}/#{config.bucket}",
        aws_sigv4: [
          access_key_id: config.access_key_id,
          secret_access_key: config.secret_access_key,
          service: "s3",
          region: "us-east-1"
        ],
        retry: false
      )

    case created do
      {:ok, %{status: status}} when status in [200, 409] ->
        config

      other ->
        IO.puts("SeaweedFS is not running (mise run seaweedfs:up): #{inspect(other)}")
        System.halt(1)
    end
  end

  @doc """
  Starts the proxy as a process of its own, on another CPU, so that its work
  does not count in the one a bench pins the VM to. Returns the port it listens
  on. It stops when its stdin closes, which happens when the calling VM exits.
  """
  def start_proxy(ms) do
    cpu = System.get_env("ALAMBIQUE_BENCH_PROXY_CPU", "5")

    port =
      Port.open({:spawn_executable, System.find_executable("taskset")}, [
        :binary,
        :exit_status,
        args: [
          "-c",
          cpu,
          System.find_executable("elixir"),
          Path.join(__DIR__, "delay_proxy.exs"),
          "18333",
          "#{ms}"
        ]
      ])

    receive do
      {^port, {:data, line}} -> String.trim(line) |> String.to_integer()
    after
      20_000 -> raise "the delay proxy did not start"
    end
  end
end
