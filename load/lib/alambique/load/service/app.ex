defmodule Alambique.Load.Service.App do
  @moduledoc false
  # The service under load: one Alambique instance on the S3 store of the load
  # test, behind a store that adds latency to every request. It is the
  # application of the `load` release (see docs/how-to-load-test.md); the
  # driver on the host calls `Alambique.Load.Service.Ops` over distribution.
  #
  # Settings come from ALAMBIQUE_LOAD_* variables; unset ones keep the
  # library's defaults. ALAMBIQUE_LOAD_OPTS is an Elixir keyword list merged
  # over the instance options.

  use Application

  alias Alambique.Load.Env
  alias Alambique.Load.Service.{DelayStore, Ops, Probe}

  @instance :load

  @impl true
  def start(_type, _args) do
    s3 = s3_config()
    wait_for_bucket(s3)

    latency = Env.int("STORE_LATENCY_MS", 25)
    counters = DelayStore.new_counters()
    :persistent_term.put({Ops, :counters}, counters)
    :persistent_term.put({Ops, :latency}, latency)
    Ops.init_pools()

    store = DelayStore.wrap(Alambique.Store.conceal({Alambique.Store.S3, s3}), latency, counters)

    Supervisor.start_link([Probe, {Alambique, options(store)}],
      strategy: :one_for_one,
      name: __MODULE__
    )
  end

  defp options(store) do
    [
      name: @instance,
      store: store,
      prefix: "load",
      data_dir: Env.get("DATA_DIR", "/data/cells"),
      migrations: Ops.migrations(),
      awake: awake_options(),
      wal: [checkpoint_bytes: Env.int("WAL_CHECKPOINT_BYTES", 8_388_608)]
    ]
    |> put_unless_nil(:idle_timeout_ms, Env.get("IDLE_TIMEOUT_MS"), &String.to_integer/1)
    |> Keyword.merge(extra_options(), &merge/3)
  end

  defp awake_options do
    []
    |> put_unless_nil(:cache_kib, Env.get("CACHE_KIB"), &String.to_integer/1)
    |> put_unless_nil(:max_cells, Env.get("MAX_CELLS"), &String.to_integer/1)
  end

  defp put_unless_nil(opts, _key, nil, _fun), do: opts
  defp put_unless_nil(opts, key, value, fun), do: Keyword.put(opts, key, fun.(value))

  defp extra_options do
    case Env.get("OPTS", "") do
      "" -> []
      code -> code |> Code.eval_string() |> elem(0)
    end
  end

  defp merge(_key, a, b) do
    if Keyword.keyword?(a) and Keyword.keyword?(b), do: Keyword.merge(a, b), else: b
  end

  defp s3_config do
    %{
      endpoint: Env.get("S3_ENDPOINT", "http://127.0.0.1:18444"),
      bucket: Env.get("S3_BUCKET", "alambique-load"),
      access_key_id: Env.get("S3_KEY", "alambique"),
      secret_access_key: Env.get("S3_SECRET", "alambique-secret")
    }
  end

  # The store container may still be starting. Any answer but a refusal of the
  # connection is enough; creating a bucket twice answers 409.
  defp wait_for_bucket(s3, attempts \\ 240) do
    result =
      try do
        Req.put(
          url: "#{s3.endpoint}/#{s3.bucket}",
          aws_sigv4: [
            access_key_id: s3.access_key_id,
            secret_access_key: s3.secret_access_key,
            service: "s3",
            region: "us-east-1"
          ],
          retry: false
        )
      rescue
        error -> {:error, error}
      end

    case result do
      {:ok, %{status: status}} when status in [200, 409] ->
        :ok

      _ when attempts > 0 ->
        Process.sleep(500)
        wait_for_bucket(s3, attempts - 1)

      other ->
        raise "the store did not accept the bucket: #{inspect(other)}"
    end
  end
end
