defmodule Alambique.Store.S3PoolTimeoutTest do
  @moduledoc """
  The wait for a pooled connection of the S3 store is bounded by its own
  `:pool_timeout`, and a request that gives up waiting is the node's own wait,
  not a failure of the store (design §3).
  """
  use ExUnit.Case, async: true

  import Alambique.CellHelpers, only: [capture: 1]

  alias Alambique.Store.{Breaker, Guard, S3}

  @moduletag :capture_log

  # A server that accepts connections and never answers: the request that
  # holds the only connection of the pool stays on it.
  setup do
    test = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    acceptor =
      spawn_link(fn ->
        accept = fn accept ->
          case :gen_tcp.accept(listen) do
            {:ok, socket} ->
              send(test, {:accepted, socket})
              accept.(accept)

            {:error, _} ->
              :ok
          end
        end

        accept.(accept)
      end)

    on_exit(fn ->
      :gen_tcp.close(listen)
      Process.exit(acceptor, :kill)
    end)

    config = %{
      endpoint: "http://127.0.0.1:#{port}",
      bucket: "bkt",
      access_key_id: "AKIDEXAMPLE",
      secret_access_key: "SEKRET",
      pool_size: 1,
      pool_timeout: 100,
      receive_timeout: 5_000
    }

    {:ok, config: config}
  end

  # Takes the only connection of the pool and keeps it.
  defp hold_connection(config) do
    holder = Task.async(fn -> S3.get(config, "held") end)
    assert_receive {:accepted, _socket}, 5_000
    holder
  end

  test "a request that waits for a connection fails after pool_timeout, not receive_timeout",
       %{config: config} do
    holder = hold_connection(config)

    started = System.monotonic_time(:millisecond)
    assert {:error, {:request_crashed, kind, reason}} = S3.head(config, "k")
    waited = System.monotonic_time(:millisecond) - started

    assert Alambique.Store.Crash.local?(kind, reason)
    assert Alambique.Store.Crash.summary(kind, reason, 80) == :pool_checkout_timeout
    assert waited >= 90, "gave up after #{waited} ms for a pool_timeout of 100"
    assert waited < 2_000, "waited #{waited} ms, as long as a read would"

    Task.shutdown(holder, :brutal_kill)
  end

  test "req_options win over :pool_timeout", %{config: config} do
    config = %{config | pool_timeout: 60_000} |> Map.put(:req_options, finch: [pool_timeout: 100])
    holder = hold_connection(config)

    assert {:error, {:request_crashed, kind, reason}} = S3.head(config, "k")
    assert Alambique.Store.Crash.local?(kind, reason)

    Task.shutdown(holder, :brutal_kill)
  end

  test "the breaker does not count it", %{config: config} do
    capture([[:alambique, :store, :failure]])
    location = "pool-timeout-#{System.unique_integer([:positive])}"

    breaker =
      Breaker.new(
        %{
          failures: 2,
          cooldown_ms: 60_000,
          probe_timeout_ms: 60_000,
          detail_limit: 80,
          log_interval_ms: 1_000
        },
        %{instance: :test, location: location},
        jitter: 0.0
      )

    store = {Breaker, %{store: {Guard, {S3, config}}, breaker: breaker}}
    holder = hold_connection(config)

    # More than `failures` of them: a counted failure would open the breaker.
    for _ <- 1..4,
        do: assert({:error, {:request_crashed, _, _}} = Breaker.head(elem(store, 1), "k"))

    refute Breaker.open?(breaker)
    assert Breaker.status(breaker).failures == 0

    assert_receive {:telemetry, [:alambique, :store, :failure], _,
                    %{location: ^location, reason: :local, counted: false} = meta}

    assert meta.detail == :pool_checkout_timeout

    Task.shutdown(holder, :brutal_kill)
  end
end
