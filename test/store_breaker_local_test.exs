defmodule Alambique.StoreBreakerLocalTest do
  @moduledoc """
  The breaker counts what the store did, not what the node did (design §3): a
  request that never left (it waited for a connection of the pool) is not a
  failure of the store, and one that left and was not answered is.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [capture: 1, eventually: 1]

  alias Alambique.Replication.Retry
  alias Alambique.Store.{Breaker, Phase, S3}

  @pool Alambique.BreakerLocalPool
  @settings %{
    failures: 3,
    cooldown_ms: 60_000,
    probe_timeout_ms: 60_000,
    detail_limit: 200,
    log_interval_ms: 10_000
  }

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    test = self()

    # Accepts connections and never answers; `:close` to a holder ends one.
    acceptor =
      spawn_link(fn ->
        for _ <- Stream.cycle([:accept]) do
          {:ok, socket} = :gen_tcp.accept(listener)

          holder =
            spawn(fn ->
              receive do
                :close -> :gen_tcp.close(socket)
              end
            end)

          :gen_tcp.controlling_process(socket, holder)
          send(test, {:accepted, holder})
        end
      end)

    start_supervised!({Finch, name: @pool, pools: %{default: [size: 1]}})

    on_exit(fn ->
      Process.unlink(acceptor)
      Process.exit(acceptor, :kill)
      :gen_tcp.close(listener)
    end)

    s3 = %{
      endpoint: "http://127.0.0.1:#{port}",
      bucket: "b",
      access_key_id: "id",
      secret_access_key: "secret",
      receive_timeout: 200,
      req_options: [finch: [name: @pool, pool_timeout: 50]]
    }

    breaker = Breaker.new(@settings, %{instance: :test, location: "loc"}, jitter: 0.0)
    %{s3: s3, breaker: breaker}
  end

  defp store(s3, breaker), do: {Breaker, %{store: {S3, s3}, breaker: breaker}}
  defp get({m, c}), do: m.get(c, "k")

  # A request that holds the only connection of the pool until `:close`.
  defp hold_pool(s3) do
    held = %{s3 | receive_timeout: 60_000}
    task = Task.async(fn -> S3.get(held, "held") end)
    assert_receive {:accepted, connection}, 5_000
    {task, connection}
  end

  defp release_pool({task, connection}) do
    send(connection, :close)
    assert {:error, _} = Task.await(task, 10_000)
  end

  test "requests that wait for a connection and give up never open it", %{s3: s3, breaker: b} do
    held = hold_pool(s3)

    for _ <- 1..10, do: assert({:error, {:request_crashed, _, _}} = get(store(s3, b)))
    refute Breaker.open?(b)
    assert Breaker.status(b).failures == 0

    release_pool(held)
  end

  test "a store that takes the request and does not answer opens it", %{s3: s3, breaker: b} do
    for _ <- 1..3, do: assert({:error, :timeout} = get(store(s3, b)))
    assert Breaker.open?(b)
  end

  test "a request killed at its deadline while it waits for a connection does not count",
       %{s3: s3, breaker: b} do
    held = hold_pool(s3)
    waiting = store(put_in(s3.req_options, finch: [name: @pool, pool_timeout: 30_000]), b)

    for _ <- 1..5,
        do: assert({:error, :timeout} = Retry.once(waiting, 50, fn -> get(waiting) end))

    refute Breaker.open?(b)

    release_pool(held)
  end

  test "a request killed at its deadline after it was sent counts", %{s3: s3, breaker: b} do
    # One connection per request: the next one does not wait for the pool to
    # replace the connection of the one killed before it.
    pool = Alambique.BreakerKilledPool
    start_supervised!({Finch, name: pool, pools: %{default: [size: 3]}}, id: pool)

    silent =
      store(
        %{s3 | receive_timeout: 60_000, req_options: [finch: [name: pool, pool_timeout: 5_000]]},
        b
      )

    for _ <- 1..3 do
      {pid, monitor} = spawn_monitor(fn -> get(silent) end)
      # Wait for the state, not for time: the kill comes after the request left.
      eventually(fn ->
        refute_received {:DOWN, ^monitor, _, _, _}, "the request ended before it was sent"
        Phase.of(pid) == {:get, :sent}
      end)

      # What `Retry` does at the deadline.
      Breaker.timed_out(silent, Phase.of(pid))
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^monitor, _, _, :killed}, 5_000
    end

    assert Breaker.open?(b)
  end

  test "a failure is reported with its reason, its operation and whether it counted",
       %{s3: s3, breaker: b} do
    capture([[:alambique, :store, :failure]])
    held = hold_pool(s3)
    get(store(s3, b))
    release_pool(held)

    assert_receive {:telemetry, [:alambique, :store, :failure], _,
                    %{operation: :get, reason: :local, counted: false, instance: :test}}

    get(store(s3, b))

    assert_receive {:telemetry, [:alambique, :store, :failure], _,
                    %{operation: :get, reason: :timeout_recv, counted: true, location: "loc"}}
  end
end
