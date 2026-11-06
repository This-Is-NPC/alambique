defmodule Alambique.Store.S3Test do
  @moduledoc "A failing HTTP client answers errors instead of raising."
  use ExUnit.Case, async: false

  alias Alambique.Store.S3

  @pool Alambique.S3TestPool

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    test = self()

    # Accepts connections and never answers.
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

    config = %{
      endpoint: "http://127.0.0.1:#{port}",
      bucket: "b",
      access_key_id: "id",
      secret_access_key: "secret",
      receive_timeout: 30_000,
      req_options: [finch: [name: @pool, pool_timeout: 100]]
    }

    %{config: config}
  end

  test "every operation answers an error when the connection pool is exhausted", %{config: c} do
    holder = Task.async(fn -> S3.get(c, "held") end)
    assert_receive {:accepted, connection}, 5_000

    for result <- [
          S3.get(c, "k"),
          S3.get_range(c, "k", 0, 10),
          S3.head(c, "k"),
          S3.put(c, "k", "v", :none, %{}),
          S3.list(c, "p", nil, 10),
          S3.delete(c, "k")
        ] do
      assert {:error, {:request_crashed, _kind, _reason}} = result
    end

    # The request that holds the connection ends when the server closes it. A
    # receive timeout would race with the requests above on a loaded machine.
    send(connection, :close)
    assert {:error, _} = Task.await(holder, 10_000)
  end
end
