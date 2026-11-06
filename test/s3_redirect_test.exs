defmodule Alambique.Store.S3RedirectTest do
  @moduledoc """
  The S3 client never follows a redirect: Req would sign the request again
  for the other host and send it the body and a valid signature.
  """
  use ExUnit.Case, async: false

  alias Alambique.Store.S3

  # Listener A answers `status` with a Location pointing at listener B, which
  # reports every request it receives to the test.
  defp stubs(status) do
    test = self()
    {:ok, a} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, b} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port_a} = :inet.port(a)
    {:ok, port_b} = :inet.port(b)

    serve(a, fn request ->
      send(test, {:first, request})

      "HTTP/1.1 #{status} Moved\r\nlocation: http://localhost:#{port_b}/stolen\r\n" <>
        "content-length: 0\r\nconnection: close\r\n\r\n"
    end)

    serve(b, fn request ->
      send(test, {:second, request})
      "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
    end)

    on_exit(fn ->
      :gen_tcp.close(a)
      :gen_tcp.close(b)
    end)

    %{
      endpoint: "http://127.0.0.1:#{port_a}",
      bucket: "b",
      access_key_id: "AKID",
      secret_access_key: "SEKRET",
      receive_timeout: 3_000
    }
  end

  defp serve(listener, respond) do
    spawn_link(fn -> accept(listener, respond) end)
  end

  defp accept(listener, respond) do
    with {:ok, socket} <- :gen_tcp.accept(listener) do
      {:ok, request} = :gen_tcp.recv(socket, 0, 2_000)
      :gen_tcp.send(socket, respond.(request))
      :gen_tcp.close(socket)
      accept(listener, respond)
    end
  end

  for status <- [301, 307, 308] do
    test "a #{status} is an error and nothing reaches the other host" do
      config = stubs(unquote(status))

      assert {:error, {:http, unquote(status), _}} = S3.put(config, "k", "BODY", :none, %{})
      assert_received {:first, _}
      refute_receive {:second, _}, 300
    end
  end

  test "a redirect on a read is an error too" do
    config = stubs(307)
    assert {:error, {:http, 307, _}} = S3.get(config, "k")
    refute_receive {:second, _}, 300
  end
end
