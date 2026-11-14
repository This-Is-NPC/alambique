# A TCP proxy to a local server that holds every chunk the client sends for
# `ms` before it passes it on: a request takes `ms` more, as over a network.
# Used by bench/restore_concurrency.exs.
#
#   elixir bench/delay_proxy.exs <target port> <ms>
#
# Prints the port it listens on, and stops when its stdin closes.

defmodule DelayProxy do
  def start(target_port, ms) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, backlog: 1024])
    {:ok, port} = :inet.port(listen)
    spawn(fn -> accept(listen, target_port, ms) end)
    port
  end

  defp accept(listen, target_port, ms) do
    {:ok, client} = :gen_tcp.accept(listen)

    spawn(fn ->
      {:ok, server} = :gen_tcp.connect({127, 0, 0, 1}, target_port, [:binary, active: false])
      spawn(fn -> pipe(server, client, 0) end)
      pipe(client, server, ms)
    end)

    accept(listen, target_port, ms)
  end

  defp pipe(from, to, ms) do
    case :gen_tcp.recv(from, 0) do
      {:ok, data} ->
        if ms > 0, do: Process.sleep(ms)
        _ = :gen_tcp.send(to, data)
        pipe(from, to, ms)

      {:error, _} ->
        :gen_tcp.close(to)
    end
  end
end

[target, ms] = Enum.map(System.argv(), &String.to_integer/1)
IO.puts(DelayProxy.start(target, ms))
IO.read(:stdio, :eof)
