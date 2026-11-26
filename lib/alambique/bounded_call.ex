defmodule Alambique.BoundedCall do
  @moduledoc false
  # Runs a user callback in a short-lived process and waits for it at most
  # `timeout` ms. The caller may trap exits and may hold a wake slot: a hang,
  # a raise, an exit or a late return must neither block it nor leave a
  # message, a link or a process behind.
  #
  # The callback runs in a runner linked to a worker. The worker relays the
  # result and watches the caller: if the caller dies, the runner is killed.
  # On expiry the caller kills the worker, waits for its `:DOWN` (so nothing
  # of it can still arrive) and flushes the result that raced the kill.

  @type result ::
          {:ok, term} | {:error, :timeout} | {:error, {:raised, Exception.kind(), term}}

  @spec run((-> term), pos_integer) :: result
  def run(fun, timeout) do
    caller = self()
    ref = make_ref()
    {pid, mon} = spawn_monitor(fn -> worker(caller, ref, fun) end)

    receive do
      {^ref, result} ->
        Process.demonitor(mon, [:flush])
        result

      {:DOWN, ^mon, :process, ^pid, reason} ->
        {:error, {:raised, :exit, reason}}
    after
      timeout ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^mon, :process, ^pid, _} -> :ok
        end

        receive do
          {^ref, _late} -> :ok
        after
          0 -> :ok
        end

        {:error, :timeout}
    end
  end

  defp worker(caller, ref, fun) do
    caller_mon = Process.monitor(caller)
    me = self()

    {runner, runner_mon} =
      :erlang.spawn_opt(fn -> send(me, {:result, run_fun(fun)}) end, [:link, :monitor])

    receive do
      {:result, result} ->
        send(caller, {ref, result})

      {:DOWN, ^caller_mon, :process, _, _} ->
        Process.exit(runner, :kill)

      {:DOWN, ^runner_mon, :process, _, reason} ->
        send(caller, {ref, {:error, {:raised, :exit, reason}}})
    end
  end

  defp run_fun(fun) do
    {:ok, fun.()}
  catch
    kind, reason -> {:error, {:raised, kind, reason}}
  end
end
