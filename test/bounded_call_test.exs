defmodule Alambique.BoundedCallTest do
  @moduledoc "A callback that is waited for at most a time, and leaves nothing behind."
  use ExUnit.Case, async: true

  alias Alambique.BoundedCall

  # The caller of a cell traps exits; so does this one.
  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  defp dead?(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> true
    after
      5_000 -> false
    end
  end

  defp mailbox, do: Process.info(self(), :messages) |> elem(1)

  test "returns the value" do
    assert BoundedCall.run(fn -> 42 end, 1_000) == {:ok, 42}
    assert mailbox() == []
  end

  test "reports a raise, a throw and an exit with their kind" do
    assert {:error, {:raised, :error, %RuntimeError{}}} =
             BoundedCall.run(fn -> raise "boom" end, 1_000)

    assert {:error, {:raised, :throw, :nope}} = BoundedCall.run(fn -> throw(:nope) end, 1_000)
    assert {:error, {:raised, :exit, :gone}} = BoundedCall.run(fn -> exit(:gone) end, 1_000)
    assert mailbox() == []
  end

  test "a hang ends at the timeout and the runner dies" do
    test = self()

    assert {:error, :timeout} =
             BoundedCall.run(
               fn ->
                 send(test, {:runner, self()})
                 Process.sleep(:infinity)
               end,
               50
             )

    assert_received {:runner, runner}
    assert dead?(runner)
    assert mailbox() == []
  end

  test "a result that comes after the timeout never reaches the mailbox" do
    test = self()

    for delay <- [60, 80, 100, 150] do
      assert {:error, :timeout} =
               BoundedCall.run(
                 fn ->
                   send(test, {:runner, self()})
                   Process.sleep(delay)
                   :late
                 end,
                 50
               )

      assert_received {:runner, runner}
      assert dead?(runner)
    end

    assert mailbox() == []
  end

  test "the runner does not outlive a caller that died" do
    test = self()

    {caller, ref} =
      spawn_monitor(fn ->
        BoundedCall.run(
          fn ->
            send(test, {:runner, self()})
            Process.sleep(:infinity)
          end,
          60_000
        )
      end)

    assert_receive {:runner, runner}
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^ref, :process, ^caller, _}
    assert dead?(runner)
  end
end
