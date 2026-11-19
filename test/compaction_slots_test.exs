defmodule Alambique.Compaction.SlotsTest do
  @moduledoc "The places of the release compactions of a node (design §9, \"Release concurrency\")."
  use ExUnit.Case, async: true

  import Alambique.CellHelpers, only: [eventually: 1]

  alias Alambique.Compaction.Slots

  setup do
    config = %{release_slots: :"slots_#{System.unique_integer([:positive])}"}
    Slots.create(config)
    %{config: config}
  end

  defp far, do: System.monotonic_time(:millisecond) + 30_000

  # A process that asks for a place, says what it got and keeps it until told.
  defp contender(config, cap, id) do
    test = self()

    spawn_link(fn ->
      result = Slots.acquire(config, cap, far())
      send(test, {:got, id, result})

      receive do
        :release ->
          with {:ok, place} <- result, do: Slots.release(config, place)
      end
    end)
  end

  test "no more than cap run at once, and the others take the places in order", %{config: config} do
    pids =
      for id <- 1..6 do
        pid = contender(config, 2, id)
        # Arrival order is the order of the rows.
        eventually(fn -> Slots.count(config) == id end)
        {id, pid}
      end

    assert_receive {:got, 1, {:ok, _}}
    assert_receive {:got, 2, {:ok, _}}
    refute_received {:got, 3, _}

    # Each place that ends goes to the oldest of those that wait.
    for {id, pid} <- pids, id <= 4 do
      send(pid, :release)
      assert_receive {:got, got, {:ok, _}}
      assert got == id + 2
    end

    for {_id, pid} <- pids, do: send(pid, :release)
    eventually(fn -> Slots.count(config) == 0 end)
  end

  test "a caller whose time passes holds nothing and leaves no row", %{config: config} do
    assert {:ok, a} = Slots.acquire(config, 1, far())
    assert :busy = Slots.acquire(config, 1, System.monotonic_time(:millisecond) + 30)
    assert Slots.count(config) == 1

    Slots.release(config, a)
    assert Slots.count(config) == 0
    assert {:ok, _} = Slots.acquire(config, 1, far())
  end

  test "the place of a process that died is not counted", %{config: config} do
    test = self()

    {pid, ref} =
      spawn_monitor(fn ->
        {:ok, _place} = Slots.acquire(config, 1, far())
        send(test, :held)
        Process.sleep(:infinity)
      end)

    assert_receive :held
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    assert {:ok, _} = Slots.acquire(config, 1, far())
  end

  test "without the table there is no cap" do
    assert {:ok, nil} = Slots.acquire(%{release_slots: :no_such_table}, 1, far())
    assert :ok = Slots.release(%{release_slots: :no_such_table}, {1, self()})
  end
end
