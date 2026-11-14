defmodule Alambique.PendingTest do
  @moduledoc """
  `awake.max_pending` counts the calls waiting on a cell, not the other
  messages of its mailbox (design §7, §8): the exit messages of the linked
  tasks of a wake, timers and unknown messages never make a lone caller busy
  or keep a cell awake.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias AlambiqueTestkit.MemoryStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"

  setup do
    setup_store()
  end

  defp pid_of(inst, cell), do: Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

  defp stray(pid, count) do
    for _ <- 1..count do
      send(pid, {:EXIT, self(), :normal})
      send(pid, :unknown)
    end
  end

  test "stray messages in the mailbox do not make a lone caller busy", env do
    inst = start_instance(env, migrations: @schema, awake: [max_pending: 1])
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert, [0])
    pid = pid_of(inst, cell)

    # The cell cannot drain its mailbox: it holds six messages and no call.
    :sys.suspend(pid)
    stray(pid, 3)
    assert Alambique.load(inst, cell) == {:ok, %{pending: 0, waking: false}}

    call = Task.async(fn -> Alambique.execute(inst, cell, @insert, [1]) end)
    eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 1, waking: false}} end)
    :sys.resume(pid)

    assert {:ok, _} = Task.await(call, 5_000)
  end

  test "a lone caller is never busy across wakes that run tasks in parallel", env do
    inst = start_instance(env, migrations: @schema, awake: [max_pending: 2])
    cell = unique_cell()
    MemoryStore.inject(env.store_config, kind: :latency, ms: 5, times: 10_000)

    for n <- 1..30 do
      assert {:ok, _} = Alambique.execute(inst, cell, @insert, [n])
      assert {:ok, _} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
      assert {:ok, _} = Alambique.touch(inst, cell)
      :ok = Alambique.hibernate(inst, cell)
    end
  end

  test "a cell hibernates on idle after a wake with stray messages", env do
    inst = start_instance(env, migrations: @schema, idle_timeout_ms: 200)
    cell = unique_cell()
    capture([[:alambique, :cell, :hibernate]])

    {:ok, _} = Alambique.execute(inst, cell, @insert, [1])
    stray(pid_of(inst, cell), 5)

    assert {_, %{reason: :idle}} = await_event([:alambique, :cell, :hibernate], cell)
    eventually(fn -> Alambique.load(inst, cell) == :none end)
  end
end
