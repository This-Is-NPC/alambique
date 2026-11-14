defmodule Alambique.LoadWakingPropertiesTest do
  @moduledoc """
  `Alambique.load/2` reports `waking: true` only while a wake runs (design §8).
  Across wakes, calls, hibernations and killed cells, a cell seen with
  `waking: false` is never seen with `waking: true` again unless a new wake
  started in between, and a cell whose call returned is not waking.

  Wake starts are counted at the `:cell_before_take_over` fault point, which
  also holds a gated wake so that the test can look at the cell while it wakes.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"
  @gate {__MODULE__, :gate}

  setup do
    test = self()

    Alambique.Fault.install(fn
      :cell_before_take_over, %{cell: cell} ->
        send(test, {:wake_started, cell})

        if :persistent_term.get(@gate, false) do
          send(test, {:at_wake, cell, self()})
          receive do: (:go -> :ok)
        end

      _, _ ->
        :ok
    end)

    on_exit(fn ->
      Alambique.Fault.uninstall()
      :persistent_term.erase(@gate)
    end)

    setup_store()
  end

  defp starts(cell) do
    receive do
      {:wake_started, ^cell} -> 1 + starts(cell)
    after
      0 -> 0
    end
  end

  # Reads `waking` and checks it against the model: `{starts, false_at}` is the
  # number of wake starts so far and the number at the last `false` seen.
  defp observe(inst, cell, {seen, false_at}) do
    seen = seen + starts(cell)

    case Alambique.load(inst, cell) do
      {:ok, %{waking: true}} ->
        assert false_at == nil or seen > false_at,
               "waking went from false to true without a new wake (#{seen} wakes)"

        {{seen, false_at}, true}

      {:ok, %{waking: false}} ->
        {{seen, seen}, false}

      :none ->
        {{seen, false_at}, :none}
    end
  end

  defp step(:touch, inst, cell, model) do
    {:ok, _} = Alambique.touch(inst, cell)
    done(inst, cell, model)
  end

  defp step(:write, inst, cell, model) do
    {:ok, _} = Alambique.execute(inst, cell, @insert, [System.unique_integer([:positive])])
    done(inst, cell, model)
  end

  defp step(:hibernate, inst, cell, model) do
    :ok = Alambique.hibernate(inst, cell)
    done(inst, cell, model)
  end

  defp step(:kill, inst, cell, model) do
    with pid when is_pid(pid) <- Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell) do
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}, 5_000
    end

    done(inst, cell, model)
  end

  # A call that held the wake: the cell is seen waking, then not.
  defp step(:gated_wake, inst, cell, model) do
    :ok = Alambique.hibernate(inst, cell)
    eventually(fn -> Alambique.load(inst, cell) == :none end)
    :persistent_term.put(@gate, true)
    task = Task.async(fn -> Alambique.touch(inst, cell) end)
    assert_receive {:at_wake, ^cell, waker}, 5_000
    :persistent_term.put(@gate, false)

    {model, now} = observe(inst, cell, model)
    assert now == true
    send(waker, :go)
    assert {:ok, _} = Task.await(task, 5_000)
    done(inst, cell, model)
  end

  # After an operation returned, no wake is running.
  defp done(inst, cell, model) do
    {model, now} = observe(inst, cell, model)
    refute now == true
    model
  end

  property "waking turns true again only with a new wake", env do
    check all(
            ops <-
              list_of(member_of([:touch, :write, :hibernate, :kill, :gated_wake]), max_length: 12),
            max_runs: 15,
            max_shrinking_steps: 0
          ) do
      inst = start_instance(env, migrations: @schema, compaction: false)
      cell = unique_cell()
      Enum.reduce(ops, {0, nil}, &step(&1, inst, cell, &2))
    end
  end
end
