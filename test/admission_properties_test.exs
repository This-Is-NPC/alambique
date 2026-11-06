defmodule Alambique.AdmissionPropertiesTest do
  @moduledoc """
  The limits of a node (design §7) against a model, over random command
  sequences: the wake slots in use never pass `awake.max_wakes` and come back
  exactly once, a wake failure that a retry would repeat is remembered for the
  backoff, and the cells awake never pass `awake.max_cells`.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Admission

  @moduletag capture_log: true

  @deterministic [:restore, :hydration, :local_file, :open, :migration, :schema_too_new]
  @transient [:lease, :owner, :timeout, :busy]

  defp config(max_wakes, backoff_ms, hint_limit \\ 1_000) do
    %{
      admission: :"admission_#{System.unique_integer([:positive])}",
      awake: %{max_wakes: max_wakes},
      hint_limit: hint_limit,
      wake: %{failure_backoff_ms: backoff_ms}
    }
  end

  # Model: `slots` in use, `held` of them reserved and not yet used by a
  # cell, `waking` the cells that hold one.
  defp command do
    one_of([
      constant(:begin),
      constant(:release),
      tuple({constant(:start), integer(0..3)}),
      tuple({constant(:end_wake), integer(0..3)}),
      tuple({constant(:fail), integer(0..3), member_of(@deterministic ++ @transient)})
    ])
  end

  defp step(:begin, {config, model}) do
    expected = if model.slots < config.awake.max_wakes, do: :ok, else: {:error, :overloaded}
    assert Admission.begin_wake(config) == expected
    if expected == :ok, do: %{model | slots: model.slots + 1, held: model.held + 1}, else: model
  end

  defp step(:release, {config, model}) do
    if model.held > 0 do
      assert Admission.release(config) == :ok
      %{model | slots: model.slots - 1, held: model.held - 1}
    else
      model
    end
  end

  # A cell starts and takes over a reserved slot.
  defp step({:start, i}, {config, model}) do
    if model.held > 0 and i not in model.waking do
      assert Admission.waking(config, Map.fetch!(model.pids, i)) == :ok
      %{model | held: model.held - 1, waking: MapSet.put(model.waking, i)}
    else
      model
    end
  end

  # The end of a wake gives the slot back once, however often it is called.
  defp step({:end_wake, i}, {config, model}) do
    assert Admission.end_wake(config, Map.fetch!(model.pids, i)) == :ok

    if i in model.waking,
      do: %{model | slots: model.slots - 1, waking: MapSet.delete(model.waking, i)},
      else: model
  end

  defp step({:fail, i, reason}, {config, model}) do
    assert Admission.failed(config, "cell-#{i}", {reason, :why}) == :ok

    model
  end

  defp run(commands, config) do
    :ok = Admission.init(config)
    pids = Map.new(0..3, &{&1, spawn_link(fn -> Process.sleep(:infinity) end)})
    model = %{slots: 0, held: 0, waking: MapSet.new(), pids: pids}

    try do
      model =
        Enum.reduce(commands, model, fn command, model ->
          model = step(command, {config, model})
          [{:wakes, slots}] = :ets.lookup(config.admission, :wakes)

          # The slots in use are the model's and never pass the limit.
          assert slots == model.slots
          assert slots <= config.awake.max_wakes
          model
        end)

      # Whatever was reserved comes back when each holder gives it up.
      for i <- model.waking, do: Admission.end_wake(config, pids[i])
      for _ <- 1..model.held//1, do: Admission.release(config)
      assert [{:wakes, 0}] = :ets.lookup(config.admission, :wakes)

      model
    after
      Enum.each(pids, fn {_, pid} -> Process.unlink(pid) && Process.exit(pid, :kill) end)
      :ets.delete(config.admission)
    end
  end

  property "wake slots in use equal the model, stay within max_wakes and all come back" do
    check all(max <- integer(1..4), commands <- list_of(command(), max_length: 40), max_runs: 300) do
      run(commands, config(max, 60_000))
    end
  end

  property "failure/2 answers with the reason that was remembered for the cell" do
    check all(
            events <-
              list_of({integer(0..3), member_of(@deterministic ++ @transient)}, max_length: 20),
            max_runs: 200
          ) do
      config = config(1, 60_000)
      :ok = Admission.init(config)

      try do
        remembered =
          Enum.reduce(events, %{}, fn {i, reason}, acc ->
            :ok = Admission.failed(config, "cell-#{i}", {reason, i})
            if reason in @deterministic, do: Map.put(acc, i, {reason, i}), else: acc
          end)

        for i <- 0..3 do
          expected = if reason = remembered[i], do: {:error, reason}
          assert Admission.failure(config, "cell-#{i}") == expected
        end
      after
        :ets.delete(config.admission)
      end
    end
  end

  property "a remembered failure is forgotten once its backoff has passed" do
    check all(i <- integer(0..3), reason <- member_of(@deterministic)) do
      config = config(1, 0)
      :ok = Admission.init(config)

      try do
        :ok = Admission.failed(config, "cell-#{i}", {reason, :why})
        assert Admission.failure(config, "cell-#{i}") == nil
        assert :ets.lookup(config.admission, {:failed, "cell-#{i}"}) == []
      after
        :ets.delete(config.admission)
      end
    end
  end

  property "the failures kept stay within hint_limit and the newest is never the one lost" do
    check all(limit <- integer(4..20), cells <- integer(1..60), max_runs: 100) do
      config = config(1, 60_000, limit)
      :ok = Admission.init(config)

      try do
        for n <- 1..cells do
          :ok = Admission.failed(config, "cell-#{n}", {:restore, n})
          assert Admission.failure(config, "cell-#{n}") == {:error, {:restore, n}}
          assert :ets.info(config.admission, :size) <= limit + 3
        end
      after
        :ets.delete(config.admission)
      end
    end
  end

  describe "with an instance" do
    setup do
      setup_store()
    end

    property "the cells awake never pass max_cells and a freed place can be taken", ctx do
      inst =
        start_instance(ctx,
          migrations: [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}],
          awake: [max_cells: 3]
        )

      registry = Alambique.Config.fetch!(inst).registry
      cells = for n <- 0..5, do: "bounded-#{n}"

      command =
        one_of([{constant(:touch), member_of(cells)}, {constant(:sleep), member_of(cells)}])

      check all(commands <- list_of(command, max_length: 12), max_runs: 40) do
        # Start from no cell awake.
        for cell <- cells, do: Alambique.hibernate(inst, cell)
        eventually(fn -> Registry.count(registry) == 0 end)

        Enum.reduce(commands, MapSet.new(), fn
          {:touch, cell}, awake ->
            cond do
              cell in awake ->
                assert {:ok, _} = Alambique.touch(inst, cell)
                awake

              MapSet.size(awake) < 3 ->
                assert {:ok, _} = Alambique.touch(inst, cell)
                MapSet.put(awake, cell)

              true ->
                assert {:error, :overloaded} = Alambique.touch(inst, cell)
                awake
            end
            |> tap(fn awake -> assert Registry.count(registry) == MapSet.size(awake) end)

          {:sleep, cell}, awake ->
            :ok = Alambique.hibernate(inst, cell)
            awake = MapSet.delete(awake, cell)
            eventually(fn -> Registry.count(registry) == MapSet.size(awake) end)
            awake
        end)
      end
    end
  end
end
