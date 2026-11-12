defmodule Alambique.AdmissionPropertiesTest do
  @moduledoc """
  The limits of a node (design §7) against a model, over random command
  sequences: the wake slots in use never pass `awake.max_wakes` and come back
  exactly once, a wake failure that a retry would repeat is remembered for the
  backoff, the cells awake never pass `awake.max_cells` and a call that
  would wake another one hibernates a cell that is idle, and the calls a
  cell admits never pass `awake.max_pending`.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Admission

  @moduletag capture_log: true

  @deterministic [:restore, :hydration, :local_file, :open, :migration, :schema_too_new]
  @transient [:lease, :owner, :timeout, :busy]

  defp config(max_wakes, backoff_ms, hint_limit \\ 1_000, queue \\ {0, 0}) do
    {max_queue, queue_ms} = queue
    id = System.unique_integer([:positive])

    %{
      admission: :"admission_#{id}",
      wake_queue: :"wake_queue_#{id}",
      awake: %{max_wakes: max_wakes, max_wake_queue: max_queue, wake_queue_ms: queue_ms},
      call_timeout_ms: 1_000,
      hint_limit: hint_limit,
      wake: %{failure_backoff_ms: backoff_ms}
    }
  end

  defp drop(config) do
    :ets.delete(config.admission)
    :ets.delete(config.wake_queue)
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
    assert Admission.begin_wake(config, 0) == expected
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
      drop(config)
    end
  end

  property "wake slots in use equal the model, stay within max_wakes and all come back" do
    check all(max <- integer(1..4), commands <- list_of(command(), max_length: 40), max_runs: 300) do
      run(commands, config(max, 60_000))
    end
  end

  # Wakes that find no slot wait in order. Each waiter is a process that calls
  # `begin_wake/2`; the model is the slots in use and the waiting list.
  defp spawn_waiter(config, test) do
    spawn(fn ->
      send(test, {:result, self(), Admission.begin_wake(config, config.awake.wake_queue_ms)})
      Process.sleep(:infinity)
    end)
  end

  defp queued_pids(config),
    do: config.wake_queue |> :ets.tab2list() |> Enum.map(fn {_seq, pid, _ref} -> pid end)

  property "queued wakes keep their order, stay within the cap and take every freed slot" do
    check all(
            max <- integer(1..3),
            cap <- integer(1..4),
            commands <- list_of(member_of([:arrive, :release, :kill_last]), max_length: 30),
            max_runs: 150
          ) do
      config = config(max, 60_000, 1_000, {cap, 60_000})
      :ok = Admission.init(config)
      test = self()

      try do
        # `slots` in use, `waiting` the queued waiters oldest first.
        model =
          Enum.reduce(commands, %{slots: 0, waiting: [], last: nil}, fn
            :arrive, m ->
              pid = spawn_waiter(config, test)

              cond do
                m.slots < max and m.waiting == [] ->
                  assert_receive {:result, ^pid, :ok}
                  %{m | slots: m.slots + 1}

                length(m.waiting) < cap ->
                  eventually(fn -> pid in queued_pids(config) end)
                  refute_received {:result, ^pid, _}
                  %{m | waiting: m.waiting ++ [pid], last: pid}

                true ->
                  assert_receive {:result, ^pid, {:error, :overloaded}}
                  m
              end

            :release, %{slots: 0} = m ->
              m

            :release, %{waiting: [first | rest]} = m ->
              Admission.release(config)
              assert_receive {:result, ^first, :ok}
              %{m | waiting: rest}

            :release, m ->
              Admission.release(config)
              %{m | slots: m.slots - 1}

            :kill_last, %{last: pid, waiting: waiting} = m ->
              if pid in waiting do
                ref = Process.monitor(pid)
                Process.exit(pid, :kill)
                assert_receive {:DOWN, ^ref, _, _, _}
                %{m | waiting: List.delete(waiting, pid), last: nil}
              else
                m
              end
          end)

        # A dead waiter leaves the table when the cap or a release meets it.
        live = Enum.filter(queued_pids(config), &Process.alive?/1)
        assert live == model.waiting
        assert length(live) <= cap
        assert [{:wakes, model.slots}] == :ets.lookup(config.admission, :wakes)
        assert model.slots <= max
        # A slot is never idle while a wake waits for it.
        assert model.waiting == [] or model.slots == max
      after
        for {_, pid, _} <- :ets.tab2list(config.wake_queue), do: Process.exit(pid, :kill)
        drop(config)
      end
    end
  end

  property "a queued wake is refused once its wait has passed, and not before" do
    check all(wait <- integer(20..80), waiters <- integer(1..4), max_runs: 15) do
      config = config(1, 60_000, 1_000, {4, wait})
      :ok = Admission.init(config)

      try do
        assert Admission.begin_wake(config, 0) == :ok
        started = System.monotonic_time(:millisecond)

        tasks =
          for _ <- 1..waiters do
            Task.async(fn ->
              {Admission.begin_wake(config, 60_000), System.monotonic_time(:millisecond)}
            end)
          end

        for task <- tasks do
          assert {{:error, :overloaded}, at} = Task.await(task, 2_000)
          assert at - started >= wait - 1
          assert at - started <= wait + 250
        end

        # The places of the waiters that gave up are free.
        assert [{:queued, 0}] = :ets.lookup(config.admission, :queued)
        assert :ets.tab2list(config.wake_queue) == []
      after
        drop(config)
      end
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
        drop(config)
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
        drop(config)
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
        drop(config)
      end
    end
  end

  defp ids(registry), do: Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])

  describe "with an instance" do
    setup do
      setup_store()
    end

    property "the cells awake never pass max_cells and the oldest idle cell gives its place",
             ctx do
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
        # Start from no cell awake. `awake` lists the cells, most recent last.
        for cell <- cells, do: Alambique.hibernate(inst, cell)
        eventually(fn -> Registry.count(registry) == 0 end)

        Enum.reduce(commands, [], fn
          {:touch, cell}, awake ->
            assert {:ok, _} = Alambique.touch(inst, cell)
            Process.sleep(2)
            awake = List.delete(awake, cell) ++ [cell]
            awake = if length(awake) > 3, do: tl(awake), else: awake
            eventually(fn -> Enum.sort(ids(registry)) == Enum.sort(awake) end)
            assert Registry.count(registry) <= 3
            awake

          {:sleep, cell}, awake ->
            :ok = Alambique.hibernate(inst, cell)
            awake = List.delete(awake, cell)
            eventually(fn -> Enum.sort(ids(registry)) == Enum.sort(awake) end)
            awake
        end)
      end
    end

    property "the calls a cell admits never pass max_pending", ctx do
      {:ok, gate} = AlambiqueTestkit.GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
      store = AlambiqueTestkit.GatedStore.wrap(gate, ctx.store)

      check all(
              max <- integer(2..12),
              callers <- integer(1..40),
              max_runs: 12,
              max_shrinking_steps: 3
            ) do
        inst =
          start_instance(%{ctx | store: store},
            migrations: [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}],
            awake: [max_pending: max]
          )

        cell = unique_cell()
        {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (0)", [])
        AlambiqueTestkit.GatedStore.hold(gate, ~r{/0000/})

        writer =
          Task.async(fn -> Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (1)", []) end)

        eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 1}} end)

        replies = burst_queries(inst, cell, callers, 50)

        # The writer holds one place; the rest go to the first callers, the others are refused.
        assert Enum.count(replies, &(&1 == {:error, :timeout})) == min(callers, max - 1)
        assert {:ok, %{pending: pending}} = Alambique.load(inst, cell)
        assert pending == min(callers + 1, max)

        AlambiqueTestkit.GatedStore.open(gate)
        assert {:ok, _} = Task.await(writer, 10_000)
        stop_supervised!(inst)
      end
    end
  end
end
