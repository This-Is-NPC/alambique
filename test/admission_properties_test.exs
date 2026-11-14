defmodule Alambique.AdmissionPropertiesTest do
  @moduledoc """
  The limits of a node (design §7) against a model, over random command
  sequences: the wake slots in use never pass `awake.max_wakes` and come back
  exactly once even when their holders die, a wake failure that a retry would repeat is remembered for the
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

  defp config(max_wakes, backoff_ms, hint_limit \\ 1_000, queue \\ {1, 0}) do
    {max_queue, queue_ms} = queue
    id = System.unique_integer([:positive])

    %{
      admission: :"admission_#{id}",
      wakes: :"wakes_#{id}",
      awake: %{max_wakes: max_wakes, max_wake_queue: max_queue, wake_queue_ms: queue_ms},
      hint_limit: hint_limit,
      wake: %{failure_backoff_ms: backoff_ms, backoff_max_ms: 10_000}
    }
  end

  defp drop(config), do: :ets.delete(config.admission)

  # The slots and the queue of a `Wakes` server run apart from any cell.
  defp with_wakes(config, fun) do
    :ok = Admission.init(config)
    {:ok, server} = Alambique.Admission.Wakes.start_link(config)

    try do
      fun.()
    after
      Process.unlink(server)
      Process.exit(server, :kill)
      drop(config)
    end
  end

  defp snapshot(config) do
    state = :sys.get_state(config.wakes)

    waiting =
      for mon <- :queue.to_list(state.order), entry = state.waiting[mon], do: entry.pid

    {state.holders |> Map.keys() |> MapSet.new(), waiting}
  end

  # A caller begins a wake; a cell takes over the slot of its holder. Each
  # reports the answer to the test.
  defp actor(config, test, holder) do
    spawn(fn ->
      answer =
        if holder,
          do: Admission.take_over(config, holder),
          else: Admission.begin_wake(config, config.awake.wake_queue_ms)

      send(test, {:answer, self(), answer})
      Process.sleep(:infinity)
    end)
  end

  # What the server must do with a process that asks for a slot of its own.
  defp ask(config, test, holder, m, max, cap) do
    pid = actor(config, test, holder)

    cond do
      m.waiting == [] and MapSet.size(m.holders) < max ->
        assert_receive {:answer, ^pid, :ok}
        {pid, %{m | holders: MapSet.put(m.holders, pid)}}

      length(m.waiting) < cap ->
        eventually(fn -> pid in elem(snapshot(config), 1) end, 500)
        {pid, %{m | waiting: m.waiting ++ [pid]}}

      true ->
        assert_receive {:answer, ^pid, {:error, :overloaded}}
        {pid, m}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
  end

  # Grants in the model what the server grants when a slot frees.
  defp grant(%{holders: holders, waiting: [next | rest]} = m, max) do
    if MapSet.size(holders) < max,
      do: grant(%{m | holders: MapSet.put(holders, next), waiting: rest}, max),
      else: m
  end

  defp grant(m, _max), do: m

  defp wake_command do
    one_of([
      constant(:arrive),
      tuple({constant(:start), integer(0..9)}),
      tuple({constant(:end), integer(0..9)}),
      tuple({constant(:kill), integer(0..9)})
    ])
  end

  property "slots held never pass max_wakes, the queue keeps order and cap, a freed slot is " <>
             "handed on, and every slot returns, whoever dies and whenever" do
    check all(
            max <- integer(1..3),
            cap <- integer(1..4),
            commands <- list_of(wake_command(), max_length: 30),
            max_runs: 150
          ) do
      config = config(max, 60_000, 1_000, {cap, 60_000})
      test = self()

      with_wakes(config, fn ->
        # `procs` is every process the run made, callers and cells; the
        # model is the holders and the queue as the server must show them.
        {m, procs} =
          Enum.reduce(commands, {%{holders: MapSet.new(), waiting: []}, []}, fn
            :arrive, {m, procs} ->
              {pid, m} = ask(config, test, nil, m, max, cap)
              {m, procs ++ [pid]}

            _, {m, []} ->
              {m, []}

            {:start, i}, {m, procs} ->
              # The cell takes over the slot of a holder. If the holder is gone,
              # or holds none, the cell asks for a slot like any wake.
              holder = Enum.at(procs, rem(i, length(procs)))

              if holder in m.holders do
                cell = actor(config, test, holder)
                assert_receive {:answer, ^cell, :ok}
                holders = m.holders |> MapSet.delete(holder) |> MapSet.put(cell)
                {%{m | holders: holders}, procs ++ [cell]}
              else
                {cell, m} = ask(config, test, holder, m, max, cap)
                {m, procs ++ [cell]}
              end

            {:end, i}, {m, procs} ->
              pid = Enum.at(procs, rem(i, length(procs)))
              Admission.end_wake(config, pid)
              {grant(%{m | holders: MapSet.delete(m.holders, pid)}, max), procs}

            {:kill, i}, {m, procs} ->
              pid = Enum.at(procs, rem(i, length(procs)))
              kill(pid)
              m = %{m | waiting: List.delete(m.waiting, pid)}
              {grant(%{m | holders: MapSet.delete(m.holders, pid)}, max), procs}
          end)
          |> then(fn {m, procs} ->
            expected = {m.holders, m.waiting}
            eventually(fn -> snapshot(config) == expected end, 500)
            {m, procs}
          end)

        {holders, waiting} = snapshot(config)
        assert MapSet.size(holders) <= max
        assert length(waiting) <= cap
        # A slot is never idle while a wake waits for it.
        assert waiting == [] or MapSet.size(holders) == max
        assert {holders, waiting} == {m.holders, m.waiting}

        # Whatever happens to the processes, every slot comes back.
        Enum.each(procs, &Process.exit(&1, :kill))
        eventually(fn -> snapshot(config) == {MapSet.new(), []} end, 500)
      end)
    end
  end

  property "a queued wake is refused once its wait has passed, and not before" do
    check all(wait <- integer(20..80), waiters <- integer(1..4), max_runs: 15) do
      config = config(1, 60_000, 1_000, {4, wait})

      with_wakes(config, fn ->
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
        assert {_, []} = snapshot(config)
      end)
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

  property "a remembered failure is answered no more once its backoff has passed" do
    check all(i <- integer(0..3), reason <- member_of(@deterministic)) do
      config = config(1, 0)
      :ok = Admission.init(config)

      try do
        :ok = Admission.failed(config, "cell-#{i}", {reason, :why})
        assert Admission.failure(config, "cell-#{i}") == nil
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

        eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 1, waking: false}} end)

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
