defmodule Alambique.LoadTest do
  @moduledoc """
  `Alambique.load/2`: the calls waiting on a cell, read without a call into it
  (design §8). A gated store holds the puts so callers stay unanswered.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"

  setup ctx do
    env = setup_store()
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)

    Map.merge(env, %{gate: gate, cell: "cell-#{ctx.test |> Atom.to_string() |> :erlang.phash2()}"})
  end

  defp start(env, depth) do
    store = GatedStore.wrap(env.gate, env.store)

    inst =
      start_instance(
        %{env | store: store},
        migrations: @schema,
        pipeline_depth: depth,
        compaction: false
      )

    {:ok, _} = Alambique.execute(inst, env.cell, @insert, [0])
    GatedStore.hold(env.gate, ~r{/0000/})
    inst
  end

  defp pending(inst, cell) do
    {:ok, %{pending: n}} = Alambique.load(inst, cell)
    n
  end

  defp writers(inst, cell, range) do
    for n <- range, do: Task.async(fn -> Alambique.execute(inst, cell, @insert, [n]) end)
  end

  test "a cell without a process is :none and stays asleep", env do
    inst = start_instance(env, migrations: @schema, compaction: false)

    assert Alambique.load(inst, env.cell) == :none
    assert %{count: 0} = Alambique.cells(inst)
  end

  test "an idle awake cell has nothing pending", env do
    inst = start(env, 1)
    assert Alambique.load(inst, env.cell) == {:ok, %{pending: 0, waking: false}}
  end

  for depth <- [1, 4] do
    test "held writers are counted at depth #{depth} and leave on release", env do
      inst = start(env, unquote(depth))
      tasks = writers(inst, env.cell, 1..6)

      eventually(fn -> pending(inst, env.cell) == 6 end)

      GatedStore.open(env.gate)
      assert Enum.all?(Task.await_many(tasks), &match?({:ok, _}, &1))
      eventually(fn -> pending(inst, env.cell) == 0 end)
    end
  end

  test "pipelined batches are counted beyond the mailbox", env do
    inst = start(env, 4)

    # One writer per batch: each is encoded and parked in the pipeline.
    tasks =
      for n <- 1..3 do
        task = hd(writers(inst, env.cell, [n]))
        eventually(fn -> pending(inst, env.cell) == n end)
        task
      end

    pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), env.cell)
    assert queued(pid) == 0

    GatedStore.open(env.gate)
    Task.await_many(tasks)
  end

  test "load answers while the cell is stuck in a transaction", env do
    inst = start(env, 1)
    test = self()

    blocker =
      Task.async(fn ->
        Alambique.transaction(inst, env.cell, fn _tx ->
          send(test, :inside)
          receive do: (:go -> :ok)
        end)
      end)

    assert_receive :inside, 5_000
    queued = writers(inst, env.cell, 1..5)
    eventually(fn -> pending(inst, env.cell) >= 5 end)

    load = Task.async(fn -> Alambique.load(inst, env.cell) end)
    assert {:ok, {:ok, %{pending: n}}} = Task.yield(load, 200)
    assert n >= 5

    # The transaction function runs in the cell process and waits for this.
    send(Alambique.Cell.whereis(Alambique.Config.fetch!(inst), env.cell), :go)
    GatedStore.open(env.gate)
    assert {:ok, _} = Task.await(blocker)
    assert Enum.all?(Task.await_many(queued), &match?({:ok, _}, &1))
    eventually(fn -> pending(inst, env.cell) == 0 end)
  end

  test "a cell is waking until its wake ends, then serving", env do
    inst = start_instance(env, migrations: @schema, compaction: false)
    {:ok, _} = Alambique.execute(inst, env.cell, @insert, [0])
    :ok = Alambique.hibernate(inst, env.cell)
    eventually(fn -> Alambique.load(inst, env.cell) == :none end)
    capture([[:alambique, :cell, :wake]])

    # The cell holds its wake here, with a process and before any restore.
    test = self()
    cell = env.cell

    Alambique.Fault.install(fn
      :cell_before_take_over, %{cell: ^cell} ->
        send(test, {:waking, self()})
        receive do: (:go -> :ok)

      _, _ ->
        :ok
    end)

    on_exit(&Alambique.Fault.uninstall/0)

    call = Task.async(fn -> Alambique.query(inst, cell, "SELECT count(*) FROM t") end)
    assert_receive {:waking, waker}, 5_000
    assert {:ok, %{waking: true}} = Alambique.load(inst, cell)

    send(waker, :go)
    assert {:ok, %{rows: [[1]]}} = Task.await(call, 5_000)
    await_event([:alambique, :cell, :wake], cell)
    assert Alambique.load(inst, cell) == {:ok, %{pending: 0, waking: false}}
  end

  test "a cell that woke without a call is serving, not waking", env do
    inst = start_instance(env, migrations: @schema, compaction: false)
    config = Alambique.Config.fetch!(inst)
    capture([[:alambique, :cell, :wake]])

    # No call is routed to it: only the end of the wake can clear `waking`.
    {:ok, _} =
      DynamicSupervisor.start_child(config.supervisor, {Alambique.Cell, {config, env.cell, []}})

    await_event([:alambique, :cell, :wake], env.cell)

    eventually(fn -> Alambique.load(inst, env.cell) == {:ok, %{pending: 0, waking: false}} end)
  end
end
