defmodule Alambique.CheckpointTest do
  @moduledoc """
  `Alambique.checkpoint/3` empties the WAL of an awake cell without
  hibernating it, and only after the store acknowledged every upload: a frame
  that is not replicated is never truncated (design §6).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Cell.Local
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"}]
  @insert "INSERT INTO t (v) VALUES (?)"

  setup do
    capture([[:alambique, :cell, :checkpoint]])
    setup_store()
  end

  defp wal_size(inst, cell) do
    case File.stat(Local.db_path(Alambique.Config.fetch!(inst), cell) <> "-wal") do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end

  defp gated(env, opts \\ []) do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    store = GatedStore.wrap(gate, env.store)
    {gate, start_instance(%{env | store: store}, [migrations: @schema] ++ opts)}
  end

  test "an awake cell checkpoints: the WAL goes to zero and the cell keeps serving", env do
    inst = start_instance(env, migrations: @schema)
    cell = unique_cell()
    for v <- 1..5, do: {:ok, _} = Alambique.execute(inst, cell, @insert, [v])
    assert wal_size(inst, cell) > 0

    assert :ok = Alambique.checkpoint(inst, cell)

    assert wal_size(inst, cell) == 0
    assert {_, %{result: :ok}} = await_event([:alambique, :cell, :checkpoint], cell)
    assert {:ok, _} = Alambique.load(inst, cell)

    {:ok, _} = Alambique.execute(inst, cell, @insert, [6])
    assert {:ok, %{rows: [[6]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    assert durable_rows(env, cell, "SELECT count(*) FROM t") == [[6]]
  end

  test "a cell that is asleep answers :asleep and is not woken", env do
    inst = start_instance(env, migrations: @schema)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert, [1])
    :ok = Alambique.hibernate(inst, cell)

    assert :asleep = Alambique.checkpoint(inst, cell)
    assert Alambique.load(inst, cell) == :none
    assert :asleep = Alambique.checkpoint(inst, unique_cell())
  end

  test "it waits for the uploads in flight before it truncates", env do
    {gate, inst} = gated(env)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert, [0])

    GatedStore.hold(gate, ~r{/0000/})
    writer = Task.async(fn -> Alambique.execute(inst, cell, @insert, [1]) end)
    eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 1}} end)

    checkpointer = Task.async(fn -> Alambique.checkpoint(inst, cell, deadline_ms: 8_000) end)
    refute Task.yield(checkpointer, 200)
    assert wal_size(inst, cell) > 0

    GatedStore.open(gate)
    assert {:ok, _} = Task.await(writer, 10_000)
    assert :ok = Task.await(checkpointer, 10_000)

    assert wal_size(inst, cell) == 0
    assert durable_rows(env, cell, "SELECT count(*) FROM t") == [[2]]
  end

  test "uploads that miss the deadline give :busy and leave the WAL and the cell", env do
    {gate, inst} = gated(env)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert, [0])

    GatedStore.hold(gate, ~r{/0000/})
    writer = Task.async(fn -> Alambique.execute(inst, cell, @insert, [1]) end)
    eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 1}} end)

    assert {:error, :busy} = Alambique.checkpoint(inst, cell, deadline_ms: 100)
    assert wal_size(inst, cell) > 0

    # The cell was not stopped: the write lands and a later call succeeds.
    GatedStore.open(gate)
    assert {:ok, _} = Task.await(writer, 10_000)
    assert :ok = Alambique.checkpoint(inst, cell)
    assert wal_size(inst, cell) == 0
    assert durable_rows(env, cell, "SELECT count(*) FROM t") == [[2]]
  end

  test "the deadline and the options are validated" do
    assert_raise ArgumentError, ~r/checkpoint_deadline_ms/, fn ->
      Alambique.Config.build(
        store: {AlambiqueTestkit.MemoryStore, nil},
        data_dir: "/tmp/x",
        wal: [checkpoint_deadline_ms: 0]
      )
    end
  end
end
