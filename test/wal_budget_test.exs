defmodule Alambique.WalBudgetTest do
  @moduledoc """
  `wal: [max_bytes:]` bounds the sum of the WAL files of the awake cells: the
  cell with the largest WAL checkpoints first, only after the store
  acknowledged its uploads (design §6).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Cell.Local
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"}]
  @insert "INSERT INTO t (v) VALUES (?)"
  @budget 20_000
  @wal [max_bytes: @budget, budget_interval_ms: 50, checkpoint_deadline_ms: 200]

  setup do
    capture([[:alambique, :wal, :budget]])
    setup_store()
  end

  defp wal_size(inst, cell) do
    case File.stat(Local.db_path(Alambique.Config.fetch!(inst), cell) <> "-wal") do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end

  defp total(inst, cells), do: cells |> Enum.map(&wal_size(inst, &1)) |> Enum.sum()

  defp write(inst, cell, count),
    do: for(v <- 1..count, do: {:ok, _} = Alambique.execute(inst, cell, @insert, [v]))

  defp budgeted(env, opts \\ []),
    do: start_instance(env, [migrations: @schema, wal: @wal] ++ opts)

  defp gated(env) do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    {gate, budgeted(%{env | store: GatedStore.wrap(gate, env.store)})}
  end

  defp await_budget(match, timeout \\ 5_000) do
    receive do
      {:telemetry, [:alambique, :wal, :budget], m, _meta} ->
        if match.(m), do: m, else: await_budget(match, timeout)
    after
      timeout -> raise "no matching [:alambique, :wal, :budget] event"
    end
  end

  test "the sum of the WALs goes under the budget", env do
    inst = budgeted(env)
    cells = for _ <- 1..5, do: unique_cell()
    for {cell, n} <- Enum.zip(cells, 1..5), do: write(inst, cell, n)

    eventually(fn -> total(inst, cells) < @budget end)

    assert %{bytes: bytes, cells: done} = await_budget(&(&1.cells >= 1))
    assert bytes >= @budget and done >= 1

    for cell <- cells do
      assert {:ok, _} = Alambique.load(inst, cell)
    end
  end

  test "a round checkpoints the cells with the largest WAL and stops under the budget", env do
    inst =
      start_instance(env,
        migrations: @schema,
        wal: [max_bytes: 100_000_000, budget_interval_ms: 3_600_000]
      )

    [big, mid, small] = cells = for _ <- 1..3, do: unique_cell()
    for {cell, n} <- Enum.zip(cells, [8, 4, 1]), do: write(inst, cell, n)
    config = Alambique.Config.fetch!(inst)

    published = fn cell ->
      {_pid, load} = Alambique.Cell.find(config, cell)
      Alambique.Cell.wal_bytes(load)
    end

    eventually(fn -> Enum.all?(cells, &(published.(&1) == wal_size(inst, &1))) end)
    [sb, sm, ss] = Enum.map(cells, &wal_size(inst, &1))
    assert sb > sm and sm > ss and ss > 0

    # Under the budget once the largest is gone: only it is checkpointed.
    budget = sm + ss + 1
    tight = %{config | wal: %{config.wal | max_bytes: budget}}
    assert {:checkpointed, %{bytes: bytes, cells: 1, busy: 0}} = Alambique.WalBudget.run(tight)
    assert bytes == sb + sm + ss
    assert [wal_size(inst, big), wal_size(inst, mid), wal_size(inst, small)] == [0, sm, ss]

    assert :ok = Alambique.WalBudget.run(tight)
  end

  test "frames whose uploads are not acknowledged stay, and the round goes on", env do
    {gate, inst} = gated(env)
    stuck = unique_cell()
    {:ok, _} = Alambique.execute(inst, stuck, @insert, [0])

    GatedStore.hold(gate, Regex.compile!(Regex.escape(stuck) <> ".*/0000/"))
    writer = Task.async(fn -> Alambique.execute(inst, stuck, @insert, [1]) end)
    eventually(fn -> Alambique.load(inst, stuck) == {:ok, %{pending: 1, waking: false}} end)

    others = for _ <- 1..4, do: unique_cell()
    for cell <- others, do: write(inst, cell, 3)

    assert %{busy: busy} = await_budget(&(&1.busy > 0))
    assert busy >= 1
    assert wal_size(inst, stuck) > 0

    GatedStore.open(gate)
    assert {:ok, _} = Task.await(writer, 10_000)
    eventually(fn -> total(inst, [stuck | others]) < @budget end)
    assert durable_rows(env, stuck, "SELECT count(*) FROM t") == [[2]]
  end

  property "after the writes settle the sum is under the budget and every row is durable", env do
    inst = budgeted(env)

    check all(
            writes <- list_of({integer(0..3), integer(1..6)}, min_length: 1, max_length: 12),
            max_runs: 15
          ) do
      cells = for _ <- 0..3, do: unique_cell()
      counts = Enum.reduce(writes, %{}, fn {i, n}, acc -> Map.update(acc, i, n, &(&1 + n)) end)
      for {i, n} <- counts, do: write(inst, Enum.at(cells, i), n)

      eventually(fn -> total(inst, cells) < @budget end, 10_000)

      for {i, n} <- counts do
        assert durable_rows(env, Enum.at(cells, i), "SELECT count(*) FROM t") == [[n]]
      end

      for cell <- cells, do: Alambique.hibernate(inst, cell)
    end
  end

  test "the budget and its interval are validated" do
    for opts <- [[max_bytes: 0], [max_bytes: -1], [max_bytes: "1"], [budget_interval_ms: 0]] do
      assert_raise ArgumentError, ~r/:wal/, fn ->
        Alambique.Config.build(
          store: {AlambiqueTestkit.MemoryStore, nil},
          data_dir: "/tmp/x",
          wal: opts
        )
      end
    end
  end
end
