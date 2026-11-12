defmodule Alambique.CellsDetailsTest do
  @moduledoc "Alambique.cells(details: true) reports the last use and the WAL size of each local cell (design §8)."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Cell.Local

  @moduletag :capture_log

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"}]
  @insert "INSERT INTO t (v) VALUES (zeroblob(1000))"
  @limit 100_000

  setup do
    setup_store()
  end

  defp details(inst) do
    %{cells: cells} = Alambique.cells(inst, details: true, limit: 1_000)
    cells
  end

  # The ids by last use, least recently used first.
  defp by_last_use(inst),
    do: inst |> details() |> Enum.sort_by(& &1.last_used) |> Enum.map(& &1.id)

  # Calls are stamped in milliseconds: wait for the clock to move so that two
  # calls never share a stamp.
  defp tick do
    now = System.monotonic_time(:millisecond)
    eventually(fn -> System.monotonic_time(:millisecond) > now end)
  end

  defp wal_file_size(inst, cell) do
    case File.stat(Local.db_path(Alambique.Config.fetch!(inst), cell) <> "-wal") do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end

  defp wal_bytes(inst, cell),
    do: details(inst) |> Enum.find(&(&1.id == cell)) |> Map.fetch!(:wal_bytes)

  test "an entry has the id, the last use and the WAL size", env do
    inst = start_instance(env, migrations: @schema)
    before = System.monotonic_time(:millisecond)
    assert {:ok, _} = Alambique.touch(inst, "a")

    eventually(fn -> Alambique.cells(inst).count == 1 end)

    # The migration of the wake leaves frames in the WAL.
    eventually(fn -> wal_bytes(inst, "a") == wal_file_size(inst, "a") end)
    assert [%{id: "a", last_used: used, wal_bytes: bytes}] = details(inst)
    assert bytes > 0
    assert used >= before and used <= System.monotonic_time(:millisecond)
    assert %{cells: ["a"]} = Alambique.cells(inst)
  end

  test "details pages like the plain listing", env do
    inst = start_instance(env, migrations: @schema)
    for id <- ~w(a b c), do: assert({:ok, _} = Alambique.touch(inst, id))
    eventually(fn -> Alambique.cells(inst).count == 3 end)

    assert %{cells: [%{id: "a"}, %{id: "b"}], next: "b", count: 3} =
             Alambique.cells(inst, details: true, limit: 2)

    assert %{cells: [%{id: "c"}], next: nil} =
             Alambique.cells(inst, details: true, after: "b", limit: 2)
  end

  test "the last use follows the calls and is the order of the eviction", env do
    inst = start_instance(env, migrations: @schema, idle_timeout_ms: 600_000)

    for id <- ~w(a b c) do
      tick()
      assert {:ok, _} = Alambique.touch(inst, id)
    end

    eventually(fn -> Alambique.cells(inst).count == 3 end)
    assert by_last_use(inst) == ~w(a b c)

    tick()
    assert {:ok, _} = Alambique.query(inst, "a", "SELECT 1")
    assert by_last_use(inst) == ~w(b c a)

    config = Alambique.Config.fetch!(inst)
    assert Enum.map(Alambique.Admission.least_recently_used(config), &elem(&1, 2)) == ~w(b c a)
  end

  property "the cells sort by their last call, whatever the calls were", env do
    inst = start_instance(env, migrations: @schema, idle_timeout_ms: 600_000)

    check all(
            calls <- list_of(member_of(~w(a b c d e)), min_length: 1, max_length: 12),
            max_runs: 15
          ) do
      for id <- calls do
        tick()
        assert {:ok, _} = Alambique.query(inst, id, "SELECT 1")
      end

      expected = calls |> Enum.reverse() |> Enum.uniq() |> Enum.reverse()

      # Cells called by an earlier run are older than every call of this one.
      assert inst |> by_last_use() |> Enum.take(-length(expected)) == expected
    end
  end

  test "the WAL size grows with the writes and falls at a checkpoint", env do
    inst =
      start_instance(env, migrations: @schema, compaction: false, wal: [checkpoint_bytes: @limit])

    # The size is taken just after an acknowledgement, so it settles to the file's.
    settled = fn -> wal_bytes(inst, "a") == wal_file_size(inst, "a") end

    assert {:ok, _} = Alambique.execute(inst, "a", @insert)
    eventually(settled)
    first = wal_bytes(inst, "a")

    assert {:ok, _} = Alambique.execute(inst, "a", @insert)
    eventually(fn -> settled.() and wal_bytes(inst, "a") > first end)

    # One write past the limit: the checkpoint resets the file and no later
    # acknowledgement refreshes the size.
    assert {:ok, _} = Alambique.execute(inst, "a", "INSERT INTO t (v) VALUES (zeroblob(200000))")
    eventually(fn -> wal_file_size(inst, "a") < @limit end)
    eventually(settled)
    assert wal_bytes(inst, "a") < @limit
  end

  test "the WAL size falls to 0 after a requested checkpoint", env do
    inst = start_instance(env, migrations: @schema, compaction: false)
    assert {:ok, _} = Alambique.execute(inst, "a", @insert)

    eventually(fn ->
      wal_bytes(inst, "a") > 0 and wal_bytes(inst, "a") == wal_file_size(inst, "a")
    end)

    assert :ok = Alambique.checkpoint(inst, "a")
    assert wal_file_size(inst, "a") == 0
    assert wal_bytes(inst, "a") == 0
  end

  test "details must be a boolean", env do
    inst = start_instance(env, migrations: @schema)
    assert_raise ArgumentError, fn -> Alambique.cells(inst, details: :yes) end
  end
end
