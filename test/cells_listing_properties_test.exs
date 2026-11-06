defmodule Alambique.CellsListingPropertiesTest do
  @moduledoc "Alambique.cells/2 lists the local cells in id order, whatever happened to them (design §8)."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @ids ~w(a b ab B c-1 c-10 c-2 d.e _x 0 zz)

  setup do
    ctx = setup_store()
    %{inst: start_instance(ctx, migrations: @schema, idle_timeout_ms: 600_000)}
  end

  defp op_gen do
    one_of([
      tuple({constant(:wake), member_of(@ids)}),
      tuple({constant(:hibernate), member_of(@ids)}),
      tuple({constant(:kill), member_of(@ids)})
    ])
  end

  defp apply_op({:wake, id}, inst, model) do
    assert {:ok, _} = Alambique.touch(inst, id)
    MapSet.put(model, id)
  end

  defp apply_op({:hibernate, id}, inst, model) do
    assert :ok = Alambique.hibernate(inst, id)
    MapSet.delete(model, id)
  end

  defp apply_op({:kill, id}, inst, model) do
    config = Alambique.Config.fetch!(inst)

    case Alambique.Cell.whereis(config, id) do
      nil ->
        model

      pid ->
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}
        MapSet.delete(model, id)
    end
  end

  # Pages of the given sizes, one after the other, until `next` is nil.
  defp walk(inst, limits, after_id \\ nil, acc \\ []) do
    [limit | rest] = limits

    %{cells: cells, next: next, count: count} =
      Alambique.cells(inst, after: after_id, limit: limit)

    assert length(cells) <= limit
    acc = [{cells, next, count} | acc]
    if next, do: walk(inst, rest ++ [limit], next, acc), else: Enum.reverse(acc)
  end

  property "pages concatenate to the sorted set of cells with a process", %{inst: inst} do
    check all(
            ops <- list_of(op_gen(), min_length: 1, max_length: 30),
            limits <- list_of(integer(1..6), min_length: 1, max_length: 4),
            probe <- string(:alphanumeric, max_length: 4),
            max_runs: 25
          ) do
      model = Enum.reduce(ops, MapSet.new(), &apply_op(&1, inst, &2))
      sorted = Enum.sort(model)

      eventually(fn ->
        %{cells: cells, count: count} = Alambique.cells(inst, limit: 1_000)
        cells == sorted and count == length(sorted)
      end)

      pages = walk(inst, limits)
      assert Enum.flat_map(pages, &elem(&1, 0)) == sorted
      assert Enum.all?(pages, fn {_, _, count} -> count == MapSet.size(model) end)

      {last, others} = List.pop_at(pages, -1)
      assert elem(last, 1) == nil
      assert Enum.all?(others, fn {_, next, _} -> next != nil end)

      # An arbitrary `after:` gives what follows it, whether or not it is a cell.
      assert Alambique.cells(inst, after: probe, limit: 1_000).cells ==
               Enum.filter(sorted, &(&1 > probe))

      for id <- sorted, do: assert(:ok = Alambique.hibernate(inst, id))
      eventually(fn -> Alambique.cells(inst).count == 0 end)
      assert %{cells: [], next: nil, count: 0} = Alambique.cells(inst)
    end
  end

  test "a page ends where the limit says, and next is nil only after the last cell", %{inst: inst} do
    for id <- ~w(a b c), do: assert({:ok, _} = Alambique.touch(inst, id))
    eventually(fn -> Alambique.cells(inst).count == 3 end)

    assert %{cells: ["a", "b"], next: "b", count: 3} = Alambique.cells(inst, limit: 2)
    assert %{cells: ["c"], next: nil, count: 3} = Alambique.cells(inst, after: "b", limit: 2)
    assert %{cells: ["a", "b", "c"], next: nil} = Alambique.cells(inst, limit: 3)
    assert %{cells: [], next: nil} = Alambique.cells(inst, after: "c")
  end

  test "invalid options raise ArgumentError", %{inst: inst} do
    for opts <- [
          [limit: 0],
          [limit: -1],
          [limit: 1.5],
          [limit: "5"],
          [after: 5],
          [after: :a],
          [bogus: 1]
        ] do
      assert_raise ArgumentError, fn -> Alambique.cells(inst, opts) end
    end
  end

  test "a process that exits does not remove the new process under its id", %{inst: inst} do
    config = Alambique.Config.fetch!(inst)
    old = spawn(fn -> Process.sleep(:infinity) end)
    new = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(new, :kill) end)

    send(config.live, {:register, config.registry, "x", old, nil})
    send(config.live, {:register, config.registry, "x", new, nil})

    # The registry's unregister of the old process, then its DOWN.
    send(config.live, {:unregister, config.registry, "x", old})
    Process.exit(old, :kill)
    :sys.get_state(config.live)

    assert [{"x", ^new}] = :ets.lookup(config.live, "x")
    assert %{cells: ["x"]} = Alambique.cells(inst)

    # The DOWN of the old process arriving alone.
    old2 = spawn(fn -> Process.sleep(:infinity) end)
    send(config.live, {:register, config.registry, "y", old2, nil})
    send(config.live, {:register, config.registry, "y", new, nil})
    Process.exit(old2, :kill)
    :sys.get_state(config.live)
    assert [{"y", ^new}] = :ets.lookup(config.live, "y")
  end
end
