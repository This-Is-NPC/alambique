defmodule Alambique.QuarantinePropertiesTest do
  @moduledoc """
  The crash counter against a model (design §10).

  Random sequences of VM crashes of two nodes over one store, with time
  passing and operators unfreezing cells. After every crash:

    * `total` is the number of crashes since the last unfreeze, so no
      increment is lost or repeated, whichever node recorded it;
    * the crashes kept are those inside the window, at most the threshold;
    * the cell is frozen exactly when some crash since the last unfreeze found
      the threshold reached inside the window.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log
  use ExUnitProperties

  import Alambique.CellHelpers

  alias AlambiqueTestkit.ManualClock

  @window_ms 10_000
  @cells ["c0", "c1", "c2"]
  @nodes [:a, :b]

  setup do
    setup_store()
  end

  defp cells,
    do: @cells |> member_of() |> list_of(max_length: 3) |> map(&Enum.uniq/1)

  defp op do
    frequency([
      {8, tuple({constant(:crash), member_of(@nodes), cells()})},
      {3, tuple({constant(:advance), integer(0..6_000)})},
      {2, tuple({constant(:unfreeze), member_of(@cells)})}
    ])
  end

  property "the counter equals the crashes inside the window and the freeze follows the threshold",
           ctx do
    check all(
            threshold <- integer(1..4),
            ops <- list_of(op(), min_length: 1, max_length: 20),
            max_runs: 60,
            max_shrinking_steps: 0
          ) do
      run = System.unique_integer([:positive])

      env = %{
        store: ctx,
        run: run,
        world: ManualClock.new(1_700_000_000_000),
        threshold: threshold,
        running: MapSet.new()
      }

      model = Map.new(@cells, &{&1, empty()})
      {env, _model} = Enum.reduce(ops, {env, model}, &step/2)
      for node <- env.running, do: stop_supervised!(name(env, node))
    end
  end

  defp empty, do: %{crashes: [], total: 0, frozen: false}

  defp name(env, node), do: :"qp_#{env.run}_#{node}"
  defp cell(env, id), do: "#{id}-#{env.run}"

  defp start_node(env, node) do
    name = name(env, node)

    start_instance(env.store,
      name: name,
      node_id: Atom.to_string(name),
      prefix: "qp-#{env.run}",
      clock: {ManualClock, ManualClock.config(env.world, 0)},
      quarantine: [threshold: env.threshold, window_ms: @window_ms]
    )

    %{env | running: MapSet.put(env.running, node)}
  end

  defp ensure_running(env, node),
    do: if(node in env.running, do: env, else: start_node(env, node))

  defp step({:advance, ms}, {env, model}) do
    ManualClock.advance(env.world, ms)
    {env, model}
  end

  # The node works on the cells, the VM dies, the node starts again.
  defp step({:crash, node, ids}, {env, model}) do
    env = ensure_running(env, node)
    name = name(env, node)
    stop_supervised!(name)

    for id <- ids,
        do: leave_busy_marker(Path.join(env.store.dir, Atom.to_string(name)), cell(env, id))

    now = ManualClock.world_ms(env.world)

    model =
      Enum.reduce(ids, model, &Map.update!(&2, &1, fn c -> crash(c, now, env.threshold) end))

    env = start_node(%{env | running: MapSet.delete(env.running, node)}, node)
    verify(env, model, node)
    {env, model}
  end

  defp step({:unfreeze, id}, {env, model}) do
    env = ensure_running(env, :a)
    :ok = Alambique.unfreeze(name(env, :a), cell(env, id))
    {env, Map.put(model, id, empty())}
  end

  defp crash(cell, now, threshold) do
    inside = Enum.filter(cell.crashes, &(&1 > now - @window_ms))

    %{
      crashes: Enum.take([now | inside], threshold),
      total: cell.total + 1,
      frozen: cell.frozen or length(inside) + 1 >= threshold
    }
  end

  defp verify(env, model, node) do
    inst = name(env, node)

    for {id, expected} <- model do
      {:ok, counter} = Alambique.crashes(inst, cell(env, id))
      assert (counter || %{total: 0}).total == expected.total, "total of #{id}"

      assert length((counter || %{recent: []}).recent) == length(expected.crashes),
             "kept of #{id}"

      assert {:ok, expected.frozen} == Alambique.frozen?(inst, cell(env, id)), "frozen of #{id}"
    end
  end
end
