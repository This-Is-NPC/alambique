defmodule Alambique.OpsNodeTasksTest do
  @moduledoc """
  The mix tasks that call a node of the cluster (design §10), run against the
  test VM itself as that node, so no peer is started.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]
  @grace_ms 5_000
  @cell "home-a"

  setup ctx do
    started? = not Node.alive?()

    if started? do
      name = :"ops_node_tasks_#{System.unique_integer([:positive])}@127.0.0.1"
      {:ok, _} = Node.start(name, :longnames)
    end

    cookie = Node.get_cookie()
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)

    on_exit(fn ->
      Mix.shell(shell)
      Node.set_cookie(cookie)
      if started?, do: Node.stop()
    end)

    old_ops = Application.get_env(:alambique, :ops)
    on_exit(fn -> restore_ops(old_ops) end)
    base = setup_store()
    stores = for name <- [:a, :b, :table], into: %{}, do: {name, start_store(name)}

    locations = [
      %Alambique.Location{id: "a", store: {MemoryStore, stores.a}, prefix: "pa"},
      %Alambique.Location{id: "b", store: {MemoryStore, stores.b}, prefix: "pb"}
    ]

    placement =
      {AlambiqueTestkit.Placement,
       locations: locations, table: {MemoryStore, stores.table}, initial: %{@cell => "a"}}

    world = ManualClock.new(1_700_000_000_000)
    clock = {ManualClock, ManualClock.config(world, 0)}

    inst =
      start_instance(base,
        placement: placement,
        migrations: @schema,
        clock: clock,
        moved_grace_ms: ctx[:moved_grace_ms] || @grace_ms,
        skew_margin_ms: ctx[:skew_margin_ms] || 500,
        deletion_grace_ms: 0
      )

    Application.put_env(:alambique, :ops, placement: placement)
    {:ok, _} = Alambique.execute(inst, @cell, "INSERT INTO t (n, v) VALUES (1, 'x')")
    :ok = Alambique.hibernate(inst, @cell)
    %{inst: inst, world: world, stores: stores, placement: placement}
  end

  defp restore_ops(nil), do: Application.delete_env(:alambique, :ops)
  defp restore_ops(old), do: Application.put_env(:alambique, :ops, old)

  defp start_store(name) do
    pid = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: name))
    MemoryStore.config(pid)
  end

  defp run(task, args) do
    Mix.Task.rerun(task, args)
    drain([])
  end

  defp drain(acc) do
    receive do
      {:mix_shell, :info, [line]} -> drain([line | acc])
    after
      0 -> acc |> Enum.reverse() |> Enum.join("\n")
    end
  end

  defp via(inst), do: ["--instance", Atom.to_string(inst), "--connect", Atom.to_string(node())]

  defp location_of(inst, cell) do
    {:ok, location} = Alambique.Placement.locate(Alambique.Config.fetch!(inst), cell, fresh: true)
    location.id
  end

  defp keys(store) do
    {:ok, keys, nil} = MemoryStore.list(store, "", nil, 100_000)
    keys
  end

  # Segments, snapshots and markers of the cell at a location; not the lease.
  defp objects(stores, name) do
    Enum.filter(
      keys(stores[name]),
      &(String.contains?(&1, "/cells/#{@cell}/") and not String.ends_with?(&1, "/lease"))
    )
  end

  describe "alambique.move" do
    test "hands the cell to the target node, which is the node to call by default", ctx do
      target = Atom.to_string(node())
      out = run("alambique.move", [@cell, target, "--instance", Atom.to_string(ctx.inst)])
      assert out == "Moved #{@cell} to #{target}"
      assert {:ok, 1} = Alambique.query(ctx.inst, @cell, "SELECT count(*) FROM t") |> count()
    end

    test "an unknown target node is explained and nothing is raised from the node", ctx do
      assert_raise Mix.Error, "could not move #{@cell}: :unknown_node", fn ->
        run("alambique.move", [@cell, "nobody@127.0.0.1"] ++ via(ctx.inst))
      end
    end

    test "needs a cell, a target and an integer timeout", ctx do
      assert_raise Mix.Error, ~r/usage: mix alambique.move CELL TARGET_NODE/, fn ->
        run("alambique.move", [@cell] ++ via(ctx.inst))
      end

      assert_raise Mix.Error, ~r/--timeout : Expected type integer/, fn ->
        run("alambique.move", [@cell, "n@h", "--timeout", "soon"] ++ via(ctx.inst))
      end
    end
  end

  defp count({:ok, %{rows: [[n]]}}), do: {:ok, n}

  describe "alambique.relocate" do
    test "moves the cell and says when the old location is cleaned", ctx do
      out = run("alambique.relocate", [@cell, "b"] ++ via(ctx.inst))
      assert out =~ "Moved #{@cell} to location b\n"
      assert out =~ ~r/Location a keeps the objects of #{@cell} until 2023-11-14T22:13:25.500Z;/
      assert out =~ "run: mix alambique.cleanup #{@cell} a"

      assert location_of(ctx.inst, @cell) == "b"
      assert {:ok, 1} = Alambique.query(ctx.inst, @cell, "SELECT count(*) FROM t") |> count()
      assert objects(ctx.stores, :a) != []
    end

    test "a cell already at the destination has no old location to report", ctx do
      assert run("alambique.relocate", [@cell, "a"] ++ via(ctx.inst)) ==
               "Moved #{@cell} to location a"

      assert objects(ctx.stores, :b) == []
    end

    @tag moved_grace_ms: 0, skew_margin_ms: 0
    test "a move that already cleaned the old location reports zero objects left", ctx do
      out = run("alambique.relocate", [@cell, "b"] ++ via(ctx.inst))

      assert out =~
               ~r/Moved #{@cell} to location b\nCleaned 0 objects of #{@cell} at location a/

      assert objects(ctx.stores, :a) == []
    end

    test "a cell the placement cannot place is explained before anything moves", ctx do
      assert_raise Mix.Error, "could not locate a/b: :invalid_cell_id", fn ->
        run("alambique.relocate", ["a/b", "b"] ++ via(ctx.inst))
      end
    end

    test "an unknown location is refused", ctx do
      assert_raise Mix.Error, ~r/could not relocate #{@cell}: /, fn ->
        run("alambique.relocate", [@cell, "zz"] ++ via(ctx.inst))
      end

      assert location_of(ctx.inst, @cell) == "a"
    end

    test "needs a cell and a location", ctx do
      assert_raise Mix.Error, ~r/usage: mix alambique.relocate CELL LOCATION_ID/, fn ->
        run("alambique.relocate", [@cell] ++ via(ctx.inst))
      end
    end
  end

  describe "alambique.cleanup" do
    setup ctx do
      {:ok, _} = Alambique.relocate(ctx.inst, @cell, "b")
      :ok
    end

    test "waits for the move grace period, then cleans and says how many objects", ctx do
      out = run("alambique.cleanup", [@cell, "a"] ++ via(ctx.inst))

      assert out =~
               "Not yet: location a keeps the objects of #{@cell} until 2023-11-14T22:13:25.500Z"

      assert objects(ctx.stores, :a) != []

      ManualClock.advance(ctx.world, @grace_ms + 1_000)
      before = length(objects(ctx.stores, :a))
      out = run("alambique.cleanup", [@cell, "a"] ++ via(ctx.inst))

      assert out ==
               "Cleaned #{before} objects of #{@cell} at location a (deleted after deletion_grace_ms)"

      assert objects(ctx.stores, :a) == []
    end

    test "--grace-ms overrides the instance grace period", ctx do
      ManualClock.advance(ctx.world, 600)
      out = run("alambique.cleanup", [@cell, "a", "--grace-ms", "0"] ++ via(ctx.inst))
      assert out =~ ~r/^Cleaned [1-9]\d* objects of #{@cell} at location a/
      assert objects(ctx.stores, :a) == []
    end

    test "refuses the location the cell lives in", ctx do
      assert_raise Mix.Error, "could not clean up #{@cell} at b: :live_location", fn ->
        run("alambique.cleanup", [@cell, "b", "--grace-ms", "0"] ++ via(ctx.inst))
      end

      assert objects(ctx.stores, :b) != []
    end

    test "needs a cell and the location it left", ctx do
      assert_raise Mix.Error, ~r/usage: mix alambique.cleanup CELL LOCATION_ID/, fn ->
        run("alambique.cleanup", [@cell] ++ via(ctx.inst))
      end

      assert_raise Mix.Error, ~r/--grace-ms : Expected type integer/, fn ->
        run("alambique.cleanup", [@cell, "a", "--grace-ms", "soon"] ++ via(ctx.inst))
      end
    end
  end

  describe "joining the cluster" do
    test "--connect is required and --instance names a running instance", ctx do
      assert_raise Mix.Error, ~r/--instance NAME is required/, fn ->
        run("alambique.relocate", [@cell, "b", "--connect", Atom.to_string(node())])
      end

      assert_raise Mix.Error, "--connect NODE is required", fn ->
        run("alambique.relocate", [@cell, "b", "--instance", Atom.to_string(ctx.inst)])
      end
    end

    test "an unreachable node is explained", ctx do
      assert_raise Mix.Error, ~r/could not connect to nobody@127.0.0.1/, fn ->
        run("alambique.relocate", [
          @cell,
          "b",
          "--instance",
          Atom.to_string(ctx.inst),
          "--connect",
          "nobody@127.0.0.1"
        ])
      end

      assert location_of(ctx.inst, @cell) == "a"
    end

    test "an instance the node does not run is named in the error", ctx do
      wrong = ["--instance", "nope", "--connect", Atom.to_string(node())]

      for {task, args} <- [
            {"alambique.move", [@cell, Atom.to_string(node())]},
            {"alambique.relocate", [@cell, "b"]},
            {"alambique.cleanup", [@cell, "a"]}
          ] do
        assert_raise Mix.Error, ~r/no Alambique instance named :nope is running/, fn ->
          run(task, args ++ wrong)
        end
      end

      assert location_of(ctx.inst, @cell) == "a"
    end

    test "--cookie is set before connecting", ctx do
      run("alambique.relocate", [@cell, "a", "--cookie", "from-flag"] ++ via(ctx.inst))
      assert Node.get_cookie() == :"from-flag"
    end
  end

  describe "alambique.freeze --connect" do
    test "freezes in the store and puts the awake cell to sleep now", ctx do
      {:ok, _} = Alambique.execute(ctx.inst, @cell, "INSERT INTO t (n, v) VALUES (2, 'y')")
      assert {:ok, _} = Alambique.owner(ctx.inst, @cell)

      out = run("alambique.freeze", [@cell] ++ via(ctx.inst))
      assert out == "Froze #{@cell}\n#{@cell} is asleep"
      assert :none = Alambique.owner(ctx.inst, @cell)
      assert {:ok, true} = Alambique.frozen?(ctx.inst, @cell)
    end

    test "says so when the node could not hibernate the cell now", ctx do
      out = run("alambique.freeze", ["a/b"] ++ via(ctx.inst))

      assert out ==
               "Froze a/b\na/b will hibernate at its next lease renewal: {:error, :invalid_cell_id}"
    end

    test "--connect needs --instance before anything is written", ctx do
      assert_raise Mix.Error, ~r/--instance NAME is required/, fn ->
        run("alambique.freeze", [@cell, "--connect", Atom.to_string(node())])
      end

      assert {:ok, false} = Alambique.frozen?(ctx.inst, @cell)
    end
  end

  describe "alambique.inspect at a location the cell left" do
    setup ctx do
      {:ok, _} = Alambique.relocate(ctx.inst, @cell, "b")
      :ok
    end

    test "the placement finds the cell and --location reads the old one" do
      assert run("alambique.inspect", [@cell]) =~ ~r/Lease:      epoch \d+, owner .*, released\n/

      out = run("alambique.inspect", [@cell, "--location", "a"])
      assert out =~ "Prefix:     pa"
      assert out =~ ~r/Lease:      epoch \d+, owner .*, released, moved to location b\n/
    end
  end

  describe "Alambique.Ops on a node that cannot be reached" do
    alias Alambique.Ops

    test "every call answers the error instead of raising" do
      down = :"nobody@127.0.0.1"
      noconnection = {:error, {:erpc, :noconnection}}

      assert Ops.move(down, :inst, @cell, down, 100) == noconnection
      assert Ops.relocate(down, :inst, @cell, "b", 100) == noconnection
      assert Ops.locate(down, :inst, @cell, 100) == noconnection
      assert Ops.cleanup_moved(down, :inst, @cell, "a", [], 100) == noconnection
    end

    test "a node that answers is asked for the location the placement gives", ctx do
      assert Ops.locate(node(), ctx.inst, @cell) == {:ok, "a"}
      {:ok, _} = Alambique.relocate(ctx.inst, @cell, "b")
      assert Ops.locate(node(), ctx.inst, @cell) == {:ok, "b"}
    end
  end
end
