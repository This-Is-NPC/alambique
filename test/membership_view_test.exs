defmodule Alambique.MembershipViewTest do
  @moduledoc "The cluster view is cached and follows the groups of the scope (design §8)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Membership

  setup do
    env = setup_store()
    %{inst: start_instance(env)}
  end

  defp reductions(fun) do
    {:reductions, before} = Process.info(self(), :reductions)
    fun.()
    {:reductions, after_} = Process.info(self(), :reductions)
    after_ - before
  end

  defp read_view(config) do
    Membership.nodes(config)
    Membership.foreign_nodes(config)
  end

  test "reading the view does not scan the groups", %{inst: inst} do
    config = Alambique.Config.fetch!(inst)

    fakes =
      for _ <- 1..500 do
        pid = spawn_link(fn -> Process.sleep(:infinity) end)
        :ok = :pg.join(config.scope, :members, pid)
        pid
      end

    eventually(fn -> length(:pg.get_members(config.scope, :members)) > 500 end)
    large = reductions(fn -> read_view(config) end)

    # A scan of 500 members is several thousand reductions.
    assert large < 500
    Enum.each(fakes, &Process.exit(&1, :kill))
  end

  test "a fresh read does not store the view, so it cannot overwrite a newer one", %{inst: inst} do
    config = Alambique.Config.fetch!(inst)
    assert puts(self(), fn -> Membership.fresh_nodes(config) end) == 0
  end

  test "leaving and joining are visible at once", %{inst: inst} do
    config = Alambique.Config.fetch!(inst)
    Membership.leave(config)
    assert Membership.nodes(config) == [node()]
    Membership.join(config)
    assert node() in Membership.nodes(config)
  end

  # Counts the `:persistent_term.put/2` calls of `pid` while `fun` runs and the
  # view settles.
  defp puts(pid, fun) do
    parent = self()

    tracer =
      spawn_link(fn ->
        receive do
          :stop -> send(parent, {:puts, count_puts(0)})
        end
      end)

    :erlang.trace_pattern({:persistent_term, :put, 2}, true, [:global])
    :erlang.trace(pid, true, [:call, {:tracer, tracer}])
    fun.()
    Process.sleep(200)
    :erlang.trace(pid, false, [:call])
    send(tracer, :stop)
    assert_receive {:puts, n}
    n
  end

  defp count_puts(n) do
    receive do
      {:trace, _, :call, {:persistent_term, :put, _}} -> count_puts(n + 1)
    after
      50 -> n
    end
  end

  test "a burst of group changes puts the view at most twice", %{inst: inst} do
    config = Alambique.Config.fetch!(inst)
    [pid] = :pg.get_members(config.scope, {:node_id, config.node_id, config.incarnation})
    group = :members

    # Each leave and join changes the view: 40 events, queued while the
    # process is suspended.
    n =
      puts(pid, fn ->
        :sys.suspend(pid)

        for _ <- 1..20 do
          :ok = :pg.leave(config.scope, group, pid)
          :ok = :pg.join(config.scope, group, pid)
        end

        :sys.resume(pid)
      end)

    assert n <= 2, "#{n} puts for 40 group changes"
    assert node() in Membership.nodes(config)
  end

  test "a refresh that changes nothing puts nothing", %{inst: inst} do
    config = Alambique.Config.fetch!(inst)
    [pid] = :pg.get_members(config.scope, {:node_id, config.node_id, config.incarnation})
    assert puts(pid, fn -> for _ <- 1..20, do: Membership.sync(config) end) == 0
  end
end
