defmodule Alambique.PeriodicTest do
  @moduledoc "A background job survives a round that fails and leaves no round behind."
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Alambique.Periodic

  test "a round that raises is logged and the next one runs" do
    test = self()
    runs = :counters.new(1, [])

    round = fn ->
      :counters.add(runs, 1, 1)
      send(test, {:ran, :counters.get(runs, 1)})
      if :counters.get(runs, 1) == 1, do: raise("boom")
    end

    log =
      capture_log(fn ->
        {:ok, job} = Periodic.start_link("probe", 10, round)
        assert_receive {:ran, 2}, 5_000
        assert_receive {:ran, 3}, 5_000
        assert Process.alive?(job)
        GenServer.stop(job)
      end)

    assert log =~ "probe round crashed"
    assert log =~ "boom"
  end

  test "stopping the job ends the round that is running" do
    test = self()

    {:ok, job} =
      Periodic.start_link("probe", 10, fn ->
        send(test, {:round, self()})
        Process.sleep(:infinity)
      end)

    assert_receive {:round, round}, 5_000
    ref = Process.monitor(round)
    GenServer.stop(job)

    assert_receive {:DOWN, ^ref, :process, ^round, _}, 5_000
  end
end
