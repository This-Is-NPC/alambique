defmodule Alambique.RestoreScalingTest do
  @moduledoc "Planning the walk of a restore costs time linear in the length of the chain (design §6.3)."
  use ExUnit.Case, async: false

  import Alambique.ReplicationHelpers

  alias Alambique.Replication

  @short 300
  @long 1200

  # What restoring a chain of `segments` files costs: the reductions of the
  # walk, and the wall time of the fastest of three restores.
  defp cost(segments) do
    {_pid, store} = start_store()
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 600_000)
    {conn, state} = new_replicator(dir, "src.db")

    Enum.reduce(1..segments, state, fn k, state ->
      run_op(conn, {:put, k, 10})
      {:ok, state} = Replication.Replicator.sync(state, ctx)
      state
    end)

    Alambique.Sqlite.close(conn)
    dest = Path.join(dir, "dest.db")
    {:reductions, before} = Process.info(self(), :reductions)

    times =
      for _ <- 1..3 do
        {us, {:ok, %{txid: ^segments}}} =
          :timer.tc(fn -> Replication.restore(ctx, "cell", dest, :latest) end)

        us
      end

    {:reductions, after_} = Process.info(self(), :reductions)
    %{reductions: div(after_ - before, 3), us: Enum.min(times)}
  end

  # Linear is 4; rescanning or copying the listing for every file is 16.
  test "planning a chain 4 times longer costs about 4 times the reductions" do
    ratio = cost(@long).reductions / cost(@short).reductions
    assert ratio < 6, "reductions grew #{Float.round(ratio, 1)} times for 4 times the segments"
  end

  # The read ahead sends a request to a task per file: what a task is given
  # must not grow with the chain.
  test "restoring a chain 4 times longer takes about 4 times as long" do
    ratio = cost(@long).us / cost(@short).us

    assert ratio < 8,
           "the restore took #{Float.round(ratio, 1)} times as long for 4 times the segments"
  end
end
