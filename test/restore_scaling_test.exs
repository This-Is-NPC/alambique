defmodule Alambique.RestoreScalingTest do
  @moduledoc "The work of a restore is linear in the length of the chain (design §6.3)."
  use ExUnit.Case, async: false

  import Alambique.ReplicationHelpers

  alias Alambique.Replication

  @short 300
  @long 1200

  # Reductions that restoring a chain of `segments` files costs the caller, the
  # process that walks the chain. Counts, not time: the same on every run.
  defp reductions(segments) do
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
    {:reductions, before} = Process.info(self(), :reductions)

    assert {:ok, %{txid: ^segments}} =
             Replication.restore(ctx, "cell", Path.join(dir, "dest.db"), :latest)

    {:reductions, after_} = Process.info(self(), :reductions)
    after_ - before
  end

  # 4 times the segments cost 4.1 times the reductions. Before the walk looked
  # files up by TXID and kept the listing out of the read ahead it was 14.6
  # times; with only the first of the two it was 6.7.
  test "a chain 4 times longer costs about 4 times the reductions" do
    ratio = reductions(@long) / reductions(@short)
    assert ratio < 5, "reductions grew #{Float.round(ratio, 1)} times for 4 times the segments"
  end
end
