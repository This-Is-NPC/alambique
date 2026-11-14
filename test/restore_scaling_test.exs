defmodule Alambique.RestoreScalingTest do
  @moduledoc "Planning the walk of a restore costs time linear in the length of the chain (design §6.3)."
  use ExUnit.Case, async: false

  import Alambique.ReplicationHelpers

  alias Alambique.Replication

  @short 150
  @long 600

  # Reductions the walk spends restoring a chain of `segments` files.
  defp walk_reductions(segments) do
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
    assert {:ok, %{txid: ^segments}} = Replication.restore(ctx, "cell", dest, :latest)
    {:reductions, after_} = Process.info(self(), :reductions)
    after_ - before
  end

  test "the walk of a chain 4 times longer costs about 4 times the reductions" do
    ratio = walk_reductions(@long) / walk_reductions(@short)

    # Linear is 4; planning that rescans the listing for every file is 16.
    assert ratio < 6, "reductions grew #{Float.round(ratio, 1)} times for 4 times the segments"
  end
end
