defmodule Alambique.StatementSlotsPropertiesTest do
  @moduledoc """
  The slots of user statements (design §7): the statements running at once
  never exceed `max_concurrent`, a refused call is `{:error, :busy}`, and
  every slot comes back whether the call finished, timed out or its caller was
  killed mid-call.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [eventually: 1]

  alias Alambique.Sqlite

  @forever "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 400000000) SELECT count(*) FROM c"

  defp worker(max, timeout_ms) do
    spawn(fn ->
      {:ok, conn} = Sqlite.open(":memory:")
      Sqlite.run(conn, @forever, [], timeout_ms: timeout_ms, max_concurrent: max)
    end)
  end

  defp sample(pid, peak) do
    peak = max(peak, Sqlite.user_statements())

    receive do
      :stop -> send(pid, {:peak, peak})
    after
      2 -> sample(pid, peak)
    end
  end

  property "running user statements never exceed the limit and every slot comes back" do
    check all(
            max <- integer(1..6),
            callers <- integer(1..12),
            killed <- list_of(integer(0..11), max_length: 6),
            max_runs: 15
          ) do
      parent = self()
      sampler = spawn_link(fn -> sample(parent, 0) end)
      workers = for _ <- 1..callers, do: worker(max, 150)
      Process.sleep(40)
      for i <- Enum.uniq(killed), i < callers, do: Process.exit(Enum.at(workers, i), :kill)

      for pid <- workers do
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, _, _, _}, 5_000
      end

      send(sampler, :stop)
      assert_receive {:peak, peak}, 1_000
      assert peak <= max
      # A killed caller is gone before its call returns on the scheduler.
      eventually(fn -> Sqlite.user_statements() == 0 end)
    end
  end

  test "a statement refused as busy never touches the connection" do
    {:ok, conn} = Sqlite.open(":memory:")
    holders = for _ <- 1..2, do: worker(2, 2_000)
    eventually(fn -> Sqlite.user_statements() == 2 end)

    assert {:error, :busy} = Sqlite.run(conn, "SELECT 1", [], max_concurrent: 2)
    assert {:ok, %{rows: [[1]]}} = Sqlite.run(conn, "SELECT 1", [])
    assert {:ok, %{rows: [[1]]}} = Sqlite.run(conn, "SELECT 1", [], max_concurrent: 3)

    for pid <- holders, do: Process.exit(pid, :kill)
    eventually(fn -> Sqlite.user_statements() == 0 end)
  end
end
