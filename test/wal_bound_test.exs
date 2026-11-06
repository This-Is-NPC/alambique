defmodule Alambique.WalBoundTest do
  @moduledoc """
  The WAL of an awake cell is checkpointed once it passes `wal.checkpoint_bytes`
  and the pipeline is empty (design §6, §7), so it stays bounded under steady
  writes; data and replication stay correct across the checkpoints.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Cell.Local
  alias AlambiqueTestkit.CrashPoints

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"}]
  @insert "INSERT INTO t (v) VALUES (zeroblob(1000))"
  @limit 262_144
  @wal if System.get_env("NO_WAL"), do: [], else: [wal: [checkpoint_bytes: @limit]]

  setup do
    on_exit(&CrashPoints.reset/0)
    capture([[:alambique, :cell, :checkpoint]])
    setup_store()
  end

  defp start(env, opts \\ []),
    do:
      start_instance(
        env,
        [migrations: @schema, compaction: false] ++ @wal ++ opts
      )

  defp wal_size(inst, cell) do
    case File.stat(Local.db_path(Alambique.Config.fetch!(inst), cell) <> "-wal") do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end

  # Largest WAL seen while `fun` runs.
  defp peak(inst, cell, fun) do
    parent = self()

    sampler = spawn_link(fn -> sample(inst, cell, 0, parent) end)

    fun.()
    send(sampler, :stop)

    receive do
      {:peak, peak} -> peak
    end
  end

  defp sample(inst, cell, peak, parent) do
    receive do
      :stop -> send(parent, {:peak, peak})
    after
      1 -> sample(inst, cell, max(peak, wal_size(inst, cell)), parent)
    end
  end

  defp count(inst, cell) do
    {:ok, %{rows: [[n]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    n
  end

  test "sequential writes keep the WAL bounded and the data intact", env do
    inst = start(env)
    cell = unique_cell()

    peak =
      peak(inst, cell, fn ->
        for _ <- 1..400, do: {:ok, _} = Alambique.execute(inst, cell, @insert)
      end)

    assert peak < 4 * @limit
    assert count(inst, cell) == 400
    assert {_, %{result: :ok}} = await_event([:alambique, :cell, :checkpoint], cell)
    assert durable_rows(env, cell, "SELECT count(*) FROM t") == [[400]]

    # The checkpoints left a file that wakes with every row.
    :ok = Alambique.hibernate(inst, cell)
    assert count(inst, cell) == 400
  end

  test "concurrent writers, which keep the pipeline full, still checkpoint", env do
    inst = start(env)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert)

    peak =
      peak(inst, cell, fn ->
        1..16
        |> Task.async_stream(
          fn _ -> for _ <- 1..60, do: {:ok, _} = Alambique.execute(inst, cell, @insert) end,
          timeout: 30_000
        )
        |> Stream.run()
      end)

    assert peak < 8 * @limit
    assert count(inst, cell) == 961
    assert durable_rows(env, cell, "SELECT count(*) FROM t") == [[961]]
  end

  test "a cell killed right after a checkpoint wakes with every acknowledged row", env do
    inst = start(env)
    cell = unique_cell()
    parent = self()

    CrashPoints.arm(:cell_after_checkpoint, fn _, _ ->
      send(parent, :checkpointed)
      Process.exit(self(), :kill)
    end)

    acked =
      Enum.count(1..400, fn _ ->
        match?({:ok, _}, Alambique.execute(inst, cell, @insert))
      end)

    assert_received :checkpointed
    assert acked >= 1
    assert [[n]] = durable_rows(env, cell, "SELECT count(*) FROM t")
    assert n >= acked

    CrashPoints.reset()
    assert count(inst, cell) == n
    {:ok, _} = Alambique.execute(inst, cell, @insert)
    assert count(inst, cell) == n + 1
  end

  test "checkpoint_bytes is validated" do
    assert_raise ArgumentError, ~r/checkpoint_bytes/, fn ->
      Alambique.Config.build(
        store: {AlambiqueTestkit.MemoryStore, nil},
        data_dir: "/tmp/x",
        wal: [checkpoint_bytes: 0]
      )
    end
  end
end
