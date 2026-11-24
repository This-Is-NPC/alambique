defmodule Alambique.OrphanFramesTest do
  @moduledoc """
  The backstop of design §15: committed frames that no write carries are
  never reported as durable. `sql: [orphan_frames: :stop]` stops the cell, so
  the next wake restores what the store holds; `:replicate` sends the frames
  as a batch. An orphan is made on the cell's own connection, as the defect of
  a transaction that ended itself did.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Feed, Sqlite, Tx}
  alias Alambique.Cell.Db
  alias Alambique.Replication.Replicator

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    setup_store()
  end

  defp boot(env, mode) do
    inst = start_instance(env, migrations: @schema, sql: [orphan_frames: mode], compaction: false)
    cell = unique_cell()
    for n <- [0, 1], do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (#{n})")
    %{inst: inst, cell: cell, env: env}
  end

  defp pid(%{inst: inst, cell: cell}), do: Cell.whereis(Config.fetch!(inst), cell)

  # Rows committed on the connection of the cell, outside any write.
  defp orphan(c, n) do
    %{conn: conn} = :sys.get_state(pid(c))
    :ok = Db.writable_script(conn, "INSERT INTO t VALUES (#{n})")
    assert unreplicated?(c)
  end

  defp unreplicated?(c), do: Replicator.unreplicated?(:sys.get_state(pid(c)).repl)

  defp rows(c) do
    {:ok, %{rows: rows}} = Alambique.query(c.inst, c.cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end

  defp store(c), do: c.env |> durable_rows(c.cell, "SELECT n FROM t ORDER BY n") |> List.flatten()

  defp asleep?(c), do: eventually(fn -> Alambique.owner(c.inst, c.cell) == :none end)

  # The change feed holds the row once, whatever the mode.
  defp feed_keys(c, last) do
    ctx = Feed.ctx(store: c.env.store, prefix: "cells-test", settle_ms: 0)

    eventually(fn ->
      {:ok, records, _} = Feed.read(ctx, nil, 1_000)
      keys = for r <- records, r.cell == c.cell, r.table == "t", do: r.key["n"]
      if last in keys, do: keys
    end)
  end

  for mode <- [:stop, :replicate] do
    describe "orphan_frames: #{inspect(mode)}" do
      setup env, do: %{c: boot(env, unquote(mode))}

      test "position never cuts above frames no write carries", %{c: c} do
        orphan(c, 5)
        result = Alambique.position(c.inst, c.cell)

        case unquote(mode) do
          :stop ->
            assert result == {:error, :orphan_frames}
            assert asleep?(c)
            assert store(c) == [0, 1]
            assert rows(c) == [0, 1]

          :replicate ->
            assert {:ok, %{txid: txid}} = result
            # The cut is a durable state: the store holds the row at it.
            assert store(c) == [0, 1, 5]
            assert rows(c) == [0, 1, 5]
            assert {:ok, %{txid: ^txid}} = Alambique.position(c.inst, c.cell)
            assert feed_keys(c, 5) |> Enum.count(&(&1 == 5)) == 1
        end

        refute unreplicated?(c)
      end

      test "hibernate never answers :ok with frames no write carries", %{c: c} do
        orphan(c, 5)
        result = Alambique.hibernate(c.inst, c.cell)

        case unquote(mode) do
          :stop -> assert result == {:error, :orphan_frames}
          :replicate -> assert result == :ok
        end

        assert asleep?(c)
        # What the store holds is what the next wake serves.
        stored = store(c)
        assert rows(c) == stored
        assert stored == if(unquote(mode) == :stop, do: [0, 1], else: [0, 1, 5])
      end

      test "a write that failed after it committed is not lost or reported durable", %{c: c} do
        # The function ends the transaction behind the library's back. The
        # connection refuses that while the transaction is open, so the
        # function lifts the library's flag first, as a defect would.
        result =
          Alambique.transaction(c.inst, c.cell, fn tx ->
            {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (6)")
            :ok = Sqlite.expect_transaction(tx.conn, false)
            :ok = Sqlite.execute(tx.conn, "COMMIT")
          end)

        case unquote(mode) do
          :stop ->
            assert result == {:error, :transaction_ended}
            assert asleep?(c)
            assert store(c) == [0, 1]

          :replicate ->
            # The caller waited for the batch; its rows are durable.
            assert result == {:error, {:committed, :transaction_ended}}
            assert store(c) == [0, 1, 6]
        end

        assert rows(c) == store(c)
        refute unreplicated?(c)
      end

      test "the pragma after a commit fails: the cell stops and every answer agrees", %{c: c} do
        Alambique.Fault.install(fn
          :db_after_commit, _ -> {:error, :injected}
          _, _ -> :ok
        end)

        on_exit(&Alambique.Fault.uninstall/0)

        result =
          Alambique.transaction(c.inst, c.cell, fn tx ->
            {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (7)")
            :done
          end)

        Alambique.Fault.uninstall()
        assert asleep?(c)

        # What the caller was told is what the store holds and the next wake serves.
        case unquote(mode) do
          :stop ->
            assert result == {:error, {:query_only, :injected}}
            assert store(c) == [0, 1]

          :replicate ->
            assert result == {:error, {:committed, {:query_only, :injected}}}
            assert store(c) == [0, 1, 7]
            assert feed_keys(c, 7) |> Enum.count(&(&1 == 7)) == 1
        end

        assert rows(c) == store(c)
        refute unreplicated?(c)
      end

      test "a freeze puts the cell to sleep with what the store holds", %{c: c} do
        orphan(c, 5)
        _ = Alambique.freeze(c.inst, c.cell)
        assert asleep?(c)
        assert :ok = Alambique.unfreeze(c.inst, c.cell)

        stored = store(c)
        assert rows(c) == stored
        assert stored == if(unquote(mode) == :stop, do: [0, 1], else: [0, 1, 5])
      end

      test "a rewind does not bring an orphan back", %{c: c} do
        {:ok, %{txid: at}} = Alambique.position(c.inst, c.cell)
        {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
        orphan(c, 5)

        assert {:ok, %{to: ^at}} = Alambique.rewind(c.inst, c.cell, {:txid, at})
        assert rows(c) == [0, 1]
        assert store(c) == [0, 1]
      end
    end
  end

  # The last look before a release, past the drain: frames made while the
  # cell hands its feed rows over are not released as `:ok`, whatever the option.
  for mode <- [:stop, :replicate] do
    test "hibernate refuses frames made after the drain (#{mode})", env do
      c = boot(env, unquote(mode))
      %{conn: conn} = :sys.get_state(pid(c))
      test = self()

      Alambique.Fault.install(fn
        :feed_before_stop_flush, _ ->
          :ok = Db.writable_script(conn, "INSERT INTO t VALUES (5)")
          send(test, :injected)

        _, _ ->
          :ok
      end)

      on_exit(&Alambique.Fault.uninstall/0)
      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")

      assert Alambique.hibernate(c.inst, c.cell) == {:error, :orphan_frames}
      assert_received :injected
      assert asleep?(c)
      assert rows(c) == store(c)
      refute 5 in store(c)
    end
  end

  test "a write that is in flight keeps frames from counting as orphans", env do
    c = boot(env, :stop)
    parent = self()

    task =
      Task.async(fn ->
        Alambique.transaction(c.inst, c.cell, fn tx ->
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (9)")
          send(parent, :written)
          :ok
        end)
      end)

    assert {:ok, :ok} = Task.await(task)
    assert {:ok, _} = Alambique.position(c.inst, c.cell)
    assert rows(c) == [0, 1, 9]
  end
end
