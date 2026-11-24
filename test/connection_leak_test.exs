defmodule Alambique.ConnectionLeakTest do
  @moduledoc """
  What code that keeps the connection of a write can do with it later, from
  another process, from a handler that runs in the process of the cell, and
  after the opener ended (design §15).
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Cell.Db
  alias Alambique.{Cell, Config, Fault, Feed, Sqlite, Tx}

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

  defp rows(c) do
    {:ok, %{rows: rows}} = Alambique.query(c.inst, c.cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end

  defp store(c), do: c.env |> durable_rows(c.cell, "SELECT n FROM t ORDER BY n") |> List.flatten()

  defp feed_keys(c) do
    ctx = Feed.ctx(store: c.env.store, prefix: "cells-test", settle_ms: 0)
    {:ok, records, _} = Feed.read(ctx, nil, 10_000)
    for r <- records, r.cell == c.cell, r.table == "t", do: r.key["n"]
  end

  defp leak(c), do: elem(Alambique.transaction(c.inst, c.cell, fn tx -> tx.conn end), 1)

  for mode <- [:stop, :replicate] do
    describe "a handle kept by a write (#{mode})" do
      test "is useless to a handler that runs in the cell", env do
        c = boot(env, unquote(mode))
        conn = leak(c)
        cell_pid = pid(c)
        test = self()
        once = :atomics.new(1, [])

        :telemetry.attach(
          "leak-#{System.unique_integer([:positive])}",
          [:alambique, :cell, :commit],
          fn _event, _measurements, _meta, _config ->
            if self() == cell_pid and :atomics.add_get(once, 1, 1) == 1 do
              guesses = [0, 1, :rand.uniform(0xFFFFFFFFFFFFFFFF)]
              forged = for key <- guesses, do: %Sqlite.Handle{ref: conn, key: key}

              attempts =
                [
                  Sqlite.checkpoint(conn),
                  Sqlite.begin_transaction(conn),
                  Sqlite.close(conn),
                  Sqlite.assign(conn, self()),
                  Sqlite.execute(conn, "PRAGMA query_only = OFF; INSERT INTO t VALUES (60)"),
                  Sqlite.execute(conn, "INSERT INTO t VALUES (60)", write: true)
                ] ++
                  Enum.flat_map(forged, fn handle ->
                    [
                      Sqlite.execute(handle, "INSERT INTO t VALUES (61)", write: true),
                      Sqlite.run(handle, "INSERT INTO t VALUES (61)", [], write: true),
                      Sqlite.checkpoint(handle),
                      Sqlite.begin_transaction(handle),
                      Sqlite.assign(handle, self()),
                      Sqlite.close(handle)
                    ]
                  end)

              send(test, {:attempts, attempts})
            end
          end,
          nil
        )

        assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
        assert_receive {:attempts, attempts}, 10_000
        assert Enum.all?(attempts, &(&1 == {:error, :not_authorized})), inspect(attempts)

        assert rows(c) == [0, 1, 2]
        assert {:ok, _} = Alambique.position(c.inst, c.cell)
        assert pid(c) == cell_pid, "the cell stopped"
        assert store(c) == [0, 1, 2]
        assert feed_keys(c) |> Enum.sort() == [0, 1, 2]

        assert Alambique.hibernate(c.inst, c.cell) == :ok
        assert rows(c) == [0, 1, 2]
      end
    end
  end

  describe "an open that fails after the connection opened" do
    @describetag :tmp_dir

    test "closes the connection and lets the next open through", %{tmp_dir: dir} do
      path = Path.join(dir, "cell.db")
      open = Alambique.NativeStats.read().connections_open

      assert {:error, _} = Db.open(path, cache_kib: "x")
      assert Alambique.NativeStats.read().connections_open == open

      assert {:ok, conn} = Db.open(path)
      assert :ok = Db.close(conn)
    end

    test "closes the connection of a reader", %{tmp_dir: dir} do
      path = Path.join(dir, "cell.db")
      {:ok, conn} = Db.open(path)
      :ok = Db.close(conn)
      open = Alambique.NativeStats.read().connections_open

      assert {:error, _} = Db.open_reader(path, "x")
      assert Alambique.NativeStats.read().connections_open == open
    end
  end

  describe "an opener that ends" do
    test "while a write is open closes the connection, and the next wake writes", env do
      c = boot(env, :stop)
      test = self()
      old = pid(c)

      writer =
        spawn(fn ->
          Alambique.transaction(c.inst, c.cell, fn tx ->
            {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (9)")
            send(test, {:leaked, tx.conn})

            receive do
              :never -> :ok
            end
          end)
        end)

      assert_receive {:leaked, conn}, 10_000
      open = Alambique.NativeStats.read().connections_open
      Process.exit(old, :kill)

      # The test still holds the handle, and the connection is closed anyway.
      assert eventually(fn -> Alambique.NativeStats.read().connections_open == open - 1 end)

      assert {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (3)")
      refute pid(c) == old
      assert rows(c) == [0, 1, 3]
      assert store(c) == [0, 1, 3]
      assert {:error, _} = Sqlite.run(conn, "SELECT 1", [])
      assert {:error, _} = Sqlite.close(conn)
      Process.exit(writer, :kill)
    end
  end

  describe "the secret of the connection" do
    test "is in no printed state, no telemetry, no fault context and no dictionary", env do
      c = boot(env, :stop)
      test = self()
      id = "secret-#{System.unique_integer([:positive])}"
      events = [[:cell, :wake], [:cell, :commit], [:cell, :checkpoint], [:cell, :hibernate]]

      :telemetry.attach_many(
        id,
        for(e <- events, do: [:alambique | e]),
        fn event, measurements, meta, _ -> send(test, {:event, event, measurements, meta}) end,
        nil
      )

      Fault.install(fn point, context ->
        send(test, {:fault, point, context})
        :ok
      end)

      {:ok, _} = Alambique.execute(c.inst, c.cell, "INSERT INTO t VALUES (2)")
      state = :sys.get_state(pid(c))
      key = state.conn.key
      assert is_integer(key) and key != 0
      Fault.uninstall()
      :telemetry.detach(id)

      seen =
        for _ <- 1..50, reduce: [] do
          acc ->
            receive do
              message -> [message | acc]
            after
              0 -> acc
            end
        end

      assert seen != []

      printed =
        inspect([state, seen, :sys.get_status(pid(c)), Process.info(pid(c))],
          limit: :infinity,
          printable_limit: :infinity
        )

      for text <- [
            Integer.to_string(key),
            Integer.to_string(key, 16),
            String.downcase(Integer.to_string(key, 16))
          ] do
        refute String.contains?(printed, text), "the secret is printed"
      end

      refute inspect(state.conn) =~ "key"
    end
  end
end
