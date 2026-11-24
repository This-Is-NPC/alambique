defmodule Alambique.ConnectionOwnerTest do
  @moduledoc "The opener and the user of a connection (`Alambique.Sqlite`)."
  use ExUnit.Case, async: true

  alias Alambique.Cell.Db
  alias Alambique.Sqlite

  @moduletag :tmp_dir

  # Runs `fun` in another process and returns what it returns.
  defp elsewhere(fun), do: Task.async(fun) |> Task.await(10_000)

  # Every call of the interface on `conn`, as a list of results.
  defp every_call(conn) do
    [
      Sqlite.run(conn, "SELECT 1", []),
      Sqlite.execute(conn, "SELECT 1"),
      Sqlite.script(conn, "CREATE TABLE s (a)"),
      Sqlite.changes(conn),
      Sqlite.transaction_status(conn),
      Sqlite.checkpoint(conn),
      Sqlite.begin_transaction(conn),
      Sqlite.finish_transaction(conn, 1, :rollback),
      Sqlite.wal_advanced?(conn),
      Sqlite.memory(conn),
      Sqlite.interrupt(conn),
      Sqlite.assign(conn, self()),
      Sqlite.close(conn)
    ]
  end

  for gated <- [false, true] do
    describe "a connection opened #{if gated, do: "gated", else: "plain"}" do
      setup %{tmp_dir: dir} do
        {:ok, conn} = Sqlite.open(Path.join(dir, "c.db"), gated: unquote(gated))
        on_exit(fn -> Sqlite.close(conn) end)
        %{conn: conn}
      end

      test "refuses every call from another process", %{conn: conn} do
        results = elsewhere(fn -> every_call(conn) end)
        assert Enum.all?(results, &(&1 == {:error, :not_owner})), inspect(results)
        assert {:ok, %{rows: [[1]]}} = Sqlite.run(conn, "SELECT 1", [])
      end

      test "is refused to a process that was never named, whatever the scheduler", %{conn: conn} do
        # Dirty IO calls (`run`, `execute`) and normal ones (`changes`) are
        # judged by the process that calls, from many processes at once.
        results =
          1..64
          |> Enum.map(fn _ ->
            Task.async(fn ->
              [Sqlite.run(conn, "SELECT 1", []), Sqlite.changes(conn), Sqlite.memory(conn)]
            end)
          end)
          |> Enum.flat_map(&Task.await(&1, 10_000))

        assert Enum.all?(results, &(&1 == {:error, :not_owner}))
        assert Enum.all?(1..200, fn _ -> match?({:ok, _}, Sqlite.run(conn, "SELECT 1", [])) end)
      end

      test "serves the user the opener names, and the opener keeps its own calls", %{conn: conn} do
        parent = self()

        user =
          spawn_link(fn ->
            receive do
              :go ->
                send(parent, {:ran, Sqlite.run(conn, "SELECT 2", []), Sqlite.close(conn)})
            end
          end)

        :ok = Sqlite.assign(conn, user)
        # The opener is no user now, and still reads the memory of the connection.
        assert {:error, :not_owner} = Sqlite.run(conn, "SELECT 1", [])
        assert {:ok, %{cache_used: _}} = Sqlite.memory(conn)
        assert :ok = Sqlite.interrupt(conn)

        send(user, :go)
        # The user runs and cannot close.
        assert_receive {:ran, {:ok, %{rows: [[2]]}}, {:error, :not_owner}}, 5_000
      end

      test "closes when the opener ends, though another process holds it", %{tmp_dir: dir} do
        path = Path.join(dir, "o.db")
        parent = self()

        opener =
          spawn(fn ->
            {:ok, conn} = Sqlite.open(path, gated: unquote(gated))
            holder = spawn(fn -> hold(parent) end)
            :ok = Sqlite.assign(conn, holder)
            send(parent, {:opened, conn, holder})
            Process.sleep(:infinity)
          end)

        assert_receive {:opened, conn, holder}, 5_000
        send(holder, {:run, conn})
        assert_receive {:ran, {:ok, %{rows: [[1]]}}}, 5_000

        Process.exit(opener, :kill)
        eventually_closed(holder, conn)
      end
    end
  end

  defp hold(parent) do
    receive do
      {:run, conn} ->
        send(parent, {:ran, Sqlite.run(conn, "SELECT 1", [])})
        hold(parent)

      {:again, conn} ->
        send(parent, {:answer, Sqlite.run(conn, "SELECT 1", [])})
        hold(parent)
    end
  end

  defp eventually_closed(holder, conn, left \\ 100) do
    send(holder, {:again, conn})

    receive do
      {:answer, {:error, :connection_closed}} ->
        :ok

      {:answer, other} when left == 0 ->
        flunk("still open: #{inspect(other)}")

      {:answer, _} ->
        Process.sleep(50)
        eventually_closed(holder, conn, left - 1)
    end
  end

  describe "the reader that is lent to the feed sender" do
    test "the sender reads while the cell closes it", %{tmp_dir: dir} do
      {:ok, cell} = Db.open(Path.join(dir, "cell.db"))
      :ok = Db.writable_script(cell, "CREATE TABLE t (a); INSERT INTO t VALUES (1)")
      {:ok, reader} = Db.open_reader(Path.join(dir, "cell.db"), 64)
      parent = self()

      sender =
        spawn_link(fn ->
          receive do
            {:read, ref} ->
              sql =
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 300000000) " <>
                  "SELECT count(*) FROM n"

              send(parent, {:read, Sqlite.run(ref, "SELECT a FROM t", [])})
              send(parent, :long)
              send(parent, {:long, Sqlite.run(ref, sql, [])})
              send(parent, {:no_close, Sqlite.close(ref), Sqlite.assign(ref, self())})
          end
        end)

      :ok = Sqlite.assign(reader, sender)
      send(sender, {:read, Sqlite.ref(reader)})
      assert_receive {:read, {:ok, %{rows: [[1]]}}}, 5_000
      assert_receive :long, 5_000
      Process.sleep(100)

      # The cell closes the reader under the running read.
      assert :ok = Db.close(reader)
      assert_receive {:long, {:error, reason}}, 5_000
      assert reason in ["interrupted", :connection_closed]
      assert_receive {:no_close, {:error, _}, {:error, _}}, 5_000
      Db.close(cell)
    end
  end
end
