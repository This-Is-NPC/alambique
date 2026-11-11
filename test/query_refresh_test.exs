defmodule Alambique.QueryRefreshTest do
  @moduledoc "A query replica refresh costs the change, not the database (design §14)."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.Query
  alias Alambique.Query.Cache
  alias Alambique.Replication.Native

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @count "SELECT count(*) FROM notes"

  setup do
    setup_store()
  end

  defp insert(inst, cell, n),
    do:
      for(
        i <- 1..n,
        do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO notes (body) VALUES (?)", [i])
      )

  # Calls to `Native.file_state/2`, the pass that reads a whole file, made
  # anywhere while `fun` runs.
  defp reads_of_whole_files(fun) do
    :erlang.trace_pattern({Native, :file_state, 2}, true, [:global])
    :erlang.trace(:all, true, [:call, {:tracer, self()}])

    try do
      fun.()
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Native, :file_state, 2}, false, [:global])
    end

    drain_traces(0)
  end

  defp drain_traces(n) do
    receive do
      {:trace, _pid, :call, {Native, :file_state, _}} -> drain_traces(n + 1)
    after
      0 -> n
    end
  end

  test "a refresh does not read the replica to find its checksum", ctx do
    owner = start_instance(ctx, migrations: @schema)
    reader = start_instance(ctx, migrations: @schema)
    insert(owner, "a", 5)
    assert [%{values: [5]}] = Query.all(reader, ["a"], @count).rows
    insert(owner, "a", 2)

    reads =
      reads_of_whole_files(fn ->
        assert [%{values: [7]}] = Query.all(reader, ["a"], @count).rows
      end)

    assert reads == 0
  end

  describe "the copy" do
    test "falls back to a plain copy when reflinks are off", ctx do
      capture([[:alambique, :query, :replica]])
      owner = start_instance(ctx, migrations: @schema)
      reader = start_instance(ctx, migrations: @schema, query: [reflink: false])
      insert(owner, "a", 5)
      Query.all(reader, ["a"], @count)
      insert(owner, "a", 2)

      assert [%{values: [7]}] = Query.all(reader, ["a"], @count).rows
      assert %{source: :refresh, copy: :copy} = await_replica(:refresh)
    end

    test "uses the filesystem's clone when it has one, and the result is the same", ctx do
      capture([[:alambique, :query, :replica]])
      owner = start_instance(ctx, migrations: @schema)
      reader = start_instance(ctx, migrations: @schema, query: [reflink: true])
      insert(owner, "a", 5)
      Query.all(reader, ["a"], @count)
      insert(owner, "a", 2)

      assert [%{values: [7]}] = Query.all(reader, ["a"], @count).rows
      assert %{source: :refresh, copy: copy} = await_replica(:refresh)
      assert copy in [:reflink, :copy]
    end

    test "the clone NIF copies a file or declines" do
      dir = Path.join(File.cwd!(), "_build")
      src = Path.join(dir, "reflink-src-#{System.unique_integer([:positive])}")
      dst = src <> ".dst"
      File.write!(src, :crypto.strong_rand_bytes(100_000))
      File.write!(dst, "")

      try do
        case Native.reflink(src, dst) do
          {:ok, true} -> assert File.read!(dst) == File.read!(src)
          {:ok, false} -> assert File.read!(dst) == ""
        end

        assert {:error, _} = Native.reflink(src <> ".none", dst)
      after
        File.rm(src)
        File.rm(dst)
      end
    end

    defp await_replica(source) do
      {_, meta} =
        await_event([:alambique, :query, :replica], "a", 5_000, &(&1.source == source))

      meta
    end
  end

  # Random chains of writes between refreshes: every cached file equals a full
  # restore at its TXID, and its recorded checksum is the checksum of its pages.
  property "a refreshed replica equals a full restore at the same TXID", base do
    check all(
            steps <- list_of(list_of(integer(0..3), max_length: 4), min_length: 2, max_length: 6),
            max_runs: 25
          ) do
      {:ok, store} = AlambiqueTestkit.MemoryStore.start_link()
      config = AlambiqueTestkit.MemoryStore.config(store)
      dir = Path.join(base.dir, "run-#{System.unique_integer([:positive])}")
      run = %{store: {AlambiqueTestkit.MemoryStore, config}, store_config: config, dir: dir}
      owner = start_instance(run, migrations: @schema)
      reader = start_instance(run, migrations: @schema)

      try do
        {:ok, _} = Alambique.execute(owner, "p", "INSERT INTO notes (body) VALUES ('first')")

        for writes <- steps do
          for w <- writes, do: write(owner, w)
          assert %{errors: []} = Query.all(reader, ["p"], @count)
          assert_replica_matches_restore(run, reader)
        end
      after
        stop_supervised(owner)
        stop_supervised(reader)
        GenServer.stop(store)
      end
    end
  end

  defp write(owner, 0),
    do: {:ok, _} = Alambique.execute(owner, "p", "INSERT INTO notes (body) VALUES ('x')")

  defp write(owner, 1),
    do: {:ok, _} = Alambique.execute(owner, "p", "UPDATE notes SET body = body || 'y'")

  defp write(owner, 2),
    do: {:ok, _} = Alambique.execute(owner, "p", "DELETE FROM notes WHERE id % 2 = 0")

  defp write(owner, 3),
    do:
      {:ok, _} =
        Alambique.execute(owner, "p", "INSERT INTO notes (body) VALUES (hex(randomblob(6000)))")

  defp assert_replica_matches_restore(run, reader) do
    dir = Cache.dir(Alambique.Config.fetch!(reader))

    [key] =
      for name <- File.ls!(dir),
          String.ends_with?(name, ".r"),
          do: String.trim_trailing(name, ".r")

    for %{txid: txid, path: path} = entry <- Cache.entries(dir, key) do
      repl =
        Alambique.Replication.ctx(
          store: run.store,
          prefix: "cells-test",
          node: "verifier",
          timeout_ms: 5_000
        )

      restored = Path.join(run.dir, "verify-#{System.unique_integer([:positive])}.db")
      File.mkdir_p!(run.dir)
      {:ok, %{txid: ^txid}} = Alambique.Replication.restore(repl, "p", restored, {:txid, txid})
      {:ok, page_size} = Cache.page_size(path)
      {:ok, sums, checksum, _} = Native.file_state(path, page_size)
      assert {:ok, ^sums, ^checksum, _} = Native.file_state(restored, page_size)
      assert entry.checksum == checksum
      File.rm(restored)
    end
  end
end
