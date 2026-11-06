defmodule Alambique.CellNifCallsTest do
  @moduledoc "A write costs few SQLite calls on the dirty IO pool (design §6)."
  # The native counters are node-wide: run alone, and compare two reads.
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.NativeStats

  setup do
    setup_store()
  end

  test "one write is at most six SQLite calls", env do
    inst =
      start_instance(env,
        migrations: [{1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"}],
        compaction: false
      )

    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (v) VALUES ('warm')")
    Process.sleep(200)

    before = NativeStats.read().sqlite_calls
    {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t (v) VALUES ('x')")
    Process.sleep(200)
    calls = NativeStats.read().sqlite_calls - before

    # Begin, the feed cleanup, the statement, the status check, commit and
    # the stamp of the feed rows: 10 calls before they shared pragma calls.
    assert calls <= 6, "#{calls} SQLite calls for one write"
  end
end
