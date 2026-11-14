defmodule Alambique.WakeSegmentGetsTest do
  @moduledoc """
  A wake reads the files of the chain it restores once (design §6.3), with
  merged files made by the compactor in the chain and level-0 files left in
  the listing.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Replication.Keys

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  setup do
    setup_store()
  end

  test "a wake on a fresh node reads each file of the chain once", ctx do
    writer =
      start_instance(ctx,
        migrations: @schema,
        retention: false,
        compaction: [interval_ms: 100, max_age_ms: 200]
      )

    cell = unique_cell()

    for n <- 1..160 do
      {:ok, _} =
        Alambique.execute(writer, cell, "INSERT INTO t (n, v) VALUES (?, ?)", [
          n,
          String.duplicate("x", 200)
        ])

      if rem(n, 40) == 0, do: Process.sleep(500)
    end

    Process.sleep(1_000)
    stop_supervised(writer)

    prefix = Keys.files_prefix("cells-test", cell)
    {:ok, keys, nil} = AlambiqueTestkit.MemoryStore.list(ctx.store_config, prefix, nil, 100_000)
    {segments, _snapshots} = Keys.parse("cells-test", cell, keys)
    assert Enum.any?(segments, &(&1.level > 0)), "the compactor made no merged file"
    assert Enum.any?(segments, &(&1.level == 0))

    capture([[:alambique, :cell, :wake], [:alambique, :replication, :restore]])
    reader = start_instance(ctx, migrations: @schema, retention: false)
    assert {:ok, _} = Alambique.query(reader, cell, "SELECT count(*) FROM t", [])

    {_, restore} = await_event([:alambique, :replication, :restore], cell)
    {_, wake} = await_event([:alambique, :cell, :wake], cell)
    assert wake.source == :restore

    gets =
      for {{:get, kind}, n} <- wake.requests,
          kind in [:segment, :merged],
          reduce: 0,
          do: (sum -> sum + n)

    assert gets == restore.segments,
           "#{gets} reads of level files for a chain of #{restore.segments}: #{inspect(wake.requests)}"
  end
end
