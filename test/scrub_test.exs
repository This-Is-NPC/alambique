defmodule Alambique.ScrubTest do
  @moduledoc "The scrub restores sampled cells and checks them (design §6.1)."
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Alambique.CellHelpers

  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT); CREATE INDEX i ON t (v)"}]
  @all [cells: 200, page_size: 20, concurrency: 4, rate: 1_000]

  setup do
    capture([[:alambique, :scrub, :cell], [:alambique, :scrub, :round]])
    ctx = setup_store()
    {:ok, Map.put(ctx, :inst, start_instance(ctx, migrations: @schema))}
  end

  # A cell with five commits, asleep.
  defp cell_with_data(inst) do
    cell = unique_cell()

    for n <- 1..5 do
      assert {:ok, _} =
               Alambique.execute(inst, cell, "INSERT INTO t VALUES (?, ?)", [
                 n,
                 String.duplicate("x", 60) <> "#{n}"
               ])
    end

    :ok = Alambique.hibernate(inst, cell)
    cell
  end

  defp object_keys(ctx, cell) do
    {:ok, keys, nil} =
      MemoryStore.list(ctx.store_config, "cells-test/cells/#{cell}/0000/", nil, 1_000)

    keys
  end

  defp rewrite(ctx, key, fun) do
    {:ok, body, _etag, meta} = MemoryStore.get(ctx.store_config, key)
    {:ok, _} = MemoryStore.put(ctx.store_config, key, fun.(body), :none, meta)
  end

  defp scrub_dirs(inst) do
    dir = Path.join(Alambique.Config.fetch!(inst).data_dir, "work")
    Enum.filter(File.ls(dir) |> elem(1), &String.starts_with?(&1, "scrub-"))
  end

  @doc false
  def span(_event, %{duration: d}, _meta, test) do
    stop = System.monotonic_time()
    send(test, {:span, stop - d, stop})
  end

  defp cell_events do
    receive do
      {:telemetry, [:alambique, :scrub, :cell], m, meta} -> [{m, meta} | cell_events()]
    after
      0 -> []
    end
  end

  describe "a round" do
    test "finds healthy cells sound and leaves no files", %{inst: inst} do
      cells = for _ <- 1..4, do: cell_with_data(inst)

      assert {:ok, %{cells: 4, ok: 4, empty: 0, failed: 0, failures: []}} =
               Alambique.scrub(inst, @all)

      events = cell_events()
      assert Enum.sort(for {_, %{cell: c, result: :ok}} <- events, do: c) == Enum.sort(cells)
      assert Enum.all?(events, fn {m, _} -> m.txid >= 5 end)

      assert_received {:telemetry, [:alambique, :scrub, :round],
                       %{cells: 4, ok: 4, empty: 0, failed: 0}, %{result: :ok}}

      assert scrub_dirs(inst) == []
    end

    test "reports a cell without data as empty", %{inst: inst} = ctx do
      cell = cell_with_data(inst)

      for key <- object_keys(ctx, cell),
          do: MemoryStore.delete(ctx.store_config, key)

      assert {:ok, %{cells: 1, ok: 0, empty: 1, failed: 0}} = Alambique.scrub(inst, @all)
    end

    test "samples nothing from an instance without cells", %{inst: inst} do
      assert {:ok, %{cells: 0, failed: 0}} = Alambique.scrub(inst)
    end

    test "covers different cells in different rounds", %{inst: inst} do
      for _ <- 1..20, do: cell_with_data(inst)

      seen =
        for _ <- 1..15, reduce: MapSet.new() do
          seen ->
            assert {:ok, %{cells: n}} = Alambique.scrub(inst, cells: 3, page_size: 3, rate: 1_000)
            assert n in 1..3
            for {_, %{cell: c}} <- cell_events(), reduce: seen, do: (seen -> MapSet.put(seen, c))
        end

      assert MapSet.size(seen) >= 8
    end

    test "keeps to the prefix", %{inst: inst} do
      keep = "keep-#{System.unique_integer([:positive])}"

      for suffix <- 1..3 do
        cell = "#{keep}-#{suffix}"
        assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1, 'a')")
        :ok = Alambique.hibernate(inst, cell)
      end

      cell_with_data(inst)
      assert {:ok, %{cells: 3, ok: 3}} = Alambique.scrub(inst, [prefix: keep] ++ @all)
      assert Enum.all?(cell_events(), fn {_, %{cell: c}} -> String.starts_with?(c, keep) end)
    end
  end

  describe "damage" do
    setup %{inst: inst} = ctx do
      healthy = for _ <- 1..3, do: cell_with_data(inst)
      {:ok, Map.merge(ctx, %{healthy: healthy, bad: cell_with_data(inst)})}
    end

    defp only_failure(inst, ctx, opts \\ []) do
      log = capture_log(fn -> send(self(), {:result, Alambique.scrub(inst, @all ++ opts)}) end)
      assert_received {:result, {:ok, summary}}
      assert summary.cells == 4
      assert summary.ok == 3
      assert [%{cell: cell, reason: reason}] = summary.failures
      assert cell == ctx.bad
      assert log =~ "#{ctx.bad}"
      assert scrub_dirs(inst) == []
      assert [{_, %{result: :error, cell: ^cell}}] = Enum.filter(cell_events(), &error?/1)
      reason
    end

    defp error?({_, %{result: :error}}), do: true
    defp error?(_), do: false

    test "flipped bytes in a segment", %{inst: inst} = ctx do
      [_, middle | _] = object_keys(ctx, ctx.bad)

      rewrite(ctx, middle, fn body ->
        at = div(byte_size(body), 2)
        <<head::binary-size(^at), byte, tail::binary>> = body
        <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>
      end)

      assert {:restore, _} = only_failure(inst, ctx)
    end

    test "a truncated segment", %{inst: inst} = ctx do
      [_, _, third | _] = object_keys(ctx, ctx.bad)
      rewrite(ctx, third, &binary_part(&1, 0, div(byte_size(&1), 2)))
      assert {:restore, _} = only_failure(inst, ctx)
    end

    # More than the tail window of files follows the hole (design §6, R3).
    test "a segment missing from the middle of the chain", %{inst: inst} = ctx do
      for n <- 6..22,
          do: {:ok, _} = Alambique.execute(inst, ctx.bad, "INSERT INTO t VALUES (?, 'x')", [n])

      :ok = Alambique.hibernate(inst, ctx.bad)
      [_, _, third | _] = object_keys(ctx, ctx.bad)
      MemoryStore.delete(ctx.store_config, third)
      assert {:restore, _} = only_failure(inst, ctx)
    end

    # The connection is defensive, so the schema cannot be edited to leave an
    # orphan page. An index over `LIKE` is written under another setting of
    # the session, which leaves it disagreeing with its table. `quick_check`
    # does not compare index content, so it is `integrity_check` that finds it.
    test "a database that restores but is damaged, by integrity_check", %{inst: inst} = ctx do
      assert {:ok, _} = Alambique.execute(inst, ctx.bad, "CREATE INDEX l ON t (v LIKE 'x%')")
      assert {:ok, _} = Alambique.execute(inst, ctx.bad, "PRAGMA case_sensitive_like = ON")
      assert {:ok, _} = Alambique.execute(inst, ctx.bad, "INSERT INTO t VALUES (9, 'Xray')")
      assert {:ok, _} = Alambique.execute(inst, ctx.bad, "PRAGMA case_sensitive_like = OFF")
      :ok = Alambique.hibernate(inst, ctx.bad)

      assert {:integrity_check, [finding]} = only_failure(inst, ctx, check: :integrity_check)
      assert finding =~ "missing from index l"
    end

    test "quick_check runs and passes healthy cells", %{inst: inst} do
      assert {:ok, %{cells: 4, ok: 4}} = Alambique.scrub(inst, @all ++ [check: :quick_check])
    end

    test "a damaged cell is reported, not frozen", %{inst: inst} = ctx do
      [first | _] = object_keys(ctx, ctx.bad)
      rewrite(ctx, first, &binary_part(&1, 0, 10))
      capture_log(fn -> Alambique.scrub(inst, @all) end)

      assert {:ok, false} = Alambique.frozen?(inst, ctx.bad)
    end
  end

  describe "verification" do
    test "compares the checksum of the restored file with the chain's", %{inst: inst} do
      cell = cell_with_data(inst)
      repl = repl_ctx(inst, cell)
      dir = Alambique.Config.fetch!(inst).data_dir
      path = Path.join(dir, "verify-#{System.unique_integer([:positive])}.db")
      on_exit(fn -> File.rm(path) end)
      {:ok, result} = Alambique.Replication.restore(repl, cell, path, :latest)

      assert {:ok, txid} = Alambique.Scrub.verify(path, result, :integrity_check)
      assert txid == result.txid

      assert {:error, {:checksum_mismatch, _, _}} =
               Alambique.Scrub.verify(
                 path,
                 %{result | checksum: result.checksum + 1},
                 :quick_check
               )
    end
  end

  describe "pace" do
    test "honours concurrency and rate", %{inst: inst} do
      for _ <- 1..6, do: cell_with_data(inst)
      opts = [cells: 200, page_size: 20, concurrency: 2, rate: 5]

      {us, {:ok, %{cells: 6}}} = :timer.tc(fn -> Alambique.scrub(inst, opts) end)

      # Three batches of two cells, each held to 400 ms.
      assert us >= 1_150_000
    end

    test "never runs more cells at once than asked", %{inst: inst} do
      for _ <- 1..6, do: cell_with_data(inst)
      test = self()
      id = {__MODULE__, make_ref()}

      :telemetry.attach(id, [:alambique, :replication, :restore], &__MODULE__.span/4, test)

      on_exit(fn -> :telemetry.detach(id) end)

      assert {:ok, %{cells: 6}} =
               Alambique.scrub(inst, cells: 200, page_size: 20, concurrency: 1, rate: 1_000)

      spans =
        for _ <- 1..6,
            do:
              (receive do
                 {:span, a, b} -> {a, b}
               end)

      sorted = Enum.sort(spans)

      assert sorted
             |> Enum.chunk_every(2, 1, :discard)
             |> Enum.all?(fn [{_, e1}, {s2, _}] -> e1 <= s2 end)
    end
  end

  describe "in the background" do
    test "runs on its own and the options are validated", ctx do
      inst =
        start_instance(ctx, migrations: @schema, scrub: [interval_ms: 50, cells: 5, rate: 100])

      cell_with_data(inst)

      assert_receive {:telemetry, [:alambique, :scrub, :round], %{cells: n}, %{instance: ^inst}},
                     5_000

      assert n >= 0

      base = [name: :cfg, store: ctx.store, data_dir: "/tmp/x"]
      assert %{scrub: nil} = Alambique.Config.build(base)

      assert %{scrub: %{cells: 10, check: :integrity_check, concurrency: 1}} =
               Alambique.Config.build(base ++ [scrub: []])

      for bad <- [[check: :full], [cells: 0], [rate: -1], [bogus: 1]] do
        assert_raise ArgumentError, fn -> Alambique.Config.build(base ++ [scrub: bad]) end
      end
    end
  end
end
