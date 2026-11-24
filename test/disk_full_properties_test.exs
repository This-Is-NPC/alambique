defmodule Alambique.DiskFullPropertiesTest do
  @moduledoc """
  Whatever the sizes of the writes against a disk that fills up, each answer is
  `:ok` or `{:error, :disk_full}` and nothing else, and the cell holds exactly
  the writes it acknowledged (design §6.2). The store holds them too: a restore
  with no local file gives the same rows, after two more writes whose puts are
  held and the later of which lands first, and nobody is answered before the
  put of its segment landed.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AckGuard, Cell, Config, HeldWrites}
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  @insert "INSERT INTO t VALUES (?, zeroblob(?))"

  setup do
    setup_store()
  end

  property "a disk that fills up refuses writes with :disk_full and keeps the acknowledged ones",
           ctx do
    gate = start_supervised!(%{id: :gate, start: {GatedStore, :start_link, []}})
    inst = start_instance(%{ctx | store: GatedStore.wrap(gate, ctx.store)}, migrations: @schema)

    check all(
            pages <- integer(8..40),
            sizes <-
              list_of(member_of([1, 100, 5_000, 60_000, 400_000]), min_length: 1, max_length: 12),
            max_runs: 15
          ) do
      cell = unique_cell()
      guard = AckGuard.attach(ctx.store, "cells-test", &(&1 == cell))
      {:ok, _} = Alambique.execute(inst, cell, "PRAGMA max_page_count = #{pages}")

      acked =
        for {size, n} <- Enum.with_index(sizes, 1), reduce: [] do
          acked ->
            case Alambique.execute(inst, cell, @insert, [n, size]) do
              {:ok, _} -> [n | acked]
              {:error, :disk_full} -> acked
            end
        end

      # A write that fits is acknowledged.
      if hd(sizes) <= 100, do: assert(1 in acked, "the first write of #{hd(sizes)} bytes")

      acked = Enum.reverse(acked)
      assert rows(inst, cell) == acked

      pid = Cell.whereis(Config.fetch!(inst), cell)

      held =
        HeldWrites.run(gate, cell, pid, 8, [1_001, 1_002], fn n ->
          case Alambique.execute(inst, cell, @insert, [n, 1]) do
            {:ok, _} = ok -> ok
            {:error, :disk_full} = full -> full
          end
        end)

      acked = acked ++ for {n, {:ok, _}} <- held, do: n
      assert rows(inst, cell) == acked
      assert AckGuard.violations(guard) == []
      AckGuard.detach(guard)

      # The store alone gives the acknowledged rows back.
      assert stored(ctx, cell) == acked
      :ok = Alambique.hibernate(inst, cell)
      File.rm_rf!(Path.dirname(Alambique.Cell.Local.db_path(Config.fetch!(inst), cell)))
      assert rows(inst, cell) == acked
    end
  end

  defp stored(ctx, cell),
    do: ctx |> durable_rows(cell, "SELECT n FROM t ORDER BY n") |> List.flatten()

  defp rows(inst, cell) do
    {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end
end
