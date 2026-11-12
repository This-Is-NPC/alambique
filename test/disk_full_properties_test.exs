defmodule Alambique.DiskFullPropertiesTest do
  @moduledoc """
  Whatever the sizes of the writes against a disk that fills up, each answer is
  `:ok` or `{:error, :disk_full}` and nothing else, and the cell holds exactly
  the writes it acknowledged, also after it hibernates and wakes (design §6.4).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]

  setup do
    setup_store()
  end

  property "a disk that fills up refuses writes with :disk_full and keeps the acknowledged ones",
           ctx do
    inst = start_instance(ctx, migrations: @schema)

    check all(
            pages <- integer(8..40),
            sizes <-
              list_of(member_of([1, 100, 5_000, 60_000, 400_000]), min_length: 1, max_length: 12),
            max_runs: 15
          ) do
      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, "PRAGMA max_page_count = #{pages}")

      acked =
        for {size, n} <- Enum.with_index(sizes, 1), reduce: [] do
          acked ->
            case Alambique.execute(inst, cell, "INSERT INTO t VALUES (?, zeroblob(?))", [n, size]) do
              {:ok, _} -> [n | acked]
              {:error, :disk_full} -> acked
            end
        end

      acked = Enum.reverse(acked)
      assert rows(inst, cell) == acked

      :ok = Alambique.hibernate(inst, cell)
      assert rows(inst, cell) == acked
    end
  end

  defp rows(inst, cell) do
    {:ok, %{rows: rows}} = Alambique.query(inst, cell, "SELECT n FROM t ORDER BY n")
    List.flatten(rows)
  end
end
