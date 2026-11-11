defmodule Alambique.Load.CellsTest do
  use ExUnit.Case, async: true

  alias Alambique.Load.Measures.Cells

  test "a repeat of a cells step puts to sleep exactly the cells the disturbed attempt woke" do
    assert Cells.added_cells(500, 1_000) |> length() == 500
    assert hd(Cells.added_cells(500, 1_000)) == "a-501"
    assert List.last(Cells.added_cells(500, 1_000)) == "a-1000"
    assert Cells.added_cells(1_000, 1_000) == []
    assert Cells.added_cells(1_000, 400) == []
  end
end
