defmodule Alambique.FeedCloseReaderTest do
  @moduledoc """
  `Alambique.Feed.Publisher.close_reader/1` always clears the reader of the
  state and says so when the connection would not close.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Alambique.{Sqlite, TestDirs}
  alias Alambique.Cell.State
  alias Alambique.Feed.Publisher

  setup do
    dir = TestDirs.new("close-reader")
    on_exit(fn -> {:ok, _} = File.rm_rf(dir) end)
    {:ok, conn} = Sqlite.open(Path.join(dir, "reader.db"))
    %{conn: conn}
  end

  defp with_reader(%State{feed: feed} = state, conn),
    do: %{state | feed: %{feed | reader: conn}}

  test "a reader that closes is cleared without a warning", %{conn: conn} do
    log =
      capture_log(fn ->
        assert %State{feed: %{reader: nil}} = Publisher.close_reader(with_reader(%State{}, conn))
      end)

    refute log =~ "could not close the feed reader"
  end

  test "a reader that will not close is cleared and logged", %{conn: conn} do
    # SQLite refuses to close the connection while its transaction is open.
    {:ok, _token} = Sqlite.begin_transaction(conn)
    state = with_reader(%State{id: "reader-cell"}, conn)

    log =
      capture_log(fn ->
        assert %State{feed: %{reader: nil}} = Publisher.close_reader(state)
      end)

    assert log =~ "could not close the feed reader of reader-cell"
  end
end
