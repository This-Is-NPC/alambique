defmodule Alambique.FeedProducerTest do
  @moduledoc "The Broadway producer of the change feed (design §13)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Feed
  alias Alambique.Feed.Producer
  alias Broadway.Message

  defmodule Pipeline do
    use Broadway

    def start(opts) do
      test = self()

      Broadway.start_link(__MODULE__,
        name: Module.concat(__MODULE__, "P#{System.unique_integer([:positive])}"),
        producer: [
          module:
            {Producer,
             Keyword.merge(
               [store_position: &send(test, {:position, &1}), poll_interval_ms: 20],
               opts
             )},
          concurrency: 1
        ],
        processors: [default: [concurrency: 4]],
        context: %{test: test, fail: opts[:fail]}
      )
    end

    @impl true
    def handle_message(_processor, message, %{test: test, fail: fail}) do
      send(test, {:record, message.data})

      if fail && fail.(message.data), do: Message.failed(message, :rejected), else: message
    end
  end

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]

  setup do
    setup_store()
  end

  defp feed_ctx(%{store: store}), do: Feed.ctx(store: store, prefix: "cells-test", settle_ms: 0)

  defp write(inst, cells, ids) do
    for id <- ids do
      cell = Enum.at(cells, rem(id, length(cells)))
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO notes VALUES (?, 'x')", [id])
    end
  end

  defp receive_records(count, acc \\ []) do
    if length(acc) == count do
      acc
    else
      receive do
        {:record, record} -> receive_records(count, [record | acc])
      after
        5_000 -> flunk("received #{length(acc)} of #{count} records")
      end
    end
  end

  defp last_position(last \\ nil) do
    receive do
      {:position, position} -> last_position(position)
    after
      100 -> last
    end
  end

  test "consumes every record", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cells = for _ <- 1..3, do: unique_cell()
    write(inst, cells, 1..30)

    {:ok, pipeline} = Pipeline.start(ctx: feed_ctx(ctx))
    records = receive_records(30)

    assert records |> Enum.map(& &1.key["id"]) |> Enum.sort() == Enum.to_list(1..30)
    assert length(Enum.uniq_by(records, & &1.id)) == 30

    eventually(fn -> last_position() end)
    Broadway.stop(pipeline)
  end

  test "keeps polling and delivers records written later", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()

    {:ok, pipeline} = Pipeline.start(ctx: feed_ctx(ctx))
    Process.sleep(60)
    write(inst, [cell], 1..3)

    assert receive_records(3) |> Enum.map(& &1.key["id"]) |> Enum.sort() == [1, 2, 3]
    Broadway.stop(pipeline)
  end

  test "resumes from the stored position without losing a record", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cells = for _ <- 1..3, do: unique_cell()
    write(inst, cells, 1..20)

    {:ok, first} = Pipeline.start(ctx: feed_ctx(ctx), max_objects: 5)
    receive_records(20)
    stored = eventually(fn -> last_position() end)
    Broadway.stop(first)
    assert {:ok, [], ^stored} = Feed.read(feed_ctx(ctx), stored, 100)

    write(inst, cells, 21..30)
    {:ok, second} = Pipeline.start(ctx: feed_ctx(ctx), position: stored)
    records = receive_records(10)

    assert records |> Enum.map(& &1.key["id"]) |> Enum.sort() == Enum.to_list(21..30)
    Broadway.stop(second)
  end

  test "a restart redelivers what was not acknowledged", ctx do
    inst = start_instance(ctx, migrations: @schema)
    cell = unique_cell()
    write(inst, [cell], 1..6)

    {:ok, first} = Pipeline.start(ctx: feed_ctx(ctx), fail: &(&1.key["id"] == 4))
    receive_records(6)
    stored = eventually(fn -> last_position() end)
    Broadway.stop(first)

    # The position stops before the failed record's object.
    assert {:ok, [%{key: %{"id" => 4}} | _], _} = Feed.read(feed_ctx(ctx), stored, 100)

    {:ok, second} = Pipeline.start(ctx: feed_ctx(ctx), position: stored)
    assert receive_records(3) |> Enum.map(& &1.key["id"]) |> Enum.sort() == [4, 5, 6]
    Broadway.stop(second)
  end
end
