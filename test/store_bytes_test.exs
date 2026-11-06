defmodule Alambique.StoreBytesTest do
  @moduledoc """
  The `bytes` of the telemetry events add up to what the store holds, by kind
  of object (docs/telemetry.md, "Bytes written to the store"). A kind that
  loses its event would make a per-cell account of the store too small.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  @events [
    [:alambique, :replication, :sync],
    [:alambique, :feed, :publish],
    [:alambique, :compaction, :merge],
    [:alambique, :compaction, :snapshot]
  ]

  setup do
    setup_store()
  end

  def relay(event, %{bytes: bytes}, meta, {test, ref}),
    do: send(test, {ref, event, meta[:cell], meta[:result], bytes})

  # The kind of object a key names, as the events do.
  defp kind(key) do
    cond do
      String.contains?(key, "/feed/") -> :feed
      match?([_, "0000"], Regex.run(~r|/cells/[^/]+/(\d{4})/|, key)) -> :segment
      match?([_, "0009"], Regex.run(~r|/cells/[^/]+/(\d{4})/|, key)) -> :snapshot
      Regex.match?(~r|/cells/[^/]+/000[1-8]/|, key) -> :merged
      true -> :other
    end
  end

  defp stored(%{store: {mod, config}}) do
    {:ok, keys, _} = mod.list(config, "cells-test/", nil, 10_000)

    keys
    |> Enum.group_by(&kind/1, fn key ->
      {:ok, body, _etag, _meta} = mod.get(config, key)
      byte_size(body)
    end)
    |> Map.new(fn {kind, sizes} -> {kind, Enum.sum(sizes)} end)
  end

  defp reported(ref, acc \\ %{}) do
    receive do
      {^ref, [_, :replication, :sync], _cell, :ok, bytes} ->
        reported(ref, add(acc, :segment, bytes))

      {^ref, [_, :feed, :publish], _cell, :ok, bytes} ->
        reported(ref, add(acc, :feed, bytes))

      {^ref, [_, :compaction, :merge], _cell, :ok, bytes} ->
        reported(ref, add(acc, :merged, bytes))

      {^ref, [_, :compaction, :snapshot], _cell, :ok, bytes} ->
        reported(ref, add(acc, :snapshot, bytes))

      {^ref, _event, _cell, _result, _bytes} ->
        reported(ref, acc)
    after
      0 -> acc
    end
  end

  defp add(acc, kind, bytes), do: Map.update(acc, kind, bytes, &(&1 + bytes))

  test "events report every byte put to the store, by kind of object", ctx do
    ref = make_ref()
    id = {__MODULE__, ref}
    :telemetry.attach_many(id, @events, &__MODULE__.relay/4, {self(), ref})
    on_exit(fn -> :telemetry.detach(id) end)

    inst =
      start_instance(ctx,
        migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}],
        compaction: [
          fanout: 2,
          max_level: 2,
          interval_ms: 10,
          max_age_ms: 10,
          snapshot_interval_txids: 6
        ],
        feed: [retention_ms: nil]
      )

    cell = unique_cell()
    body = String.duplicate("x", 3_000)

    for i <- 1..14 do
      {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO notes VALUES (?, ?)", [i, body])
    end

    # Quiet: the feed published every batch and the compactor has nothing left.
    eventually(fn ->
      first = stored(ctx)
      Process.sleep(150)

      first == stored(ctx) and first[:feed] != nil and first[:snapshot] != nil and
        first[:merged] != nil
    end)

    assert %{segment: _, feed: _, merged: _, snapshot: _} = totals = reported(ref)
    held = stored(ctx)

    for kind <- [:segment, :feed, :merged, :snapshot] do
      assert totals[kind] == held[kind], "#{kind}: events #{totals[kind]}, store #{held[kind]}"
    end
  end
end
