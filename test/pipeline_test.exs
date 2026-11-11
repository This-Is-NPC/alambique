defmodule Alambique.PipelineTest do
  @moduledoc """
  Pipelined commits (design §6): answers in TXID order, reads and cuts behind
  the newest batch, failures, the feed bound and the effective depth. A gated
  store decides when each put lands.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Feed}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"

  setup ctx do
    env = setup_store()
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    Map.merge(env, %{gate: gate, tag: ctx.test |> Atom.to_string() |> :erlang.phash2()})
  end

  # An instance whose puts of level-0 files are held after the cell woke.
  defp start(env, depth, opts \\ []) do
    store = GatedStore.wrap(env.gate, env.store)
    cell = "cell-#{env.tag}"

    inst =
      start_instance(
        %{env | store: store},
        [migrations: @schema, pipeline_depth: depth, compaction: false] ++ opts
      )

    {:ok, _} = Alambique.execute(inst, cell, @insert, [0])
    GatedStore.hold(env.gate, ~r{/0000/})
    %{inst: inst, cell: cell, pid: Cell.whereis(Config.fetch!(inst), cell)}
  end

  defp key(%{inst: inst, cell: cell}, txid),
    do: Keys.segment(repl_ctx(inst, cell).prefix, cell, 0, txid, txid)

  defp state(%{pid: pid}), do: :sys.get_state(pid)
  defp txids(owner), do: Enum.map(state(owner).pipe.queue, & &1.txid)

  # Sends a write and returns once the cell processed its flush: the batch is
  # encoded when the pipeline had a free slot.
  defp send_write(owner, n, request \\ nil) do
    req = :gen_server.send_request(owner.pid, request || {:execute, @insert, [n], nil})
    state(owner)
    state(owner)
    req
  end

  defp answer(req, timeout \\ 0), do: :gen_server.wait_response(req, timeout)

  defp land(env, owner, txid) do
    pid = Enum.find(state(owner).pipe.queue, &(&1.txid == txid)).pid
    GatedStore.release(env.gate, key(owner, txid))
    eventually(fn -> not Process.alive?(pid) end)
    state(owner)
    :ok
  end

  defp rows(env, owner),
    do:
      env
      |> durable_rows(owner.cell, "SELECT n FROM t ORDER BY n", "cells-test")
      |> List.flatten()

  describe "acknowledgement" do
    test "writes are answered in TXID order whatever order the puts land in", env do
      capture([[:alambique, :cell, :commit]])
      owner = start(env, 4)
      head = state(owner).repl.txid

      reqs = for n <- 1..3, do: send_write(owner, n)
      assert txids(owner) == [head + 1, head + 2, head + 3]

      land(env, owner, head + 3)
      land(env, owner, head + 2)
      assert Enum.all?(reqs, &(answer(&1) == :timeout))
      assert rows(env, owner) == [0]

      land(env, owner, head + 1)
      assert Enum.all?(reqs, &match?({:reply, {:ok, _}}, answer(&1, 1_000)))
      assert rows(env, owner) == [0, 1, 2, 3]
      assert state(owner).pipe.acked.txid == head + 3

      for {txid, inflight} <- Enum.zip([head + 1, head + 2, head + 3], [1, 2, 3]) do
        {m, meta} =
          await_event([:alambique, :cell, :commit], owner.cell, 1_000, &(&1.txid == txid))

        assert meta.result == :ok
        assert %{transactions: 1, inflight: ^inflight, queued_ms: queued} = m
        assert queued >= 0
      end
    end

    test "a full pipeline makes the next batch bigger", env do
      owner = start(env, 2)
      head = state(owner).repl.txid

      reqs = for n <- 1..4, do: send_write(owner, n)
      assert txids(owner) == [head + 1, head + 2]
      assert length(state(owner).pending) == 2

      land(env, owner, head + 1)
      # The slot is free: the two waiting writes go out as one batch.
      eventually(fn -> txids(owner) == [head + 2, head + 3] end)
      land(env, owner, head + 2)
      land(env, owner, head + 3)
      assert Enum.all?(reqs, &match?({:reply, {:ok, _}}, answer(&1, 1_000)))
      assert rows(env, owner) == [0, 1, 2, 3, 4]
    end

    test "depth 1 keeps one batch in flight", env do
      owner = start(env, 1)
      head = state(owner).repl.txid
      reqs = for n <- 1..2, do: send_write(owner, n)
      assert txids(owner) == [head + 1]
      land(env, owner, head + 1)
      eventually(fn -> txids(owner) == [head + 2] end)
      land(env, owner, head + 2)
      assert Enum.all?(reqs, &match?({:reply, {:ok, _}}, answer(&1, 1_000)))
    end
  end

  describe "reads and cuts" do
    test "a read waits for the newest batch", env do
      owner = start(env, 4)
      head = state(owner).repl.txid
      write1 = send_write(owner, 1)
      write2 = send_write(owner, 2)
      read = send_write(owner, 0, {:query, "SELECT count(*) FROM t", [], nil})

      land(env, owner, head + 2)
      assert answer(read) == :timeout
      land(env, owner, head + 1)
      assert {:reply, {:ok, %{rows: [[3]]}}} = answer(read, 1_000)
      assert {:reply, {:ok, _}} = answer(write1, 1_000)
      assert {:reply, {:ok, _}} = answer(write2, 1_000)
    end

    test "position waits for every batch and returns the acknowledged head", env do
      owner = start(env, 4)
      head = state(owner).repl.txid
      for n <- 1..2, do: send_write(owner, n)

      position = Task.async(fn -> Alambique.position(owner.inst, owner.cell) end)
      # The cell waits for the uploads and answers no other call meanwhile.
      assert Task.yield(position, 200) == nil
      for txid <- [head + 2, head + 1], do: GatedStore.release(env.gate, key(owner, txid))
      assert {:ok, %{txid: txid}} = Task.await(position, 5_000)
      assert txid == head + 2
    end

    test "an isolated write is a batch of its own, encoded behind the open one", env do
      owner = start(env, 4)
      head = state(owner).repl.txid

      reqs = [
        :gen_server.send_request(owner.pid, {:execute, @insert, [1], nil}),
        :gen_server.send_request(owner.pid, {:execute, @insert, [2], nil}),
        :gen_server.send_request(owner.pid, {:isolated, {:execute, @insert, [3], nil}})
      ]

      eventually(fn -> txids(owner) == [head + 1, head + 2] end)
      for txid <- [head + 2, head + 1], do: land(env, owner, txid)
      assert Enum.all?(reqs, &match?({:reply, {:ok, _}}, answer(&1, 1_000)))
      assert rows(env, owner) == [0, 1, 2, 3]
    end
  end

  describe "failure" do
    test "a failed put leaves every queued write unknown and stops the cell", env do
      owner = start(env, 4, op_timeout_ms: 300)
      head = state(owner).repl.txid
      reqs = for n <- 1..3, do: send_write(owner, n)

      land(env, owner, head + 3)
      for _ <- 1..30, do: GatedStore.fail(env.gate, key(owner, head + 1))

      for req <- reqs, do: assert({:reply, {:error, :outcome_unknown}} = answer(req, 5_000))
      eventually(fn -> Cell.whereis(Config.fetch!(owner.inst), owner.cell) == nil end)

      # Nothing past the gap is part of the state a restore gives.
      assert rows(env, owner) == [0]
    end

    test "a crash of an upload process stops the cell like a failed put", env do
      owner = start(env, 4)
      send_write(owner, 1)
      req = send_write(owner, 2)
      [first | _] = state(owner).pipe.queue
      Process.exit(first.pid, :kill)
      assert {:reply, {:error, :outcome_unknown}} = answer(req, 5_000)
    end
  end

  defp published(feed) do
    {:ok, records, _} = Feed.read(feed, nil, 100)
    Enum.map(records, & &1.key["n"])
  end

  describe "change feed" do
    test "rows of a batch that is not acknowledged are not published", env do
      owner = start(env, 4)
      head = state(owner).repl.txid
      feed = Feed.ctx(store: env.store, prefix: "cells-test", settle_ms: 0)
      reqs = for n <- 1..2, do: send_write(owner, n)

      # The first batch is acknowledged and published; the second is encoded
      # and uploading, so its row waits.
      land(env, owner, head + 1)
      assert {:reply, {:ok, _}} = answer(hd(reqs), 1_000)
      eventually(fn -> published(feed) == [0, 1] end)
      assert answer(List.last(reqs)) == :timeout

      land(env, owner, head + 2)
      assert {:reply, {:ok, _}} = answer(List.last(reqs), 1_000)
      eventually(fn -> published(feed) == [0, 1, 2] end)
    end
  end
end
