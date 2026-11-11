defmodule Alambique.PipelineStopTest do
  @moduledoc """
  What the callers of a cell that stops learn (design §6). A write whose batch
  was sent may be durable whatever stopped the cell, so it is answered
  `:outcome_unknown`; a read and a write that was never sent get the reason;
  a batch whose own put met another writer's object is refused definitively.
  A gated store keeps puts in flight, or lands one and loses its answer.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{GatedStore, ManualClock}

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"
  @ttl 60_000

  setup do
    env = setup_store()
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)

    world = ManualClock.new(1_800_000_000_000)

    opts = [
      migrations: @schema,
      pipeline_depth: 2,
      compaction: false,
      lease_ttl_ms: @ttl,
      clock: {ManualClock, ManualClock.config(world, 0)}
    ]

    a = start_instance(%{env | store: GatedStore.wrap(gate, env.store)}, opts)
    b = start_instance(env, opts)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(a, cell, @insert, [0])
    GatedStore.hold(gate, ~r{/0000/})

    %{env: env, gate: gate, world: world, a: a, b: b, cell: cell}
    |> Map.put(:pid, Cell.whereis(Config.fetch!(a), cell))
  end

  defp key(w, txid), do: Keys.segment(repl_ctx(w.a, w.cell).prefix, w.cell, 0, txid, txid)

  # Sends a write and returns once its put waits at the gate.
  defp send_write(w, n) do
    req = :gen_server.send_request(w.pid, {:execute, @insert, [n], nil})
    :sys.get_state(w.pid)
    txid = List.last(:sys.get_state(w.pid).pipe.queue).txid
    :ok = GatedStore.await(w.gate, key(w, txid))
    {req, txid}
  end

  # The put lands, its answer is lost and the retry waits at the gate: the
  # object is in the store and the cell does not know.
  defp land_unseen(w, txid) do
    GatedStore.lose_ack(w.gate, key(w, txid))
    :ok = GatedStore.await(w.gate, key(w, txid))
  end

  defp answer(req), do: :gen_server.wait_response(req, 5_000)

  defp rows(w) do
    w.env
    |> durable_rows(w.cell, "SELECT n FROM t ORDER BY n")
    |> List.flatten()
  end

  # The lease of `a` ends and `b` takes the cell (and its own write at `n`).
  defp take_over(w, n) do
    ManualClock.advance(w.world, @ttl + 1_000)
    {:ok, _} = Alambique.execute(w.b, w.cell, @insert, [n])
  end

  for landed <- [false, true] do
    describe "a write in flight, #{if landed, do: "landed in the store", else: "not landed"}" do
      test "is unknown when the lease is lost; a read gets the reason", w do
        {req, txid} = send_write(w, 1)
        if unquote(landed), do: land_unseen(w, txid)
        read = :gen_server.send_request(w.pid, {:query, "SELECT n FROM t", [], nil})
        :sys.get_state(w.pid)

        take_over(w, 100)
        send(w.pid, :renew)

        assert {:reply, {:error, :outcome_unknown}} = answer(req)
        assert {:reply, {:error, :lease_lost}} = answer(read)

        assert rows(w) == if(unquote(landed), do: [0, 1, 100], else: [0, 100])
      end

      test "is unknown when the lease expired", w do
        {req, txid} = send_write(w, 1)
        if unquote(landed), do: land_unseen(w, txid)

        ManualClock.advance(w.world, @ttl + 1_000)
        call = :gen_server.send_request(w.pid, {:query, "SELECT 1", [], nil})

        assert {:reply, {:error, :lease_expired}} = answer(call)
        assert {:reply, {:error, :outcome_unknown}} = answer(req)
        assert rows(w) == if(unquote(landed), do: [0, 1], else: [0])
      end
    end
  end

  describe "a fenced owner" do
    test "refuses the batch that met the fence and cannot vouch for the one behind", w do
      {first, _} = send_write(w, 1)
      {second, txid} = send_write(w, 2)

      take_over(w, 100)
      GatedStore.release(w.gate, key(w, txid - 1))

      assert {:reply, {:error, :fenced}} = answer(first)
      assert {:reply, {:error, :outcome_unknown}} = answer(second)
      assert rows(w) == [0, 100]
    end

    test "answers unknown for a landed batch when a later one is fenced", w do
      {first, txid} = send_write(w, 1)
      land_unseen(w, txid)
      {second, txid2} = send_write(w, 2)

      take_over(w, 100)
      GatedStore.release(w.gate, key(w, txid2))

      assert {:reply, {:error, :outcome_unknown}} = answer(first)
      assert {:reply, {:error, :fenced}} = answer(second)
      assert rows(w) == [0, 1, 100]
    end

    test "a write that was never sent keeps the reason", w do
      {first, txid} = send_write(w, 1)
      {second, _} = send_write(w, 2)
      # Depth 2 is full: the third write waits, committed locally, unsent.
      third = :gen_server.send_request(w.pid, {:execute, @insert, [3], nil})
      :sys.get_state(w.pid)
      assert [_] = :sys.get_state(w.pid).pending

      take_over(w, 100)
      GatedStore.release(w.gate, key(w, txid))

      assert {:reply, {:error, :fenced}} = answer(first)
      assert {:reply, {:error, :outcome_unknown}} = answer(second)
      assert {:reply, {:error, :fenced}} = answer(third)
      assert rows(w) == [0, 100]
    end
  end
end
