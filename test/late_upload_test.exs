defmodule Alambique.LateUploadTest do
  @moduledoc """
  A put that was already sent when its owner stopped can reach the store
  after the lease was released and another instance took the cell (design
  §6, "Late puts"). A detached store keeps the request alive after the cell
  kills its upload process, and a gate holds it until the new owner has
  committed.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, DetachedStore, Feed, Lease}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{GatedStore, ManualClock}

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"
  # Two rows in one transaction: a batch is all or nothing.
  @insert2 "INSERT INTO t (n) VALUES (?), (? + 1)"
  @ttl 60_000

  setup do
    env = setup_store()
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)

    world = ManualClock.new(1_800_000_000_000)

    opts = [
      migrations: @schema,
      pipeline_depth: 4,
      compaction: false,
      lease_ttl_ms: @ttl,
      clock: {ManualClock, ManualClock.config(world, 0)}
    ]

    stranded = DetachedStore.wrap(GatedStore.wrap(gate, env.store), self())
    a = start_instance(%{env | store: stranded}, opts)
    b = start_instance(env, opts)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(a, cell, @insert, [0])

    %{env: env, gate: gate, world: world, a: a, b: b, cell: cell}
    |> Map.put(:pid, Cell.whereis(Config.fetch!(a), cell))
  end

  defp key(w, txid), do: Keys.segment(repl_ctx(w.a, w.cell).prefix, w.cell, 0, txid, txid)

  # Sends a write and returns once its put waits at the gate.
  defp send_write(w, sql, params) do
    req = :gen_server.send_request(w.pid, {:execute, sql, params, nil})
    :sys.get_state(w.pid)
    txid = List.last(:sys.get_state(w.pid).pipe.queue).txid
    :ok = GatedStore.await(w.gate, key(w, txid))
    {req, txid}
  end

  defp answer(req), do: :gen_server.wait_response(req, 5_000)

  defp rows(w) do
    w.env |> durable_rows(w.cell, "SELECT n FROM t ORDER BY n") |> List.flatten()
  end

  defp epoch_at(w, txid) do
    {mod, config} = w.env.store
    {:ok, _, _, meta} = mod.get(config, key(w, txid))
    String.to_integer(meta["epoch"])
  end

  defp lease(w) do
    config = Config.fetch!(w.a)
    {:ok, location} = Alambique.Placement.locate(config, w.cell)
    Lease.read(Config.lease_ctx(config, location), w.cell)
  end

  # The owner stops by itself, kills its uploads and releases the lease; its
  # puts stay in flight. Returns its epoch.
  defp stop_owner(w, reqs) do
    {:ok, %{epoch: epoch}} = lease(w)
    ref = Process.monitor(w.pid)
    ManualClock.advance(w.world, @ttl + 1_000)
    call = :gen_server.send_request(w.pid, {:query, "SELECT 1", [], nil})

    assert {:reply, {:error, :lease_expired}} = answer(call)
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000
    assert {:ok, %{released: true}} = lease(w)

    for req <- reqs, do: assert({:reply, {:error, :outcome_unknown}} = answer(req))
    epoch
  end

  # The second instance takes the cell and commits `n`.
  defp take_over(w, n), do: {:ok, _} = Alambique.execute(w.b, w.cell, @insert, [n])

  # Lets a held put go and returns what it met: `{:ok, _}` landed,
  # `{:error, :precondition_failed}` was refused.
  defp late_put(w, key) do
    GatedStore.release(w.gate, key)
    assert_receive {:put_done, ^key, result}, 5_000
    result
  end

  describe "a segment" do
    test "that meets the fence or a commit of the new owner is refused", w do
      GatedStore.hold(w.gate, ~r{/0000/})
      {r1, t} = send_write(w, @insert, [1])
      {r2, _} = send_write(w, @insert, [2])
      old_epoch = stop_owner(w, [r1, r2])

      take_over(w, 100)
      new_epoch = epoch_at(w, t)
      assert new_epoch > old_epoch

      assert {:error, :precondition_failed} = late_put(w, key(w, t))
      assert {:error, :precondition_failed} = late_put(w, key(w, t + 1))

      assert [epoch_at(w, t), epoch_at(w, t + 1)] == [new_epoch, new_epoch]
      assert rows(w) == [0, 100]

      take_over(w, 101)
      assert rows(w) == [0, 100, 101]
      assert {:ok, [0, 100, 101]} = column(w)
    end

    test "past the new owner's head lands, is not part of the chain and is replaced", w do
      GatedStore.hold(w.gate, ~r{/0000/})
      {r1, t} = send_write(w, @insert, [1])
      {r2, _} = send_write(w, @insert, [2])
      {r3, _} = send_write(w, @insert, [3])
      old_epoch = stop_owner(w, [r1, r2, r3])

      # The fence takes t and the commit t + 1.
      take_over(w, 100)
      assert {:error, :precondition_failed} = late_put(w, key(w, t))
      assert {:error, :precondition_failed} = late_put(w, key(w, t + 1))

      # The put of t + 2 lands: an object of the old epoch past the head.
      assert {:ok, _} = late_put(w, key(w, t + 2))
      assert epoch_at(w, t + 2) == old_epoch
      assert rows(w) == [0, 100]
      assert {:ok, [0, 100]} = column(w)

      # The new owner's next commit takes its key from the zombie.
      take_over(w, 101)
      assert epoch_at(w, t + 2) > old_epoch
      assert rows(w) == [0, 100, 101]
      assert {:ok, [0, 100, 101]} = column(w)
    end

    test "that lands before the new owner wakes is applied whole, or not at all", w do
      GatedStore.hold(w.gate, ~r{/0000/})
      {r1, t} = send_write(w, @insert2, [1, 1])
      {r2, _} = send_write(w, @insert2, [10, 10])
      stop_owner(w, [r1, r2])

      # The first lands before the takeover and the second after.
      assert {:ok, _} = late_put(w, key(w, t))
      take_over(w, 100)
      assert {:error, :precondition_failed} = late_put(w, key(w, t + 1))

      assert rows(w) == [0, 1, 2, 100]
      assert {:ok, [0, 1, 2, 100]} = column(w)
    end
  end

  describe "a feed object" do
    test "put late by the old sender duplicates records and loses none", w do
      GatedStore.hold(w.gate, ~r{/feed/})
      {:ok, _} = Alambique.execute(w.a, w.cell, @insert, [1])
      [feed_key] = await_feed_put(w)
      stop_owner(w, [])

      take_over(w, 100)
      assert {:ok, _} = late_put(w, feed_key)

      clock = {ManualClock, ManualClock.config(w.world, 0)}
      ctx = Feed.ctx(store: w.env.store, prefix: "cells-test", settle_ms: 0, clock: clock)

      eventually(
        fn ->
          {:ok, records, _} = Feed.read(ctx, nil, 1000)
          records |> Enum.map(& &1.key["n"]) |> Enum.uniq() |> Enum.sort() == [0, 1, 100]
        end,
        10_000
      )

      assert rows(w) == [0, 1, 100]
    end
  end

  defp await_feed_put(w) do
    eventually(fn -> GatedStore.held(w.gate) != [] end, 5_000)
    GatedStore.held(w.gate)
  end

  # The rows of the cell as the owner answers them.
  defp column(w) do
    with {:ok, %{rows: rows}} <- Alambique.query(w.b, w.cell, "SELECT n FROM t ORDER BY n", []),
         do: {:ok, List.flatten(rows)}
  end
end
