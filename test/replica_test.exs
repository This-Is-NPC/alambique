defmodule Alambique.ReplicaTest do
  @moduledoc """
  Read replicas (design §21). Two instances share one store: `owner` holds the
  cell, `reader` is a node that does not, so it keeps the replica. In one VM
  the reader cannot forward to the owner (it is not in its `:pg` scope), so a
  read that falls back to the owner answers `{:error, {:unavailable, _, _}}`:
  that is how these tests see which side answered.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Location, Replica}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{GatedStore, MemoryStore}

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @replicas [poll_ms: 50, marker_ttl_ms: 200, refresh_timeout_ms: 1_000]

  setup do
    capture([
      [:alambique, :replica, :read],
      [:alambique, :replica, :stop],
      [:alambique, :replica, :refresh]
    ])

    ctx = setup_store()
    owner = start_instance(ctx, migrations: @schema, compaction: false)
    reader = start_instance(ctx, migrations: @schema, compaction: false, replicas: @replicas)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(owner, cell, "INSERT INTO t (n) VALUES (1)")
    Map.merge(ctx, %{owner: owner, reader: reader, cell: cell})
  end

  defp bounded(t, ms \\ 1_000, sql \\ "SELECT n FROM t ORDER BY n"),
    do: Alambique.query(t.reader, t.cell, sql, [], consistency: {:bounded, ms}, timeout: 5_000)

  defp rows({:ok, %{rows: rows}}), do: List.flatten(rows)

  # Reads until the replica answers.
  defp ready(t) do
    Enum.find_value(1..400, fn _ ->
      case bounded(t) do
        {:ok, _} = ok ->
          ok

        _ ->
          Process.sleep(25)
          nil
      end
    end) || flunk("the replica never answered")
  end

  test "a read goes to the owner until the switch is on and the replica is built", t do
    assert {:ok, false} = Alambique.replicas(t.reader, t.cell)
    assert {:error, {:unavailable, _, _}} = bounded(t)

    {_, %{source: :owner, reason: :no_replica}} =
      await_event([:alambique, :replica, :read], t.cell)

    assert :ok = Alambique.replicas(t.reader, t.cell, true)
    assert {:ok, true} = Alambique.replicas(t.reader, t.cell)
    assert [1] = rows(ready(t))

    {m, meta} =
      await_event([:alambique, :replica, :read], t.cell, 5_000, &(&1.source == :replica))

    assert meta.txid >= 1
    assert m.lag_ms >= 0
  end

  test "the replica follows the owner within the bound", t do
    :ok = Alambique.replicas(t.reader, t.cell, true)
    ready(t)

    {:ok, _} = Alambique.execute(t.owner, t.cell, "INSERT INTO t (n) VALUES (2)")
    # A bound of 1 ms cannot be met by the last poll: the read waits for a refresh.
    assert [1, 2] = rows(bounded(t, 1))
  end

  test "on_stale: :owner sends a stale read to the owner at once", t do
    reader = start_instance(t, replicas: [on_stale: :owner, poll_ms: 5_000, marker_ttl_ms: 5_000])
    :ok = Alambique.replicas(reader, t.cell, true)

    read = fn ms ->
      Alambique.query(reader, t.cell, "SELECT n FROM t", [], consistency: {:bounded, ms})
    end

    assert eventually(fn -> match?({:ok, _}, read.(60_000)) end)
    Process.sleep(20)
    assert {:error, {:unavailable, _, _}} = read.(1)

    {_, meta} =
      await_event([:alambique, :replica, :read], t.cell, 5_000, &(&1[:reason] == :stale))

    assert meta.source == :owner
  end

  test "a read that waits for a refresh gives up after refresh_timeout_ms", t do
    :ok = Alambique.replicas(t.reader, t.cell, true)
    ready(t)
    {_, store} = t.store

    # The refresh lists the cell, which takes longer than the read may wait.
    fault =
      MemoryStore.inject(store,
        kind: :latency,
        ms: 2_500,
        ops: [:list],
        key: Regex.compile!(t.cell)
      )

    Process.sleep(5)
    assert {:error, {:unavailable, _, _}} = bounded(t, 1)

    {_, %{source: :owner, reason: :timeout}} =
      await_event([:alambique, :replica, :read], t.cell, 5_000, &(&1[:reason] == :timeout))

    MemoryStore.remove(store, fault)
  end

  test "a replica refuses to write", t do
    :ok = Alambique.replicas(t.reader, t.cell, true)
    ready(t)

    for sql <- [
          "INSERT INTO t (n) VALUES (9)",
          "CREATE TABLE u (x)",
          "ATTACH ':memory:' AS m",
          "PRAGMA query_only = OFF",
          "PRAGMA wal_checkpoint(TRUNCATE)"
        ] do
      assert {:error, :read_only} = bounded(t, 60_000, sql)
    end

    assert [1] = rows(bounded(t, 60_000))
  end

  test "options other than strong are refused with execute and transaction", t do
    assert {:error, :bad_option} =
             Alambique.execute(t.owner, t.cell, "DELETE FROM t", [], consistency: :strong)

    assert {:error, :bad_option} =
             Alambique.transaction(t.owner, t.cell, fn _ -> :ok end, consistency: {:bounded, 5})

    assert {:error, :bad_option} =
             Alambique.query(t.owner, t.cell, "SELECT 1", [], consistency: {:bounded, 0})

    assert {:error, :bad_option} =
             Alambique.query(t.owner, t.cell, "SELECT 1", [],
               consistency: {:bounded, 5},
               verify_lease: true
             )

    assert {:ok, _} = Alambique.query(t.owner, t.cell, "SELECT 1", [], consistency: :strong)
  end

  test "the owner node answers a bounded read itself", t do
    assert {:ok, %{rows: [[1]]}} =
             Alambique.query(t.owner, t.cell, "SELECT n FROM t", [], consistency: {:bounded, 100})
  end

  test "turning the switch off stops the replica and removes its file", t do
    :ok = Alambique.replicas(t.reader, t.cell, true)
    ready(t)
    config = Config.fetch!(t.reader)
    assert [_] = Path.wildcard(Path.join(Replica.dir(config), "*/replica.db"))

    :ok = Alambique.replicas(t.reader, t.cell, false)

    {_, meta} =
      await_event([:alambique, :replica, :stop], t.cell, 5_000, &(&1.reason == :marker_off))

    assert meta.reason == :marker_off
    assert Path.wildcard(Path.join(Replica.dir(config), "*")) == []
    assert {:error, {:unavailable, _, _}} = bounded(t)
  end

  test "a replica of a cell that moved to another location stops", t do
    stores =
      for name <- [:a, :b, :table], into: %{} do
        pid = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: name))
        {name, MemoryStore.config(pid)}
      end

    locations = [
      %Location{id: "a", store: {MemoryStore, stores.a}, prefix: "pa"},
      %Location{id: "b", store: {MemoryStore, stores.b}, prefix: "pb"}
    ]

    placement =
      {AlambiqueTestkit.Placement,
       locations: locations, table: {MemoryStore, stores.table}, initial: %{"home" => "a"}}

    opts = [placement: placement, migrations: @schema, compaction: false]
    owner = start_instance(t, opts)
    reader = start_instance(t, [replicas: @replicas] ++ opts)
    {:ok, _} = Alambique.execute(owner, "home", "INSERT INTO t (n) VALUES (1)")
    :ok = Alambique.replicas(owner, "home", true)

    read = fn ->
      Alambique.query(reader, "home", "SELECT n FROM t", [], consistency: {:bounded, 60_000})
    end

    assert eventually(fn -> match?({:ok, _}, read.()) end)

    assert {:ok, %Location{id: "b"}} = Alambique.relocate(owner, "home", "b")
    {_, meta} = await_event([:alambique, :replica, :stop], "home", 5_000)
    assert meta.reason == :moved
    assert Path.wildcard(Path.join(Replica.dir(Config.fetch!(reader)), "*/replica.db")) == []
  end

  test "a replica far behind keeps the segments it fetched when one cannot be", t do
    reader =
      start_instance(t,
        migrations: @schema,
        compaction: false,
        replicas: @replicas,
        op_timeout_ms: 300
      )

    t = %{t | reader: reader}
    {_, store} = t.store
    :ok = Alambique.replicas(reader, t.cell, true)
    ready(t)

    # The replica cannot read any segment while the owner writes 20.
    hold = MemoryStore.inject(store, kind: :error, ops: [:get], key: ~r{/0000/})

    for n <- 2..21,
        do: {:ok, _} = Alambique.execute(t.owner, t.cell, "INSERT INTO t (n) VALUES (#{n})")

    # Now only the newest segment is unreadable. Before, one failure threw
    # away the 19 fetched before it, at every poll. It is set before the hold
    # is lifted, so no poll can fetch all 20 in between.
    head = :sys.get_state(Cell.whereis(Config.fetch!(t.owner), t.cell)).repl.txid

    MemoryStore.inject(store,
      kind: :error,
      ops: [:get],
      key: Regex.compile!("-#{Keys.hex(head)}\\.ltx")
    )

    MemoryStore.remove(store, hold)

    # A loaded host may split the 19 over several refreshes (each segment has
    # a deadline of its own), so the applied segments are summed.
    assert 19 = await_applied(19)
  end

  defp await_applied(total, applied \\ 0, deadline \\ deadline_in(15_000))

  defp await_applied(total, applied, _deadline) when applied >= total, do: applied

  defp await_applied(total, applied, deadline) do
    wait = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:telemetry, [:alambique, :replica, :refresh], %{segments: n}, %{result: :ok}} ->
        await_applied(total, applied + n, deadline)

      {:telemetry, [:alambique, :replica, :refresh], _, _} ->
        await_applied(total, applied, deadline)
    after
      wait -> flunk("the replica applied #{applied} of #{total} segments")
    end
  end

  defp deadline_in(ms), do: System.monotonic_time(:millisecond) + ms

  describe "a tail past a gap" do
    # An owner at depth 2 whose uploads of level-0 files are held after the
    # first write, and a reader with a ready replica. Writes 2 and 3 are the
    # batches `head + 1` and `head + 2`.
    setup t do
      {:ok, gate} = GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
      opts = [migrations: @schema, pipeline_depth: 2, compaction: false, op_timeout_ms: 300]
      gated = start_instance(%{t | store: GatedStore.wrap(gate, t.store)}, opts)
      cell = unique_cell()
      {:ok, _} = Alambique.execute(gated, cell, "INSERT INTO t (n) VALUES (1)")
      t = Map.merge(t, %{gated: gated, cell: cell, gate: gate})
      :ok = Alambique.replicas(t.reader, cell, true)
      ready(t)
      GatedStore.hold(gate, ~r{/0000/})
      pid = Cell.whereis(Config.fetch!(gated), cell)
      head = :sys.get_state(pid).repl.txid

      reqs =
        for n <- 2..3 do
          req =
            :gen_server.send_request(pid, {:execute, "INSERT INTO t (n) VALUES (?)", [n], nil})

          :sys.get_state(pid)
          :sys.get_state(pid)
          req
        end

      prefix = repl_ctx(gated, cell).prefix
      key = fn txid -> Keys.segment(prefix, cell, 0, txid, txid) end
      GatedStore.await(gate, key.(head + 1))
      GatedStore.await(gate, key.(head + 2))
      Map.merge(t, %{head: head, key: key, reqs: reqs})
    end

    # Writes older than the bound are in the result: wait past 1 ms, then read
    # with a bound that makes the replica refresh.
    defp settled(t) do
      Process.sleep(5)
      rows(bounded(t, 1))
    end

    test "the replica ignores it and follows when the gap closes", t do
      GatedStore.release(t.gate, t.key.(t.head + 2))
      assert settled(t) == [1]
      assert settled(t) == [1]

      GatedStore.release(t.gate, t.key.(t.head + 1))
      for req <- t.reqs, do: assert({:reply, {:ok, _}} = :gen_server.wait_response(req, 5_000))
      assert settled(t) == [1, 2, 3]
    end

    test "the replica follows a new owner that overwrites the tail", t do
      GatedStore.release(t.gate, t.key.(t.head + 2))
      assert settled(t) == [1]

      # The first batch never lands: the owner stops, the next one takes over.
      for _ <- 1..30, do: GatedStore.fail(t.gate, t.key.(t.head + 1))
      for req <- t.reqs, do: assert({:reply, {:error, _}} = :gen_server.wait_response(req, 5_000))

      taker = start_instance(t, migrations: @schema, compaction: false)
      assert {:ok, _} = Alambique.take_over(taker, t.cell, :test, timeout: 5_000)
      {:ok, _} = Alambique.execute(taker, t.cell, "INSERT INTO t (n) VALUES (4)")
      assert settled(t) == [1, 4]
    end
  end
end
