defmodule Alambique.MigrationPropertiesTest do
  @moduledoc """
  Invariant M1 (design §17): a cell on N-1 is served correctly by code for N,
  and migrating it never loses rows.

  Random expand-only migrations (columns, backfills, seeded rows, tables) are
  run by three releases at once (N-2, N-1 and N, each an instance over one
  store) that wake the same cells in random order, with store faults and
  crash points during the wakes. After every operation and at the end:

    * the durable rows hold every acknowledged write, nothing that was never
      attempted, and exactly the rows the migrations up to the cell's version
      seed (nothing is lost or repeated by migrating);
    * the schema index lists the cell at its durable version, so it is never
      missing;
    * reads of the columns and tables the serving code knows succeed on cells
      of any version it serves, and code more than one version behind refuses
      a cell with `schema_too_new`;
    * after the sweeper of the newest release ran, every cell is at N, listed
      only there, and the change feed holds each row exactly once;
    * a write is acknowledged only once its segment is in the store, also when
      the later of two batches in flight lands first, and until the first
      fault every write is acknowledged or refused with `schema_too_new`.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AckGuard, Cell, Config, Feed, HeldWrites}
  alias AlambiqueTestkit.{CrashPoints, GatedStore, ManualClock, MemoryStore}

  @moduletag :capture_log
  @moduletag timeout: 600_000

  @kinds [:column, :backfill, :seed, :table]
  @cells ["m0", "m1"]
  @points [
    :schema_before_put,
    :schema_after_put,
    :repl_before_capture,
    :repl_after_put,
    :cell_after_ack,
    :feed_before_publish,
    :feed_after_put
  ]

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # Generators

  defp op do
    inner =
      frequency([
        {6, tuple({constant(:write), member_of([:old2, :old, :new]), member_of(@cells)})},
        {4, tuple({constant(:read), member_of([:old2, :old, :new]), member_of(@cells)})}
      ])

    fault =
      one_of([
        tuple({constant(:store), member_of([:error, :timeout, :lost_ack]), integer(1..4)}),
        tuple({constant(:segment), integer(1..4)}),
        tuple({constant(:crash), member_of(@points)}),
        constant(:none)
      ])

    # The puts of the pair are held, so no fault runs with it.
    held = tuple({constant(:held), member_of([:old2, :old, :new]), member_of(@cells)})

    frequency([
      {10, tuple({constant(:run), fault, inner})},
      {2, tuple({constant(:run), constant(:none), held})},
      {2, constant(:sweep)}
    ])
  end

  # Migrations

  defp migrations(kinds) do
    [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}] ++
      for {kind, i} <- Enum.with_index(kinds, 2), do: {i, sql(kind, i)}
  end

  defp sql(:column, k), do: "ALTER TABLE t ADD COLUMN c#{k} INTEGER"
  defp sql(:backfill, k), do: "ALTER TABLE t ADD COLUMN c#{k} INTEGER; UPDATE t SET c#{k} = n"
  defp sql(:seed, k), do: "INSERT INTO t (n) VALUES (-#{k})"
  defp sql(:table, k), do: "CREATE TABLE u#{k} (x)"

  defp seeds(kinds, version),
    do: for({:seed, k} <- Enum.with_index(kinds, 2), k <= version, do: -k) |> Enum.sort()

  # Scenario

  property "a cell on N-1 is served by code for N and migrating loses nothing", ctx do
    check all(
            kinds <- list_of(member_of(@kinds), min_length: 2, max_length: 4),
            ops <- list_of(op(), min_length: 4, max_length: 12),
            max_runs: 25,
            max_shrinking_steps: 0
          ) do
      n = length(kinds) + 1
      all = migrations(kinds)
      world = ManualClock.new(1_000_000)
      clock = {ManualClock, ManualClock.config(world, 0)}
      prefix = "cells-test"

      versions = %{old2: max(n - 2, 1), old: n - 1, new: n}
      {:ok, gate} = GatedStore.start_link()
      gated = %{ctx | store: GatedStore.wrap(gate, ctx.store)}

      insts =
        for {code, version} <- versions, into: %{} do
          {code,
           start_instance(gated,
             migrations: Enum.take(all, version),
             clock: clock,
             op_timeout_ms: 300,
             # A failed put reaches the cell, which retries it after a short backoff.
             retry: [max_attempts: 1],
             upload: [retry_backoff_ms: 5, retry_backoff_max_ms: 20],
             feed: [window_ms: 500]
           )}
        end

      cells = for c <- @cells, do: "#{c}-#{System.unique_integer([:positive])}"
      names = Map.new(Enum.zip(@cells, cells))
      guard = AckGuard.attach(ctx.store, prefix, &(&1 in cells))

      state = %{
        seq: 0,
        acked: Map.new(cells, &{&1, []}),
        unknown: Map.new(cells, &{&1, []}),
        faulty?: false,
        gate: gate,
        guard: guard
      }

      state =
        Enum.reduce(ops, state, fn op, state ->
          state = step(op, state, ctx, insts, versions, kinds, names, world)
          sleep_all(insts, cells)
          ManualClock.advance(world, 4_000)
          state
        end)

      # The newest release sweeps; then everything is at N.
      sleep_all(insts, cells)
      # A wake that failed because of the store waits out its backoff.
      for _ <- 1..3 do
        await_wake_backoff(insts.new, cells)
        Alambique.sweep(insts.new)
      end

      sleep_all(insts, cells)

      for cell <- cells, durable_version(ctx, cell) > 0 do
        assert durable_version(ctx, cell) == n, "#{cell} not migrated by the sweeper"
        assert index_of(ctx, cell) == [n], "#{cell} listed at #{inspect(index_of(ctx, cell))}"
        check_rows(ctx, kinds, state, cell)
      end

      check_feed(ctx, insts, clock, cells, prefix, world)
      assert_acks_landed(state)
      MemoryStore.clear(ctx.store_config)
      AckGuard.detach(guard)
      GenServer.stop(gate)
    end
  end

  defp assert_acks_landed(%{guard: guard}) do
    assert AckGuard.violations(guard) == [],
           "acknowledged before the put landed: #{inspect(AckGuard.violations(guard))}"
  end

  defp step({:run, fault, inner}, state, ctx, insts, versions, kinds, names, _world) do
    # The cell is awake before its segment puts fail: it is the write that meets them.
    with {:segment, _} <- fault, {_kind, code, name} <- inner do
      prepare(insts, code, names[name])
      _ = safely(fn -> Alambique.query(insts[code], names[name], "SELECT 1") end)
    end

    arm(fault, ctx)
    state = %{state | faulty?: state.faulty? or fault != :none}

    try do
      {state, cell} = run(inner, state, insts, versions, kinds, names)
      disarm()
      assert_acks_landed(state)
      verify(ctx, kinds, state, cell)
      state
    after
      disarm()
    end
  end

  defp step(:sweep, state, ctx, insts, _versions, _kinds, _names, _world) do
    Alambique.sweep(insts.new)
    state |> tap(fn _ -> MemoryStore.clear(ctx.store_config) end)
  end

  defp arm({:store, kind, times}, ctx),
    do: MemoryStore.inject(ctx.store_config, kind: kind, times: times, ms: 1)

  # Puts of segments fail: the cell puts them again while its lease holds.
  defp arm({:segment, times}, ctx) do
    MemoryStore.inject(ctx.store_config,
      kind: :error,
      ops: [:put],
      key: ~r{/0000/},
      times: times,
      reason: {:http, 503, ""}
    )
  end

  defp arm({:crash, point}, _ctx), do: CrashPoints.arm(point, :raise)
  defp arm(:none, _ctx), do: :ok

  defp disarm do
    CrashPoints.reset()
  end

  defp run({:write, code, name}, state, insts, versions, _kinds, names) do
    cell = names[name]
    n = state.seq + 1
    prepare(insts, code, cell)

    result = safely(fn -> write(insts[code], cell, n) end)
    {record(%{state | seq: n}, cell, n, result, code, versions), cell}
  end

  # Two writes of one release whose puts are held, the later one landing first.
  defp run({:held, code, name}, state, insts, versions, _kinds, names) do
    cell = names[name]
    prepare(insts, code, cell)
    _ = safely(fn -> Alambique.query(insts[code], cell, "SELECT 1") end)
    pid = Cell.whereis(Config.fetch!(insts[code]), cell)

    if pid do
      ns = [state.seq + 1, state.seq + 2]

      answers =
        HeldWrites.run(
          state.gate,
          cell,
          pid,
          8,
          ns,
          &safely(fn -> write(insts[code], cell, &1) end)
        )

      state = %{state | seq: state.seq + 2}

      {Enum.reduce(answers, state, fn {n, result}, state ->
         record(state, cell, n, result, code, versions)
       end), cell}
    else
      {state, cell}
    end
  end

  defp run({:read, code, name}, state, insts, versions, kinds, names) do
    cell = names[name]
    prepare(insts, code, cell)

    case safely(fn -> Alambique.query(insts[code], cell, "SELECT n FROM t") end) do
      {:ok, %{rows: rows}} ->
        seen = List.flatten(rows)
        assert Enum.all?(state.acked[cell], &(&1 in seen)), "read lost an acknowledged row"
        known = state.acked[cell] ++ state.unknown[cell]
        assert Enum.all?(seen, &(&1 < 0 or &1 in known)), "read shows a row never written"
        probe(insts[code], cell, kinds, versions[code])

      {:error, {:schema_too_new, v}} ->
        assert v > versions[code] + 1

      _ ->
        :ok
    end

    {state, cell}
  end

  defp write(inst, cell, n),
    do: Alambique.execute(inst, cell, "INSERT INTO t (n) VALUES (?)", [n])

  # Until the first fault a write is acknowledged, or refused because the cell
  # is two versions ahead of the code.
  defp record(state, cell, n, result, code, versions) do
    case result do
      {:ok, _} ->
        update_in(state.acked[cell], &[n | &1])

      {:error, {:schema_too_new, v}} ->
        assert v > versions[code] + 1
        state

      other ->
        assert state.faulty?, "#{cell}: write #{n} answered #{inspect(other)} with no fault"
        update_in(state.unknown[cell], &[n | &1])
    end
  end

  # Every column and table that the serving code knows must exist.
  defp probe(inst, cell, kinds, version) do
    for {kind, k} <- Enum.with_index(kinds, 2), k <= version do
      sql =
        case kind do
          :table -> "SELECT count(*) FROM u#{k}"
          :seed -> "SELECT count(*) FROM t WHERE n = -#{k}"
          _ -> "SELECT c#{k} FROM t LIMIT 1"
        end

      assert {:ok, _} = Alambique.query(inst, cell, sql), "#{sql} failed on #{cell}"
    end
  end

  # Another release may not hold the cell when this one wants it.
  defp prepare(insts, code, cell) do
    for {other, inst} <- insts,
        other != code,
        do: safely(fn -> Alambique.hibernate(inst, cell) end)
  end

  defp sleep_all(insts, cells) do
    for {_, inst} <- insts, cell <- cells, do: safely(fn -> Alambique.hibernate(inst, cell) end)
  end

  defp safely(fun) do
    fun.()
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Checks on the durable state

  defp durable_version(ctx, cell) do
    case durable_rows(ctx, cell, "PRAGMA user_version") do
      [[v]] -> v
    end
  end

  defp index_of(%{store_config: config}, cell) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/schema/", nil, 100_000)

    for "cells-test/schema/" <> rest <- keys,
        [v, ^cell] <- [String.split(rest, "/")],
        do: String.to_integer(v)
  end

  defp verify(ctx, kinds, state, cell) do
    MemoryStore.clear(ctx.store_config)
    version = durable_version(ctx, cell)

    if version >= 1 do
      assert version in index_of(ctx, cell),
             "#{cell} at version #{version} is missing from the index: #{inspect(index_of(ctx, cell))}"

      check_rows(ctx, kinds, state, cell, version)
    end
  end

  defp check_rows(ctx, kinds, state, cell, version \\ nil) do
    version = version || durable_version(ctx, cell)
    rows = ctx |> durable_rows(cell, "SELECT n FROM t") |> List.flatten()
    {seeded, written} = Enum.split_with(rows, &(&1 < 0))

    assert Enum.sort(seeded) == seeds(kinds, version),
           "#{cell}: seeded rows #{inspect(Enum.sort(seeded))} at version #{version}"

    assert written == Enum.uniq(written)
    assert Enum.all?(state.acked[cell], &(&1 in written)), "#{cell} lost an acknowledged write"
    possible = state.acked[cell] ++ state.unknown[cell]
    assert Enum.all?(written, &(&1 in possible)), "#{cell} holds a row that was never written"
  end

  defp check_feed(ctx, insts, clock, cells, prefix, world) do
    feed = Feed.ctx(store: ctx.store, prefix: prefix, clock: clock, settle_ms: 0)

    expected =
      for cell <- cells, durable_version(ctx, cell) > 0, into: %{} do
        {cell, ctx |> durable_rows(cell, "SELECT n FROM t") |> List.flatten() |> Enum.sort()}
      end

    seen = fn ->
      ManualClock.advance(world, 1_000)
      records = read_all(feed)

      for {cell, _} <- expected, into: %{} do
        mine = Enum.filter(records, &(&1.cell == cell and &1.table == "t" and &1.op == :insert))
        {cell, mine |> Enum.uniq_by(& &1.id) |> Enum.map(& &1.values["n"]) |> Enum.sort()}
      end
    end

    # Rows written by a crashed owner are published when the cell wakes again.
    await_wake_backoff(insts.new, Map.keys(expected))

    for cell <- Map.keys(expected) do
      {:ok, _} = Alambique.query(insts.new, cell, "SELECT 1")
      :ok = Alambique.hibernate(insts.new, cell)
    end

    eventually(fn -> seen.() == expected end, 10_000)
  end

  defp read_all(feed, position \\ nil, acc \\ []) do
    {:ok, records, next} = Feed.read(feed, position, 100)
    if records == [], do: acc, else: read_all(feed, next, acc ++ records)
  end
end
