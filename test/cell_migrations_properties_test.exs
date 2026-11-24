defmodule Alambique.CellMigrationsPropertiesTest do
  @moduledoc """
  Migrations chosen per cell (design §17): several namespaces, each with its
  own migration list that grows while the instance runs, as when workers of
  one namespace are deployed.

  Random writes, reads, migrations of cells that may be awake, deploys (a
  namespace gains a migration), hibernations and restarts of the instance run with store faults and crash points during
  the wakes. After every operation:

    * every acknowledged write is durable, and nothing never attempted is;
    * a cell that was asleep and answered is at its namespace's version at
      that moment or ahead of it;
    * a cell that `Alambique.migrate/3` answers a version for is at its
      namespace's version at that moment or ahead of it;
    * the schema index lists every cell at version 1 or later at its durable
      version (perhaps also at an older one, never nowhere);
    * a write is acknowledged only once its segment is in the store, also when
      the later of two batches in flight lands first, and until the first
      fault every write is acknowledged.

  A second property stores cells ahead of the code: one further ahead than
  the migration window is refused with `schema_too_new` and the version it
  has, one within the window, at its edge too, is served and keeps its
  version.

  At the end, once the breaker of the store lets requests through again, the
  sweeper brings every listed cell to the version of its
  namespace and lists it only there. After a restart that takes until the
  leases that crashed cells left expire, and an entry left behind a migrated
  cell stays until then.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AckGuard, Cell, Config, HeldWrites}
  alias AlambiqueTestkit.{CrashPoints, GatedStore, MemoryStore}

  @moduletag :capture_log
  @moduletag timeout: 120_000

  @namespaces ["a", "b", "c"]
  @cells ["a.1", "a.2", "b.1", "c.1"]
  @max_version 5
  @points [
    :schema_before_put,
    :schema_after_put,
    :repl_before_capture,
    :repl_after_put,
    :cell_after_ack
  ]

  defmodule Lists do
    @moduledoc false
    @key {__MODULE__, :versions}

    def set(versions), do: :persistent_term.put(@key, versions)
    def clear, do: :persistent_term.erase(@key)
    def version(cell), do: Map.fetch!(:persistent_term.get(@key), namespace(cell))

    def migrations(cell), do: list(version(cell))

    def list(version) do
      for k <- 1..version//1 do
        {k,
         if(k == 1,
           do: "CREATE TABLE t (n INTEGER PRIMARY KEY)",
           else: "ALTER TABLE t ADD COLUMN c#{k} INTEGER"
         )}
      end
    end

    defp namespace(cell), do: cell |> String.split(".") |> hd()
  end

  setup do
    on_exit(fn ->
      CrashPoints.reset()
      Lists.clear()
    end)

    setup_store()
  end

  test "migrating from version 0 drops a stale version 0 index entry", ctx do
    Lists.set(%{"a" => 1, "b" => 0, "c" => 0})
    name = :"cell_migrations_example_#{System.unique_integer([:positive])}"
    start(ctx, name)
    cell = "a.1-#{System.unique_integer([:positive])}"

    # The entry a version-0 wake left behind, with no local file.
    {:ok, _} =
      MemoryStore.put(ctx.store_config, "cells-test/schema/0000000000/#{cell}", "", :none, %{})

    assert {:ok, _} = Alambique.touch(name, cell)
    :ok = Alambique.hibernate(name, cell)
    assert {:ok, _} = Alambique.sweep(name)

    assert index_of(ctx, cell) == [1]
    stop_supervised(name)
  end

  test "the sweeper drops the entry that a crash left behind a migrated cell", ctx do
    Lists.set(%{"a" => 3, "b" => 0, "c" => 0})
    name = :"cell_migrations_example_#{System.unique_integer([:positive])}"
    start(ctx, name)
    cell = "a.1-#{System.unique_integer([:positive])}"

    assert {:ok, _} = Alambique.touch(name, cell)
    :ok = Alambique.hibernate(name, cell)
    assert index_of(ctx, cell) == [3]

    # The entry of the previous version, which the crashed wake did not delete.
    {:ok, _} =
      MemoryStore.put(ctx.store_config, "cells-test/schema/0000000002/#{cell}", "", :none, %{})

    assert index_of(ctx, cell) == [2, 3]
    assert {:ok, _} = Alambique.sweep(name)

    assert index_of(ctx, cell) == [3]
    stop_supervised(name)
  end

  # Generators

  defp op do
    inner =
      frequency([
        {6, tuple({constant(:write), member_of(@cells)})},
        {4, tuple({constant(:read), member_of(@cells)})},
        {2, tuple({constant(:hibernate), member_of(@cells)})},
        {3, tuple({constant(:migrate), member_of(@cells), boolean()})}
      ])

    fault =
      one_of([
        tuple({constant(:store), member_of([:error, :timeout, :lost_ack]), integer(1..4)}),
        tuple({constant(:segment), integer(1..4)}),
        tuple({constant(:crash), member_of(@points)}),
        constant(:none)
      ])

    # The puts of the pair are held, so no fault runs with it.
    held = tuple({constant(:held), member_of(@cells)})

    frequency([
      {10, tuple({constant(:run), fault, inner})},
      {2, tuple({constant(:run), constant(:none), held})},
      {3, tuple({constant(:deploy), member_of(@namespaces)})},
      {1, constant(:restart)}
    ])
  end

  # Scenario

  property "every cell is served and kept at the version of its namespace", ctx do
    check all(
            starts <- list_of(integer(0..2), length: length(@namespaces)),
            ops <- list_of(op(), min_length: 6, max_length: 16),
            max_runs: 40,
            max_shrinking_steps: 0
          ) do
      versions = Map.new(Enum.zip(@namespaces, starts))
      Lists.set(versions)
      name = :"cell_migrations_#{System.unique_integer([:positive])}"
      {:ok, gate} = GatedStore.start_link()
      raw = ctx.store
      ctx = %{ctx | store: GatedStore.wrap(gate, raw)}
      start(ctx, name)

      # The namespace of "a.1-17" is "a".
      cells = for c <- @cells, do: "#{c}-#{System.unique_integer([:positive])}"
      names = Map.new(Enum.zip(@cells, cells))
      guard = AckGuard.attach(raw, "cells-test", &(&1 in cells))

      state = %{
        seq: 0,
        faulty?: false,
        gate: gate,
        guard: guard,
        restarted: false,
        acked: Map.new(cells, &{&1, []}),
        unknown: Map.new(cells, &{&1, []})
      }

      state = Enum.reduce(ops, state, &step(&1, &2, ctx, name, names))

      # Faults are off: every cell sleeps, then the sweeper runs.
      MemoryStore.clear(ctx.store_config)
      for cell <- cells, do: safely(fn -> Alambique.hibernate(name, cell) end)
      # Failures in a row opened the breaker of the store (design §3): it
      # refuses requests until its cool-down ends, and a wake that failed
      # because of the store waits out its backoff.
      await_breaker_probing(name)

      for _ <- 1..3 do
        await_wake_backoff(name, cells)
        Alambique.sweep(name)
      end

      if state.restarted, do: settle(ctx, name, cells)
      for cell <- cells, do: safely(fn -> Alambique.hibernate(name, cell) end)

      for cell <- cells, durable_version(ctx, cell) > 0 do
        n = Lists.version(cell)
        assert durable_version(ctx, cell) == n, "#{cell} not migrated by the sweeper"
        assert index_of(ctx, cell) == [n], "#{cell} listed at #{inspect(index_of(ctx, cell))}"
        check_rows(ctx, state, cell)
      end

      assert_acks_landed(state)
      AckGuard.detach(guard)
      GenServer.stop(gate)
      stop_supervised(name)
      MemoryStore.clear(ctx.store_config)
    end
  end

  # Cells stored ahead of the code

  defp ahead_op(count) do
    index = integer(0..(count - 1))

    frequency([
      {4, map(index, &{:read, &1})},
      {4, map(index, &{:write, &1})},
      {3, map(tuple({index, boolean()}), fn {i, wake} -> {:migrate, i, wake} end)},
      {2, map(index, &{:hibernate, &1})}
    ])
  end

  # A cell stored `edge` versions past the window is refused with the version
  # it has, and a cell within the window, at its edge included, is served
  # without changing its version.
  property "a cell ahead of the code is refused beyond the window and served within it", ctx do
    check all(
            n <- integer(1..4),
            edges <- list_of(member_of([-1, 0, 1, 2]), min_length: 2, max_length: 4),
            ops <- list_of(ahead_op(length(edges)), min_length: 4, max_length: 14),
            max_runs: 15,
            max_shrinking_steps: 0
          ) do
      run_ahead(ctx, n, edges, ops)
    end
  end

  defp run_ahead(ctx, n, edges, ops) do
    main = start_instance(ctx, migrations: Lists.list(n))
    limit = n + Config.fetch!(main).migration_window

    cells =
      edges
      |> Enum.map(&ahead_cell(ctx, limit + &1, limit))
      |> Enum.with_index()
      |> Map.new(fn {cell, i} -> {i, cell} end)

    cells = Enum.reduce(ops, cells, &ahead_step(&1, &2, main))

    for {_, cell} <- cells do
      _ = Alambique.hibernate(main, cell.id)
      assert durable_rows(ctx, cell.id, "PRAGMA user_version") == [[cell.version]]
      assert durable_rows(ctx, cell.id, "SELECT n FROM t ORDER BY n") == sorted(cell)
    end

    stop_supervised(main)
  end

  # A cell that newer code woke and wrote three rows into.
  defp ahead_cell(ctx, version, limit) do
    cell = unique_cell()
    newer = start_instance(ctx, migrations: Lists.list(version))

    for k <- 1..3,
        do: {:ok, _} = Alambique.execute(newer, cell, "INSERT INTO t (n) VALUES (?)", [k])

    :ok = Alambique.hibernate(newer, cell)
    stop_supervised(newer)
    assert durable_rows(ctx, cell, "PRAGMA user_version") == [[version]]

    %{id: cell, version: version, refused?: version > limit, rows: [1, 2, 3]}
  end

  defp sorted(cell), do: cell.rows |> Enum.sort() |> Enum.map(&[&1])
  defp refusal(cell), do: {:error, {:schema_too_new, cell.version}}

  defp ahead_step({:read, i}, cells, main) do
    cell = cells[i]
    answer = Alambique.query(main, cell.id, "SELECT n FROM t ORDER BY n")

    if cell.refused? do
      assert answer == refusal(cell)
    else
      assert {:ok, %{rows: rows}} = answer
      assert rows == sorted(cell)
    end

    cells
  end

  defp ahead_step({:write, i}, cells, main) do
    cell = cells[i]
    value = 100 + System.unique_integer([:positive])
    answer = Alambique.execute(main, cell.id, "INSERT INTO t (n) VALUES (?)", [value])

    if cell.refused? do
      assert answer == refusal(cell)
      cells
    else
      assert {:ok, _} = answer
      Map.put(cells, i, %{cell | rows: [value | cell.rows]})
    end
  end

  # A cell nobody owns migrates at its next wake, unless `wake` is set.
  defp ahead_step({:migrate, i, wake}, cells, main) do
    cell = cells[i]
    asleep? = Alambique.owner(main, cell.id) == :none
    answer = Alambique.migrate(main, cell.id, wake: wake)

    cond do
      cell.refused? and wake -> assert answer == refusal(cell)
      asleep? and not wake -> assert answer == {:ok, :asleep}
      true -> assert answer == {:ok, cell.version}
    end

    cells
  end

  defp ahead_step({:hibernate, i}, cells, main) do
    _ = Alambique.hibernate(main, cells[i].id)
    cells
  end

  defp assert_acks_landed(%{guard: guard}) do
    assert AckGuard.violations(guard) == [],
           "acknowledged before the put landed: #{inspect(AckGuard.violations(guard))}"
  end

  # A restart is a new instance: the leases that a crashed cell left are not
  # its own and expire first (design §5). Sweep until every cell is migrated and
  # listed only at its version: a stale entry that a crash left stays until the
  # sweeper reaches the cell, which waits for the lease.
  defp settle(ctx, name, cells) do
    deadline = System.monotonic_time(:millisecond) + 10_000

    Stream.repeatedly(fn ->
      Process.sleep(200)
      await_wake_backoff(name, cells)
      Alambique.sweep(name)
      for cell <- cells, do: safely(fn -> Alambique.hibernate(name, cell) end)

      Enum.all?(cells, fn cell ->
        n = Lists.version(cell)
        durable_version(ctx, cell) in [0, n] and index_of(ctx, cell) in [[], [n]]
      end)
    end)
    |> Enum.find(&(&1 or System.monotonic_time(:millisecond) > deadline))
  end

  defp await_breaker_probing(name) do
    eventually(
      fn -> Enum.all?(Alambique.breaker_status(name), fn {_, %{state: s}} -> s != :open end) end,
      15_000
    )
  end

  defp start(ctx, name),
    do:
      start_instance(ctx,
        name: name,
        migrations: {Lists, :migrations},
        op_timeout_ms: 300,
        # A failed put reaches the cell, which retries it after a short backoff.
        retry: [max_attempts: 1],
        upload: [retry_backoff_ms: 5, retry_backoff_max_ms: 20]
      )

  defp step({:deploy, ns}, state, _ctx, _name, _names) do
    versions =
      Map.update!(:persistent_term.get({Lists, :versions}), ns, &min(&1 + 1, @max_version))

    Lists.set(versions)
    state
  end

  defp step(:restart, state, ctx, name, _names) do
    :ok = stop_supervised(name)
    start(ctx, name)
    %{state | restarted: true}
  end

  defp step({:run, fault, inner}, state, ctx, name, names) do
    # The cell is awake before its segment puts fail: it is the write that meets them.
    with {:segment, _} <- fault, base when is_binary(base) <- elem(inner, 1) do
      _ = safely(fn -> Alambique.query(name, names[base], "SELECT 1") end)
    end

    arm(fault, ctx)
    state = %{state | faulty?: state.faulty? or fault != :none}

    try do
      {state, cell} = run(inner, state, name, names)
      CrashPoints.reset()
      assert_acks_landed(state)
      verify(ctx, state, cell)
      state
    after
      CrashPoints.reset()
    end
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

  defp run({:write, base}, state, name, names) do
    cell = names[base]
    n = state.seq + 1
    asleep? = Alambique.owner(name, cell) == :none
    expected = Lists.version(cell)
    result = safely(fn -> write(name, cell, n) end)
    {record(%{state | seq: n}, name, cell, n, result, asleep?, expected), cell}
  end

  # Two writes whose puts are held, the later one landing first.
  defp run({:held, base}, state, name, names) do
    cell = names[base]
    expected = Lists.version(cell)
    asleep? = Alambique.owner(name, cell) == :none
    _ = safely(fn -> Alambique.query(name, cell, "SELECT 1") end)
    pid = Cell.whereis(Config.fetch!(name), cell)

    if pid do
      ns = [state.seq + 1, state.seq + 2]

      answers =
        HeldWrites.run(state.gate, cell, pid, 8, ns, &safely(fn -> write(name, cell, &1) end))

      state = %{state | seq: state.seq + 2}

      {Enum.reduce(answers, state, fn {n, result}, state ->
         record(state, name, cell, n, result, asleep?, expected)
       end), cell}
    else
      {state, cell}
    end
  end

  defp run({:read, base}, state, name, names) do
    cell = names[base]
    asleep? = Alambique.owner(name, cell) == :none
    expected = Lists.version(cell)

    case safely(fn -> Alambique.query(name, cell, "SELECT n FROM t") end) do
      {:ok, %{rows: rows}} ->
        seen = List.flatten(rows)
        assert Enum.all?(state.acked[cell], &(&1 in seen)), "read lost an acknowledged row"
        known = state.acked[cell] ++ state.unknown[cell]
        assert Enum.all?(seen, &(&1 in known)), "read shows a row never written"
        served(name, cell, asleep?, expected)

      {:error, {:schema_too_new, v}} ->
        flunk("#{cell} refused at version #{v}")

      _ ->
        :ok
    end

    {state, cell}
  end

  defp run({:migrate, base, wake}, state, name, names) do
    cell = names[base]
    expected = Lists.version(cell)

    case safely(fn -> Alambique.migrate(name, cell, wake: wake) end) do
      {:ok, version} when is_integer(version) ->
        assert version >= expected, "#{cell} migrated to #{version} < #{expected}"

      {:error, {:schema_too_new, v}} ->
        flunk("#{cell} refused at version #{v}")

      _ ->
        :ok
    end

    {state, cell}
  end

  defp run({:hibernate, base}, state, name, names) do
    cell = names[base]
    safely(fn -> Alambique.hibernate(name, cell) end)
    {state, cell}
  end

  defp write(name, cell, n),
    do: Alambique.execute(name, cell, "INSERT INTO t (n) VALUES (?)", [n])

  # Until the first fault a write is acknowledged.
  defp record(state, name, cell, n, result, asleep?, expected) do
    case result do
      {:ok, _} ->
        served(name, cell, asleep?, expected)
        update_in(state.acked[cell], &[n | &1])

      {:error, {:schema_too_new, v}} ->
        flunk("#{cell} refused at version #{v}")

      other ->
        # A cell of a namespace with no migration has no table yet.
        assert state.faulty? or expected == 0 or not asleep?,
               "#{cell}: write #{n} answered #{inspect(other)} with no fault"

        update_in(state.unknown[cell], &[n | &1])
    end
  end

  # A cell that woke for this call is at the version of its namespace, or
  # ahead of it when the wake found it ahead.
  defp served(name, cell, asleep?, expected) do
    case safely(fn -> Alambique.query(name, cell, "PRAGMA user_version") end) do
      {:ok, %{rows: [[version]]}} ->
        assert version >= 1
        if asleep?, do: assert(version >= expected, "#{cell} woke at #{version} < #{expected}")

      _ ->
        :ok
    end
  end

  defp safely(fun) do
    fun.()
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Checks on the durable state

  defp durable_version(ctx, cell) do
    [[version]] = durable_rows(ctx, cell, "PRAGMA user_version")
    version
  end

  defp index_of(%{store_config: config}, cell) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/schema/", nil, 100_000)

    for "cells-test/schema/" <> rest <- keys,
        [v, ^cell] <- [String.split(rest, "/")],
        do: String.to_integer(v)
  end

  defp verify(ctx, state, cell) do
    MemoryStore.clear(ctx.store_config)
    version = durable_version(ctx, cell)

    if version >= 1 do
      assert version in index_of(ctx, cell),
             "#{cell} at version #{version} is missing from the index: #{inspect(index_of(ctx, cell))}"

      check_rows(ctx, state, cell)
    end
  end

  defp check_rows(ctx, state, cell) do
    rows = ctx |> durable_rows(cell, "SELECT n FROM t") |> List.flatten()
    assert rows == Enum.uniq(rows)
    assert Enum.all?(state.acked[cell], &(&1 in rows)), "#{cell} lost an acknowledged write"
    possible = state.acked[cell] ++ state.unknown[cell]
    assert Enum.all?(rows, &(&1 in possible)), "#{cell} holds a row that was never written"
  end
end
