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
      version (perhaps also at an older one, never nowhere).

  At the end the sweeper brings every listed cell to the version of its
  namespace and lists it only there. After a restart that takes until the
  leases that crashed cells left expire, and an entry left behind a migrated
  cell stays until then.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias AlambiqueTestkit.{CrashPoints, MemoryStore}

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

    def migrations(cell) do
      for k <- 1..version(cell)//1 do
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
        tuple({constant(:crash), member_of(@points)}),
        constant(:none)
      ])

    frequency([
      {10, tuple({constant(:run), fault, inner})},
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
      start(ctx, name)

      # The namespace of "a.1-17" is "a".
      cells = for c <- @cells, do: "#{c}-#{System.unique_integer([:positive])}"
      names = Map.new(Enum.zip(@cells, cells))

      state = %{
        seq: 0,
        restarted: false,
        acked: Map.new(cells, &{&1, []}),
        unknown: Map.new(cells, &{&1, []})
      }

      state = Enum.reduce(ops, state, &step(&1, &2, ctx, name, names))

      # Faults are off: every cell sleeps, then the sweeper runs.
      MemoryStore.clear(ctx.store_config)
      for cell <- cells, do: safely(fn -> Alambique.hibernate(name, cell) end)
      for _ <- 1..3, do: Alambique.sweep(name)
      if state.restarted, do: settle(ctx, name, cells)
      for cell <- cells, do: safely(fn -> Alambique.hibernate(name, cell) end)

      for cell <- cells, durable_version(ctx, cell) > 0 do
        n = Lists.version(cell)
        assert durable_version(ctx, cell) == n, "#{cell} not migrated by the sweeper"
        assert index_of(ctx, cell) == [n], "#{cell} listed at #{inspect(index_of(ctx, cell))}"
        check_rows(ctx, state, cell)
      end

      stop_supervised(name)
      MemoryStore.clear(ctx.store_config)
    end
  end

  # A restart is a new instance: the leases that a crashed cell left are not
  # its own and expire first (design §5). Sweep until every cell is migrated and
  # listed only at its version: a stale entry that a crash left stays until the
  # sweeper reaches the cell, which waits for the lease.
  defp settle(ctx, name, cells) do
    deadline = System.monotonic_time(:millisecond) + 10_000

    Stream.repeatedly(fn ->
      Process.sleep(200)
      Alambique.sweep(name)
      for cell <- cells, do: safely(fn -> Alambique.hibernate(name, cell) end)

      Enum.all?(cells, fn cell ->
        n = Lists.version(cell)
        durable_version(ctx, cell) in [0, n] and index_of(ctx, cell) in [[], [n]]
      end)
    end)
    |> Enum.find(&(&1 or System.monotonic_time(:millisecond) > deadline))
  end

  defp start(ctx, name),
    do:
      start_instance(ctx,
        name: name,
        migrations: {Lists, :migrations},
        op_timeout_ms: 300
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
    arm(fault, ctx)

    try do
      {state, cell} = run(inner, state, name, names)
      CrashPoints.reset()
      verify(ctx, state, cell)
      state
    after
      CrashPoints.reset()
    end
  end

  defp arm({:store, kind, times}, ctx),
    do: MemoryStore.inject(ctx.store_config, kind: kind, times: times, ms: 1)

  defp arm({:crash, point}, _ctx), do: CrashPoints.arm(point, :raise)
  defp arm(:none, _ctx), do: :ok

  defp run({:write, base}, state, name, names) do
    cell = names[base]
    n = state.seq + 1
    asleep? = Alambique.owner(name, cell) == :none
    expected = Lists.version(cell)

    result =
      safely(fn -> Alambique.execute(name, cell, "INSERT INTO t (n) VALUES (?)", [n]) end)

    state = %{state | seq: n}

    state =
      case result do
        {:ok, _} ->
          served(name, cell, asleep?, expected)
          update_in(state.acked[cell], &[n | &1])

        {:error, {:schema_too_new, v}} ->
          flunk("#{cell} refused at version #{v}")

        _ ->
          update_in(state.unknown[cell], &[n | &1])
      end

    {state, cell}
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
