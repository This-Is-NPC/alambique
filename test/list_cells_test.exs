defmodule Alambique.ListCellsTest do
  @moduledoc "Alambique.list_cells/2 pages through the stored cells with the schema index (design §17)."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{CountingStore, Location, Schema}
  alias AlambiqueTestkit.MemoryStore

  @moduletag :capture_log

  @migrations [
    {1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"},
    {2, "ALTER TABLE t ADD COLUMN v TEXT"},
    {3, "ALTER TABLE t ADD COLUMN w TEXT"}
  ]

  setup do
    ctx = setup_store()
    {CountingStore, counter} = store = CountingStore.wrap(ctx.store)
    Map.merge(ctx, %{store: store, counter: counter})
  end

  # An instance whose code is at `version`, over the shared store.
  defp at(ctx, version, opts \\ []),
    do: start_instance(ctx, Keyword.put(opts, :migrations, Enum.take(@migrations, version)))

  defp wake(inst, cells),
    do: for(cell <- cells, do: assert({:ok, _} = Alambique.touch(inst, cell)))

  defp list!(inst, opts \\ []) do
    assert {:ok, %{cells: cells, next: next}} = Alambique.list_cells(inst, opts)
    {cells, next}
  end

  # Every page until `next` is nil.
  defp walk(inst, opts, after_id \\ nil, acc \\ []) do
    {cells, next} = list!(inst, Keyword.put(opts, :after, after_id))
    assert length(cells) <= Keyword.fetch!(opts, :limit)
    acc = [{cells, next} | acc]
    assert length(acc) <= 100, "next never ends"
    if next, do: walk(inst, opts, next, acc), else: Enum.reverse(acc)
  end

  describe "one location" do
    test "lists asleep and awake cells of every version, version 0 included", ctx do
      v0 = at(ctx, 0)
      v2 = at(ctx, 2)
      wake(v0, ~w(c0 d0))
      assert {:ok, _} = Alambique.execute(v2, "c2", "INSERT INTO t (n) VALUES (1)")
      wake(v2, ~w(a2))
      assert :ok = Alambique.hibernate(v2, "c2")

      assert {~w(a2 c0 c2 d0), nil} = list!(v2)
      assert Alambique.owner(v2, "c2") == :none
      assert {:ok, _} = Alambique.owner(v2, "a2")
    end

    test "an instance without migrations keeps the index", ctx do
      inst = at(ctx, 0)
      wake(inst, ~w(x y))

      assert {~w(x y), nil} = list!(inst)
      assert {:ok, [0]} = versions(inst)
    end

    test "an empty location lists nothing", ctx do
      assert {[], nil} = list!(at(ctx, 1))
    end

    test "filters by prefix, which may be a cell id", ctx do
      inst = at(ctx, 1)
      wake(inst, ~w(a ab abc abd b ba))

      assert {~w(ab abc abd), nil} = list!(inst, prefix: "ab")
      assert {~w(b ba), nil} = list!(inst, prefix: "b")
      assert {[], nil} = list!(inst, prefix: "c")
      assert {~w(a ab abc abd b ba), nil} = list!(inst, prefix: "")
    end

    test "pages of one id follow next", ctx do
      inst = at(ctx, 1)
      wake(inst, ~w(a b c))

      assert [{["a"], "a"}, {["b"], "b"}, {["c"], nil}] = walk(inst, limit: 1)
      assert [{["a", "b"], "b"}, {["c"], nil}] = walk(inst, limit: 2)
      assert [{~w(a b c), nil}] = walk(inst, limit: 3)
    end

    test "after inside and outside the prefix", ctx do
      inst = at(ctx, 1)
      wake(inst, ~w(a ab abc abd b))

      assert {~w(abd), nil} = list!(inst, prefix: "ab", after: "abc")
      assert {~w(ab abc abd), nil} = list!(inst, prefix: "ab", after: "a")
      assert {~w(abc abd), nil} = list!(inst, prefix: "ab", after: "ab")
      assert {[], nil} = list!(inst, prefix: "ab", after: "b")
      assert {[], nil} = list!(inst, prefix: "ab", after: "abd")
      assert {~w(ab abc abd), nil} = list!(inst, prefix: "ab", after: "")
    end

    test "a cell with entries at two versions is listed once", ctx do
      inst = at(ctx, 1)
      wake(inst, ~w(a b c))
      config = Alambique.Config.fetch!(inst)
      {:ok, [location]} = Alambique.Placement.locations(config)

      assert :ok =
               Schema.put(config, location, "b", 3, System.monotonic_time(:millisecond) + 5_000)

      assert {:ok, [1, 3]} = versions(inst)
      assert {~w(a b c), nil} = list!(inst)
      assert [{["a"], "a"}, {["b"], "b"}, {["c"], nil}] = walk(inst, limit: 1)
    end

    test "a cell that migrated is listed once, at its new version", ctx do
      old = at(ctx, 1)
      wake(old, ~w(m n))
      for cell <- ~w(m n), do: assert(:ok = Alambique.hibernate(old, cell))
      new = at(ctx, 2)
      wake(new, ~w(m))

      assert {~w(m n), nil} = list!(new)
      assert {:ok, [1, 2]} = versions(new)
    end

    test "rejects invalid options", ctx do
      inst = at(ctx, 1)

      for opts <- [
            [limit: 0],
            [limit: 1_001],
            [limit: "5"],
            [prefix: :a],
            [after: 5],
            [timeout_ms: 0],
            [location: 5],
            [bogus: 1]
          ] do
        assert_raise ArgumentError, fn -> Alambique.list_cells(inst, opts) end
      end
    end

    test "the instance options set the page size, the largest limit and are validated", ctx do
      inst = at(ctx, 1, list_cells: [page_size: 2, max_limit: 3])
      wake(inst, ~w(a b c d))

      assert {~w(a b), "b"} = list!(inst)
      assert {~w(a b c), "c"} = list!(inst, limit: 3)
      assert_raise ArgumentError, fn -> Alambique.list_cells(inst, limit: 4) end

      for bad <- [[page_size: 0], [max_limit: 1.5], [page_size: 5, max_limit: 4], [bogus: 1]] do
        assert_raise ArgumentError, fn -> Alambique.Config.build(base_options(ctx, bad)) end
      end
    end

    test "a store failure is an error", ctx do
      inst = at(ctx, 1)
      wake(inst, ~w(a))
      MemoryStore.inject(ctx.store_config, kind: :error, ops: [:list], reason: :boom)
      assert {:error, _} = Alambique.list_cells(inst, timeout_ms: 300)
    end
  end

  describe "several locations" do
    setup ctx do
      stores = for name <- [:a, :b, :table], into: %{}, do: {name, start_store(name)}

      locations = [
        %Location{id: "a", store: {MemoryStore, stores.a}, prefix: "pa"},
        %Location{id: "b", store: {MemoryStore, stores.b}, prefix: "pb"}
      ]

      placement =
        {AlambiqueTestkit.Placement,
         locations: locations,
         table: {MemoryStore, stores.table},
         initial: %{"home-a" => "a", "home-b" => "b", "other-a" => "a"}}

      %{
        inst: start_instance(ctx, placement: placement, migrations: @migrations),
        locations: locations
      }
    end

    test "needs a location and lists each one's cells", %{inst: inst, locations: [a, b]} do
      wake(inst, ~w(home-a other-a home-b))

      assert {:error, :location_required} = Alambique.list_cells(inst)
      assert {~w(home-a other-a), nil} = list!(inst, location: "a")
      assert {~w(home-a other-a), nil} = list!(inst, location: a)
      assert {~w(home-b), nil} = list!(inst, location: b)
      assert {~w(home-a), nil} = list!(inst, location: "a", prefix: "home")

      assert {:error, {:unknown_location, "zz"}} = Alambique.list_cells(inst, location: "zz")
    end

    test "a relocated cell is listed only at its new location", %{inst: inst} do
      wake(inst, ~w(home-a other-a))
      assert :ok = Alambique.hibernate(inst, "home-a")
      assert {:ok, %Location{id: "b"}} = Alambique.relocate(inst, "home-a", "b")

      assert {~w(other-a), nil} = list!(inst, location: "a")
      assert {~w(home-a), nil} = list!(inst, location: "b")
    end
  end

  describe "cost" do
    test "a new cell that migrates deletes no index entry", ctx do
      inst = at(ctx, 2)
      CountingStore.reset(ctx.counter)
      wake(inst, ["fresh"])

      assert CountingStore.count(ctx.counter, :delete) == 0
      assert {:ok, [2]} = versions(inst)
    end

    test "an entry the cell wrote at version 0 moves when it migrates later", ctx do
      inst = start_instance(ctx, migrations: {__MODULE__, :grown, []})
      :persistent_term.put({__MODULE__, :grown}, [])
      on_exit(fn -> :persistent_term.erase({__MODULE__, :grown}) end)
      wake(inst, ["late"])
      assert {:ok, [0]} = versions(inst)

      :persistent_term.put({__MODULE__, :grown}, Enum.take(@migrations, 1))
      assert {:ok, 1} = Alambique.migrate(inst, "late")
      assert {:ok, [1]} = versions(inst)
    end

    test "a page costs the same whatever the number of cells", ctx do
      old = at(ctx, 1)
      new = at(ctx, 2)
      wake(old, for(n <- 1..5, do: "o#{n}"))
      wake(new, for(n <- 1..5, do: "n#{n}"))
      baseline = requests(ctx, new, limit: 3)

      wake(old, for(n <- 6..50, do: "o#{n}"))
      wake(new, for(n <- 6..50, do: "n#{n}"))
      assert requests(ctx, new, limit: 3) == baseline
      assert requests(ctx, new, limit: 3, prefix: "n", after: "n4") == baseline

      # Two versions: one survey request each plus one, and one listing each.
      assert baseline == 2 * 2 + 1
    end

    test "the formula holds for random cells, prefixes and limits", ctx do
      insts = for version <- 0..3, do: at(ctx, version)
      last = List.last(insts)
      counter = :counters.new(1, [])

      check all(
              ids <- uniq_list_of(string(~c"ab", min_length: 1, max_length: 4), max_length: 8),
              versions <- list_of(integer(0..3), length: length(ids)),
              prefix <- string(~c"ab", max_length: 3),
              limit <- integer(1..5),
              max_runs: 40
            ) do
        :counters.add(counter, 1, 1)
        run = "r#{:counters.get(counter, 1)}-"
        cells = Enum.map(ids, &(run <> &1))

        woken = Enum.zip(cells, versions)
        for {cell, v} <- woken, do: assert({:ok, _} = Alambique.touch(Enum.at(insts, v), cell))

        for {cell, v} <- Enum.take_every(woken, 2),
            do: assert(:ok = Alambique.hibernate(Enum.at(insts, v), cell))

        # Stale entries at another version, as a migration in flight leaves.
        config = Alambique.Config.fetch!(last)
        {:ok, [location]} = Alambique.Placement.locations(config)

        for {cell, v} <- Enum.take_every(woken, 3),
            do: assert(:ok = Schema.put(config, location, cell, rem(v + 1, 4), deadline()))

        prefix = run <> prefix
        model = cells |> Enum.filter(&String.starts_with?(&1, prefix)) |> Enum.sort()

        {:ok, live} = versions(last)
        CountingStore.reset(ctx.counter)
        pages = walk_counting(ctx, last, prefix, limit)

        assert Enum.flat_map(pages, &elem(&1, 0)) == model

        assert Enum.all?(pages, fn {cells, _, requests} ->
                 length(cells) <= limit and requests <= 2 * length(live) + 1
               end)

        {final, others} = List.pop_at(pages, -1)
        assert elem(final, 1) == nil
        assert Enum.all?(others, &(elem(&1, 1) != nil))
      end
    end
  end

  defp walk_counting(ctx, inst, prefix, limit, after_id \\ nil, acc \\ []) do
    CountingStore.reset(ctx.counter)
    {cells, next} = list!(inst, prefix: prefix, limit: limit, after: after_id)
    acc = [{cells, next, CountingStore.count(ctx.counter, :list)} | acc]
    assert length(acc) <= 100, "next never ends"
    if next, do: walk_counting(ctx, inst, prefix, limit, next, acc), else: Enum.reverse(acc)
  end

  defp requests(ctx, inst, opts) do
    CountingStore.reset(ctx.counter)
    list!(inst, opts)
    CountingStore.count(ctx.counter, :list)
  end

  defp versions(inst) do
    config = Alambique.Config.fetch!(inst)
    {:ok, [location]} = Alambique.Placement.locations(config)
    Schema.versions(config, location, deadline())
  end

  @doc false
  def grown(_cell), do: :persistent_term.get({__MODULE__, :grown}, [])

  defp deadline, do: System.monotonic_time(:millisecond) + 5_000

  defp start_store(name) do
    pid = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: name))
    MemoryStore.config(pid)
  end

  defp base_options(ctx, list_cells),
    do: [name: :bad, store: ctx.store, data_dir: ctx.dir, list_cells: list_cells]
end
