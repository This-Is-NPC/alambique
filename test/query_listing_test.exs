defmodule Alambique.QueryListingTest do
  @moduledoc "Where Alambique.Query lists and reads cells: locations, bad ids and listing failures (design §14, §18)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Location, Query}
  alias Alambique.Query.{ListError, Row}
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]
  @count "SELECT count(*) FROM notes"

  defmodule Tenants do
    @moduledoc false
    @behaviour Alambique.Placement

    def validate_id(_opts, "t-" <> _), do: :ok
    def validate_id(_opts, _id), do: {:error, :not_a_tenant}
    def locate(opts, _id), do: {:ok, hd(opts[:locations])}
    def locations(opts), do: {:ok, opts[:locations]}
  end

  defmodule Down do
    @moduledoc false
    @behaviour Alambique.Placement

    def locate(opts, _id), do: {:ok, hd(opts[:locations])}

    def locations(opts),
      do:
        if(:atomics.get(opts[:down], 1) == 1, do: {:error, :down}, else: {:ok, opts[:locations]})
  end

  setup do
    base = setup_store()
    stores = for name <- [:a, :b], into: %{}, do: {name, start_store(name)}

    locations = [
      %Location{id: "a", store: {MemoryStore, stores.a}, prefix: "pa"},
      %Location{id: "b", store: {MemoryStore, stores.b}, prefix: "pb"}
    ]

    Map.merge(base, %{stores: stores, locations: locations})
  end

  defp start_store(name) do
    pid = start_supervised!(Supervisor.child_spec({MemoryStore, []}, id: name))
    MemoryStore.config(pid)
  end

  defp instance(ctx, placement),
    do: start_instance(ctx, placement: placement, migrations: @schema)

  defp insert(inst, cell, n),
    do:
      for(
        i <- 1..n,
        do: {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO notes (body) VALUES ('#{i}')")
      )

  # Two locations, a tenant cell in each. Tenants always locate to "a", so the
  # cell of "b" is written through an instance that places it there.
  defp seeded(ctx) do
    only_a = {Tenants, locations: [hd(ctx.locations)]}
    only_b = {Tenants, locations: [List.last(ctx.locations)]}
    insert(instance(ctx, only_a), "t-1", 1)
    insert(instance(ctx, only_b), "t-2", 2)
    instance(ctx, {Tenants, locations: ctx.locations})
  end

  describe "the :location option" do
    test "names a location of the placement, or gives it, or lists several", ctx do
      reader = seeded(ctx)
      [a, b] = ctx.locations

      assert Query.cells(reader) |> Enum.sort() == ["t-1", "t-2"]
      assert Query.cells(reader, location: "a") |> Enum.to_list() == ["t-1"]
      assert Query.cells(reader, location: "b") |> Enum.to_list() == ["t-2"]
      assert Query.cells(reader, location: a) |> Enum.to_list() == ["t-1"]
      assert Query.cells(reader, location: [b, a]) |> Enum.to_list() == ["t-2", "t-1"]

      assert [%Row{cell: "t-2", values: [2]}] =
               Query.all(reader, ["t-2"], @count, [], location: "b").rows
    end

    test "an unknown id is refused with the reason", ctx do
      reader = seeded(ctx)

      assert_raise ArgumentError, ~r/unknown :location "zz": /, fn ->
        Enum.to_list(Query.cells(reader, location: "zz"))
      end
    end

    test "cells named explicitly are read in one location", ctx do
      reader = seeded(ctx)

      assert_raise ArgumentError, ~r/listed cells are read in one :location/, fn ->
        Query.all(reader, ["t-1"], @count, [], location: ctx.locations)
      end
    end

    test "a Config stands for its instance name", ctx do
      reader = seeded(ctx)
      config = Alambique.Config.fetch!(reader)
      assert Query.cells(config) |> Enum.sort() == ["t-1", "t-2"]
    end
  end

  describe "objects and ids the placement does not accept" do
    test "a stray object under the cells directory is not a cell", ctx do
      reader = seeded(ctx)
      {:ok, _} = MemoryStore.put(ctx.stores.a, "pa/cells/stray", "x", :none, %{})
      assert Query.cells(reader, location: "a") |> Enum.to_list() == ["t-1"]
    end

    test "a listed cell the placement rejects is reported, not dropped as moved", ctx do
      reader = seeded(ctx)
      alien = Alambique.Replication.Keys.frozen("pa", "alien")
      {:ok, _} = MemoryStore.put(ctx.stores.a, alien, "{}", :none, %{})

      assert Query.cells(reader, location: "a") |> Enum.sort() == ["alien", "t-1"]

      assert %{rows: [%Row{cell: "t-1"}], errors: [{"alien", _}]} =
               Query.all(reader, {:prefix, ""}, @count, [], location: "a")
    end

    test "an explicit id the placement rejects is not read", ctx do
      reader = seeded(ctx)

      assert %{rows: [%Row{cell: "t-1"}], errors: [{"alien", :invalid_cell}]} =
               Query.all(reader, ["alien", "t-1"], @count)
    end

    test "an id that is not a string is an error", ctx do
      reader = seeded(ctx)

      assert %{rows: [], errors: errors} =
               Query.all(reader, [123, :atom, {:t, 1}, %{a: 1}, 1.5], @count)

      assert Enum.sort(errors) ==
               Enum.sort(
                 for cell <- [123, :atom, {:t, 1}, %{a: 1}, 1.5], do: {cell, :invalid_cell}
               )
    end
  end

  describe "listing failures raise Alambique.Query.ListError" do
    test "a store that fails to list", ctx do
      reader = instance(ctx, {Tenants, locations: ctx.locations})
      MemoryStore.inject(ctx.stores.a, kind: :error, times: 1_000, ops: [:list], reason: :boom)

      error =
        assert_raise ListError, fn ->
          reader |> Query.cells(location: "a", timeout_ms: 200) |> Enum.to_list()
        end

      assert error.reason == :boom
      assert Exception.message(error) == "listing cells failed: :boom"

      assert_raise ListError, fn ->
        Query.all(reader, :all, @count, [], location: "a", timeout_ms: 200)
      end
    end

    test "a placement that cannot name its locations", ctx do
      down = :atomics.new(1, [])
      reader = instance(ctx, {Down, locations: ctx.locations, down: down})
      assert Query.cells(reader) |> Enum.to_list() == []
      :atomics.put(down, 1, 1)

      assert_raise ListError, "listing cells failed: {:placement, :down}", fn ->
        Enum.to_list(Query.cells(reader))
      end
    end
  end
end
