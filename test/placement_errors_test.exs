defmodule Alambique.PlacementErrorsTest do
  @moduledoc "How the instance treats a placement that fails or answers badly (design §18)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, Location, Placement}

  defmodule Dynamic do
    @moduledoc false
    # Answers what the test put in the agent at the time of the call.
    @behaviour Alambique.Placement

    def validate_options(_opts), do: :ok
    def locate(opts, _cell), do: Agent.get(opts[:agent], & &1.locate)
    def locations(opts), do: Agent.get(opts[:agent], & &1.locations)
  end

  setup do
    ctx = setup_store()
    agent = start_supervised!({Agent, fn -> %{} end})
    a = %Location{id: "a", store: ctx.store, prefix: "pa"}
    b = %Location{id: "b", store: ctx.store, prefix: "pb"}
    answer(agent, locate: {:ok, a}, locations: {:ok, [a, b]})
    inst = start_instance(ctx, placement: {Dynamic, agent: agent})
    %{agent: agent, a: a, b: b, config: Config.fetch!(inst)}
  end

  defp answer(agent, answers), do: Agent.update(agent, &Map.merge(&1, Map.new(answers)))

  test "a placement that fails to locate is an error that names it", ctx do
    answer(ctx.agent, locate: {:error, :down})
    assert {:error, {:placement, :down}} = Placement.locate(ctx.config, "t1")
  end

  test "a location that is not valid is refused and not remembered", ctx do
    bad = %Location{id: "", store: nil, prefix: nil}
    answer(ctx.agent, locate: {:ok, bad})
    assert {:error, {:bad_location, ^bad}} = Placement.locate(ctx.config, "t1")

    answer(ctx.agent, locate: {:ok, ctx.a})
    assert {:ok, ctx.a} == Placement.locate(ctx.config, "t1")
  end

  test "a placement that fails to list its locations is an error that names it", ctx do
    answer(ctx.agent, locations: {:error, :down})
    assert {:error, {:placement, :down}} = Placement.locations(ctx.config)
    assert {:error, {:placement, :down}} = Placement.location(ctx.config, "a")
    assert {:error, {:placement, :down}} = Placement.listed_location(ctx.config, nil)
  end

  test "a list with a location that is not valid is refused", ctx do
    list = [ctx.a, %Location{id: "x", store: :nope, prefix: "p"}]
    answer(ctx.agent, locations: {:ok, list})
    assert {:error, {:bad_locations, ^list}} = Placement.locations(ctx.config)
  end

  test "listing needs a location when there are several", ctx do
    assert {:error, :location_required} = Placement.listed_location(ctx.config, nil)
    assert {:ok, ctx.b} == Placement.listed_location(ctx.config, "b")
    assert {:error, {:unknown_location, "zzz"}} = Placement.listed_location(ctx.config, "zzz")
  end

  test "a location given to a listing must be valid", ctx do
    assert {:ok, ctx.a} == Placement.listed_location(ctx.config, ctx.a)

    assert_raise ArgumentError, ~r/not a valid location/, fn ->
      Placement.listed_location(ctx.config, %Location{id: "", store: nil, prefix: nil})
    end

    assert_raise ArgumentError, ~r/location id or a location/, fn ->
      Placement.listed_location(ctx.config, :a)
    end
  end

  test "the single placement rejects options that are not a keyword list" do
    assert Placement.Single.validate_options(:nope) == "options must be a keyword list"
    assert Placement.Single.validate_options([]) == ":store must be {module, config}"
    assert Placement.Single.validate_options(store: {Foo, 1}, prefix: "") =~ ":prefix"
    assert Placement.Single.validate_options(store: {Foo, 1}) == :ok
  end
end
