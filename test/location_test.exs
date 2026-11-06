defmodule Alambique.LocationTest do
  @moduledoc "What counts as a well formed location (design §18) and the optional callbacks of a store (design §3)."
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.{Location, Store}
  alias AlambiqueTestkit.MemoryStore

  defmodule Plain do
    @moduledoc false
  end

  defmodule Secret do
    @moduledoc false
    def location(%{bucket: bucket}), do: {:bucket, bucket}
    def conceal(%{key: key} = config), do: %{config | key: fn -> key end}
  end

  defp location(fields),
    do: struct!(Location, [id: "a", store: {MemoryStore, %{}}, prefix: "p"] ++ fields)

  describe "Location.valid?/1" do
    test "accepts a location with an id, a store module and a prefix" do
      assert Location.valid?(location([]))
      assert Location.valid?(location(store: {Plain, :anything}))
    end

    test "refuses an empty or non-string id or prefix, and a store that is not a module" do
      for bad <- [
            location(id: ""),
            location(id: :a),
            location(id: nil),
            location(prefix: ""),
            location(prefix: :p),
            location(prefix: nil),
            location(store: {"MemoryStore", %{}}),
            location(store: MemoryStore),
            location(store: nil)
          ] do
        refute Location.valid?(bad), inspect(bad)
      end
    end

    test "refuses what is not a location" do
      refute Location.valid?(%{id: "a", store: {MemoryStore, %{}}, prefix: "p"})
      refute Location.valid?({"a", {MemoryStore, %{}}, "p"})
      refute Location.valid?(nil)
    end

    property "any non-empty id and prefix with a module store is valid, and each field matters" do
      check all(
              id <- StreamData.string(:printable, min_length: 1),
              prefix <- StreamData.string(:printable, min_length: 1),
              config <- StreamData.term()
            ) do
        good = %Location{id: id, store: {MemoryStore, config}, prefix: prefix}
        assert Location.valid?(good)
        refute Location.valid?(%{good | id: ""})
        refute Location.valid?(%{good | prefix: ""})
        refute Location.valid?(%{good | id: :id})
      end
    end
  end

  describe "Store.location/1 and Store.conceal/1" do
    test "use the callbacks of a store that has them" do
      store = {Secret, %{bucket: "b", key: "k"}}
      assert Store.location(store) == {:bucket, "b"}
      assert {Secret, %{bucket: "b", key: key}} = Store.conceal(store)
      assert key.() == "k"
    end

    test "fall back to the config, or to the store itself, for a store without them" do
      store = {Plain, %{key: "k"}}
      assert Store.location(store) == %{key: "k"}
      assert Store.conceal(store) == store

      unloaded = {No.Such.Store, :config}
      assert Store.location(unloaded) == :config
      assert Store.conceal(unloaded) == unloaded
    end

    test "load a store module that is not loaded yet" do
      dir = Alambique.TestDirs.new("store-beam")
      on_exit(fn -> File.rm_rf(dir) end)
      mod = Module.concat(Alambique.LocationTest, "Lazy#{System.unique_integer([:positive])}")

      [{^mod, beam}] =
        Code.compile_quoted(
          quote do
            defmodule unquote(mod) do
              def location(config), do: {:lazy, config}
              def conceal(config), do: {:hidden, config}
            end
          end
        )

      File.write!(Path.join(dir, "#{mod}.beam"), beam)
      :code.purge(mod)
      :code.delete(mod)
      :code.purge(mod)
      refute :code.is_loaded(mod)
      Code.prepend_path(dir)
      on_exit(fn -> Code.delete_path(dir) end)

      assert Store.conceal({mod, :c}) == {mod, {:hidden, :c}}
      :code.purge(mod)
      :code.delete(mod)
      :code.purge(mod)
      assert Store.location({mod, :c}) == {:lazy, :c}
    end

    test "the in-memory store has neither" do
      store = {MemoryStore, %{server: :s}}
      assert Store.location(store) == %{server: :s}
      assert Store.conceal(store) == store
    end

    test "a value that is not a store is returned as it is" do
      assert Store.conceal(:not_a_store) == :not_a_store
      assert Store.conceal(nil) == nil
    end
  end
end
