defmodule Alambique.StoreConformanceTest do
  @moduledoc "The start checks that the store honours If-None-Match and If-Match."
  use ExUnit.Case, async: true

  import Alambique.CellHelpers, only: [setup_store: 0]

  alias Alambique.Store.Conformance

  # A store that drops the conditions named in `ignore`, as one without
  # conditional writes does, or fails every put with `fail`.
  defmodule LaxStore do
    @moduledoc false
    @behaviour Alambique.Store

    def wrap(inner, opts), do: {__MODULE__, %{inner: inner, opts: opts}}

    @impl true
    def put(%{inner: {m, c}, opts: opts}, key, body, condition, meta) do
      cond do
        Keyword.has_key?(opts, :fail) -> {:error, opts[:fail]}
        kind(condition) in Keyword.get(opts, :ignore, []) -> m.put(c, key, body, :none, meta)
        true -> m.put(c, key, body, condition, meta)
      end
    end

    defp kind(:if_none_match), do: :if_none_match
    defp kind({:if_match, _}), do: :if_match
    defp kind(:none), do: :none

    @impl true
    def get(%{inner: {m, c}}, key), do: m.get(c, key)
    @impl true
    def head(%{inner: {m, c}}, key), do: m.head(c, key)
    @impl true
    def get_range(%{inner: {m, c}}, key, o, l), do: m.get_range(c, key, o, l)
    @impl true
    def list(%{inner: {m, c}}, prefix, after_key, limit), do: m.list(c, prefix, after_key, limit)
    @impl true
    def delete(%{inner: {m, c}}, key), do: m.delete(c, key)
    @impl true
    def location(%{inner: inner}), do: Alambique.Store.location(inner)
  end

  setup do
    env = setup_store()
    on_exit(fn -> File.rm_rf(env.dir) end)
    env
  end

  defp keys(%{store: {m, c}}), do: elem(m.list(c, "", nil, 1_000), 1)

  describe "Conformance.check/3" do
    test "a conforming store passes and the scratch key is deleted", ctx do
      assert Conformance.check(ctx.store, "p", 1_000) == :ok
      assert keys(ctx) == []
    end

    test "a store that ignores If-None-Match fails", ctx do
      store = LaxStore.wrap(ctx.store, ignore: [:if_none_match])
      assert Conformance.check(store, "p", 1_000) == {:error, {:not_conformant, [:if_none_match]}}
      assert keys(ctx) == []
    end

    test "a store that ignores If-Match fails", ctx do
      store = LaxStore.wrap(ctx.store, ignore: [:if_match])
      assert Conformance.check(store, "p", 1_000) == {:error, {:not_conformant, [:if_match]}}
      assert keys(ctx) == []
    end

    test "a store that fails every put is inconclusive", ctx do
      store = LaxStore.wrap(ctx.store, fail: :econnrefused)
      assert Conformance.check(store, "p", 1_000) == {:inconclusive, :econnrefused}
    end
  end

  describe "instance start" do
    defp opts(ctx, store, extra \\ []) do
      name = :"conformance_#{System.unique_integer([:positive])}"

      Keyword.merge(
        [
          name: name,
          store: store,
          prefix: "conformance",
          data_dir: Path.join(ctx.dir, Atom.to_string(name)),
          node_id: Atom.to_string(name),
          lease_ttl_ms: 3_000,
          skew_margin_ms: 500,
          op_timeout_ms: 2_000
        ],
        extra
      )
    end

    test "is refused for a store without conditional writes", ctx do
      store = LaxStore.wrap(ctx.store, ignore: [:if_none_match, :if_match])

      assert {:error, {reason, _}} = start_supervised({Alambique, opts(ctx, store)})
      assert reason == {:store_not_conformant, "default", [:if_none_match, :if_match]}
    end

    test "check_store: false starts it anyway", ctx do
      store = LaxStore.wrap(ctx.store, ignore: [:if_none_match, :if_match])
      assert {:ok, _} = start_supervised({Alambique, opts(ctx, store, check_store: false)})
    end

    test "starts on a conforming store and leaves no scratch key", ctx do
      assert {:ok, _} = start_supervised({Alambique, opts(ctx, ctx.store)})
      assert keys(ctx) == []
    end

    test "starts when the store cannot be reached for the check", ctx do
      store = LaxStore.wrap(ctx.store, fail: :econnrefused)
      assert {:ok, _} = start_supervised({Alambique, opts(ctx, store)})
    end
  end
end
