defmodule Alambique.KeyValidationTest do
  @moduledoc "Keys and prefixes that name another place are refused: trash manifests, leases, prefixes and the mix tasks."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.LeaseHelpers

  alias Alambique.{Lease, Location, Trash}
  alias Alambique.Ops.Options
  alias Alambique.Placement.Single
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  defp new_store do
    pid = start_supervised!({MemoryStore, []})
    {MemoryStore, MemoryStore.config(pid)}
  end

  describe "trash manifests" do
    setup do
      store = new_store()
      {mod, cfg} = store
      ctx = Alambique.Replication.ctx(store: store, prefix: "app", node: "n", timeout_ms: 5_000)
      {:ok, mod: mod, cfg: cfg, ctx: ctx}
    end

    @tag :capture_log
    test "a cell with dot segments is refused and nothing is deleted", env do
      victim = "app/cells/x/../y/0000/0001.ltx"
      {:ok, _} = env.mod.put(env.cfg, victim, "x", :none, %{})
      key = "#{Trash.prefix("app")}0000000000000001/x/forged"
      body = Jason.encode!(%{cell: "x/../y", keys: [victim]})
      {:ok, _} = env.mod.put(env.cfg, key, body, :none, %{})

      assert %{manifests: 0, objects: 0, errors: 1} =
               Trash.reap_ctx(env.ctx, Map.new(Trash.defaults()))

      assert {:ok, _, _, _} = env.mod.get(env.cfg, victim)
    end

    @tag :capture_log
    test "a cell that differs from the manifest key is refused", env do
      other = Keys.segment("app", "other", 0, 1, 1)
      {:ok, _} = env.mod.put(env.cfg, other, "x", :none, %{})
      key = "#{Trash.prefix("app")}0000000000000001/mine/forged"
      body = Jason.encode!(%{cell: "other", keys: [other]})
      {:ok, _} = env.mod.put(env.cfg, key, body, :none, %{})

      assert %{manifests: 0, errors: 1} =
               Trash.reap_ctx(env.ctx, Map.new(Trash.defaults()))

      assert {:ok, _, _, _} = env.mod.get(env.cfg, other)
    end
  end

  describe "lease bodies" do
    test "a body that names another cell is corrupt" do
      world = ManualClock.new(1_000_000_000)
      store = new_store()
      ctx = node_ctx(store, world, unique_prefix("lease"))
      {:ok, _} = Lease.acquire(ctx, "a", "n@host")
      {:ok, _} = Lease.acquire(ctx, "b", "n@host")

      {mod, cfg} = store
      {:ok, body, _, _} = mod.get(cfg, "#{ctx.prefix}/cells/a/lease")
      {:ok, _} = mod.put(cfg, "#{ctx.prefix}/cells/b/lease", body, :none, %{})

      assert {:error, :corrupt_lease} = Lease.read(ctx, "b")
    end
  end

  describe "location prefixes" do
    @bad [
      "",
      "/app",
      "app/",
      "a//b",
      "app/../x",
      "..",
      ".",
      "a/./b",
      "a\nb",
      "a\0b",
      "a\x7Fb"
    ]

    test "Single and Location refuse them" do
      store = {MemoryStore, %{}}

      for prefix <- @bad do
        assert is_binary(Single.validate_options(store: store, prefix: prefix)),
               inspect(prefix)

        refute Location.valid?(%Location{id: "a", store: store, prefix: prefix}),
               inspect(prefix)
      end
    end

    test "nested and dotted names stay valid" do
      store = {MemoryStore, %{}}

      for prefix <- ["app", "app/prod", "a.b", "...", "my-app_1", "a/b/c"] do
        assert :ok = Single.validate_options(store: store, prefix: prefix)
        assert Location.valid?(%Location{id: "a", store: store, prefix: prefix})
      end
    end

    property "a prefix with a dot segment is never valid" do
      check all(
              head <- string(:alphanumeric, max_length: 5),
              dot <- member_of([".", ".."]),
              tail <- string(:alphanumeric, max_length: 5)
            ) do
        prefix = Enum.join([head, dot, tail], "/")
        refute Location.valid?(%Location{id: "a", store: {MemoryStore, %{}}, prefix: prefix})
      end
    end
  end

  describe "mix task options" do
    setup do
      old = Application.get_env(:alambique, :ops)
      Application.put_env(:alambique, :ops, store: new_store(), prefix: "p")

      on_exit(fn ->
        if old,
          do: Application.put_env(:alambique, :ops, old),
          else: Application.delete_env(:alambique, :ops)
      end)
    end

    test "a cell argument that is not an id is refused" do
      for cell <- ["", "..", "a/b", "a\0b", "x/../y"] do
        assert_raise Mix.Error, ~r/invalid cell/, fn -> Options.ctx([], cell) end
      end

      assert %{prefix: "p"} = Options.ctx([], "good-cell")
    end

    test "the store the tasks build hides its credentials" do
      ctx = Options.ctx(bucket: "b", access_key_id: "k", secret_access_key: "very-secret")

      refute inspect(ctx.store) =~ "very-secret"
      assert {Alambique.Store.S3, %{secret_access_key: fun}} = ctx.store
      assert is_function(fun, 0)
    end
  end
end
