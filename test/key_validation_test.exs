defmodule Alambique.KeyValidationTest do
  @moduledoc "Keys and prefixes that name another place are refused."
  use ExUnit.Case, async: false

  alias Alambique.Trash
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.MemoryStore

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
end
