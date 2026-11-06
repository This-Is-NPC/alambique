defmodule Alambique.TouchOwnerTest do
  @moduledoc "touch/3 and owner/3 with hints on one node (design §8)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Config, CountingStore, Membership}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    ctx = setup_store()
    counting = CountingStore.wrap(ctx.store)
    ctx = %{ctx | store: counting}
    {_, counts} = counting
    %{ctx: ctx, counts: counts, inst: start_instance(ctx, migrations: @schema)}
  end

  describe "touch" do
    test "wakes an asleep cell on this node", %{inst: inst} do
      cell = unique_cell()
      assert :none = Alambique.owner(inst, cell)
      assert {:ok, node()} == Alambique.touch(inst, cell)
      assert {:ok, node()} == Alambique.owner(inst, cell)
      assert %{cells: [^cell]} = Alambique.cells(inst)
      assert {:ok, node()} == Alambique.touch(inst, cell, timeout: 5_000)
    end

    test "refuses a frozen cell and an invalid id", %{inst: inst} do
      cell = unique_cell()
      assert :ok = Alambique.freeze(inst, cell)
      assert {:error, :frozen} = Alambique.touch(inst, cell)
      assert :none = Alambique.owner(inst, cell)
      assert {:error, _} = Alambique.touch(inst, "bad/id")
    end
  end

  describe "owner with hint: true" do
    test "answers a hinted member without a store request; owner/2 reads the lease", ctx do
      %{inst: inst, counts: counts} = ctx
      config = Config.fetch!(inst)
      cell = unique_cell()
      Membership.put_hint(config, cell, node())
      CountingStore.reset(counts)

      assert {:ok, node()} == Alambique.owner(inst, cell, hint: true)
      assert CountingStore.count(counts, :get) == 0
      assert CountingStore.count(counts, :head) == 0

      assert :none = Alambique.owner(inst, cell)
      assert CountingStore.count(counts, :get) + CountingStore.count(counts, :head) > 0
    end

    test "drops a hint whose node is not a member and reads the lease", ctx do
      %{inst: inst, counts: counts} = ctx
      config = Config.fetch!(inst)
      cell = unique_cell()
      Membership.put_hint(config, cell, :nobody@nowhere)
      CountingStore.reset(counts)

      assert :none = Alambique.owner(inst, cell, hint: true)
      assert CountingStore.count(counts, :get) + CountingStore.count(counts, :head) > 0
      assert :error = Membership.hint(config, cell)
    end

    test "a local process answers before any hint", %{inst: inst} do
      cell = unique_cell()
      Membership.put_hint(Config.fetch!(inst), cell, :nobody@nowhere)
      assert {:ok, _} = Alambique.touch(inst, cell)
      assert {:ok, node()} == Alambique.owner(inst, cell, hint: true)
    end
  end
end
