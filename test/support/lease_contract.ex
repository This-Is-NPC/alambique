defmodule Alambique.LeaseContract do
  @moduledoc """
  Lease behaviour tests that need no fault injection, so they run against any
  store. The using module defines `new_store/0`, returning `{module, config}`.

      use Alambique.LeaseContract, tags: :seaweedfs
  """

  defmacro __using__(opts) do
    quote do
      use ExUnit.Case, async: false

      import Alambique.LeaseHelpers

      alias Alambique.Lease
      alias AlambiqueTestkit.ManualClock

      for tag <- unquote(List.wrap(Keyword.get(opts, :tags, []))), do: @moduletag(tag)

      @start 1_000_000_000

      setup do
        world = ManualClock.new(@start)
        store = new_store()
        prefix = unique_prefix("lease")
        {:ok, world: world, store: store, prefix: prefix, ctx: node_ctx(store, world, prefix)}
      end

      test "acquire on a new cell gives epoch 1", %{ctx: ctx} do
        assert {:ok, lease} = Lease.acquire(ctx, "c", "a@host")
        assert %Lease{cell: "c", epoch: 1, owner: "a@host", seq: 1, released: false} = lease
        assert lease.expires_at_ms == @start + ttl_ms()
        assert is_binary(lease.etag) and is_binary(lease.token)
        assert Lease.valid?(ctx, lease)
        assert {:ok, ^lease} = Lease.read(ctx, "c")
      end

      test "read returns :none for an unknown cell", %{ctx: ctx} do
        assert :none = Lease.read(ctx, "nobody")
      end

      test "acquire reports the owner while the lease is live", %{ctx: ctx, world: world} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        assert {:error, {:owned, "a", expires}} = Lease.acquire(ctx, "c", "b")
        assert expires == lease.expires_at_ms

        # Expired by the acquirer's clock but still inside the skew margin.
        ManualClock.advance(world, ttl_ms() + skew_ms())
        assert {:error, {:owned, "a", ^expires}} = Lease.acquire(ctx, "c", "b")
      end

      test "acquire after expiry plus margin takes epoch + 1", %{ctx: ctx, world: world} do
        {:ok, first} = Lease.acquire(ctx, "c", "a")
        ManualClock.advance(world, ttl_ms() + skew_ms() + 1)

        assert {:ok, second} = Lease.acquire(ctx, "c", "b")
        assert second.epoch == 2 and second.seq == first.seq + 1 and second.owner == "b"
        assert second.token != first.token
      end

      test "acquire after release does not wait for expiry", %{ctx: ctx} do
        {:ok, first} = Lease.acquire(ctx, "c", "a")
        assert :ok = Lease.release(ctx, first)
        assert {:ok, %Lease{released: true}} = Lease.read(ctx, "c")
        assert {:ok, %Lease{epoch: 2, owner: "b"}} = Lease.acquire(ctx, "c", "b")
      end

      test "renew keeps the epoch and extends the expiry", %{ctx: ctx, world: world} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        ManualClock.advance(world, 10_000)

        assert {:ok, renewed} = Lease.renew(ctx, lease)
        assert renewed.epoch == lease.epoch and renewed.token == lease.token
        assert renewed.seq == lease.seq + 1
        assert renewed.expires_at_ms == lease.expires_at_ms + 10_000
        assert renewed.etag != lease.etag
        assert {:ok, ^renewed} = Lease.read(ctx, "c")

        assert {:ok, again} = Lease.renew(ctx, renewed)
        assert again.seq == renewed.seq + 1
      end

      test "renew from an outdated copy of our own lease adopts the stored one", %{ctx: ctx} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        {:ok, renewed} = Lease.renew(ctx, lease)
        assert {:ok, ^renewed} = Lease.renew(ctx, lease)
      end

      test "renew after another node acquired is lost and changes nothing", %{
        ctx: ctx,
        world: world
      } do
        {:ok, old} = Lease.acquire(ctx, "c", "a")
        ManualClock.advance(world, ttl_ms() + skew_ms() + 1)
        {:ok, new} = Lease.acquire(ctx, "c", "b")

        assert {:error, :lost} = Lease.renew(ctx, old)
        assert {:ok, ^new} = Lease.read(ctx, "c")
      end

      test "release marks the lease released and keeps the epoch", %{ctx: ctx} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        assert :ok = Lease.release(ctx, lease)

        assert {:ok, stored} = Lease.read(ctx, "c")
        assert %Lease{released: true, epoch: 1, owner: "a", token: token} = stored
        assert token == lease.token
        refute Lease.valid?(ctx, stored)
      end

      test "release twice is ok, renew after release is lost", %{ctx: ctx} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        assert :ok = Lease.release(ctx, lease)
        assert :ok = Lease.release(ctx, lease)
        assert {:error, :lost} = Lease.renew(ctx, lease)
      end

      test "release after another node acquired is lost", %{ctx: ctx, world: world} do
        {:ok, old} = Lease.acquire(ctx, "c", "a")
        ManualClock.advance(world, ttl_ms() + skew_ms() + 1)
        {:ok, new} = Lease.acquire(ctx, "c", "b")

        assert {:error, :lost} = Lease.release(ctx, old)
        assert {:ok, ^new} = Lease.read(ctx, "c")
      end

      test "valid? ends skew margin before expiry", %{ctx: ctx, world: world} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        ManualClock.advance(world, ttl_ms() - skew_ms() - 1)
        assert Lease.valid?(ctx, lease)
        ManualClock.advance(world, 1)
        refute Lease.valid?(ctx, lease)
      end

      test "unknown JSON fields are ignored", %{ctx: ctx, store: {store, config}, prefix: prefix} do
        body =
          Jason.encode!(%{
            cell: "c",
            epoch: 4,
            owner: "z",
            token: "t",
            seq: 9,
            expires_at_ms: @start + 5,
            released: true,
            future_field: %{"x" => 1}
          })

        {:ok, _} = store.put(config, "#{prefix}/cells/c/lease", body, :none, %{})
        assert {:ok, %Lease{epoch: 4, seq: 9, owner: "z"}} = Lease.read(ctx, "c")
        assert {:ok, %Lease{epoch: 5, seq: 10}} = Lease.acquire(ctx, "c", "a")
      end

      test "a malformed lease object is an error, not a free cell", %{
        ctx: ctx,
        store: {store, config},
        prefix: prefix
      } do
        {:ok, _} = store.put(config, "#{prefix}/cells/c/lease", "{not json", :none, %{})
        assert {:error, :corrupt_lease} = Lease.read(ctx, "c")
        assert {:error, :corrupt_lease} = Lease.acquire(ctx, "c", "a")
      end

      test "take_over on a new cell works like acquire", %{ctx: ctx} do
        assert {:ok, %Lease{epoch: 1, owner: "a"}, nil} = Lease.take_over(ctx, "c", "a")
      end

      test "take_over replaces a live lease at once and fences its holder", %{ctx: ctx} do
        {:ok, old} = Lease.acquire(ctx, "c", "a")

        assert {:ok, new, %{owner: "a", epoch: 1}} = Lease.take_over(ctx, "c", "b", cause: :test)
        assert %Lease{epoch: 2, owner: "b", seq: seq, released: false} = new
        assert seq == old.seq + 1 and new.token != old.token
        assert {:ok, ^new} = Lease.read(ctx, "c")

        assert {:error, :lost} = Lease.renew(ctx, old)
        assert {:error, :lost} = Lease.release(ctx, old)
        assert {:error, :lost} = Lease.verify(ctx, old)
        assert :ok = Lease.verify(ctx, new)
      end

      test "take_over replaces a released lease", %{ctx: ctx} do
        {:ok, old} = Lease.acquire(ctx, "c", "a")
        :ok = Lease.release(ctx, old)
        assert {:ok, %Lease{epoch: 2, owner: "b"}, %{owner: "a"}} = Lease.take_over(ctx, "c", "b")
      end

      test "take_over refuses a cell that moved", %{ctx: ctx} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        :ok = Lease.mark_moved(ctx, lease, "elsewhere")
        assert {:error, {:moved, "elsewhere"}} = Lease.take_over(ctx, "c", "b")
      end

      test "take_over with the epoch the caller saw", %{ctx: ctx} do
        {:ok, _} = Lease.acquire(ctx, "c", "a")
        {:ok, _, _} = Lease.take_over(ctx, "c", "b", epoch: 1)

        # The suspicion was about epoch 1, which the new holder already replaced.
        assert {:error, {:epoch_changed, 2}} = Lease.take_over(ctx, "c", "z", epoch: 1)
        assert {:ok, %Lease{epoch: 2, owner: "b"}} = Lease.read(ctx, "c")
        assert {:error, {:epoch_changed, 0}} = Lease.take_over(ctx, "none", "z", epoch: 1)
        assert :none = Lease.read(ctx, "none")
      end

      test "verify is lost for a released lease and for an unknown cell", %{ctx: ctx} do
        {:ok, lease} = Lease.acquire(ctx, "c", "a")
        assert :ok = Lease.verify(ctx, lease)
        :ok = Lease.release(ctx, lease)
        assert {:error, :lost} = Lease.verify(ctx, lease)
        assert {:error, :lost} = Lease.verify(ctx, %{lease | cell: "other"})
      end

      test "twenty nodes taking over a live lease: one holder per epoch", %{
        ctx: ctx,
        store: store,
        world: world,
        prefix: prefix
      } do
        {:ok, _} = Lease.acquire(ctx, "race", "old")

        results =
          1..20
          |> Enum.map(fn i ->
            Task.async(fn ->
              Lease.take_over(node_ctx(store, world, prefix), "race", "n#{i}")
            end)
          end)
          |> Task.await_many(60_000)

        winners = for {:ok, lease, _} <- results, do: lease
        assert winners != []
        assert winners |> Enum.map(& &1.epoch) |> Enum.uniq() |> length() == length(winners)

        assert Enum.all?(results, fn
                 {:ok, _, _} -> true
                 {:error, {:contended, _, epoch}} -> epoch >= 2
               end)

        assert {:ok, %Lease{epoch: top}} = Lease.read(ctx, "race")
        assert top == winners |> Enum.map(& &1.epoch) |> Enum.max()
      end

      test "twenty nodes racing for a new cell have one winner", %{
        store: store,
        world: world,
        prefix: prefix
      } do
        results =
          1..20
          |> Enum.map(fn i ->
            Task.async(fn -> Lease.acquire(node_ctx(store, world, prefix), "race", "n#{i}") end)
          end)
          |> Task.await_many(60_000)

        assert [{:ok, winner}] = Enum.filter(results, &match?({:ok, _}, &1))
        assert winner.epoch == 1

        losers = results -- [{:ok, winner}]
        assert length(losers) == 19
        assert Enum.all?(losers, &match?({:error, {:owned, _, _}}, &1))
        assert Enum.all?(losers, fn {:error, {:owned, owner, _}} -> owner == winner.owner end)
      end

      test "twenty nodes racing for an expired lease have one winner", %{
        ctx: ctx,
        store: store,
        world: world,
        prefix: prefix
      } do
        {:ok, _} = Lease.acquire(ctx, "race", "old")
        ManualClock.advance(world, ttl_ms() + skew_ms() + 1)

        results =
          1..20
          |> Enum.map(fn i ->
            Task.async(fn -> Lease.acquire(node_ctx(store, world, prefix), "race", "n#{i}") end)
          end)
          |> Task.await_many(60_000)

        assert [{:ok, %Lease{epoch: 2}}] = Enum.filter(results, &match?({:ok, _}, &1))
        assert 19 == Enum.count(results, &match?({:error, {:owned, _, _}}, &1))
      end
    end
  end
end
